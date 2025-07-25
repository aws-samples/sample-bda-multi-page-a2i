#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
import json
import boto3
import time
import io
import os
import logging
from pdf2image import convert_from_bytes, pdfinfo_from_bytes
from datetime import datetime
from typing import Dict, List, Any, Optional
import gc
from PIL import Image, ImageFile

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker')
a2i_client = boto3.client('sagemaker-a2i-runtime')

# Constants
INPUT_PDF_BUCKET = os.getenv("BDA_INPUT_BUCKET")  # Bucket containing the source PDF
OUTPUT_BUCKET = os.getenv("BDA_OUTPUT_BUCKET")  # Bucket for storing outputs
WORKTEAM_ARN = os.getenv("BDA_WORKTEAM_ARN")
ROLE_ARN = os.getenv("ROLE_ARN")
TEMPLATE_FILENAME = os.getenv("TEMPLATE_FILENAME", "bda_template_multi.html")
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.70"))
PRE_SIGNED_URL_EXPERIATION = int(os.getenv("PRE_SIGNED_URL_EXPERIATION", "86400")) # Default of 24 hours
TASK_TIME_LIMIT = int(os.getenv("TASK_TIME_LIMIT", "3600"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
RETRY_INTERVAL = int(os.getenv("RETRY_INTERVAL", "10"))
TARGET_DPI = int(os.getenv("TARGET_DPI", "300"))
FLOW_DEFINITION_NAME = os.getenv("FLOW_DEFINITION_NAME", "bda-review-flow-definition")

def copy_bda_outputs_to_folder(execution_id: str) -> Dict[str, str]:
    """
    Copy all BDA output files for a given execution to a single target folder
    with structure: aggregated_result/execution_id/page_number/result.json
    
    Args:
        execution_id: BDA execution ID
        
    Returns:
        Dictionary mapping original paths to copied paths
    """
    logger.info(f"Copying BDA outputs for execution: {execution_id}")
    
    source_prefix = f"bda-output/{execution_id}/0/custom_output/"
    target_base_prefix = f"aggregated_result/{execution_id}"
    file_mapping: Dict[str, str] = {}
    
    # List all files in the BDA output folder
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=OUTPUT_BUCKET, Prefix=source_prefix)
    
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                source_key = obj["Key"]
                
                # Extract the page number from the source key
                parts = source_key.split('/')
                # Find the index after 'custom_output'
                page_number = None
                for i, part in enumerate(parts):
                    if part == 'custom_output' and i + 1 < len(parts):
                        page_number = parts[i + 1]
                        break
                
                if page_number is None:
                    logger.warning(f"Could not extract page number from key: {source_key}")
                    continue
                
                # Build the new target key with the correct structure
                # Format: aggregated_result/execution_id/page_number/filename.json
                filename = parts[-1]  # Get the filename (last part)
                target_key = f"{target_base_prefix}/{page_number}/{filename}"
                
                # Copy the file
                s3_client.copy_object(
                    Bucket=OUTPUT_BUCKET,
                    CopySource={'Bucket': OUTPUT_BUCKET, 'Key': source_key},
                    Key=target_key
                )
                
                file_mapping[source_key] = target_key
                logger.info(f"Copied {source_key} to {target_key}")
    
    logger.info(f"Copied {len(file_mapping)} BDA output files to 'aggregated_result/{execution_id}/' folder")
    return file_mapping

def extract_page_number(file_path: str) -> int:
    """Extract page number from file path without using regex
    
    Args:
        file_path: The S3 key or file path to parse
        
    Returns:
        The extracted page number as an integer
    """
    parts = file_path.split('/')
    # Find the index right after 'custom_output'
    for i, part in enumerate(parts):
        if part == 'custom_output' and i + 1 < len(parts):
            # Return the next element which should be the page number
            return int(parts[i + 1])
    return 0  # Default if pattern not found

def list_result_files(execution_id: str) -> List[str]:
    """
    List all result.json files for a given execution ID
    
    Args:
        execution_id: The BDA execution ID
        
    Returns:
        List of result file keys sorted by page number
    """
    logger.info(f"Listing result files for execution ID: {execution_id}")

    prefix = f"bda-output/{execution_id}/0/custom_output/"
    paginated_response = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=OUTPUT_BUCKET,
        Prefix=prefix
    )
    
    result_files = []
    for page in paginated_response:
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"].endswith("/result.json"):
                    result_files.append(obj["Key"])
    
    # Sort by page number using the helper function
    result_files.sort(key=extract_page_number)
    
    logger.info(f"Found {len(result_files)} result files: {result_files}")
    return result_files

def get_page_number(result_key: str) -> Optional[int]:
    """
    Extract page number from result.json key
    
    Args:
        result_key: S3 key of the result file
        
    Returns:
        Page number (1-based) or None if not found
    """
    parts = result_key.split('/')
    # Find the index right after 'custom_output'
    for i, part in enumerate(parts):
        if part == 'custom_output' and i + 1 < len(parts):
            # Return the next element which should be the page number plus 1 (1-based)
            return int(parts[i + 1]) + 1
    return None

def convert_pdf_to_pngs(execution_id: str, bucket: str, pdf_key: str) -> List[str]:
    """
    Convert PDF to images processing one page at a time to reduce memory usage
    
    Args:
        execution_id: The BDA execution ID
        bucket: The S3 bucket for the file to retreive
        pdf_key: The key of the object to retrieve from the bucket
    
    Returns:
        S3 presigned_urls for the converted image
    """
    logger.info(f"Using source PDF: s3://{bucket}/{pdf_key}, for execution id: {execution_id}")

    #Disabling PIL image size limit - large images will be processed
    Image.MAX_IMAGE_PIXELS = None  # Disable decompression bomb check completely
    ImageFile.LOAD_TRUNCATED_IMAGES = True  # Allow loading truncated images
    
    logger.info(f"Using source PDF: s3://{bucket}/{pdf_key}, for execution id: {execution_id}")
    
    # Download the PDF from S3
    response = s3_client.get_object(Bucket=bucket, Key=pdf_key)
    pdf_data = response['Body'].read()
    
    # Get total page count
    try:
        pdf_info = pdfinfo_from_bytes(pdf_data)
        total_pages = pdf_info['Pages']
        logger.info(f"PDF has {total_pages} pages")
    except Exception as e:
        logger.warning(f"Could not determine total page count: {str(e)}")
        total_pages = None
    
    # Create output path with execution_id
    output_prefix = f"input_image/{execution_id}"
    
    image_keys = []
    presigned_urls = []
    
    # Process each page individually
    logger.info(f"Converting PDF pages to high-resolution images at {TARGET_DPI} DPI")
    page_num = 1
    while True:
        try:
            # Process one page at a time
            logger.info(f"Processing page {page_num}{' of ' + str(total_pages) if total_pages else ''}...")
            images = convert_from_bytes(
                pdf_data, 
                dpi=TARGET_DPI, 
                first_page=page_num, 
                last_page=page_num
            )
            
            if not images:
                break  # No more pages
                
            image = images[0]  # Should only be one image
            
            # Create an in-memory image
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG', optimize=True)
            img_byte_arr.seek(0)
            
            # Generate key for this image
            image_key = f"{output_prefix}/page_{page_num}.png"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=OUTPUT_BUCKET,
                Key=image_key,
                Body=img_byte_arr,
                ContentType='image/png'
            )
            
            image_keys.append(image_key)
            logger.info(f"Uploaded page {page_num} to s3://{OUTPUT_BUCKET}/{image_key}")
            
            # Generate pre-signed URL
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': OUTPUT_BUCKET, 'Key': image_key},
                ExpiresIn=PRE_SIGNED_URL_EXPERIATION
            )
            presigned_urls.append(url)
            
            # Garbage collection
            del images
            del image
            del img_byte_arr
            gc.collect()
            
            # Move to next page
            page_num += 1
            
        except Exception as e:
            if page_num == 1:
                # If first page fails, report the error
                logger.error(f"Error processing PDF: {str(e)}")
                raise
            else:
                # Likely reached end of document
                logger.info(f"Processed {page_num-1} pages total")
                break
    
    return presigned_urls

def process_result_file(key: str) -> Dict[str, Any]:
    """
    Get and process a result.json file
    
    Args:
        key: S3 key of the result file
        
    Returns:
        Dictionary containing page number and low confidence fields
    """
    response = s3_client.get_object(Bucket=OUTPUT_BUCKET, Key=key)
    result = json.loads(response['Body'].read().decode('utf-8'))
    
    page_number = get_page_number(key)
    
    # Extract low confidence fields
    low_confidence_fields = []
    
    # Navigate to explainability_info
    if 'explainability_info' in result and isinstance(result['explainability_info'], list) and len(result['explainability_info']) > 0:
        fields_info = result['explainability_info'][0]
        
        # Extract fields recursively
        extract_fields_recursively(fields_info, "", low_confidence_fields, page_number)
    
    return {
        'page_number': page_number,
        'low_confidence_fields': low_confidence_fields
    }

def extract_fields_recursively(obj: Dict[str, Any], prefix: str, low_confidence_fields: List[Dict[str, Any]], page_number: int) -> None:
    """
    Extract fields recursively from the result JSON
    
    Args:
        obj: JSON object to extract fields from
        prefix: Current path prefix for nested fields
        low_confidence_fields: List to append low confidence fields to
        page_number: Current page number
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            current_path = f"{prefix}.{key}" if prefix else key
            
            # If this is a leaf node with confidence score
            if isinstance(value, dict) and 'confidence' in value:
                confidence = value.get('confidence', 0)
                
                # If confidence is below threshold, add to low_confidence_fields
                if confidence < CONFIDENCE_THRESHOLD:
                    field_info = {
                        'field_name': current_path,
                        'value': value.get('value', ''),
                        'confidence': confidence,
                        'type': value.get('type', 'string'),
                        'geometry': value.get('geometry', [])
                    }
                    
                    # Update page numbers in geometry if needed
                    if 'geometry' in value and isinstance(value['geometry'], list):
                        for geom in value['geometry']:
                            if 'page' in geom:
                                geom['page'] = page_number
                    
                    low_confidence_fields.append(field_info)

            # If this is a list of values (like ENDORSEMENTS)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    list_path = f"{current_path}[{i}]"
                    if isinstance(item, dict) and 'confidence' in item:
                        confidence = item.get('confidence', 0)
                        if confidence < CONFIDENCE_THRESHOLD:
                            field_info = {
                                'field_name': list_path,
                                'value': item.get('value', ''),
                                'confidence': confidence,
                                'type': item.get('type', 'string'),
                                'geometry': item.get('geometry', [])
                            }
                            
                            # Update page numbers in geometry if needed
                            if 'geometry' in item and isinstance(item['geometry'], list):
                                for geom in item['geometry']:
                                    if 'page' in geom:
                                        geom['page'] = page_number
                            
                            low_confidence_fields.append(field_info)

            # If this is another nested object
            elif isinstance(value, dict):
                extract_fields_recursively(value, current_path, low_confidence_fields, page_number)

def get_or_create_task_ui() -> str:
    """
    Check if a Human Task UI exists; create it if it doesn't
    
    Returns:
        Human Task UI ARN
    """
    task_ui_name = "bda-review-task-ui"  # Fixed name for reuse
    
    # Check if the task UI already exists
    try:
        response = sagemaker_client.describe_human_task_ui(
            HumanTaskUiName=task_ui_name
        )
        logger.info(f"Found existing Human Task UI: {response['HumanTaskUiArn']}")
        return response['HumanTaskUiArn']
    except Exception as e:
        logger.info(f"Human Task UI {task_ui_name} not found or error: {e}. Creating new one.")
        
        # Get the HTML template content from local file
        template_path = os.path.join(os.path.dirname(__file__), TEMPLATE_FILENAME)
        with open(template_path, 'r') as file:
            template_content = file.read()
        
        human_task_ui_response = sagemaker_client.create_human_task_ui(
            HumanTaskUiName=task_ui_name,
            UiTemplate={
                'Content': template_content
            }
        )
        human_task_ui_arn = human_task_ui_response['HumanTaskUiArn']
        logger.info(f"Created Human Task UI: {human_task_ui_arn}")
        return human_task_ui_arn

def get_or_create_flow_definition(human_task_ui_arn: str) -> str:
    """
    Check if a Flow Definition exists; create it if it doesn't
    
    Args:
        human_task_ui_arn: Human Task UI ARN
        
    Returns:
        Flow Definition ARN
    """
    A2I_OUTPUT_PATH = f"s3://{OUTPUT_BUCKET}/a2i-output/"
    
    # Check if the flow definition already exists
    try:
        response = sagemaker_client.describe_flow_definition(
            FlowDefinitionName=FLOW_DEFINITION_NAME
        )
        
        status = response.get('FlowDefinitionStatus')
        if status == 'Active':
            logger.info(f"Found existing active Flow Definition: {response['FlowDefinitionArn']}")
            return response['FlowDefinitionArn']
        else:
            logger.info(f"Found Flow Definition but status is {status}, not 'Active'. Creating new one.")
    except Exception as e:
        logger.info(f"Flow Definition {FLOW_DEFINITION_NAME} not found or error: {e}. Creating new one.")
    
    # Create a new flow definition
    flow_def_response = sagemaker_client.create_flow_definition(
        FlowDefinitionName=FLOW_DEFINITION_NAME,
        HumanLoopConfig={
            "WorkteamArn": WORKTEAM_ARN,
            "HumanTaskUiArn": human_task_ui_arn,
            "TaskTitle": "Review Document Fields",
            "TaskDescription": "Review and correct extracted fields with low confidence scores",
            "TaskCount": 1,
            "TaskAvailabilityLifetimeInSeconds": PRE_SIGNED_URL_EXPERIATION,
            "TaskTimeLimitInSeconds": TASK_TIME_LIMIT
        },
        OutputConfig={
            "S3OutputPath": A2I_OUTPUT_PATH
        },
        RoleArn=ROLE_ARN,
    )
    
    flow_definition_arn = flow_def_response['FlowDefinitionArn']
    logger.info(f"Created Flow Definition: {flow_definition_arn}")
    
    # Wait for the flow definition to become active
    if wait_for_flow_definition(FLOW_DEFINITION_NAME):
        return flow_definition_arn
    else:
        message = f"The Flow Definition {FLOW_DEFINITION_NAME} did not become active in the expected time"
        logger.error(message)
        raise Exception(message)

def wait_for_flow_definition(flow_definition_name: str) -> bool:
    """
    Wait for a Flow Definition to become active
    
    Args:
        flow_definition_name: Flow Definition name
        
    Returns:
        True if activated successfully, False otherwise
    """
    logger.info(f"Waiting for Flow Definition to activate...")
    
    retries = 0
    while retries < MAX_RETRIES:

        # Wait before checking 
        time.sleep(RETRY_INTERVAL)

        # See if the flow definition is active
        response = sagemaker_client.describe_flow_definition(
            FlowDefinitionName=flow_definition_name
        )
        
        status = response.get('FlowDefinitionStatus', '')
        logger.info(f"Flow Definition current status: {status}")
        
        if status == 'Active':
            return True
        elif status == 'Failed':
            logger.error(f"Flow Definition creation failed: {response.get('FailureReason', 'Unknown')}")
            return False
        else:
            logger.info(f"Waiting {RETRY_INTERVAL} seconds, retry {retries} of {MAX_RETRIES}...")
            retries += 1
    
    logger.info(f"Timed out waiting for activation")
    return False

def start_human_loop(execution_id: str, page_data: List[Dict[str, Any]], presigned_urls: List[str], flow_definition_arn: str) -> Dict[str, Any]:
    """
    Start a human loop for reviewing fields across multiple document pages
    
    Args:
        execution_id: BDA execution ID
        page_data: List of page data with low confidence fields
        presigned_urls: List of presigned URLs for page images
        flow_definition_arn: Flow Definition ARN
        
    Returns:
        Dictionary with response information
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    human_loop_name = f"review-loop-{timestamp}"
    
    # Organize fields by page
    fields_by_page = {}
    
    # Prepare data for each page
    for page_info in page_data:
        page_number = page_info['page_number']
        page_num_str = str(page_number)
        
        # Add fields for this page
        if page_info['low_confidence_fields']:
            fields_by_page[page_num_str] = page_info['low_confidence_fields']
    
    # Only proceed if we have fields to review
    if not fields_by_page:
        logger.info("No fields to review. Skipping human review.")
        return {
            'statusCode': 200,
            'body': 'No fields requiring review'
        }
    
    # Prepare input data for human review
    human_loop_input = {
        "presigned_urls": presigned_urls,
        "fields_by_page": fields_by_page,
        "execution_id": execution_id
    }
    
    logger.info(f"Human loop input: {json.dumps(human_loop_input, default=str, indent=2)}")
    
    response = a2i_client.start_human_loop(
        HumanLoopName=human_loop_name,
        FlowDefinitionArn=flow_definition_arn,
        HumanLoopInput={
            'InputContent': json.dumps(human_loop_input, default=str)
        }
    )
    
    logger.info(f"Started human loop: {human_loop_name}")
    return {
        'statusCode': 200,
        'body': {
            'human_loop_name': human_loop_name,
            'human_loop_arn': response.get('HumanLoopArn'),
            'fields_by_page': {k: len(v) for k, v in fields_by_page.items()}
        }
    }

def process_execution_id(execution_id: str, bucket: str, pdf_key: str) -> Dict[str, Any]:
    """
    Process all results for a given execution ID
    
    Args:
        execution_id: BDA execution ID
        bucket: The S3 bucket for the file to retreive
        pdf_key: The key of the object to retrieve from the bucket
        
    Returns:
        Dictionary with response information
    """
    # Convert PDF to page images first
    presigned_urls = convert_pdf_to_pngs(execution_id, bucket, pdf_key)
    if not presigned_urls:
        return {
            'statusCode': 500,
            'body': 'Failed to convert PDF to images'
        }
    
    # List all result files for this execution ID
    result_files = list_result_files(execution_id)
    if not result_files:
        logger.info(f"No result files found for execution ID: {execution_id}")
        return {
            'statusCode': 404,
            'body': f"No result files found for execution ID: {execution_id}"
        }
    
    # Process each result file
    page_data = []
    for result_file in result_files:
        page_info = process_result_file(result_file)
        page_data.append(page_info)
    
    # Check if any page has low confidence fields
    has_low_confidence = any(len(page['low_confidence_fields']) > 0 for page in page_data)
    if not has_low_confidence:
        logger.info(f"No low confidence fields found for execution ID: {execution_id}")
        return {
            'statusCode': 200,
            'body': 'No fields requiring review'
        }
    
    # Get or create task UI and flow definition (reuse if they exist)
    human_task_ui_arn = get_or_create_task_ui()
    flow_definition_arn = get_or_create_flow_definition(human_task_ui_arn)
    
    return start_human_loop(execution_id, page_data, presigned_urls, flow_definition_arn)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler that processes document and creates human review tasks
    
    Args:
        event: Lambda event containing s3_uri, bda_invocation_arn, and bda_output_bucket
        context: Lambda context
        
    Returns:
        Dictionary with response information
    """
    try:
        logger.info(f"Got the following event: {event}")
        
        # Get required fields from the step function input
        s3_uri = event.get('s3_uri')
        bda_invocation_arn = event.get('bda_invocation_arn')
        bda_output_bucket = event.get('bda_output_bucket')
        
        global OUTPUT_BUCKET
        if bda_output_bucket:
            OUTPUT_BUCKET = bda_output_bucket
            logger.info(f"Using output bucket from input: {OUTPUT_BUCKET}")
        
        # Validate required fields are present
        if not s3_uri or not bda_invocation_arn:
            error_msg = "Missing required fields: s3_uri and bda_invocation_arn must be provided"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': error_msg
            }
            
        # Extract execution ID from the BDA invocation ARN
        execution_id = bda_invocation_arn.split('/')[-1]
        logger.info(f"Processing execution ID: {execution_id}")
        
        # Copy bda output to separate folder to aggregate it with A2I output
        copy_bda_outputs_to_folder(execution_id)

        # Get the object name from s3_uri
        parts = s3_uri.replace('s3://', '').split('/')
        pdf_key = '/'.join(parts[1:])
        
        return process_execution_id(execution_id, INPUT_PDF_BUCKET, pdf_key)
            
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        raise

# If called from command line run the lambda handler
if __name__ == "__main__":
    lambda_handler({}, {})