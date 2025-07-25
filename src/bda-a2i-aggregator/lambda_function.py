#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
import json
import boto3
import os
import logging
import re
from typing import Dict, Any, List, Tuple, Optional

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')


def extract_s3_info_from_event(event) -> Tuple[str, str]:
    """
    Extract S3 bucket and key from the event
    
    Args:
        event: Lambda event
    
    Returns:
        Tuple of (bucket, key)
    """
    # For EventBridge events
    if 'detail' in event:
        detail = event['detail']
        if 's3' in detail and 'bucket' in detail['s3'] and 'object' in detail['s3']:
            return detail['s3']['bucket']['name'], detail['s3']['object']['key']
    
    # For S3 events
    if 'Records' in event and len(event['Records']) > 0:
        s3_event = event['Records'][0].get('s3', {})
        bucket = s3_event.get('bucket', {}).get('name')
        key = s3_event.get('object', {}).get('key')
        if bucket and key:
            return bucket, key
    
    # If directly provided in event
    bucket = event.get('bucket') or event.get('s3_bucket')
    key = event.get('key') or event.get('s3_key')
    
    if bucket and key:
        return bucket, key
    
    raise ValueError("Could not extract S3 bucket and key from event")

def get_a2i_output(bucket: str, key: str) -> Dict:
    """
    Get the A2I output from S3
    
    Args:
        bucket: S3 bucket
        key: S3 key
    
    Returns:
        A2I output as a dictionary
    """
    logger.info(f"Getting A2I output from s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse A2I output as JSON")
        raise ValueError(f"A2I output is not valid JSON: {content[:100]}...")

def extract_execution_id(a2i_output: Dict) -> str:
    """
    Extract execution_id from A2I output
    
    Args:
        a2i_output: A2I output as a dictionary
    
    Returns:
        Execution ID
    """
    # The execution_id is in inputContent
    input_content = a2i_output.get('inputContent')
    if isinstance(input_content, str):
        # If inputContent is a string, parse it as JSON
        try:
            input_content = json.loads(input_content)
        except json.JSONDecodeError:
            logger.warning("inputContent is not valid JSON, trying to process as is")
            # Try to extract execution_id from the string directly if possible
            if "execution_id" in input_content:
                match = re.search(r'"execution_id"\s*:\s*"([^"]+)"', input_content)
                if match:
                    return match.group(1)
    
    execution_id = input_content.get('execution_id')
    if not execution_id:
        raise ValueError("Could not find execution_id in A2I output")
    
    return execution_id

def extract_human_reviewed_fields(a2i_output: Dict) -> Dict[str, Any]:
    """
    Extract human-reviewed fields from A2I output
    
    Args:
        a2i_output: A2I output as a dictionary
    
    Returns:
        Dictionary of field_name -> value
    """
    # Get the human answers
    human_answers = a2i_output.get('humanAnswers', [])
    if not human_answers:
        logger.warning("No human answers found in A2I output")
        return {}
    
    # Use the first human answer
    answer_content = human_answers[0].get('answerContent', {})
    if not answer_content:
        logger.warning("No answer content found in human answer")
        return {}
    
    # Filter out any confirmation fields or other metadata
    reviewed_fields = {}
    for field_name, value in answer_content.items():
        if isinstance(value, dict) and 'on' in value:
            # This is a confirmation field, skip it
            continue
        
        # Add the field to the reviewed fields dict
        reviewed_fields[field_name] = value
    
    return reviewed_fields

def get_fields_by_page(a2i_output: Dict) -> Dict[str, int]:
    """
    Get a mapping of field names to page numbers
    
    Args:
        a2i_output: A2I output as a dictionary
    
    Returns:
        Dictionary of field_name -> page_number
    """
    # Get the fields_by_page from inputContent
    input_content = a2i_output.get('inputContent')
    if isinstance(input_content, str):
        try:
            input_content = json.loads(input_content)
        except json.JSONDecodeError:
            logger.warning("inputContent is not valid JSON, using empty fields_by_page")
            return {}
    
    fields_by_page = input_content.get('fields_by_page', {})
    
    # Create a mapping of field names to page numbers
    field_to_page = {}
    for page_num_str, fields in fields_by_page.items():
        try:
            page_num = int(page_num_str)
            for field in fields:
                field_name = field.get('field_name')
                if field_name:
                    field_to_page[field_name] = page_num
        except (ValueError, TypeError):
            logger.warning(f"Could not process page number '{page_num_str}'")
    
    return field_to_page

def list_bda_output_files(bucket: str, execution_id: str) -> List[str]:
    """
    List all BDA output files for a given execution ID
    
    Args:
        bucket: S3 bucket
        execution_id: Execution ID
    
    Returns:
        List of file keys
    """
    # Define the prefix for BDA outputs
    prefix = f"aggregated_result/{execution_id}/"
    
    # List all objects with this prefix
    result_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"].endswith('/result.json'):
                    result_files.append(obj['Key'])
    
    logger.info(f"Found {len(result_files)} BDA output files: {result_files}")
    return result_files

def extract_page_number_from_path(file_path: str) -> int:
    """
    Extract page number from file path
    
    Args:
        file_path: S3 key
    
    Returns:
        Page number (1-based)
    """
    # Example path: aggregated_result/execution_id/0/result.json
    parts = file_path.split('/')
    try:
        # The page number is typically the second-to-last part before the filename
        page_num = int(parts[-2])
        # Convert to 1-based (assuming the BDA output uses 0-based)
        return page_num + 1
    except (IndexError, ValueError):
        return 0

def load_bda_output(bucket: str, key: str) -> Dict:
    """
    Load BDA output from S3
    
    Args:
        bucket: S3 bucket
        key: S3 key
    
    Returns:
        BDA output as a dictionary
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse BDA output as JSON: {key}")
        raise ValueError(f"BDA output is not valid JSON: {content[:100]}...")

def update_field_in_bda_output(bda_output: Dict, field_name: str, human_value: Any) -> bool:
    """
    Update a field in the BDA output with a human-reviewed value
    
    Args:
        bda_output: BDA output as a dictionary
        field_name: Field name to update
        human_value: Human-reviewed value
    
    Returns:
        True if field was updated, False otherwise
    """
    updated = False
    
    # Check if explainability_info exists
    if 'explainability_info' in bda_output and bda_output['explainability_info']:
        explainability = bda_output['explainability_info'][0]
        
        # First check if field_name exists directly in explainability_info
        if field_name in explainability:
            field_obj = explainability[field_name]
            field_obj['new_value'] = human_value
            field_obj['human_reviewed'] = True
            logger.info(f"Updated field '{field_name}' with human-reviewed value '{human_value}'")
            updated = True
        
        array_match = re.match(r'(.+)\[(\d+)\]\$', field_name)
        if not updated and array_match:
            base_field = array_match.group(1)
            index = int(array_match.group(2))
            
            # Check if base_field exists and is an array
            if base_field in explainability and isinstance(explainability[base_field], list):
                if index < len(explainability[base_field]):
                    field_obj = explainability[base_field][index]
                    field_obj['new_value'] = human_value
                    field_obj['human_reviewed'] = True
                    logger.info(f"Updated array field '{field_name}' with human-reviewed value '{human_value}'")
                    updated = True
            
            # Check if base_field is a nested field (like diagnosis.immunostains)
            elif '.' in base_field:
                parts = base_field.split('.')
                current = explainability
                
                # Navigate to the nested field
                found = True
                for part in parts:
                    if part in current:
                        current = current[part]
                    else:
                        found = False
                        break
                
                # Check if current is an array
                if found and isinstance(current, list) and index < len(current):
                    field_obj = current[index]
                    field_obj['new_value'] = human_value
                    field_obj['human_reviewed'] = True
                    logger.info(f"Updated nested array field '{field_name}' with human-reviewed value '{human_value}'")
                    updated = True
        
        # Handle nested fields (e.g., diagnosis.tumor_size)
        elif not updated and '.' in field_name:
            parts = field_name.split('.')
            current = explainability
            
            # Navigate to the nested field
            found = True
            for part in parts[:-1]:
                if part in current:
                    current = current[part]
                else:
                    found = False
                    break
            
            # Update the field
            if found:
                last_part = parts[-1]
                if last_part in current:
                    field_obj = current[last_part]
                    field_obj['new_value'] = human_value
                    field_obj['human_reviewed'] = True
                    logger.info(f"Updated nested field '{field_name}' with human-reviewed value '{human_value}'")
                    updated = True
    
    # Also update in inference_result if applicable
    if 'inference_result' in bda_output:
        inference_result = bda_output['inference_result']
        
        # Handle direct fields
        if field_name in inference_result:
            inference_result[field_name] = human_value
            logger.info(f"Also updated field '{field_name}' in inference_result")
    
    if not updated:
        logger.warning(f"Field '{field_name}' not found in BDA output")
    
    return updated

def save_bda_output(bucket: str, key: str, bda_output: Dict) -> None:
    """
    Save updated BDA output to S3
    
    Args:
        bucket: S3 bucket
        key: S3 key
        bda_output: BDA output as a dictionary
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(bda_output, indent=2),
        ContentType='application/json'
    )
    logger.info(f"Saved updated BDA output to s3://{bucket}/{key}")

def update_bda_outputs(output_bucket: str, execution_id: str, human_reviewed_fields: Dict[str, Any], field_to_page: Dict[str, int]) -> List[str]:
    """
    Update BDA outputs with human-reviewed data
    
    Args:
        output_bucket: S3 bucket containing BDA outputs
        execution_id: Execution ID
        human_reviewed_fields: Dictionary of field_name -> value
        field_to_page: Dictionary of field_name -> page_number
    
    Returns:
        List of updated file paths
    """
    # List all BDA output files for this execution
    bda_output_files = list_bda_output_files(output_bucket, execution_id)
    
    # Map page numbers to file paths
    page_to_file = {}
    for file_path in bda_output_files:
        page_num = extract_page_number_from_path(file_path)
        page_to_file[page_num] = file_path
    
    logger.info(f"Page to file mapping: {page_to_file}")
    
    # Track which files we've updated
    updated_files = []
    
    # Process each human-reviewed field
    for field_name, value in human_reviewed_fields.items():
        # Get the page number for this field
        page_num = field_to_page.get(field_name)
        
        if page_num is not None and page_num in page_to_file:
            file_path = page_to_file[page_num]
            logger.info(f"Field '{field_name}' belongs to page {page_num}, file: {file_path}")
            
            # Load the BDA output for this page
            bda_output = load_bda_output(output_bucket, file_path)
            
            # Update the field in the BDA output
            if update_field_in_bda_output(bda_output, field_name, value):
                # Save the updated BDA output
                save_bda_output(output_bucket, file_path, bda_output)
                if file_path not in updated_files:
                    updated_files.append(file_path)
        else:
            # If we couldn't determine the page, try updating all files
            logger.warning(f"Could not determine page for field '{field_name}'. Trying all files.")
            for file_path in bda_output_files:
                bda_output = load_bda_output(output_bucket, file_path)
                if update_field_in_bda_output(bda_output, field_name, value):
                    # Save the updated BDA output
                    save_bda_output(output_bucket, file_path, bda_output)
                    if file_path not in updated_files:
                        updated_files.append(file_path)
                    break
    
    return updated_files

def lambda_handler(event, context):
    """
    Lambda handler to process A2I output and update BDA outputs with human-reviewed data
    
    Args:
        event: Event data containing information about the A2I output
        context: Lambda context
    
    Returns:
        Dictionary with response information
    """
    try:
        logger.info(f"Processing event: {json.dumps(event, default=str)}")
        
        if not output_bucket:
            # Try to extract from event if not provided in environment
            if 'Records' in event and len(event['Records']) > 0:
                output_bucket = event['Records'][0].get('s3', {}).get('bucket', {}).get('name')
        
        if not output_bucket:
            raise ValueError("Could not determine output bucket name")
            
        # Get the A2I output S3 location from the event
        a2i_output_bucket, a2i_output_key = extract_s3_info_from_event(event)
        
        # Get the A2I output from S3
        a2i_output = get_a2i_output(a2i_output_bucket, a2i_output_key)
        
        # Extract execution_id and human-reviewed fields
        execution_id = extract_execution_id(a2i_output)
        human_reviewed_fields = extract_human_reviewed_fields(a2i_output)
        
        logger.info(f"Processing A2I output for execution_id: {execution_id}")
        logger.info(f"Found {len(human_reviewed_fields)} human-reviewed fields: {list(human_reviewed_fields.keys())}")
        
        # Get field to page mapping
        field_to_page = get_fields_by_page(a2i_output)
        
        # Update BDA outputs with human-reviewed data
        updated_files = update_bda_outputs(output_bucket, execution_id, human_reviewed_fields, field_to_page)
        
        return {
            'statusCode': 200,
            'body': {
                'execution_id': execution_id,
                'updated_files': updated_files,
                'human_reviewed_fields': len(human_reviewed_fields)
            }
        }
    
    except Exception as e:
        logger.error(f"Error processing A2I output: {str(e)}")
        raise