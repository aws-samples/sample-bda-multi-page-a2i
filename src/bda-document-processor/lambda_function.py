#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
import os
import boto3
import time
import logging
from typing import Tuple, Dict, Any

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
bda_client = boto3.client('bedrock-data-automation-runtime')
s3_client = boto3.client('s3')

# Get constants
OUTPUT_BUCKET = os.getenv("BDA_OUTPUT_BUCKET")  # Bucket for storing outputs
DATA_AUTOMATION_PROJECT_ARN = os.getenv("DATA_AUTOMATION_PROJECT_ARN")
DATA_AUTOMATION_PROFILE_ARN = os.getenv("DATA_AUTOMATION_PROFILE_ARN")

# Get MAX_RETRIES and RETRY_INTERVAL from environment variables with defaults
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
RETRY_INTERVAL = int(os.getenv("RETRY_INTERVAL", "10"))

def check_bda_invocation_status(bda_invocation_arn: str) -> str:
    """
    Check Bedrock data automation Invocation status
    
    Args:
        bda_invocation_arn: The ARN of the BDA invocation
        
    Returns:
        The status of the BDA invocation
    """
    response = bda_client.get_data_automation_status(
        invocationArn=bda_invocation_arn
    )
    
    logger.info(f"BDA returned the following response: {response}")

    status = response.get('status')
    logger.info(f"Data automation status: {status}")

    return status

def wait_for_completion(bda_invocation_arn: str) -> Tuple[bool, str]:
    """
    Wait for the data automation job to complete
    
    Args:
        bda_invocation_arn: The ARN of the BDA invocation
        
    Returns:
        A tuple containing:
            - A boolean indicating if the operation was successful
            - The status string of the operation
    """
    retries = 0
    while retries < MAX_RETRIES:

        # Sleep before checking
        time.sleep(RETRY_INTERVAL)

        # Get the status of the job
        status = check_bda_invocation_status(bda_invocation_arn)
        
        # Handle different statuses
        if status == 'Success':
            logger.info("Data automation completed successfully!")
            return True, status
        elif status in ['ServiceError']:
            logger.warning(f"Data automation failed with status: {status}")
            return False, status
        elif status in ['Created', 'InProgress']:
            logger.info(f"Data automation in progress, status: {status}, Retry: {retries} of {MAX_RETRIES}. Waiting...")
            retries += 1
            continue
        else:
            logger.warning(f"Unexpected status: {status}")
            return False, status
    
    logger.info(f"Maximum retries reached ({MAX_RETRIES}), but job is still running")
    return True, "IN_PROGRESS"

def invoke_data_automation(s3_uri: str) -> str:
    """
    Invoke Bedrock data automation project
    
    Args:
        s3_uri: The S3 URI pointing to the document to process
        
    Returns:
        The invocation ARN for the BDA job
    """
    logger.info(f"Invoking data automation project: {DATA_AUTOMATION_PROJECT_ARN}, for s3_uri: {s3_uri}")

    response = bda_client.invoke_data_automation_async(
        inputConfiguration={
            's3Uri': s3_uri
        },
        outputConfiguration={
            's3Uri': f's3://{OUTPUT_BUCKET}/bda-output'
        },
        dataAutomationConfiguration={  
            'dataAutomationProjectArn': DATA_AUTOMATION_PROJECT_ARN,
        },
        dataAutomationProfileArn=DATA_AUTOMATION_PROFILE_ARN,
    )

    logger.info(f"BDA returned the following response: {response}")

    return response["invocationArn"]

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda function handler
    
    Args:
        event: The Lambda event data
        context: The Lambda context object
        
    Returns:
        A dictionary with the processing results
    """
    try:
        logger.info(f"Got the following event: {event}")

        # Get the S3 URI from the supplied event
        bucket_name = event['detail']['bucket']['name']
        object_key = event['detail']['object']['key']
        s3_uri = f"s3://{bucket_name}/{object_key}"
        logger.info(f"s3_uri: {s3_uri}")

        # Get the BDA ARN needed for this doc
        bda_invocation_arn = invoke_data_automation(s3_uri)
        logger.info(f"bda_invocation_arn: {bda_invocation_arn}")

        # Wait for completion or handle timeout
        success, status = wait_for_completion(bda_invocation_arn)

        if not success:
            error_msg = f"Processing failed with status: {status}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        if status == "IN_PROGRESS":
            logger.info(f"Processing is still in progress. ARN: {bda_invocation_arn}")
        else:
            logger.info(f"Processing completed successfully")
        
        result = {
            "s3_uri": s3_uri,
            "bda_invocation_arn": bda_invocation_arn,
            "bda_output_bucket": OUTPUT_BUCKET
        }

        logger.info(f"Returning: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        raise

# Main if called from command line
if __name__ == "__main__":
    lambda_handler({}, {})