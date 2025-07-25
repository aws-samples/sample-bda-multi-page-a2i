import boto3
import json

human_loop_name="acord-review-loop-20250515194108"
a2i_client = boto3.client('sagemaker-a2i-runtime')
sagemaker_client = boto3.client('sagemaker')
try:
    loop_status = a2i_client.describe_human_loop(
        HumanLoopName=human_loop_name
    )
    print(loop_status)
except Exception as e:
    print(e)


response = sagemaker_client.describe_flow_definition(
    FlowDefinitionName="bda-review-flow-definition"
)
print(json.dumps(response, default=str, indent=2))