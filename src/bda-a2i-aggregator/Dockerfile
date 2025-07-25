#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
FROM public.ecr.aws/lambda/python:3.12

# Copy files over
COPY . ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip3 install --no-cache-dir -r ./requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]