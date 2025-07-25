#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
FROM public.ecr.aws/lambda/python:3.12

# Install system dependencies (poppler-utils)
RUN dnf update -y && \
    dnf install -y poppler poppler-utils \
    libjpeg-devel \
    fontconfig-devel \
    libtiff-devel \
    libpng-devel \
    gcc \
    gcc-c++ \
    zip \
    poppler-utils \
    poppler-data \
    freetype \
    fontconfig \
    dejavu-sans-fonts \
    liberation-fonts \
    && dnf clean all

# Set necessary environment variables
# ENV PATH="/usr/bin:/usr/local/bin:${PATH}"

# Copy files to the lambda root
COPY . ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip3 install --no-cache-dir -r ./requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]