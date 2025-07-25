#!/bin/bash

#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# This script is used to build and deploy the solution.
# AWS CDK is used as the main packaging and deployment solution.
# This script will package code and pull in required modules
# to allow for the rest of the CDK deployment process to run.
#
# Additionaly, this script will check to ensure that the 
# necessary versions of CDK, NPM, and other required 
# software is installed.
#

# Remove the output directory if it exists as it may container artifacts from a previous build
rm -rf ./out
mkdir out

# Copy all of the necessary code into the build directory
cp -r ./src ./out
cp -r ./test ./out

# Install all requirements
npm install --force

if [[ $? -ne 0 ]]; then
    echo "Failed to install npm packages"
    exit 1
fi

# Make sure that environment is setup
npx cdk bootstrap $@

if [[ $? -ne 0 ]]; then
    echo "Failed to bootstrap CDK"
    exit 1
fi

# Test the CDK code
npx cdk synth --all $@

if [[ $? -ne 0 ]]; then
    echo "Failed to synthesize CDK"
    exit 1
fi

# Deploy the solution
# If parameters are provided, pass them to cdk deploy
if [ "$#" -gt 0 ]; then
    echo "Deploying with provided parameters"
    npx cdk deploy --all --require-approval never --outputs-file ./out/build-outputs.json $@
else
    echo "Deploying with default parameters (creating new resources)"
    npx cdk deploy --all --require-approval never --outputs-file ./out/build-outputs.json
fi

if [[ $? -ne 0 ]]; then
    echo "Failed to deploy CDK"
    exit 1
fi

echo "Build complete, please see 'out' directory for build artifacts"