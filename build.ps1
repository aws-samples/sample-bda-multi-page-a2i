#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#

# Remove old build artifacts
rm -rf ./out

# Make a new build directory
mkdir ./out

# Copy all of the necessary code into the build directory
cp -r ./src ./out
cp -r ./test ./out

# Now run the CDK to deploy everything
npm install --force

# Now run the CDK build
cdk bootstrap
cdk synth --all
cdk deploy --all --require-approval never --outputs-file ./out/build-outputs.json

Write-Output "Build complete, please see 'out' directory for build artifacts"