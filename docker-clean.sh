#!/bin/bash

#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# This script will clean out local docker images
#

docker rmi $(docker images -q) --force
docker builder prune --all --force
docker system prune --all --force