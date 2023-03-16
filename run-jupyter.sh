#!/bin/bash

docker build -t aws-metrics-transfer .
aws_access_key=$(aws configure get default.aws_access_key_id)
aws_secret_access_key=$(aws configure get default.aws_secret_access_key)
aws_session=$(aws configure get default.aws_session_token)
nsand_access_key=$(aws configure get modis.aws_access_key_id)
nsand_secret=$(aws configure get modis.aws_secret_access_key)
nsand_session=$(aws configure get modis.aws_session_token)
aws_region="us-east-1"
aws_bucket="ncai-humidity"

docker run -it --privileged --rm -p 8080:8888 -e AWS_ACCESS_KEY_ID=$aws_access_key -e AWS_SECRET_ACCESS_KEY=$aws_secret_access_key -e AWS_SESSION_TOKEN=$aws_session -e OUTPUT_BUCKET_NAME=$aws_bucket -e NSAND_ACCESS=$nsand_access_key  -e NSAND_SECRET=$nsand_secret -e NSAND_SESSION=$nsand_session -e AWS_REGION=$aws_region -v /home/ec2-user:/home/jovyan/work aws-metrics-transfer
