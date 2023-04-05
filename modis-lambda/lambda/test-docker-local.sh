#!/bin/bash

docker build -t modis-lambda .
aws_access_key=$(aws configure get default.aws_access_key_id)
aws_secret_access_key=$(aws configure get default.aws_secret_access_key)
aws_session=$(aws configure get default.aws_session_token)
nsand_access_key=$(aws configure get modis.aws_access_key_id)
nsand_secret=$(aws configure get modis.aws_secret_access_key)
nsand_session=$(aws configure get modis.aws_session_token)
aws_region="us-east-1"
aws_bucket="ncai-humidity"

docker run -it  -p 9000:8080 \
	 -e AWS_ACCESS_KEY_ID=$aws_access_key \
	 -e AWS_SECRET_ACCESS_KEY=$aws_secret_access_key \
	 -e AWS_SESSION_TOKEN=$aws_session \
	 -e AWS_REGION=us-east-1 \
	 -e NSAND_ACCESS=$nsand_access_key \
	 -e NSAND_SECRET=$nsand_secret \
	 -e NSAND_SESSION=$nsand_session \
	 -e AWS_LAMBDA_FUNCTION_MEMORY_SIZE=256 \
	 modis-lambda
#
#
#  -v /Users/scottwilkins/Projects/SPI/data:/var/task/data:rw \
