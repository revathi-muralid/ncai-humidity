#!/bin/bash

docker build -t modis-lambda .

# aws_access_key=$(aws configure get spi-programmatic.aws_access_key_id)
# aws_secret_access_key=$(aws configure get spi-programmatic.aws_secret_access_key)
#nsand_access_key=$(aws configure get nsand-reporter.aws_access_key_id)
#nsand_secret=$(aws configure get nsand-reporter.aws_secret_access_key)
#sand_access_key=$(aws configure get sand-reporter.aws_access_key_id)
#sand_secret=$(aws configure get sand-reporter.aws_secret_access_key)
#aws_region="us-east-1"
#aws_bucket="bdp-report-data-storage-3812f04"

docker run -it  -p 9000:8080 \
	 -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
	 -e AWS_SECRET_KEY=$AWS_SECRET_KEY \
	 -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
	 -e AWS_REGION=us-east-1 modis-lambda
#
#
#  -v /Users/scottwilkins/Projects/SPI/data:/var/task/data:rw \
