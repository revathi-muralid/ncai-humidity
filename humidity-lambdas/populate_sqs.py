import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sqs = boto3.client("sqs", region_name="us-east-1")


def get_input_keys(bucket, prefix, suffix):
    """Get a list of all keys in an S3 bucket."""
    kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            key = obj["Key"]
            if key.endswith(suffix):
                yield key

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break


def main():
    src_bucket = os.environ["SRC_BUCKET"]
    src_path = os.environ["SRC_PATH"]
    dst_bucket = os.environ["DST_BUCKET"]
    dst_path = os.environ["DST_PATH"]
    precip = os.environ["PRECIPVAR"]
    start_year = os.environ["START_YEAR"]
    end_year = os.environ["END_YEAR"]
    queue_url = os.environ["SQS_QUEUE_URL"]

    numkeys = 0
    for key in get_input_keys(src_bucket, src_path, ".nc"):
        message = {
            "src_bucket": src_bucket,
            "dst_bucket": dst_bucket,
            "dst_path": dst_path,
            "precipvar": precip,
            "start_year": start_year,
            "end_year": end_year,
            "object_key": key,
        }
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
        logger.info(response)
        print(response)
        numkeys = numkeys + 1

    print(f"## Sent {numkeys} keys to {queue_url}")

    return {"statusCode": 200, "body": json.dumps("populate_sqs Complete")}


if __name__ == "__main__":
    main()
