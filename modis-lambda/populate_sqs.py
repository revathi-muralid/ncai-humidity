import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sqs = boto3.client("sqs", region_name="us-east-1")


def main():
    # queue_url = os.environ["SQS_QUEUE_URL"]
    queue_url = "https://sqs.us-east-1.amazonaws.com/666852933323/humidity_process_queue-630c167"

    for key in range(0, 2):
        message = {
            "myind": key,
        }
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
        logger.info(response)
        print(response)


if __name__ == "__main__":
    main()
