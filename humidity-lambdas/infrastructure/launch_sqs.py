import pulumi
import pulumi_aws as aws


def launch_sqs(sqs_queue_name: str, dlq_queue, visibility_timeout: int = 1200) -> dict:
    """
    Builds:
        * SQS Queue with permissions

    """

    redrive_policy = dlq_queue.arn.apply(
        lambda arn: f"""{{
                "deadLetterTargetArn": "{arn}",
                "maxReceiveCount": 2
            }}"""
    )

    # Launch SQS queue to receive updated files to process
    sqs_queue = aws.sqs.Queue(
        sqs_queue_name,
        visibility_timeout_seconds=visibility_timeout,
        redrive_policy=redrive_policy,
    )

    sqs_policy = sqs_queue.arn.apply(
        lambda arn: f"""{{
                "Version":"2012-10-17",
                "Statement":[{{
                    "Effect": "Allow",
                    "Principal": {{
                        "AWS": "911289748819"
                        }},
                    "Action": "sqs:*",
                    "Resource": "{arn}"
               }}]
            }}
            """
    )

    # Attach policy to sqs queue
    sqs_eb_policy = aws.sqs.QueuePolicy(
        f"{sqs_queue_name}-policy",
        queue_url=sqs_queue.id,
        policy=sqs_policy,
    )

    pulumi.export(sqs_queue_name, sqs_queue.arn)

    # return the sqs resource to be used by other resources
    return {"sqs_queue": sqs_queue}
