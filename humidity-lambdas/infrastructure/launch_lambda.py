import pulumi
import pulumi_aws as aws
from pulumi_awsx.ecr import Image


def launch_humidity_lambda(
    lambda_name: str,
    lambda_timeout: int,
    lambda_memory: int,
    lambda_handler: str,
    environment_variables: dict,
    lambda_image: Image,
    input_sqs: aws.sqs.Queue,
) -> dict:
    """
    Builds R Processing Lambda for ARC Pipeline

    Builds:
        * ECR
        * Lambda Image on ECR
        * Supporting roles/resources
        * Lambda
    Connects:
        * SQS to Lambda

    Parameters:
        lambda_name (str): name for lambda and other associated resources
        lambda_timeout (int): timeout in seconds for lambda function
        lambda_memory (int): RAM for lambda function in MB
        lambda_handler (str): Dunction inside lambda to handle events should be "<file>.<function_name>"
        envionment_variables (dict): Environment variables to pass into Lambda
        input_sqs (aws.sqs.Queue): Pulumi AWS queue to connect to lambda for triggering

    Returns:
        (dict): Pulumi Lambda function and Lambda Image
    """

    # Create cloudwatch log group for process lambda logging
    cloudwatch = aws.cloudwatch.LogGroup(
        f"{lambda_name}-log-group", retention_in_days=14
    )

    # Create role for lambda
    lambda_role = aws.iam.Role(
        f"{lambda_name}_iam",
        assume_role_policy="""{
             "Version": "2012-10-17",
             "Statement": [
                 {
                     "Action": "sts:AssumeRole",
                     "Principal": {
                         "Service": "lambda.amazonaws.com"
                     },
                     "Effect": "Allow",
                     "Sid": ""
                 }
             ]
         }
         """,
    )

    # Attach privileges to lambda for s3
    s3_attach_policy = aws.iam.RolePolicyAttachment(
        f"{lambda_name}_s3_policy",
        role=lambda_role.name,
        policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess",
    )

    # Attach privileges to lambda for sqs
    sqs_attach_policy = aws.iam.RolePolicyAttachment(
        f"{lambda_name}_sqs_policy",
        role=lambda_role.name,
        policy_arn="arn:aws:iam::aws:policy/AmazonSQSFullAccess",
    )

    # Attach basic lambda execution privileges
    lambda_basic_execution = aws.iam.RolePolicyAttachment(
        f"{lambda_name}_basic_policy",
        role=lambda_role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )

    # Lambda policy for logging
    lambda_logging = aws.iam.Policy(
        f"{lambda_name}-lambdaLogging",
        path="/",
        description="IAM policy for logging from a lambda",
        policy="""{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Action": [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          "Resource": "arn:aws:logs:*:*:*",
          "Effect": "Allow"
        }
      ]
    }
    """,
    )

    # Attach lambda logging policy to role
    lambda_logs = aws.iam.RolePolicyAttachment(
        f"{lambda_name}_lambdaLogs",
        role=lambda_role.name,
        policy_arn=lambda_logging.arn,
    )

    # Set up container based lambda using image that has just been created
    # Key points: make sure to pass in the environment variables
    #    and to add the CMD override to the appropriate functions file
    #    if needed.  If not, use default function call from image.
    nodd_lambda = aws.lambda_.Function(
        f"{lambda_name}-lambda",
        architectures=["arm64"],
        image_uri=lambda_image.image_uri,
        role=lambda_role.arn,
        package_type="Image",
        memory_size=lambda_memory,
        timeout=lambda_timeout,
        environment=aws.lambda_.FunctionEnvironmentArgs(
            variables=environment_variables
        ),
        image_config=aws.lambda_.FunctionImageConfigArgs(commands=[lambda_handler]),
        opts=pulumi.ResourceOptions(depends_on=[lambda_logs, cloudwatch]),
    )

    # Connect lambda to sqs queue - batch size of 1
    sqs_connection = aws.lambda_.EventSourceMapping(
        f"{lambda_name}_sqs_connection",
        event_source_arn=input_sqs.arn,
        function_name=nodd_lambda.arn,
        batch_size=1,
    )

    pulumi.export(f"{lambda_name}-lambda", nodd_lambda.id)

    return {"lambda_function": nodd_lambda, "lambda-image": lambda_image}
