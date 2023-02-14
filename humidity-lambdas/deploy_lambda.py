import os
import sys
import pulumi
import pulumi_aws as aws
from pulumi import automation as auto
from pulumi_aws_tags import register_auto_tags

from infrastructure.launch_sqs import launch_sqs
from infrastructure.launch_ecr import launch_ecr
from infrastructure.launch_lambda import launch_humidity_lambda


# os.environ["AWS_PROFILE"] = ""


def main():
    """
    Launch pipeline including the following components:
        * Create Zipfiles for Lambdas
        * Launch Lambda Stacks
        * Remove Zipfiles
    """

    # Set stage for pipeline
    stage = "prod"
    # To destroy our program, we can run python main.py destroy
    destroy = False

    stack_name = auto.fully_qualified_stack_name("ncics", "ncai-humidity", "lambda")

    if stage == "prod":
        stack = auto.create_or_select_stack(
            stack_name=stack_name,
            project_name="ncai-humidity",
            program=launch_lambda,
        )

    print("successfully initialized stack")

    # for inline programs, we must manage plugins ourselves
    print("installing plugins...")
    stack.workspace.install_plugin("aws", "v4.0.0")
    print("plugins installed")

    # set stack configuration specifying the AWS region to deploy
    print("setting up config")
    stack.set_config("aws:region", auto.ConfigValue(value="us-east-1"))
    print("config set")

    print("refreshing stack...")
    stack.refresh(on_output=print)
    print("refresh complete")

    if destroy:
        print("destroying stack...")
        stack.destroy(on_output=print)
        print("stack destroy complete")
        sys.exit()

    print(stack.preview())

    print("updating stack...")
    up_res = stack.up(on_output=print)


def launch_lambda(stage: str = "prod"):
    """
    Launch pulumi stacks:
        * Ensure Tagging
        * Launch filesize lambda
        * Launch processing lambdas (including sqs)
    """

    # Automatically inject tags to created AWS resources.
    register_auto_tags(
        {
            "project": "ncai-humidity",
            "stage": "prod",
            "owner": "revathi",
            "user:Project": pulumi.get_project(),
            "user:Stack": pulumi.get_stack(),
        }
    )

    print(f"Launching {stage} Deployment")

    # Dead Letter Queue
    dlq_queue = aws.sqs.Queue("humidity-process-dlq", visibility_timeout_seconds=300)
    pulumi.export("humidity-process-dlq", dlq_queue.arn)

    sqs = launch_sqs(
        sqs_queue_name="humidity_process_queue",
        dlq_queue=dlq_queue,
    )

    ecr = launch_ecr(repository_name="humidity_process", container_path="./lambda")

    launch_humidity_lambda(
        lambda_name="humidity_process",
        lambda_timeout=300,  # Probably need to adjust this
        lambda_memory=3024,  # Probably need to adjust this
        lambda_handler="download_hadisd_lambda.lambda_handler",
        environment_variables={"PLACEHOLDER": "TEXT"},
        lambda_image=ecr["image"],
        input_sqs=sqs["sqs_queue"],
    )

    # Update Stack README
    with open("./Pulumi.README.md") as f:
        pulumi.export("readme", f.read())


if __name__ == "__main__":
    main()
