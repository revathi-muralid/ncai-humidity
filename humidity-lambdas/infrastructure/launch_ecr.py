import pulumi
import pulumi_awsx as awsx


def launch_ecr(repository_name: str, container_path: str) -> dict:
    #  """
    #     Builds Container Repository.

    #     Builds:
    #         * ECR
    #         * Image
    #         * Moves Image to ECR

    #     Parameters:
    #         repository_name (str): name to be used to label repository and image on ECR
    #         container_path (str): path to directory containing Dockerfile for container

    #     Returns:
    #         (dict): Pulumi Image
    # """

    spi_repository = awsx.ecr.Repository(f"{repository_name}-ecr")

    spi_process_image = awsx.ecr.Image(
        f"{repository_name}-image",
        repository_url=spi_repository.url,
        path=container_path,
    )

    pulumi.export("ecr", spi_repository.url)
    pulumi.export("image", spi_process_image.image_uri)

    return {"image": spi_process_image}
