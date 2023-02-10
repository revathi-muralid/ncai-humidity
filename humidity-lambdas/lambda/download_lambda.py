######################

# . Note this is a template from a previous lambda....

######


import os
import json
import logging
import awswrangler as wr
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def transfer(source_bucket: str, object_key: str) -> dict:
    """
    Reads in csv.gz and writes out csv and parquet files.

    Parameters:
        source_bucket (str): originating s3 bucket
        object_key (str): name of csv.gz object key

    Returns:
        (dict): Return code and message

    """
    # 'Path' to file:
    object_path = f"s3://{source_bucket}/{object_key}"

    logger.info("Input data path:" + object_path)

    ghcn_columns = [
        "ID",
        "DATE",
        "ELEMENT",
        "DATA_VALUE",
        "M_FLAG",
        "Q_FLAG",
        "S_FLAG",
        "OBS_TIME",
    ]

    ghcn_dtypes = {
        "ID": str,
        "DATE": str,
        "ELEMENT": str,
        "DATA_VALUE": int,
        "M_FLAG": str,
        "Q_FLAG": str,
        "S_FLAG": str,
        "OBS_TIME": str,
    }

    start = datetime.now()
    ghcn_data = wr.s3.read_csv(
        object_path, header=None, names=ghcn_columns, dtype=ghcn_dtypes
    )
    end = datetime.now()

    logger.info("Read in Data in " + str(end - start))

    # Output Bucket
    output_bucket = "noaa-ghcn-pds"

    if "by_year" in object_key:
        prefix_flag = "by_year"
        hive_prefix = "YEAR="
    elif "by_station" in object_key:
        prefix_flag = "by_station"
        hive_prefix = "STATION="

    file_id = object_key.split("/").pop()[:-7]

    # Write Parquet File
    parquet_output_path = (
        f"s3://{output_bucket}/parquet/{prefix_flag}/{hive_prefix}{file_id}/"
    )

    try:
        start = datetime.now()
        wr.s3.to_parquet(
            df=ghcn_data,
            path=parquet_output_path,
            dataset=True,
            compression="snappy",
            partition_cols=["ELEMENT"],
            max_rows_by_file=500000,
            concurrent_partitioning=True,
            mode="overwrite",
            # s3_additional_kwargs={"ACL": "public-read"},
        )
        end = datetime.now()

        logger.info("Wrote in Parquet Data in " + str(end - start))

    except Exception as e:
        logger.critical(e, exc_info=True)

    # Write CSV File
    csv_output_path = f"s3://{output_bucket}/csv/{prefix_flag}/{file_id}.csv"

    try:
        start = datetime.now()
        wr.s3.to_csv(
            df=ghcn_data,
            path=csv_output_path,
            index=False
            # s3_additional_kwargs={"ACL": "public-read"},
        )
        end = datetime.now()

        logger.info("Wrote in CSV Data in " + str(end - start))

    except Exception as e:
        logger.critical(e, exc_info=True)

    return {"statusCode": 200, "body": json.dumps("Wrote GHCN Data")}


def lambda_handler(event, context):
    logger.info("## ENVIRONMENT VARIABLES")
    logger.info(os.environ)
    logger.info("## EVENT")
    logger.info(event)

    # Message
    event_body = event["Records"][0]["body"]
    event_message = json.loads(json.loads(event_body)["Message"])

    logger.info("## Message")
    logger.info(event_message)

    # Event Info
    source_bucket = event_message["detail"]["bucket"]["name"]
    object_key = event_message["detail"]["object"]["key"]

    # Record Info

    logger.info("## Source Bucket")
    logger.info(source_bucket)
    logger.info("## Object Key")
    logger.info(object_key)

    transfer_results = transfer(source_bucket=source_bucket, object_key=object_key)

    return transfer_results
