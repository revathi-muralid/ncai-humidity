######################

# Taken from LAADS website
# https://data.laadsdaac.earthdatacloud.nasa.gov/s3credentialsREADME

######

import os
import json
import logging
import requests
import argparse
import base64
import boto3

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def retrieve_credentials(event):
    """Makes the Oauth calls to authenticate with EDS and return a set of s3
    same-region, read-only credentials.
    """
    login_resp = requests.get(
        event['s3_endpoint'], allow_redirects=False
    )
    login_resp.raise_for_status()

    auth = f"{event['edl_username']}:{event['edl_password']}"
    encoded_auth  = base64.b64encode(auth.encode('ascii'))

    auth_redirect = requests.post(
        login_resp.headers['location'],
        data = {"credentials": encoded_auth},
        headers= { "Origin": event['s3_endpoint'] },
        allow_redirects=False
    )
    auth_redirect.raise_for_status()

    final = requests.get(auth_redirect.headers['location'], allow_redirects=False)

    results = requests.get(event['s3_endpoint'], cookies={'accessToken': final.cookies['accessToken']})
    results.raise_for_status()

    return json.loads(results.content)


def lambda_handler(event, context):

    creds = retrieve_credentials(event)
    bucket = event['bucket_name']

    # create client with temporary credentials
    client = boto3.client(
        's3',
        aws_access_key_id=creds["accessKeyId"],
        aws_secret_access_key=creds["secretAccessKey"],
        aws_session_token=creds["sessionToken"]
    )
    # use the client for readonly access.
    response = client.list_objects_v2(Bucket=bucket, Prefix="")

    return {
        'statusCode': 200,
        'body': json.dumps([r["Key"] for r in response['Contents']])
    }

}
