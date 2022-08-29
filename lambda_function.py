import os
import logging
import json
import base64
import boto3
import requests
from requests.auth import HTTPBasicAuth
from requests_aws4auth import AWS4Auth
from datetime import date
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ["AWS_REGION"]
host = "https://" + os.environ["es_endpoint"]
secret_name = os.environ["secret_name"]
index = os.environ["index_prefix"] + str(date.today())
type = "_doc"
url = host + "/" + index + "/" + type + "/"
headers = {"Content-Type": "application/json"}
session = boto3.session.Session()


def get_secret(secret_name):

    client = session.client(service_name="secretsmanager", region_name=region)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(e)
        raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])

    return json.loads(secret)


def handler(event, context):
    count = 0
    if secret_name:
        secret = get_secret(secret_name)
        auth = HTTPBasicAuth(secret["master_user_name"], secret["master_user_password"])
    else:
        credentials = session.get_credentials()
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            "es",
            session_token=credentials.token,
        )

    for record in event["Records"]:
        # Kinesis data is base64-encoded, so decode here
        message = json.loads(base64.b64decode(record["kinesis"]["data"]))
        id = message["random_id"]
        message["@timestamp"] = message["datetime"]
        if not message["ip"]:
            message.pop["ip"]
        r = requests.put(url + id, auth=auth, json=message, headers=headers)
        count += 1
    return "Processed " + str(count) + " items."
