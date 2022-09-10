import os
import logging
import json
import base64
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, helpers, AWSV4SignerAuth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ["AWS_REGION"]
host = os.environ["es_endpoint"]
secret_name = os.environ["secret_name"]
index_prefix = os.environ["index_prefix"]
type = "_doc"
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
    if secret_name:
        secret = get_secret(secret_name)
        auth = (secret["master_user_name"], secret["master_user_password"])
    else:
        credentials = session.get_credentials()
        auth = AWSV4SignerAuth(credentials, region)

    client = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = auth,
        http_compress = True,
        use_ssl = True,
        verify_certs=True
    )

    actions = []
    for record in event["Records"]:
        # Kinesis data is base64-encoded, so decode here
        message = json.loads(base64.b64decode(record["kinesis"]["data"]))
        id = message["random_id"]
        message_time = message["datetime"]
        message["@timestamp"] = message_time
        if not message["ip"]:
            message.pop["ip"]
        message_date = str(datetime.fromisoformat(message_time).date())
        index = index_prefix + message_date
        action = {
            '_op_type': 'update',
            '_index': index,
            '_id': id,
            'doc': message
        }
        actions.append(action)

    success, errors = helpers.bulk(client,actions,max_retries=3)
    if errors:
        logger.error(errors)
    total = len(event["Records"])
    print(f"Success processed {success}/{total} items.")
