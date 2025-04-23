import os
import logging
import json
import base64
import boto3
import requests
from datetime import datetime
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, helpers, AWSV4SignerAuth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ["AWS_REGION"]
type = "_doc"
headers = {"Content-Type": "application/json"}
session = boto3.session.Session()


def get_secret(aws_secret_value):

    client = session.client(service_name="secretsmanager", region_name=region)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=aws_secret_value)
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

def process_kinesis_record(record):
    # Kinesis data is base64-encoded, so decode here
    message = json.loads(base64.b64decode(record["kinesis"]["data"]))
    message_time = message["datetime"]
    message["@timestamp"] = message_time
    if "ip" in message and not message["ip"]:
        message.pop("ip")
    message_date = str(datetime.fromisoformat(message_time).date())
    return message, message_date


def elasticsearch_handler(event, context):
    es_host = os.environ["es_endpoint"]
    elastic_search_secret = os.environ["secret_name"]
    es_index_prefix = os.environ["index_prefix"]

    if elastic_search_secret:
        secret = get_secret(elastic_search_secret)
        auth = (secret["master_user_name"], secret["master_user_password"])
    else:
        credentials = session.get_credentials()
        auth = AWSV4SignerAuth(credentials, region)

    client = OpenSearch(
        hosts=[{"host": es_host, "port": 443}],
        http_auth=auth,
        http_compress=True,
        use_ssl=True,
        verify_certs=True,
    )

    actions = []
    for record in event["Records"]:
        message, message_date = process_kinesis_record(record)
        index = es_index_prefix + message_date
        action = {"_index": index, "_id":  message["random_id"], "_source": message}
        actions.append(action)

    success, errors = helpers.bulk(client, actions, max_retries=3, raise_on_error=False)
    if errors:
        logger.error(errors)
    total = len(event["Records"])
    print(f"Successfully processed {success}/{total} items for opensearch")


def splunk_handler(event, context):
    secret = get_secret('splunk-quayio-audit-log')
    # splunk HEC configuration
    splunk_hec_url = secret["splunk_hec_url"]
    splunk_hec_token = secret["splunk_hec_token"]
    splunk_index = secret["splunk_index"]
    events = []

    for record in event["Records"]:
        message, message_date = process_kinesis_record(record)
        # Create splunk payload
        event = {
            "event": message,
            "sourcetype": "json",
            "index": splunk_index + message_date,
            "time": message["datetime"],
            "_id": message["random_id"],
        }
        events.append(event)

    success_count = 0
    errors = []

    try:
        response = requests.post(
            splunk_hec_url,
            headers={'Authorization': f'Splunk {splunk_hec_token}'},
            json=events,
            timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        errors.append(str(e))

    if errors:
        logger.error(errors)
    total = len(event["Records"])
    print(f"Successfully processed {success_count}/{total} items to Splunk")

def handler(event, context):
    elasticsearch_handler(event,context)
    splunk_handler(event,context)