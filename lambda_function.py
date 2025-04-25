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

def _process_kinesis_record(record):
    # Kinesis data is base64-encoded, so decode here
    message = json.loads(base64.b64decode(record["kinesis"]["data"]))
    message_time = message["datetime"]
    message["@timestamp"] = message_time
    if "ip" in message and not message["ip"]:
        message.pop("ip")
    return message


def elasticsearch_handler(processed_records, context):
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
    for message in processed_records:
        if message is None:
            continue
        index = es_index_prefix + str(datetime.fromisoformat(message["datetime"]).date())
        action = {"_index": index, "_id":  message["random_id"], "_source": message}
        actions.append(action)

    success, errors = helpers.bulk(client, actions, max_retries=3, raise_on_error=False)
    if errors:
        logger.error(errors)
    total = len(processed_records)
    print(f"Successfully processed {success}/{total} items for opensearch")


def _send_to_splunk(events, splunk_hec_url, splunk_hec_token):
    try:
        response = requests.post(
            splunk_hec_url,
            headers={'Authorization': f'Splunk {splunk_hec_token}'},
            json=events,
            timeout=30
        )
        response.raise_for_status()
        return len(events)
    except Exception as e:
        logger.error(str(e))
        return 0

def splunk_handler(processed_records, context):
    secret = get_secret(os.environ["secret_name"])
    # splunk HEC configuration
    splunk_hec_url = secret["splunk_hec_url"]
    splunk_hec_token = secret["splunk_hec_token"]
    splunk_index = secret["splunk_index"]
    success_count = 0
    events = []
    max_batch_size = 500

    for message in processed_records:
        if message is None:
            continue
        # Create splunk payload
        event = {
            "event": message,
            "sourcetype": "json",
            "index": splunk_index,
            "time": message["datetime"],
            "_id": message["random_id"],
        }
        events.append(event)
        # if the number of events reaches the max batch size, send them to Splunk
        if len(events) >= max_batch_size:
            sent = _send_to_splunk(events, splunk_hec_url, splunk_hec_token)
            success_count += sent
            events = []
    # Send remaining events
    if events:
        sent = _send_to_splunk(events, splunk_hec_url, splunk_hec_token)
        success_count += sent

    total = len(processed_records)
    print(f"Successfully processed {success_count}/{total} items to Splunk")

def handler(event, context):
    processed_records = [_process_kinesis_record(r) for r in event["Records"]]
    elasticsearch_handler(processed_records,context)
    splunk_handler(processed_records,context)