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

EXTENDED_FIELDS = {
    "request_url", "http_method", "performer_username", "performer_email",
    "performer_kind", "auth_type", "user_agent", "request_id", "x_forwarded_for"
}
type = "_doc"
headers = {"Content-Type": "application/json"}
session = boto3.session.Session()

# Reusable clients for Lambda warm container optimization
opensearch_client = None
splunk_session = None


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

def _strip_extended_fields(record):
    """Remove extended logging fields for ES to keep logs minimal."""
    return {k: v for k, v in record.items() if k not in EXTENDED_FIELDS}


def _build_opensearch_client(es_endpoint, secret):
    if secret:
        auth = (secret["master_user_name"], secret["master_user_password"])
    else:
        credentials = session.get_credentials()
        auth = AWSV4SignerAuth(credentials, region)

    return OpenSearch(
        hosts=[{"host": es_endpoint, "port": 443}],
        http_auth=auth,
        http_compress=True,
        use_ssl=True,
        verify_certs=True,
    )

def elasticsearch_handler(processed_records, client):
    es_index_prefix = os.environ["index_prefix"]
    actions = []
    for message in processed_records:
        if message is None:
            continue
        index = es_index_prefix + str(datetime.fromisoformat(message["datetime"]).date())
        action = {"_index": index, "_id":  message["random_id"], "_source": message}
        actions.append(action)

    success_count = 0
    # parallel_bulk handles chunking internally (default chunk_size=500)
    for success, info in helpers.parallel_bulk(
        client,
        actions,
        thread_count=4,
        chunk_size=500,
        max_retries=3,
        raise_on_error=False
    ):
        if success:
            success_count += 1
        else:
            logger.error(info)

    total = len(processed_records)
    print(f"Successfully processed {success_count}/{total} items for opensearch")

def _send_to_splunk(events, splunk_hec_url, session):
    try:
        response = session.post(
            splunk_hec_url,
            json=events,
            timeout=12
        )
        response.raise_for_status()
        return len(events)
    except Exception as e:
        logger.error(str(e))
        return 0

def splunk_handler(processed_records, session, splunk_hec_url, splunk_index):
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
        }
        events.append(event)
        # if the number of events reaches the max batch size, send them to Splunk
        if len(events) >= max_batch_size:
            sent = _send_to_splunk(events, splunk_hec_url, session)
            success_count += sent
            events = []
    # Send remaining events
    if events:
        sent = _send_to_splunk(events, splunk_hec_url, session)
        success_count += sent

    total = len(processed_records)
    print(f"Successfully processed {success_count}/{total} items to Splunk")

def handler(event, context):
    global opensearch_client, splunk_session

    # Get secret once per invocation
    secret = get_secret(os.environ["secret_name"])

    # Initialize OpenSearch client once for warm Lambda containers
    if opensearch_client is None:
        opensearch_client = _build_opensearch_client(
            os.environ["es_endpoint"],
            secret
        )

    # Initialize Splunk session once for warm Lambda containers (if not disabled)
    splunk_disabled = secret.get("splunk_disabled", False)
    if not (splunk_disabled and str(splunk_disabled).lower() == "true"):
        if splunk_session is None:
            splunk_hec_token = secret["splunk_hec_token"]
            splunk_session = requests.Session()
            splunk_session.headers.update({
                'Authorization': f'Splunk {splunk_hec_token}'
            })

    processed_records = [_process_kinesis_record(r) for r in event["Records"]]

    # Minimal logs to ES (strip extended fields)
    es_records = [_strip_extended_fields(r) for r in processed_records]
    elasticsearch_handler(es_records, opensearch_client)

    # Full logs with extended fields to Splunk
    if not (splunk_disabled and str(splunk_disabled).lower() == "true"):
        splunk_handler(processed_records, splunk_session, secret["splunk_hec_url"], secret["splunk_index"])
