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

region = os.environ.get("AWS_REGION", "us-east-1")

ES_ALLOWED_FIELDS = {
    "random_id", "kind_id", "account_id", "performer_id",
    "repository_id", "ip", "metadata", "datetime", "@timestamp"
}
type = "_doc"
headers = {"Content-Type": "application/json"}
session = boto3.session.Session()

# Global reusable clients for warm Lambda container reuse
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

def _build_opensearch_client(es_endpoint, secret):
    """Build an OpenSearch client with provided endpoint and secret."""
    elastic_search_secret = os.environ["secret_name"]

    if elastic_search_secret:
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

def _process_kinesis_record(record):
    # Kinesis data is base64-encoded, so decode here
    message = json.loads(base64.b64decode(record["kinesis"]["data"]))
    message_time = message["datetime"]
    message["@timestamp"] = message_time
    if "ip" in message and not message["ip"]:
        message.pop("ip")
    return message

def _filter_for_es(record):
    """Keep only allowed fields for ES to minimize log size."""
    return {k: v for k, v in record.items() if k in ES_ALLOWED_FIELDS}

def elasticsearch_handler(processed_records, client):
    """Send records to OpenSearch using pre-built client."""
    es_index_prefix = os.environ["index_prefix"]

    actions = []
    for message in processed_records:
        if message is None:
            continue
        index = es_index_prefix + str(datetime.fromisoformat(message["datetime"]).date())
        filtered = _filter_for_es(message)
        action = {"_index": index, "_id":  message["random_id"], "_source": filtered}
        actions.append(action)

    success, errors = helpers.bulk(client, actions, max_retries=3, raise_on_error=False)
    if errors:
        logger.error(errors)
    total = len(processed_records)
    print(f"Successfully processed {success}/{total} items for opensearch")

def _send_to_splunk(events, splunk_hec_url, session):
    """Send events to Splunk using a pre-configured session."""
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
    """Send records to Splunk using a pre-configured session."""
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

    # Get secret once for reuse
    secret = get_secret(os.environ["secret_name"])

    # Initialize OpenSearch client (reuse if already exists in warm container)
    if opensearch_client is None:
        es_endpoint = os.environ["es_endpoint"]
        opensearch_client = _build_opensearch_client(es_endpoint, secret)

    # Initialize Splunk session (reuse if already exists in warm container)
    if splunk_session is None:
        splunk_session = requests.Session()
        splunk_hec_token = secret["splunk_hec_token"]
        splunk_session.headers.update({'Authorization': f'Splunk {splunk_hec_token}'})

    processed_records = [_process_kinesis_record(r) for r in event["Records"]]

    # Minimal logs to ES (only allowed fields)
    elasticsearch_handler(processed_records, opensearch_client)

    # Full logs with extended fields to Splunk (skip if disabled)
    splunk_disabled = secret.get("splunk_disabled", False)
    if not (splunk_disabled and str(splunk_disabled).lower() == "true"):
        splunk_hec_url = secret["splunk_hec_url"]
        splunk_index = secret["splunk_index"]
        splunk_handler(processed_records, splunk_session, splunk_hec_url, splunk_index)
