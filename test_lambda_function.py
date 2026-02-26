import json
import base64
import pytest
from unittest.mock import patch, MagicMock

from lambda_function import (
    _filter_for_es,
    _process_kinesis_record,
    handler,
    ES_ALLOWED_FIELDS,
)


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Set required environment variables for all tests."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("es_endpoint", "test-es-endpoint")
    monkeypatch.setenv("secret_name", "test-secret")
    monkeypatch.setenv("index_prefix", "logs-")


@pytest.fixture(autouse=True)
def reset_global_clients():
    """Reset global client variables before and after each test."""
    import lambda_function
    lambda_function.opensearch_client = None
    yield
    lambda_function.opensearch_client = None


@pytest.fixture
def sample_full_record():
    """Sample full log record with all fields (as sent to Splunk)."""
    return {
        "datetime": "2026-02-18T10:30:00",
        "@timestamp": "2026-02-18T10:30:00",
        "random_id": "abc123",
        "kind_id": 5,
        "account_id": 12345,
        "performer_id": 67890,
        "repository_id": 11111,
        "ip": "192.168.1.1",
        "metadata": {"oauth_token_id": 999},
        "request_url": "/api/login",
        "http_method": "POST",
        "performer_username": "john_doe",
        "performer_email": "john@example.com",
        "performer_kind": "user",
        "auth_type": "oauth",
        "user_agent": "Mozilla/5.0",
        "request_id": "req-456",
        "x_forwarded_for": "10.0.0.1",
    }


@pytest.fixture
def sample_es_record():
    """Sample record with only ES allowed fields."""
    return {
        "datetime": "2026-02-18T10:30:00",
        "@timestamp": "2026-02-18T10:30:00",
        "random_id": "abc123",
        "kind_id": 5,
        "account_id": 12345,
        "performer_id": 67890,
        "repository_id": 11111,
        "ip": "192.168.1.1",
        "metadata": {"oauth_token_id": 999},
    }


def create_kinesis_record(data: dict) -> dict:
    """Helper to create a Kinesis record with base64-encoded data."""
    encoded = base64.b64encode(json.dumps(data).encode()).decode()
    return {"kinesis": {"data": encoded}}


class TestFilterForEs:
    """Tests for _filter_for_es function."""

    def test_keeps_only_allowed_fields(self, sample_full_record):
        result = _filter_for_es(sample_full_record)

        for field in ES_ALLOWED_FIELDS:
            if field in sample_full_record:
                assert field in result

        disallowed = {"request_url", "http_method", "performer_username", 
                      "performer_email", "performer_kind", "auth_type", 
                      "user_agent", "request_id", "x_forwarded_for"}
        for field in disallowed:
            assert field not in result

    def test_preserves_all_es_fields(self, sample_full_record):
        result = _filter_for_es(sample_full_record)

        assert result["datetime"] == "2026-02-18T10:30:00"
        assert result["random_id"] == "abc123"
        assert result["kind_id"] == 5
        assert result["account_id"] == 12345
        assert result["performer_id"] == 67890
        assert result["repository_id"] == 11111
        assert result["ip"] == "192.168.1.1"
        assert result["metadata"] == {"oauth_token_id": 999}

    def test_returns_only_allowed_fields_from_es_record(self, sample_es_record):
        result = _filter_for_es(sample_es_record)

        assert result == sample_es_record

    def test_returns_empty_dict_for_empty_input(self):
        result = _filter_for_es({})

        assert result == {}

    def test_filters_out_non_allowed_fields(self):
        record = {
            "datetime": "2026-02-18T10:30:00",
            "random_id": "abc123",
            "request_url": "/api/test",
            "user_agent": "curl/7.0",
            "some_other_field": "value",
        }

        result = _filter_for_es(record)

        assert "datetime" in result
        assert "random_id" in result
        assert "request_url" not in result
        assert "user_agent" not in result
        assert "some_other_field" not in result


class TestProcessKinesisRecord:
    """Tests for _process_kinesis_record function."""

    def test_decodes_base64_kinesis_data(self, sample_es_record):
        kinesis_record = create_kinesis_record(sample_es_record)
        result = _process_kinesis_record(kinesis_record)

        assert result["random_id"] == "abc123"
        assert result["kind_id"] == 5

    def test_adds_timestamp_field(self):
        record = {"datetime": "2026-02-18T10:30:00", "random_id": "abc123"}
        kinesis_record = create_kinesis_record(record)
        result = _process_kinesis_record(kinesis_record)

        assert "@timestamp" in result
        assert result["@timestamp"] == "2026-02-18T10:30:00"

    def test_removes_empty_ip_field(self):
        record = {
            "datetime": "2026-02-18T10:30:00",
            "random_id": "abc123",
            "ip": "",
        }
        kinesis_record = create_kinesis_record(record)
        result = _process_kinesis_record(kinesis_record)

        assert "ip" not in result

    def test_preserves_non_empty_ip_field(self):
        record = {
            "datetime": "2026-02-18T10:30:00",
            "random_id": "abc123",
            "ip": "10.0.0.1",
        }
        kinesis_record = create_kinesis_record(record)
        result = _process_kinesis_record(kinesis_record)

        assert result["ip"] == "10.0.0.1"


class TestHandler:
    """Tests for handler function - verifies ES gets filtered records, Splunk gets full records."""

    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    def test_es_receives_only_allowed_fields(
        self,
        mock_get_secret,
        mock_build_client,
        mock_session_class,
        mock_es_handler,
        mock_splunk_handler,
        sample_full_record,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test",
            "master_user_password": "test",
            "splunk_hec_token": "test-token",
            "splunk_hec_url": "http://test.com",
            "splunk_index": "test-index",
        }
        mock_build_client.return_value = MagicMock()
        mock_session_class.return_value = MagicMock()

        event = {"Records": [create_kinesis_record(sample_full_record)]}
        context = MagicMock()

        handler(event, context)

        es_records = mock_es_handler.call_args[0][0]
        assert len(es_records) == 1

        # elasticsearch_handler now receives unfiltered records
        es_record = es_records[0]
        # Verify it receives the full record (including non-ES fields)
        assert "random_id" in es_record
        assert "datetime" in es_record
        assert "request_url" in es_record
        assert "http_method" in es_record
        assert "performer_username" in es_record

    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    def test_splunk_receives_full_records(
        self,
        mock_get_secret,
        mock_build_client,
        mock_session_class,
        mock_es_handler,
        mock_splunk_handler,
        sample_full_record,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test",
            "master_user_password": "test",
            "splunk_hec_token": "test-token",
            "splunk_hec_url": "http://test.com",
            "splunk_index": "test-index",
        }
        mock_build_client.return_value = MagicMock()
        mock_session_class.return_value = MagicMock()

        event = {"Records": [create_kinesis_record(sample_full_record)]}
        context = MagicMock()

        handler(event, context)

        splunk_records = mock_splunk_handler.call_args[0][0]
        assert len(splunk_records) == 1
        assert "request_url" in splunk_records[0]
        assert "http_method" in splunk_records[0]
        assert "performer_username" in splunk_records[0]
        assert "random_id" in splunk_records[0]
        assert "kind_id" in splunk_records[0]

    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    def test_handler_processes_multiple_records(
        self,
        mock_get_secret,
        mock_build_client,
        mock_session_class,
        mock_es_handler,
        mock_splunk_handler,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test",
            "master_user_password": "test",
            "splunk_hec_token": "test-token",
            "splunk_hec_url": "http://test.com",
            "splunk_index": "test-index",
        }
        mock_build_client.return_value = MagicMock()
        mock_session_class.return_value = MagicMock()

        records = [
            {"datetime": "2026-02-18T10:30:00", "random_id": "1", "kind_id": 1, "request_url": "/a"},
            {"datetime": "2026-02-18T10:31:00", "random_id": "2", "kind_id": 2, "http_method": "GET"},
            {"datetime": "2026-02-18T10:32:00", "random_id": "3", "kind_id": 3, "user_agent": "curl"},
        ]
        event = {"Records": [create_kinesis_record(r) for r in records]}
        context = MagicMock()

        handler(event, context)

        es_records = mock_es_handler.call_args[0][0]
        splunk_records = mock_splunk_handler.call_args[0][0]

        assert len(es_records) == 3
        assert len(splunk_records) == 3

        # elasticsearch_handler now receives unfiltered records (same as splunk_handler)
        # Verify both handlers receive the full records
        assert "request_url" in es_records[0]
        assert "http_method" in es_records[1]
        assert "user_agent" in es_records[2]

        assert "request_url" in splunk_records[0]
        assert "http_method" in splunk_records[1]
        assert "user_agent" in splunk_records[2]

    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    def test_es_and_splunk_both_called(
        self,
        mock_get_secret,
        mock_build_client,
        mock_session_class,
        mock_es_handler,
        mock_splunk_handler,
        sample_es_record,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test",
            "master_user_password": "test",
            "splunk_hec_token": "test-token",
            "splunk_hec_url": "http://test.com",
            "splunk_index": "test-index",
        }
        mock_build_client.return_value = MagicMock()
        mock_session_class.return_value = MagicMock()

        event = {"Records": [create_kinesis_record(sample_es_record)]}
        context = MagicMock()

        handler(event, context)

        mock_es_handler.assert_called_once()
        mock_splunk_handler.assert_called_once()
