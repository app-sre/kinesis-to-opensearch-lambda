import json
import base64
import pytest
from unittest.mock import patch, MagicMock

from lambda_function import (
    _strip_extended_fields,
    _process_kinesis_record,
    handler,
    EXTENDED_FIELDS,
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
    """Reset global client variables between tests."""
    import lambda_function
    lambda_function.opensearch_client = None
    lambda_function.splunk_session = None
    yield
    lambda_function.opensearch_client = None
    lambda_function.splunk_session = None


@pytest.fixture
def sample_record_with_extended_fields():
    """Sample log record containing extended fields."""
    return {
        "datetime": "2026-02-18T10:30:00",
        "random_id": "abc123",
        "message": "User logged in",
        "level": "INFO",
        "request_url": "/api/login",
        "http_method": "POST",
        "performer_username": "john_doe",
        "performer_email": "john@example.com",
        "performer_kind": "user",
        "auth_type": "oauth",
        "user_agent": "Mozilla/5.0",
        "request_id": "req-456",
        "x_forwarded_for": "192.168.1.1",
    }


@pytest.fixture
def sample_record_minimal():
    """Sample log record without extended fields."""
    return {
        "datetime": "2026-02-18T10:30:00",
        "random_id": "abc123",
        "message": "User logged in",
        "level": "INFO",
    }


def create_kinesis_record(data: dict) -> dict:
    """Helper to create a Kinesis record with base64-encoded data."""
    encoded = base64.b64encode(json.dumps(data).encode()).decode()
    return {"kinesis": {"data": encoded}}


class TestStripExtendedFields:
    """Tests for _strip_extended_fields function."""

    def test_strips_all_extended_fields(self, sample_record_with_extended_fields):
        result = _strip_extended_fields(sample_record_with_extended_fields)

        for field in EXTENDED_FIELDS:
            assert field not in result

    def test_preserves_non_extended_fields(self, sample_record_with_extended_fields):
        result = _strip_extended_fields(sample_record_with_extended_fields)

        assert result["datetime"] == "2026-02-18T10:30:00"
        assert result["random_id"] == "abc123"
        assert result["message"] == "User logged in"
        assert result["level"] == "INFO"

    def test_returns_same_record_when_no_extended_fields(self, sample_record_minimal):
        result = _strip_extended_fields(sample_record_minimal)

        assert result == sample_record_minimal

    def test_returns_empty_dict_for_empty_input(self):
        result = _strip_extended_fields({})

        assert result == {}

    def test_handles_partial_extended_fields(self):
        record = {
            "datetime": "2026-02-18T10:30:00",
            "random_id": "abc123",
            "request_url": "/api/test",
            "user_agent": "curl/7.0",
        }

        result = _strip_extended_fields(record)

        assert "datetime" in result
        assert "random_id" in result
        assert "request_url" not in result
        assert "user_agent" not in result


class TestProcessKinesisRecord:
    """Tests for _process_kinesis_record function."""

    def test_decodes_base64_kinesis_data(self, sample_record_minimal):
        kinesis_record = create_kinesis_record(sample_record_minimal)
        result = _process_kinesis_record(kinesis_record)

        assert result["message"] == "User logged in"
        assert result["random_id"] == "abc123"

    def test_adds_timestamp_field(self, sample_record_minimal):
        kinesis_record = create_kinesis_record(sample_record_minimal)
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
    """Tests for handler function - verifies ES gets stripped records, Splunk gets full records."""

    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    def test_es_receives_stripped_records(
        self,
        mock_es_handler,
        mock_splunk_handler,
        mock_get_secret,
        mock_build_client,
        mock_session,
        sample_record_with_extended_fields,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test_user",
            "master_user_password": "test_pass",
            "splunk_hec_url": "https://splunk.test",
            "splunk_hec_token": "test-token",
            "splunk_index": "test-index",
            "splunk_disabled": False,
        }
        mock_build_client.return_value = MagicMock()
        mock_session.return_value = MagicMock()

        event = {"Records": [create_kinesis_record(sample_record_with_extended_fields)]}
        context = MagicMock()

        handler(event, context)

        es_records = mock_es_handler.call_args[0][0]
        assert len(es_records) == 1
        for field in EXTENDED_FIELDS:
            assert field not in es_records[0]

    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    def test_splunk_receives_full_records(
        self,
        mock_es_handler,
        mock_splunk_handler,
        mock_get_secret,
        mock_build_client,
        mock_session,
        sample_record_with_extended_fields,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test_user",
            "master_user_password": "test_pass",
            "splunk_hec_url": "https://splunk.test",
            "splunk_hec_token": "test-token",
            "splunk_index": "test-index",
            "splunk_disabled": False,
        }
        mock_build_client.return_value = MagicMock()
        mock_session.return_value = MagicMock()

        event = {"Records": [create_kinesis_record(sample_record_with_extended_fields)]}
        context = MagicMock()

        handler(event, context)

        splunk_records = mock_splunk_handler.call_args[0][0]
        assert len(splunk_records) == 1
        assert "request_url" in splunk_records[0]
        assert "http_method" in splunk_records[0]
        assert "performer_username" in splunk_records[0]

    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    def test_handler_processes_multiple_records(
        self,
        mock_es_handler,
        mock_splunk_handler,
        mock_get_secret,
        mock_build_client,
        mock_session,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test_user",
            "master_user_password": "test_pass",
            "splunk_hec_url": "https://splunk.test",
            "splunk_hec_token": "test-token",
            "splunk_index": "test-index",
            "splunk_disabled": False,
        }
        mock_build_client.return_value = MagicMock()
        mock_session.return_value = MagicMock()

        records = [
            {"datetime": "2026-02-18T10:30:00", "random_id": "1", "request_url": "/a"},
            {"datetime": "2026-02-18T10:31:00", "random_id": "2", "http_method": "GET"},
            {"datetime": "2026-02-18T10:32:00", "random_id": "3", "user_agent": "curl"},
        ]
        event = {"Records": [create_kinesis_record(r) for r in records]}
        context = MagicMock()

        handler(event, context)

        es_records = mock_es_handler.call_args[0][0]
        splunk_records = mock_splunk_handler.call_args[0][0]

        assert len(es_records) == 3
        assert len(splunk_records) == 3

        for es_record in es_records:
            for field in EXTENDED_FIELDS:
                assert field not in es_record

        assert "request_url" in splunk_records[0]
        assert "http_method" in splunk_records[1]
        assert "user_agent" in splunk_records[2]

    @patch("lambda_function.requests.Session")
    @patch("lambda_function._build_opensearch_client")
    @patch("lambda_function.get_secret")
    @patch("lambda_function.splunk_handler")
    @patch("lambda_function.elasticsearch_handler")
    def test_es_and_splunk_both_called(
        self,
        mock_es_handler,
        mock_splunk_handler,
        mock_get_secret,
        mock_build_client,
        mock_session,
        sample_record_minimal,
    ):
        mock_get_secret.return_value = {
            "master_user_name": "test_user",
            "master_user_password": "test_pass",
            "splunk_hec_url": "https://splunk.test",
            "splunk_hec_token": "test-token",
            "splunk_index": "test-index",
            "splunk_disabled": False,
        }
        mock_build_client.return_value = MagicMock()
        mock_session.return_value = MagicMock()

        event = {"Records": [create_kinesis_record(sample_record_minimal)]}
        context = MagicMock()

        handler(event, context)

        mock_es_handler.assert_called_once()
        mock_splunk_handler.assert_called_once()
