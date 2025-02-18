import pytest
from dbt.adapters.deltastream.credentials import (
    DeltastreamCredentials,
    create_deltastream_client,
)
from dbt_common.exceptions import DbtRuntimeError


def valid_credentials_data():
    return {
        "timezone": "UTC",
        "url": "https://api.deltastream.io/v2",
        "organization_id": "org1",
        "role": "admin",
        "store": "store1",
        "database": "test_db",
        "schema": "test_schema",
        "token": "valid-token",
    }


def test_missing_token():
    data = valid_credentials_data()
    data["token"] = ""
    with pytest.raises(DbtRuntimeError, match="Must specify authentication token"):
        DeltastreamCredentials(**data)


def test_missing_database():
    data = valid_credentials_data()
    data["database"] = ""
    with pytest.raises(DbtRuntimeError, match="Must specify database"):
        DeltastreamCredentials(**data)


def test_missing_schema():
    data = valid_credentials_data()
    data["schema"] = ""
    with pytest.raises(DbtRuntimeError, match="Must specify schema"):
        DeltastreamCredentials(**data)


def test_valid_credentials_properties():
    data = valid_credentials_data()
    creds = DeltastreamCredentials(**data)
    assert creds.type == "deltastream"
    assert creds.unique_field == data["database"]


def test_create_deltastream_client(monkeypatch):
    data = valid_credentials_data()
    creds = DeltastreamCredentials(**data)

    # Create a dummy APIConnection to simulate client creation.
    class DummyAPIConnection:
        pass

    monkeypatch.setattr(
        "dbt.adapters.deltastream.credentials.APIConnection",
        lambda *args, **kwargs: DummyAPIConnection(),
    )
    client = create_deltastream_client(creds)
    assert isinstance(client, DummyAPIConnection)


def test_create_deltastream_client_auth_error(monkeypatch):
    data = valid_credentials_data()
    creds = DeltastreamCredentials(**data)
    # Simulate APIConnection raising AuthenticationError
    from deltastream.api.error import AuthenticationError

    def dummy_api_connection(*args, **kwargs):
        raise AuthenticationError("Test error")

    monkeypatch.setattr(
        "dbt.adapters.deltastream.credentials.APIConnection", dummy_api_connection
    )
    with pytest.raises(AuthenticationError, match="Test error"):
        create_deltastream_client(creds)
