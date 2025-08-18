import os
import pytest


def _required_env() -> dict:
    return {
        "token": os.environ.get("DELTASTREAM_API_TOKEN", ""),
        "organization_id": os.environ.get("DELTASTREAM_ORGANIZATION_ID", ""),
        "database": os.environ.get("DELTASTREAM_DATABASE", ""),
        "schema": os.environ.get("DELTASTREAM_SCHEMA", ""),
    }


@pytest.fixture(scope="session")
def dbt_profile_target() -> str:
    return "dev"


@pytest.fixture(scope="session")
def dbt_profile_data(dbt_profile_target):
    # Check if integration tests are explicitly requested
    if not os.getenv("RUN_INTEGRATION_TESTS") == "1":
        pytest.skip(
            "Integration tests not requested: set RUN_INTEGRATION_TESTS=1 to enable"
        )

    # Get required environment variables
    required = _required_env()

    # Use default values if environment variables are not set
    # This allows tests to run but may fail if actual connection is needed
    if not all(required.values()):
        print(
            "Warning: Some DELTASTREAM_* environment variables are missing. Tests may fail."
        )
        for key, value in required.items():
            if not value:
                print(f"Missing: DELTASTREAM_{key.upper()}")
                # Set default values to prevent immediate failure
                required[key] = f"test_{key}"

    url = os.environ.get("DELTASTREAM_URL", "https://api.deltastream.io/v2")

    output = {
        "type": "deltastream",
        "threads": 1,
        "url": url,
        "token": required["token"],
        "organization_id": required["organization_id"],
        "database": required["database"],
        "schema": required["schema"],
    }
    return {
        "test": {
            "outputs": {dbt_profile_target: output},
            "target": dbt_profile_target,
        }
    }
