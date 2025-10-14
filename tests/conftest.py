import os
from collections.abc import Mapping

import pytest

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


_ENV_NAMES: Mapping[str, str] = {
    "token": "DELTASTREAM_API_TOKEN",
    "organization_id": "DELTASTREAM_ORGANIZATION_ID",
    "database": "DELTASTREAM_DATABASE",
    "schema": "DELTASTREAM_SCHEMA",
}


def _required_env() -> dict[str, str]:
    return {key: os.getenv(env_name, "") for key, env_name in _ENV_NAMES.items()}


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

    missing_envs = [
        env_name for key, env_name in _ENV_NAMES.items() if not required[key]
    ]
    if missing_envs:
        formatted = ", ".join(missing_envs)
        pytest.skip(
            "Integration tests require configured DeltaStream secrets. Missing: "
            f"{formatted}. Configure these as environment variables or GitHub Action secrets."
        )

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
