from dataclasses import dataclass
from typing import Optional

from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger

from deltastream.api.conn import APIConnection
from deltastream.api.error import AuthenticationError


logger = AdapterLogger("Deltastream")


@dataclass
class DeltastreamCredentials(Credentials):
    # Connection parameters
    timezone: str = "UTC"
    session_id: Optional[str] = None
    url: str = "https://api.deltastream.io/v2"

    organization_id: str = ""
    role: Optional[str] = None
    store: Optional[str] = None
    compute_pool: Optional[str] = None
    database: str = ""
    schema: str = ""

    # Authentication
    token: str = ""  # Required

    @property
    def type(self):
        return "deltastream"

    @property
    def unique_field(self):
        return self.database

    def _connection_keys(self):
        return (
            "compute_pool",
            "database",
            "organization_id",
            "role",
            "schema",
            "session_id",
            "store",
            "timezone",
            "token",
            "url",
        )

    def __post_init__(self):
        if not self.token:
            raise DbtRuntimeError("Must specify authentication token")
        if not self.database or self.database == "":
            raise DbtRuntimeError("Must specify database")
        if not self.schema or self.schema == "":
            raise DbtRuntimeError("Must specify schema")
        if self.organization_id == "":
            raise DbtRuntimeError("Must specify organization ID")


def create_deltastream_client(credentials: DeltastreamCredentials) -> APIConnection:
    try:

        async def token_provider() -> str:
            return credentials.token

        return APIConnection(
            server_url=credentials.url,
            token_provider=token_provider,
            session_id=credentials.session_id,
            timezone=credentials.timezone,
            organization_id=credentials.organization_id,
            role_name=credentials.role,
            database_name=credentials.database,
            schema_name=credentials.schema,
            store_name=credentials.store,
            compute_pool_name=credentials.compute_pool,
        )
    except AuthenticationError:
        logger.info("Unable to connect to Deltastream, authentication failed")
        raise
