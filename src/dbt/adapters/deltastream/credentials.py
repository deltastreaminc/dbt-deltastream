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
            credentials.url,
            token_provider,
            credentials.session_id,
            credentials.timezone,
            credentials.organization_id,
            credentials.role,
            credentials.database,
            credentials.schema,
            credentials.store,
        )
    except AuthenticationError:
        logger.info("Unable to connect to Deltastream, authentication failed")
        raise
