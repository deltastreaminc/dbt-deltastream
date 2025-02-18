from dbt.adapters.deltastream.impl import DeltastreamAdapter
from dbt.adapters.deltastream.credentials import DeltastreamCredentials

from dbt.adapters.base import AdapterPlugin
from dbt.include import deltastream

Plugin = AdapterPlugin(
    adapter=DeltastreamAdapter,  # type: ignore
    credentials=DeltastreamCredentials,
    include_path=deltastream.PACKAGE_PATH,
)
