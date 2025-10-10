from unittest import mock

from dbt.adapters.base import BaseAdapter
from contextlib import contextmanager
from dbt.adapters.deltastream.relation import DeltastreamRelation
from dbt.adapters.deltastream.column import DeltastreamColumn


def adapter_factory():
    class MockAdapter(BaseAdapter):
        ConnectionManager = mock.MagicMock(TYPE="mock")
        Relation = DeltastreamRelation
        Column = DeltastreamColumn
        responder = mock.MagicMock()
        # some convenient defaults
        responder.quote.side_effect = lambda identifier: '"{}"'.format(identifier)
        responder.date_function.side_effect = lambda: "unitdate()"
        responder.is_cancelable.side_effect = lambda: False

        @contextmanager
        def exception_handler(self, *args, **kwargs):
            self.responder.exception_handler(*args, **kwargs)
            yield

        def execute(self, *args, **kwargs):
            return self.responder.execute(*args, **kwargs)

        def drop_relation(self, *args, **kwargs):
            return self.responder.drop_relation(*args, **kwargs)

        def truncate_relation(self, *args, **kwargs):
            return self.responder.truncate_relation(*args, **kwargs)

        def rename_relation(self, *args, **kwargs):
            return self.responder.rename_relation(*args, **kwargs)

        def get_columns_in_relation(self, *args, **kwargs):
            return self.responder.get_columns_in_relation(*args, **kwargs)

        def expand_column_types(self, *args, **kwargs):
            return self.responder.expand_column_types(*args, **kwargs)

        def expand_target_column_types(self, *args, **kwargs):
            return self.responder.expand_target_column_types(*args, **kwargs)

        def list_relations_without_caching(self, *args, **kwargs):
            return self.responder.list_relations_without_caching(*args, **kwargs)

        def create_schema(self, *args, **kwargs):
            return self.responder.create_schema(*args, **kwargs)

        def drop_schema(self, *args, **kwargs):
            return self.responder.drop_schema(*args, **kwargs)

        def get_relation(self, *args, **kwargs):
            return self.responder.get_relation(*args, **kwargs)

        def get_fully_qualified_relation_str(self, *args, **kwargs):
            return self.responder.get_fully_qualified_relation_str(*args, **kwargs)

        def standardize_grants_dict(self, *args, **kwargs):
            return self.responder.standardize_grants_dict(*args, **kwargs)

        @classmethod
        def quote(cls, identifier):
            return cls.responder.quote(identifier)

        def convert_text_type(self, *args, **kwargs):
            return self.responder.convert_text_type(*args, **kwargs)

        def convert_number_type(self, *args, **kwargs):
            return self.responder.convert_number_type(*args, **kwargs)

        def convert_integer_type(self, *args, **kwargs):
            return self.responder.convert_integer_type(*args, **kwargs)

        def convert_boolean_type(self, *args, **kwargs):
            return self.responder.convert_boolean_type(*args, **kwargs)

        def convert_datetime_type(self, *args, **kwargs):
            return self.responder.convert_datetime_type(*args, **kwargs)

        def convert_date_type(self, *args, **kwargs):
            return self.responder.convert_date_type(*args, **kwargs)

        def convert_time_type(self, *args, **kwargs):
            return self.responder.convert_time_type(*args, **kwargs)

        def list_schemas(self, *args, **kwargs):
            return self.responder.list_schemas(*args, **kwargs)

        @classmethod
        def date_function(cls):
            return cls.responder.date_function()

        @classmethod
        def is_cancelable(cls):
            return cls.responder.is_cancelable()

        @staticmethod
        def _strip_quotes(identifier: str):
            return identifier.strip('"')

    return MockAdapter
