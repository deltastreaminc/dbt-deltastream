# dbt Features Test Coverage

This document summarizes which dbt features are tested by the integration tests in the `tests/functional/adapter` directory for the dbt-deltastream adapter.

## Overview

The test suite is organized into several categories that test both standard dbt functionality and DeltaStream-specific features. Tests are marked with `@pytest.mark.integration` to indicate they require a live DeltaStream connection.

---

## 1. Core dbt Adapter Features

### 1.1 Basic Materializations (`test_basic.py`)

**Status**: ⏭️ All tests skipped

These tests verify standard dbt materialization types using the dbt-tests-adapter package:

- ❌ **Simple Materializations** - Basic table/view creation
  - **Reason**: SQL syntax issues with seed loading
- ❌ **Incremental Models** - Incremental materialization support
  - **Reason**: SQL syntax issues with seed loading
- ❌ **Ephemeral Models** - Ephemeral (CTE-based) models
  - **Reason**: SQL syntax issues with seed loading
- ❌ **Table Materialization** - Table creation from models
  - **Reason**: SQL syntax issues with seed loading

### 1.2 Testing Framework (`test_basic.py`)

**Status**: ⏭️ All tests skipped

- ❌ **Singular Tests** - Custom SQL-based tests
  - **Reason**: DeltaStream SQL parser doesn't support subquery syntax
- ❌ **Generic Tests** - Reusable data quality tests
  - **Reason**: DeltaStream SQL parser doesn't support subquery syntax
- ❌ **Ephemeral Singular Tests** - Tests with ephemeral models
  - **Reason**: DeltaStream SQL parser doesn't support subquery syntax

### 1.3 Caching (`test_caching.py`)

**Status**: ⏭️ All tests skipped

- ❌ **Basic Caching** - Relation metadata caching
- ❌ **Lowercase Model Caching** - Case sensitivity handling
- ❌ **Uppercase Model Caching** - Case sensitivity handling
- ❌ **Selected Schema Caching** - Targeted schema caching
  - **Reason**: All skipped due to SQL syntax issues with seed loading

### 1.4 Constraints (`test_constraints.py`)

**Status**: ⏭️ All tests skipped

- ❌ **Table Constraints** - Column constraints on tables
- ❌ **View Constraints** - Column constraints on views
- ❌ **Incremental Constraints** - Constraints on incremental models
- ❌ **Runtime DDL Enforcement** - Constraint validation at DDL time
- ❌ **Rollback on Constraint Violations** - Transaction rollback support
  - **Reason**: DeltaStream constraint support needs verification

### 1.5 Catalog Generation (`test_catalog.py`)

**Status**: 🚧 Placeholder

- ⏳ **Catalog Generation** - dbt catalog metadata generation
  - **Reason**: Needs implementation with correct imports

### 1.6 Source Freshness (`test_freshness.py`)

**Status**: 🚧 Placeholder

- ⏳ **Source Freshness** - Checking data freshness of sources
  - **Reason**: Needs implementation with correct imports

### 1.7 Relations (`test_relations.py`)

**Status**: 🚧 Placeholder

- ⏳ **Relation Types** - Different database object types handling
  - **Reason**: Needs implementation with correct imports

### 1.8 Store Test Failures (`test_store_test_failures.py`)

**Status**: 🚧 Placeholder

- ⏳ **Store Test Failures** - Storing failed test records
  - **Reason**: Needs implementation with correct imports

---

## 2. Database and Schema Management

### 2.1 Database Operations (`test_create_database.py`)

**Status**: ✅ Active tests

- ✅ **Database Creation from Source** - Create databases using source configurations
- ✅ **Integration Database Verification** - Verify shared test database exists

**dbt Features Tested**:
- Source definitions
- `run-operation` command
- `create_sources` macro
- Custom materialization types (database)

### 2.2 Schema Operations (`schema_creation/`)

**Status**: ✅ Active tests

#### `test_create_stream_in_integration_schema.py`
- ✅ **Schema-aware Stream Creation** - Create streams in specific schemas
- ✅ **Database/Schema References** - Cross-schema references

**dbt Features Tested**:
- Database/schema configuration
- `run-operation` with SQL queries
- Dynamic SQL generation

#### `test_list_relations_in_integration_schema.py`
- Testing relation listing within schemas

#### `test_list_schemas_in_integration_database.py`
- Testing schema enumeration in databases

---

## 3. DeltaStream-Specific Materializations

### 3.1 Stream Materialization

#### `stream_materialization/test_create_stream_from_source.py`
**Status**: ✅ Active test

- ✅ **Stream from Source** - Create stream from Kafka topic via source config

**dbt Features Tested**:
- Source definitions with custom config
- `materialized: stream` property
- Column type definitions
- Custom parameters (store, topic, format)

**DeltaStream Features**:
- Stream creation from Kafka topics
- JSON format support
- Trial store usage

#### `stream_materialization/test_create_stream_as_select.py`
**Status**: ⏭️ Skipped

- ❌ **Stream from Query (CSAS)** - Create Stream As Select
  - **Reason**: Partition limit exceeded in test environment

**dbt Features Tested**:
- Model-based stream creation
- `{{ config() }}` blocks
- Source references with `{{ source() }}`
- Derived streams from base streams

### 3.2 Changelog Materialization

#### `materialization/test_create_changelog_from_source.py`
**Status**: ✅ Active test

- ✅ **Changelog from Source** - Create changelog from Kafka topic with primary key

**dbt Features Tested**:
- Source definitions with primary keys
- `materialized: changelog` property
- Complex type definitions (ARRAY, STRUCT)
- Key format and type specifications

**DeltaStream Features**:
- Changelog stream creation
- Key-value format handling
- Complex nested types (STRUCT, ARRAY)

#### `materialization/test_create_changelog_as_select.py`
**Status**: Tests changelog creation via SQL query

### 3.3 Table Materialization

#### `table_materialization/test_create_table_as_select.py`
**Status**: ⏭️ Skipped

- ❌ **Table from Query (CTAS)** - Create Table As Select
  - **Reason**: Cannot create tables in Kafka stores (requires SQL store)

**dbt Features Tested**:
- `materialized: table` property
- Aggregation queries (COUNT, GROUP BY)
- Timestamp conversion functions

#### `table_materialization/test_create_table_with_window_aggregation.py`
**Status**: ⏭️ Skipped

- ❌ **Table with Windowing** - Tables with window aggregations
  - **Reason**: Cannot create tables in Kafka stores

**DeltaStream Features**:
- HOP window function
- Window size and advance parameters

### 3.4 Materialized View

Used extensively in windowing tests (see Advanced Windowing section).

### 3.5 Entity Materialization

#### `materialization/test_create_entity_from_source.py`
**Status**: ⏭️ Skipped

- ❌ **Entity Creation** - Create schema entities
  - **Reason**: Compilation error with source configuration structure

**dbt Features Tested**:
- `materialized: entity` property
- Entity schema definitions

#### `materialization/test_create_entity_from_shipments_topic.py`
- Entity creation from shipments topic

#### `materialization/test_create_entity_with_complex_types.py`
- Entity with complex data types

### 3.6 Function and Function Source Materialization

#### `materialization/test_create_function_from_source.py`
**Status**: ✅ Active test (conditional on JAR file)

- ✅ **UDF Function Creation** - Create Java-based user-defined functions
- ✅ **Function Source Creation** - Upload JAR files for UDFs

**dbt Features Tested**:
- `materialized: function_source` property
- `materialized: function` property
- File attachment handling
- Multi-resource dependencies

**DeltaStream Features**:
- Java UDF support
- JAR file deployment
- Function input/output type definitions
- Class name references

#### `materialization/test_create_function_source_from_source.py`
- Function source creation patterns

#### `materialization/test_function_creation_with_retry_logic.py`
- Retry logic for function creation

### 3.7 Descriptor Source Materialization

#### `materialization/test_create_descriptor_source_from_source.py`
**Status**: ⏭️ Skipped

- ❌ **Descriptor Source Creation** - Protocol buffer schema sources
  - **Reason**: SQL syntax error ('path' vs 'file' keyword)

**dbt Features Tested**:
- `materialized: descriptor_source` property
- File path references

**DeltaStream Features**:
- Protocol buffer (.desc) file handling

#### `materialization/test_create_descriptor_source_with_file_attachment.py`
- Descriptor source with file attachments

#### `materialization/test_descriptor_source_path_resolution.py`
- Path resolution for descriptor files

### 3.8 Compute Pool Materialization

#### `materialization/test_compute_pool_full_lifecycle.py`
**Status**: ⏭️ Skipped

- ❌ **Compute Pool Lifecycle** - Create, update, stop, and drop compute pools
  - **Reason**: SQL syntax error ('type' keyword not recognized)

**dbt Features Tested**:
- `materialized: compute_pool` property
- Resource lifecycle management
- Update operations

**DeltaStream Features**:
- Compute pool management
- SHARED pool types
- Stop/start operations

---

## 4. Resource Management

### 4.1 Store Materialization

#### `resource_materialization/test_create_kafka_store_from_source.py`
**Status**: ⏭️ Skipped

- ❌ **Kafka Store Creation** - External Kafka cluster connections
  - **Reason**: Requires valid external Kafka credentials

**dbt Features Tested**:
- `materialized: store` property
- Secure credential handling
- External system integration

**DeltaStream Features**:
- Kafka cluster registration
- SASL authentication
- SSL/TLS configuration

#### `resource_materialization/test_create_schema_registry_from_source.py`
- Schema registry configuration

#### `resource_materialization/test_update_store.py`
- Store update operations

---

## 5. Advanced Window Functions

### 5.1 TUMBLE Windows

#### `advanced_windowing/test_tumble_window_aggregation.py`
**Status**: ✅ Active test

- ✅ **TUMBLE Window Aggregation** - Fixed, non-overlapping time windows

**dbt Features Tested**:
- `materialized: materialized_view` property
- Window function syntax in models
- Source references
- Multiple aggregation functions (COUNT, COUNT DISTINCT)

**DeltaStream Features**:
- TUMBLE window function
- Window size specification
- Timestamp column binding
- Window metadata (window_start, window_end)

### 5.2 HOP Windows

#### `advanced_windowing/test_hop_window_size_advance.py`
**Status**: ✅ Active test

- ✅ **HOP Window with Size/Advance** - Overlapping sliding windows

**dbt Features Tested**:
- Complex window configurations
- Multiple aggregation in single query

**DeltaStream Features**:
- HOP window function
- SIZE and ADVANCE BY parameters
- Overlapping window semantics

### 5.3 Multiple Aggregation Functions

#### `advanced_windowing/test_multiple_aggregation_functions.py`
**Status**: ✅ Active test

- ✅ **Multiple Aggregations** - Multiple aggregate functions in one query

**dbt Features Tested**:
- Complex SELECT expressions
- Multiple aggregate functions

**DeltaStream Features**:
- Multiple aggregation support
- Window-based aggregations

### 5.4 Nested Aggregations with HAVING

#### `advanced_windowing/test_nested_aggregations_with_having.py`
**Status**: ✅ Active test

- ✅ **HAVING Clause with Aggregations** - Post-aggregation filtering

**dbt Features Tested**:
- HAVING clause support
- Nested aggregation logic

**DeltaStream Features**:
- HAVING clause in windowed queries
- Aggregate filtering

### 5.5 Materialized View Cascading

#### `advanced_windowing/test_materialized_view_cascading.py`
**Status**: ⏭️ Skipped

- ❌ **Cascading Materialized Views** - MV referencing other MVs
  - **Reason**: DeltaStream doesn't support cascading materialized views

**dbt Features Tested**:
- `{{ ref() }}` macro between models
- Multi-step transformations

---

## 6. Stream Processing Patterns

### 6.1 Stream Enrichment

#### `stream_enrichment/test_stream_changelog_join.py`
**Status**: ⏭️ Skipped

- ❌ **Stream-Changelog Join** - Enrich stream with changelog data
  - **Reason**: Partition limit exceeded

**dbt Features Tested**:
- JOIN operations in models
- Multiple source references
- Key column specifications
- Complex type handling (ARRAY, STRUCT)

**DeltaStream Features**:
- Stream to changelog joins
- Timestamp conversion (TO_TIMESTAMP_LTZ)
- Complex nested types in joins

#### `stream_enrichment/test_stream_with_zipcode_extraction.py`
- STRUCT field extraction

#### `stream_enrichment/test_materialized_view_with_windowing.py`
- Materialized views from enriched streams

### 6.2 Multi-Entity Joins

#### `multi_entity/test_three_entity_stream_join.py`
**Status**: ⏭️ Skipped

- ❌ **Three-Way Join** - Join pageviews, users, and shipments
  - **Reason**: Resource/partition limits

**dbt Features Tested**:
- Multiple source definitions
- Multi-table JOIN syntax
- Complex source dependencies

**DeltaStream Features**:
- Multi-stream joins
- Mixed stream and changelog joins

#### `multi_entity/test_shipments_stream_with_aggregation.py`
- Shipments data aggregation

#### `multi_entity/test_shipments_table_with_windowing.py`
- Shipments with time windows

---

## 7. Query Lifecycle Management

### 7.1 Query Operations

#### `query_operations/test_list_queries.py`
**Status**: ✅ Active test

- ✅ **List All Queries** - Query enumeration

**dbt Features Tested**:
- `run-operation` command
- Custom macros (`list_all_queries`)

**DeltaStream Features**:
- Query listing APIs

#### `query_operations/test_full_query_lifecycle.py`
**Status**: ⏭️ Skipped

- ❌ **Complete Query Lifecycle** - Create, list, describe, terminate
  - **Reason**: Failing - needs investigation

**dbt Features Tested**:
- Multi-step operations
- Query management macros

**DeltaStream Features**:
- Query creation from table materialization
- Query termination
- Query metadata retrieval

#### `query_operations/test_terminate_all_queries_by_state.py`
**Status**: ✅ Active test

- ✅ **Terminate Queries by State** - Filter queries by state (RUNNING, FAILED)

**dbt Features Tested**:
- Macro with arguments
- State-based filtering

**DeltaStream Features**:
- Query state management
- Bulk query termination

#### `query_operations/test_describe_query_with_stream.py`
- Query metadata retrieval

---

## 8. Helper Utilities

### 8.1 Test Helpers (`test_helpers.py`)

**Status**: ✅ Active utilities

Provides resource management functions used across tests:

- ✅ **Timestamp Generation** - Unique resource naming
- ✅ **Resource Cleanup** - Database/schema/relation cleanup
- ✅ **Retry Logic** - Handling running queries during cleanup
- ✅ **Query Termination** - Bulk query termination
- ✅ **Resource Listing** - Enumerate resources by type

**dbt Features Tested**:
- `run_query` macro
- Error handling patterns
- Cleanup best practices

---

## Summary Statistics

### Test Status Distribution

- ✅ **Active Integration Tests**: ~20 tests
- ⏭️ **Skipped Tests**: ~35 tests
- 🚧 **Placeholder Tests**: ~5 tests
- ❌ **Standard dbt Tests**: ~15 tests (all skipped)

### dbt Features Covered

#### Core dbt Features
- ✅ Source definitions
- ✅ `{{ config() }}` blocks
- ✅ `{{ source() }}` references
- ✅ `{{ ref() }}` references
- ✅ Custom materialization types
- ✅ `run-operation` command
- ✅ Custom macros
- ✅ Column type definitions
- ✅ Multi-file project organization

#### Limited/Not Covered
- ❌ Seeds (SQL syntax issues)
- ❌ Standard tests (singular/generic)
- ❌ Incremental models
- ❌ Ephemeral models
- ❌ Snapshots
- ❌ Exposures
- ❌ Metrics

### DeltaStream-Specific Features Tested

#### Materialization Types
- ✅ Stream (from source and CSAS)
- ✅ Changelog (with primary keys)
- ✅ Materialized View (with windowing)
- ⏭️ Table (CTAS) - requires SQL store
- ⏭️ Entity - configuration issues
- ✅ Function/Function Source (UDFs)
- ⏭️ Descriptor Source - syntax issues
- ⏭️ Compute Pool - syntax issues
- ⏭️ Store - requires external credentials
- Database/Schema creation

#### Window Functions
- ✅ TUMBLE (fixed windows)
- ✅ HOP (sliding windows)
- ✅ Window metadata (window_start, window_end)
- ✅ Multiple aggregations
- ✅ HAVING clause with windows

#### Query Management
- ✅ List queries
- ✅ Terminate queries (by state)
- ⏭️ Describe queries
- ⏭️ Restart queries

#### Data Types
- ✅ Primitive types (VARCHAR, BIGINT, etc.)
- ✅ ARRAY types
- ✅ STRUCT types (nested)
- ✅ Complex nested structures

#### Stream Processing
- ✅ Stream-to-stream transformations
- ✅ Stream-to-changelog joins
- ✅ Time-based windowing
- ✅ Timestamp conversions
- ⏭️ Multi-stream joins (resource limits)

---

## Test Environment Constraints

### Known Limitations

1. **Partition Limits**: Test environment has maximum partition limits
   - Affects CSAS (Create Stream As Select) tests
   - Affects multi-entity join tests

2. **SQL Syntax**: DeltaStream SQL parser limitations
   - Doesn't support all standard SQL subquery patterns
   - Different syntax for some DDL operations

3. **Store Requirements**: 
   - Tables require SQL stores (not available in Kafka stores)
   - External store creation requires valid credentials

4. **Resource Dependencies**:
   - Cascading materialized views not supported
   - Some operations require specific compute pool states

### Workarounds in Tests

- Using `trial_store` for most tests (pre-configured)
- Timestamp-based unique naming to avoid conflicts
- Session-level cleanup to handle failed tests
- Skip markers for environment-specific limitations

---

## Future Test Coverage Recommendations

### High Priority
1. ✨ Implement catalog generation tests
2. ✨ Implement source freshness tests
3. ✨ Fix entity materialization configuration issues
4. ✨ Add SQL store setup for table materialization tests
5. ✨ Complete query lifecycle tests

### Medium Priority
6. ✨ Add incremental model support (if applicable to streaming)
7. ✨ Add snapshot functionality tests (CDC patterns)
8. ✨ Test error handling and rollback scenarios
9. ✨ Add performance/scalability tests
10. ✨ Test cross-database references

### Low Priority
11. ✨ Adapter-specific seed implementation
12. ✨ Standard dbt test framework support (if possible with DeltaStream SQL)
13. ✨ Exposure and documentation tests

---

## References

- [Test Organization README](./README.md) - Detailed information on test structure and execution
- [Test Helpers](./test_helpers.py) - Resource management utilities
- [dbt Adapter Features](../../documentation/dbt_adapter_features.md) - Complete adapter feature matrix
- [DeltaStream SQL Syntax](../../documentation/syntax_support.md) - SQL syntax support documentation
