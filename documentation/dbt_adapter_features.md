# dbt Adapter Features Support

This document provides a comprehensive overview of dbt features and their support status in the dbt-deltastream adapter.

## Core dbt Features

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **Authentication via API Token**            | ✅ Fully Supported              | Via `token` and `organization_id` in profiles.yml                                                           |
| **dbt init support**                        | ✅ Fully Supported              | Creates standard dbt project structure                                                                      |
| **dbt debug support**                       | ✅ Fully Supported              | Connection testing and configuration validation                                                             |
| **dbt docs generate**                       | ✅ Fully Supported              | Documentation generation via `get_catalog` and `get_catalog_relations_parallel`                             |
| **dbt docs serve**                          | ✅ Fully Supported              | Standard dbt documentation server                                                                           |
| **dbt compile**                             | ✅ Fully Supported              | Standard dbt SQL compilation                                                                                |
| **dbt run**                                 | ✅ Fully Supported              | Executes models with supported materializations                                                             |
| **dbt test**                                | ⚠️ Partial Support              | Data quality tests work but limited by DeltaStream SQL syntax (no subqueries in some contexts)              |
| **dbt seed**                                | ✅ Fully Supported              | Loads CSV data into pre-existing entities (requires `entity` config)                                        |
| **dbt snapshot**                            | ❌ Not Supported                | Not implemented - snapshots don't align with streaming paradigm                                             |
| **dbt source**                              | ✅ Fully Supported              | Define sources in YAML; create with `create_sources` macro                                                  |
| **dbt freshness**                           | ❌ Not Supported                | Explicitly raises error - not applicable to streaming context                                               |
| **dbt run-operation**                       | ✅ Fully Supported              | Supports custom macros and operations                                                                       |
| **dbt clean**                               | ✅ Fully Supported              | Standard dbt cleanup operations                                                                             |
| **dbt deps**                                | ✅ Fully Supported              | Standard dbt package management                                                                             |
| **dbt ls/list**                             | ✅ Fully Supported              | List resources in project                                                                                   |
| **dbt parse**                               | ✅ Fully Supported              | Parse project and generate manifest                                                                         |

## Materializations

| Materialization                             | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **table**                                   | ✅ Fully Supported              | Standard dbt materialization - creates tables via CREATE TABLE AS SELECT                                    |
| **view**                                    | ✅ Fully Supported              | Standard dbt materialization - creates views                                                                |
| **materialized_view**                       | ✅ Fully Supported              | DeltaStream-specific - continuously updated aggregations                                                    |
| **incremental**                             | ❌ Not Supported                | Explicitly raises error - use `materialized_view` instead (native incremental in DeltaStream)               |
| **ephemeral**                               | ✅ Fully Supported              | Standard dbt CTE-based models                                                                               |
| **seed**                                    | ✅ Fully Supported              | Custom implementation - inserts into existing entities (not creating new tables)                            |

## Custom DeltaStream Materializations

| Materialization                             | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **stream**                                  | ✅ Fully Supported              | Pure streaming transformation - can be YAML-only or SQL model                                               |
| **changelog**                               | ✅ Fully Supported              | Change data capture stream with primary keys - can be YAML-only or SQL model                                |
| **store**                                   | ✅ Fully Supported              | External system connections (Kafka, Kinesis, PostgreSQL, etc.) - YAML-only resource                        |
| **entity**                                  | ✅ Fully Supported              | Entity definitions within stores - YAML-only resource                                                       |
| **compute_pool**                            | ✅ Fully Supported              | Dedicated compute resource pools - YAML-only resource                                                       |
| **database**                                | ✅ Fully Supported              | Database resource definitions - YAML-only resource                                                          |
| **function**                                | ✅ Fully Supported              | User-defined functions (UDFs) in Java - YAML-only resource with automatic retry logic                      |
| **function_source**                         | ✅ Fully Supported              | JAR file sources for UDFs - YAML-only resource with file attachment support                                 |
| **descriptor_source**                       | ✅ Fully Supported              | Protocol buffer schema sources - YAML-only resource with file attachment support                            |
| **schema_registry**                         | ✅ Fully Supported              | Schema registry connections (Confluent, etc.) - YAML-only resource                                          |

## Advanced Features

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **Macros**                                  | ✅ Fully Supported              | Full Jinja2 support with custom DeltaStream macros                                                          |
| **Hooks (pre/post)**                        | ✅ Fully Supported              | Standard dbt pre-hook and post-hook support                                                                 |
| **Packages**                                | ✅ Fully Supported              | Compatible with dbt packages ecosystem                                                                      |
| **Vars**                                    | ✅ Fully Supported              | Standard dbt variable support                                                                               |
| **Profiles**                                | ✅ Fully Supported              | Standard dbt profile configuration                                                                          |
| **Exposures**                               | ✅ Fully Supported              | Standard dbt exposures                                                                                      |
| **Metrics**                                 | ✅ Fully Supported              | Standard dbt metrics (if using dbt ≥1.6)                                                                    |
| **Analyses**                                | ✅ Fully Supported              | Standard dbt analyses                                                                                       |

## Testing Features

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **Generic tests**                           | ⚠️ Partial Support              | Limited by DeltaStream SQL parser (no subqueries in some contexts)                                          |
| **Singular tests**                          | ⚠️ Partial Support              | Limited by DeltaStream SQL parser (no subqueries in some contexts)                                          |
| **Unit tests**                              | ❌ Not Supported                | Not supported due to DeltaStream engine limitations                                                         |
| **Data tests**                              | ⚠️ Partial Support              | Work but syntax limitations apply                                                                           |
| **Schema tests**                            | ⚠️ Partial Support              | Limited by SQL syntax constraints                                                                           |
| **Custom generic tests**                    | ⚠️ Partial Support              | Can be defined but subject to SQL syntax limitations                                                        |

## Database Operations

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **CREATE DATABASE**                         | ✅ Fully Supported              | Via `database` materialization                                                                              |
| **CREATE SCHEMA**                           | ✅ Fully Supported              | Python implementation in `impl.py`                                                                          |
| **DROP SCHEMA**                             | ✅ Fully Supported              | Python implementation in `impl.py`                                                                          |
| **DROP RELATION**                           | ✅ Fully Supported              | Supports all relation types (tables, views, streams, changelogs)                                            |
| **TRUNCATE RELATION**                       | ✅ Fully Supported              | Python implementation in `impl.py`                                                                          |
| **RENAME RELATION**                         | ❌ Not Supported                | Explicitly raises error - not supported by DeltaStream                                                      |
| **LIST SCHEMAS**                            | ✅ Fully Supported              | Via `SHOW SCHEMAS IN DATABASE` command                                                                      |
| **LIST RELATIONS**                          | ✅ Fully Supported              | Via `SHOW RELATIONS IN SCHEMA` command                                                                      |

## Query Management

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **List queries**                            | ✅ Fully Supported              | Via `list_all_queries` macro using `LIST QUERIES` SQL command                                               |
| **Describe query**                          | ✅ Fully Supported              | Via `describe_query` macro using `DESCRIBE QUERY` SQL command                                               |
| **Terminate query**                         | ✅ Fully Supported              | Via `terminate_query` and `terminate_all_queries` macros                                                    |
| **Restart query**                           | ✅ Fully Supported              | Via `restart_query` macro using `RESTART QUERY` SQL command                                                 |
| **Cancel query**                            | ❌ Not Supported                | `is_cancelable()` returns False - not implemented                                                           |

## Metadata and Catalog

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **get_catalog**                             | ✅ Fully Supported              | Via `deltastream.sys.relations` system view                                                                 |
| **get_catalog_relations**                   | ✅ Fully Supported              | Parallel implementation via `DESCRIBE RELATION COLUMNS` for better performance                              |
| **Column metadata**                         | ✅ Fully Supported              | Via `DESCRIBE RELATION COLUMNS` command                                                                     |
| **Relation metadata**                       | ✅ Fully Supported              | Via `DESCRIBE RELATION` command                                                                             |
| **Schema metadata**                         | ✅ Fully Supported              | Via schema and database listing commands                                                                    |

## Data Constraints

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **CHECK constraints**                       | ❌ Not Supported                | `ConstraintSupport.NOT_SUPPORTED` in adapter                                                                |
| **NOT NULL constraints**                    | ❌ Not Supported                | `ConstraintSupport.NOT_SUPPORTED` in adapter                                                                |
| **UNIQUE constraints**                      | ❌ Not Supported                | `ConstraintSupport.NOT_SUPPORTED` in adapter                                                                |
| **PRIMARY KEY constraints**                 | ❌ Not Supported                | `ConstraintSupport.NOT_SUPPORTED` (but changelog materialization supports PK semantics)                     |
| **FOREIGN KEY constraints**                 | ❌ Not Supported                | `ConstraintSupport.NOT_SUPPORTED` in adapter                                                                |

## Access Control

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **GRANT statements**                        | ❌ Not Supported                | Not implemented                                       |
| **REVOKE statements**                       | ❌ Not Supported                | Not implemented                                       |
| **apply_grants**                            | ❌ Not Supported                | Not implemented                                       |
| **Persist docs to database**                | ❌ Not Supported                | No documentation implementation                                                                      |

## Special Features

| Feature                                     | Status                         | Implementation Details                                                                                      |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **File attachment (JAR)**                   | ✅ Fully Supported              | For `function_source` materialization with path resolution                                                  |
| **File attachment (Protocol Buffers)**      | ✅ Fully Supported              | For `descriptor_source` materialization with compiled `.desc` files                                         |
| **Automatic retry logic**                   | ✅ Fully Supported              | For function creation when source not ready (exponential backoff)                                           |
| **Multi-statement applications**            | ✅ Fully Supported              | Via `application` macro for atomic execution                                                                |
| **Parallel catalog queries**                | ✅ Fully Supported              | Via `get_catalog_relations_parallel` using thread pool                                                      |

## Notes

### Syntax Limitations

- DeltaStream SQL parser has limitations with subqueries in certain contexts
- Some generic tests may fail due to these SQL syntax constraints
- Singular tests with complex subqueries may not work

### Streaming-Specific Behaviors

- Incremental models are not supported because DeltaStream streams/changelogs/materialized views are natively incremental
- Snapshots don't align with the streaming paradigm where data is continuously processed
- Source freshness checks are not applicable in streaming context

### Seeds Behavior

- Unlike standard dbt, seeds in dbt-deltastream insert data into **pre-existing** entities
- Seeds do not create new tables - the target entity must exist before seeding
- Requires `entity` configuration in seed YAML (and optionally `store`)

### YAML-Only Resources

- Many DeltaStream-specific resources (stores, entities, compute pools, etc.) are YAML-only configurations
- These can be managed (in DAG) or unmanaged (as sources requiring specific macros to create)
- See README for detailed documentation on managed vs unmanaged resources
