# Syntax Support

Supported syntax elements from Deltastream SQL are listed in the table below.

| Statement                         | Supported | Notes                                                 |
| --------------------------------- | --------- | ----------------------------------------------------- |
| **ACCEPT INVITATION**             | ❌         | Not implemented                                       |
| **CAN I**                         | ❌         | Not implemented                                       |
| **COPY DESCRIPTOR_SOURCE**        | ❌         | Not implemented                                       |
| **COPY FUNCTION_SOURCE**          | ❌         | Not implemented                                       |
| **DESCRIBE ENTITY**               | ❌         | Not implemented                                       |
| **DESCRIBE QUERY**                | ✅         | Available via `describe_query` macro                  |
| **DESCRIBE QUERY METRICS**        | ❌         | Not implemented                                       |
| **DESCRIBE QUERY EVENTS**         | ❌         | Not implemented                                       |
| **DESCRIBE QUERY STATE**          | ❌         | Not implemented                                       |
| **DESCRIBE RELATION**             | ❌         | Not implemented                                       |
| **DESCRIBE RELATION COLUMNS**     | ✅         | Used internally for column metadata                   |
| **DESCRIBE ROLE**                 | ❌         | Not implemented                                       |
| **DESCRIBE SECURITY INTEGRATION** | ❌         | Not implemented                                       |
| **DESCRIBE <statement>**          | ❌         | Not implemented                                       |
| **DESCRIBE STORE**                | ❌         | Not implemented                                       |
| **DESCRIBE USER**                 | ❌         | Not implemented                                       |
| **GENERATE COLUMNS**              | ❌         | Not implemented                                       |
| **GENERATE TEMPLATE**             | ❌         | Not implemented                                       |
| **GRANT OWNERSHIP**               | ❌         | Not implemented                                       |
| **GRANT PRIVILEGES**              | ❌         | Not implemented                                       |
| **GRANT ROLE**                    | ❌         | Not implemented                                       |
| **INVITE USER**                   | ❌         | Not implemented                                       |
| **LIST API_TOKENS**               | ❌         | Not implemented                                       |
| **LIST COMPUTE_POOLS**            | ❌         | Not implemented                                       |
| **LIST DATABASES**                | ❌         | Not implemented                                       |
| **LIST DESCRIPTORS**              | ❌         | Not implemented                                       |
| **LIST DESCRIPTOR_SOURCES**       | ❌         | Not implemented                                       |
| **LIST ENTITIES**                 | ❌         | Not implemented                                       |
| **LIST FUNCTIONS**                | ❌         | Not implemented                                       |
| **LIST FUNCTION_SOURCES**         | ❌         | Not implemented                                       |
| **LIST INVITATIONS**              | ❌         | Not implemented                                       |
| **LIST METRICS INTEGRATIONS**     | ❌         | Not implemented                                       |
| **LIST ORGANIZATIONS**            | ❌         | Not implemented                                       |
| **LIST QUERIES**                  | ✅         | Available via `list_all_queries` macro                |
| **LIST RELATIONS**                | ✅         | Used internally                                        |
| **LIST ROLES**                    | ❌         | Not implemented                                       |
| **LIST SCHEMAS**                  | ✅         | Used internally                                        |
| **LIST SCHEMA_REGISTRIES**        | ❌         | Not implemented                                       |
| **LIST SECRETS**                  | ❌         | Not implemented                                       |
| **LIST SECURITY INTEGRATIONS**    | ❌         | Not implemented                                       |
| **LIST STORES**                   | ❌         | Not implemented                                       |
| **LIST USERS**                    | ❌         | Not implemented                                       |
| **PRINT ENTITY**                  | ❌         | Not implemented                                       |
| **REJECT INVITATION**             | ❌         | Not implemented                                       |
| **REVOKE INVITATION**             | ❌         | Not implemented                                       |
| **REVOKE PRIVILEGES**             | ❌         | Not implemented                                       |
| **REVOKE ROLE**                   | ❌         | Not implemented                                       |
| **SET DEFAULT**                   | ❌         | Not implemented                                       |
| **USE**                           | ❌         | Not implemented                                       |
| **START COMPUTE_POOL**            | ❌         | Not implemented                                       |
| **STOP COMPUTE_POOL**             | ❌         | Not implemented                                       |
| **ALTER API_TOKEN**               | ❌         | Not implemented                                       |
| **ALTER SECURITY INTEGRATION**    | ❌         | Not implemented                                       |
| **CREATE API_TOKEN**              | ❌         | Not implemented                                       |
| **CREATE CHANGELOG**              | ✅         | Supported via `changelog` materialization             |
| **CREATE COMPUTE_POOL**           | ✅         | Supported via `compute_pool` materialization          |
| **CREATE DATABASE**               | ✅         | Supported via `database` materialization              |
| **CREATE DESCRIPTOR_SOURCE**      | ✅         | Supported via `descriptor_source` materialization     |
| **CREATE ENTITY**                 | ✅         | Supported via `entity` materialization                |
| **CREATE FUNCTION_SOURCE**        | ✅         | Supported via `function_source` materialization       |
| **CREATE FUNCTION**               | ✅         | Supported via `function` materialization              |
| **CREATE INDEX**                  | ❌         | Not implemented                                       |
| **CREATE METRICS INTEGRATION**    | ❌         | Not implemented                                       |
| **CREATE ORGANIZATION**           | ❌         | Not implemented                                       |
| **CREATE ROLE**                   | ❌         | Not implemented                                       |
| **CREATE SCHEMA_REGISTRY**        | ✅         | Supported via `schema_registry` materialization       |
| **CREATE SCHEMA**                 | ✅         | Supported for database schema creation                |
| **CREATE SECRET**                 | ❌         | Not implemented                                       |
| **CREATE SECURITY INTEGRATION**   | ❌         | Not implemented                                       |
| **CREATE STORE**                  | ✅         | Supported via `store` materialization                 |
| **CREATE STREAM**                 | ✅         | Supported via `stream` materialization                |
| **CREATE TABLE**                  | ✅         | Supported via `table` materialization                 |
| **DROP API_TOKEN**                | ❌         | Not implemented                                       |
| **DROP CHANGELOG**                | ✅         | Supported                                             |
| **DROP COMPUTE_POOL**             | ❌         | Not explicitly implemented                            |
| **DROP DATABASE**                 | ❌         | Not explicitly implemented                            |
| **DROP DESCRIPTOR_SOURCE**        | ✅         | Supported                                             |
| **DROP ENTITY**                   | ❌         | Not explicitly implemented                            |
| **DROP FUNCTION_SOURCE**          | ✅         | Supported                                             |
| **DROP FUNCTION**                 | ✅         | Supported                                             |
| **DROP METRICS INTEGRATION**      | ❌         | Not implemented                                       |
| **DROP RELATION**                 | ✅         | Supported for relation re-creation                             |
| **DROP ROLE**                     | ❌         | Not implemented                                       |
| **DROP SCHEMA**                   | ✅         | Supported                                             |
| **DROP SCHEMA_REGISTRY**          | ✅         | Supported                                             |
| **DROP SECRET**                   | ❌         | Not implemented                                       |
| **DROP SECURITY INTEGRATION**     | ❌         | Not implemented                                       |
| **DROP STORE**                    | ✅         | Supported                                             |
| **DROP STREAM**                   | ✅         | Supported                                             |
| **DROP USER**                     | ❌         | Not implemented                                       |
| **UPDATE COMPUTE_POOL**           | ✅         | Supported                                             |
| **UPDATE ENTITY**                 | ✅         | Supported                                             |
| **UPDATE SCHEMA_REGISTRY**        | ✅         | Supported                                             |
| **UPDATE SECRET**                 | ❌         | Not implemented                                       |
| **UPDATE STORE**                  | ✅         | Supported                                             |
| **APPLICATION**                   | ✅         | Supported via `application` macro                     |
| **CREATE CHANGELOG AS SELECT**    | ✅         | Supported                                             |
| **CREATE STREAM AS SELECT**       | ✅         | Supported                                             |
| **CREATE TABLE AS SELECT**        | ✅         | Supported                                             |
| **INSERT INTO**                   | ❌         | Not implemented                                       |
| **Resume Query**                  | ❌         | Not implemented                                       |
| **RESTART QUERY**                 | ✅         | Available via `restart_query` macro                   |
| **SELECT**                        | ✅         | Supported in model SQL                                |
| **TERMINATE QUERY**               | ✅         | Available via `terminate_query` macro                 |
| **START SANDBOX**                 | ❌         | Not implemented                                       |
| **DESCRIBE SANDBOX**              | ❌         | Not implemented                                       |
| **STOP SANDBOX**                  | ❌         | Not implemented                                       |
