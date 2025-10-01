# dbt-deltastream

A [dbt](https://www.getdbt.com/) adapter for [DeltaStream](https://deltastream.io) - a streaming processing engine based on Apache Flink.

## Features

- Seamless integration with DeltaStream's streaming capabilities
- Support for DeltaStream core concepts through dbt materialization types:
  - `table`: Traditional batch table materialization
  - `materialized_view`: Continuously updated view
  - `stream`: Pure streaming transformation
  - `changelog`: Change data capture (CDC) stream
  - `seed`: Load CSV data into existing entities
  - `store`: External system connection (Kafka, PostgreSQL, etc.)
  - `entity`: Entity definition in a store
  - `database`: Database definition
  - `function`: User-defined functions (UDFs)
  - `function_source`: Function source for Java JAR files
  - `descriptor_source`: Descriptor source for protocol buffer schemas
  - `schema_registry`: Schema registry connection for Confluent Schema Registry or similar

## Installation

```bash
pip install dbt-deltastream
```

Requirements:

- Python >= 3.11
- dbt-core >= 1.8.0

## Configuration

Add to your `profiles.yml`:

```yaml
your_profile_name:
  target: dev
  outputs:
    dev:
      type: deltastream

      # Required Parameters
      token: your-api-token # Authentication token
      database: your-database # Target database name
      schema: your-schema # Target schema name
      organization_id: your-org-id # Organization identifier

      # Optional Parameters
      url: https://api.deltastream.io/v2 # DeltaStream API URL
      timezone: UTC # Timezone for operations
      session_id: your-session-id # Custom session identifier for debugging purpose
      role: your-role # User role
      store: your-store # Target store name
      compute_pool: your-compute-pool # Compute pool name
```

The following parameters are supported in the profile configuration:

### Required Parameters

- `token`: Authentication token for DeltaStream API
- `database`: Target default database name
- `schema`: Target default schema name
- `organization_id`: Organization identifier

### Optional Parameters

- `url`: DeltaStream API URL (default: <https://api.deltastream.io/v2>)
- `timezone`: Timezone for operations (default: UTC)
- `session_id`: Custom session identifier for debugging
- `compute_pool`: Compute pool name to be used if any else use the default compute pool (for models that require one)

- `role`: User role
- `store`: target default store name

### Best practices

When configuring your project for production, it is recommended to use environment variables to store sensitive information such as the `token`:

```yaml
your_profile_name:
  target: prod
  outputs:
    prod:
      type: deltastream
      token: "{{ env_var('DELTASTREAM_API_TOKEN') }}"
      ...
```

## Materializations

DeltaStream supports two types of model definitions:

1. YAML-only resources for defining infrastructure components
2. SQL models for data transformations

### YAML-Only Resources

These models don't contain SQL SELECT statements but define infrastructure components using YAML configuration.
YAML-only resources can be used to define external system connections such as streams, changelogs, and stores.
They can be either: managed or unmanaged by dbt DAG.

#### Managed

When a YAML-only resource is managed by dbt DAG, it is automatically included in the DAG by creating them as _models_, for instance:

```yaml
version: 2
models:
  - name: my_kafka_stream
    config:
      materialized: stream
      parameters:
        topic: "user_events"
        value.format: "json"
        store: "my_kafka_store"
```

In that case, we're running into a dbt limitation where we need to create a placeholder .sql file for the model to be detected. That .sql file would contain any content as long as it doesn't contain a "SELECT". We expect that limitation to be lifted in future dbt versions but it's still part of discussions.

Then it can be referenced in downstream model using the regular `ref` function:

```sql
SELECT * FROM {{ ref('my_kafka_stream') }}
```

#### Unmanaged

When a YAML-only resource is not managed by dbt DAG, it has to be created as _sources_, for instance:

```yaml
version: 2
sources:
  - name: kafka
    schema: public
    tables:
      - name: pageviews
        description: "Pageviews stream"
        config:
          materialized: stream
          parameters:
            topic: pageviews
            store: "my_kafka_store"
            "value.format": JSON
        columns:
          - name: viewtime
            type: BIGINT
          - name: userid
            type: VARCHAR
          - name: pageid
            type: VARCHAR
```

Then it requires to execute specific macros to create the resources on demand.
To create all sources, run:

```bash
dbt run-operation create_sources
```

To create a specific source, run:

```bash
dbt run-operation create_source_by_name --args '{source_name: user_events}'
```

Then it can be referenced in downstream model using the regular `source` function:

```sql
SELECT * FROM {{ source('kafka', 'pageviews') }}
```

### YAML-Only Resources Examples

Following example can be created both as managed (models) or as unmanaged (sources).

#### Managed example

```yaml
version: 2
models:
  - name: my_kafka_store
    config:
      materialized: store
      parameters:
        type: KAFKA # required
        access_region: "AWS us-east-1"
        uris: "kafka.broker1.url:9092,kafka.broker2.url:9092"
        tls.ca_cert_file: "@/certs/us-east-1/self-signed-kafka-ca.crt"
  - name: ps_store
    config:
      materialized: store
      parameters:
        type: POSTGRESQL # required
        access_region: "AWS us-east-1"
        uris: "postgresql://mystore.com:5432/demo"
        postgres.username: "user"
        postgres.password: "password"
  - name: user_events_stream
    config:
      materialized: stream
      columns:
        event_time:
          type: TIMESTAMP
          not_null: true
        user_id:
          type: VARCHAR
        action:
          type: VARCHAR
      parameters:
        topic: 'user_events'
        value.format: 'json'
        key.format: 'primitive'
        key.type: 'VARCHAR'
        timestamp: 'event_time'
  - name: order_changes
    config:
      materialized: changelog
      columns:
        order_id:
          type: VARCHAR
          not_null: true
        status:
          type: VARCHAR
        updated_at:
          type: TIMESTAMP
      primary_key:
        - order_id
      parameters:
        topic: 'order_updates'
        value.format: 'json'
  - name: pv_kinesis
    config:
      materialized: entity
      store: kinesis_store
      parameters:
        'kinesis.shards' = 3
  - name: my_compute_pool
    config:
      materialized: compute_pool
      parameters:
        'compute_pool.size' = 'small',
        'compute_pool.timeout_min' = 5
  - name: my_function_source
    config:
      materialized: function_source
      parameters:
        file: '@/path/to/my-functions.jar'
        description: 'Custom utility functions'
  - name: my_descriptor_source
    config:
      materialized: descriptor_source
      parameters:
        file: '@/path/to/schemas.desc'
        description: 'Protocol buffer schemas for data structures'
  - name: my_custom_function
    config:
      materialized: function
      parameters:
        args:
          - name: input_text
            type: VARCHAR
        returns: VARCHAR
        language: JAVA
        source.name: 'my_function_source'
        class.name: 'com.example.TextProcessor'
  - name: my_schema_registry
    config:
      materialized: schema_registry
      parameters:
        type: "CONFLUENT",
        access_region: "AWS us-east-1",
        uris: "https://url.to.schema.registry.listener:8081",
        'confluent.username': 'fake_username',
        'confluent.password': 'fake_password',
        'tls.client.cert_file': '@/path/to/tls/client_cert_file',
        'tls.client.key_file': '@/path/to/tls_key'
```

#### Unmanaged example

```yaml
version: 2
sources:
- name: example # source name, not used in DeltaStream but required by dbt for the {{ source("example", "...") }}
  tables:
  - name: my_kafka_store
    config:
      materialized: store
      parameters:
        type: KAFKA # required
        access_region: "AWS us-east-1"
        uris: "kafka.broker1.url:9092,kafka.broker2.url:9092"
        tls.ca_cert_file: "@/certs/us-east-1/self-signed-kafka-ca.crt"
  - name: ps_store
    config:
      materialized: store
      parameters:
        type: POSTGRESQL # required
        access_region: "AWS us-east-1"
        uris: "postgresql://mystore.com:5432/demo"
        postgres.username: "user"
        postgres.password: "password"
  - name: user_events_stream
    config:
      materialized: stream
      columns:
        event_time:
          type: TIMESTAMP
          not_null: true
        user_id:
          type: VARCHAR
        action:
          type: VARCHAR
      parameters:
        topic: 'user_events'
        value.format: 'json'
        key.format: 'primitive'
        key.type: 'VARCHAR'
        timestamp: 'event_time'
  - name: order_changes
    config:
      materialized: changelog
      columns:
        order_id:
          type: VARCHAR
          not_null: true
        status:
          type: VARCHAR
        updated_at:
          type: TIMESTAMP
      primary_key:
        - order_id
      parameters:
        topic: 'order_updates'
        value.format: 'json'
  - name: pv_kinesis
    config:
      materialized: entity
      store: kinesis_store
      parameters:
        'kinesis.shards': 3
  - name: my_compute_pool
    config:
      materialized: compute_pool
      parameters:
        'compute_pool.size': 'small'
        'compute_pool.timeout_min': 5
  - name: my_function_source
    config:
      materialized: function_source
      parameters:
        file: '@/path/to/my-functions.jar'
        description: 'Custom utility functions'
  - name: my_descriptor_source
    config:
      materialized: descriptor_source
      parameters:
        file: '@/path/to/schemas.desc'
        description: 'Protocol buffer schemas for data structures'
  - name: my_custom_function
    config:
      materialized: function
      parameters:
        args:
          - name: input_text
            type: VARCHAR
        returns: VARCHAR
        language: JAVA
        source.name: 'my_function_source'
        class.name: 'com.example.TextProcessor'
  - name: my_schema_registry
    config:
      materialized: schema_registry
      parameters:
        type: "CONFLUENT",
        access_region: "AWS us-east-1",
        uris: "https://url.to.schema.registry.listener:8081",
        'confluent.username': 'fake_username',
        'confluent.password': 'fake_password',
        'tls.client.cert_file': '@/path/to/tls/client_cert_file',
        'tls.client.key_file': '@/path/to/tls_key'
```

### SQL Models

These models contain SQL SELECT statements for data transformations.

#### Stream (SQL)

Creates a continuous streaming transformation:

```sql
{{ config(
    materialized='stream',
    parameters={
        'topic': 'purchase_events',
        'value.format': 'json'
    }
) }}

SELECT
    event_time,
    user_id,
    action
FROM {{ ref('source_stream') }}
WHERE action = 'purchase'
```

#### Changelog (SQL)

Captures changes in the data stream:

```sql
{{ config(
    materialized='changelog',
    parameters={
        'topic': 'order_updates',
        'value.format': 'json'
    }
) }}

SELECT
    order_id,
    status,
    updated_at
FROM {{ ref('orders_stream') }}
```

#### Table

Creates a traditional batch table:

```sql
{{ config(materialized='table') }}

SELECT
    date,
    SUM(amount) as daily_total
FROM {{ ref('transactions') }}
GROUP BY date
```

#### Materialized View

Creates a continuously updated view:

```sql
{{ config(materialized='materialized_view') }}

SELECT
    product_id,
    COUNT(*) as purchase_count
FROM {{ ref('purchase_events') }}
GROUP BY product_id
```

## Seeds

Load CSV data into existing DeltaStream entities using the `seed` materialization. Unlike traditional dbt seeds that create new tables, DeltaStream seeds insert data into pre-existing entities.

### Key Features

- **Target Existing Entities**: Seeds insert data into existing entities rather than creating new ones
- **Flexible Store Support**: Can target entities with or without store specifications
- **Batch Processing**: Efficiently processes CSV data in configurable batch sizes
- **WITH Parameters**: Support for entity-specific parameters via WITH clauses

### Configuration

Seeds must be configured in YAML with the following properties:

**Required:**

- `entity`: The name of the target entity to insert data into

**Optional:**

- `store`: The name of the store containing the entity (omit if entity is not in a store)
- `with_params`: A dictionary of parameters for the WITH clause
- `quote_columns`: Control which columns get quoted. Default: `false` (no columns quoted). Can be:
  - `true`: Quote all columns
  - `false`: Quote no columns (default)
  - `string`: If set to `'*'`, quote all columns
  - `list`: List of column names to quote

### YAML Configuration Examples

**With Store (quoting enabled):**

```yaml
# seeds.yml
version: 2

seeds:
  - name: user_data_with_store_quoted
    config:
      entity: 'user_events'
      store: 'kafka_store'
      with_params:
        kafka.topic.retention.ms: '86400000'
        partitioned: true
      quote_columns: true  # Quote all columns
```

### Usage

1. Place CSV files in your `seeds/` directory
2. Configure seeds in YAML with the required `entity` parameter
3. Optionally specify `store` if the entity is in a store
4. Run `dbt seed` to load the data

**Important**: The target entity must already exist in DeltaStream before running seeds. Seeds only insert data, they do not create entities.

## Function and Source Materializations

DeltaStream supports user-defined functions (UDFs) and their dependencies through specialized materializations.

### Automatic Retry for Function Creation

When creating functions that depend on function sources, the adapter automatically handles timing issues with an intelligent retry mechanism:

- **Automatic Detection**: Function creation statements are automatically detected and retry logic is applied
- **SQLState-Based Retry**: Uses proper SQLState codes (3D018) instead of text matching for reliable error detection
- **Exponential Backoff**: Starts with 2-second intervals, increasing by 1.5x each retry (capped at 10 seconds)
- **Configurable Timeout**: Default 30-second timeout with clear error messages
- **Transparent Operation**: No changes needed to existing code - retry logic is applied automatically

### File Attachment Support

The adapter provides seamless file attachment for function sources and descriptor sources:

- **Standardized Interface**: Common file handling logic for both function sources and descriptor sources
- **Path Resolution**: Supports both absolute paths and relative paths (including `@` syntax for project-relative paths)
- **Automatic Validation**: Files are validated for existence and accessibility before attachment
- **Thread-Safe Storage**: Uses connection thread-local storage for pending file management

### Function Source

Creates a function source from a JAR file containing Java functions:

```sql
{{ config(
    materialized='function_source',
    parameters={
        'file': '@/path/to/my-functions.jar',
        'description': 'Custom utility functions'
    }
) }}

SELECT 1 as placeholder
```

### Descriptor Source

Creates a descriptor source from compiled protocol buffer descriptor files:

```sql
{{ config(
    materialized='descriptor_source',
    parameters={
        'file': '@/path/to/schemas.desc',
        'description': 'Protocol buffer schemas for data structures'
    }
) }}

SELECT 1 as placeholder
```

**Note**: Descriptor sources require compiled `.desc` files, not raw `.proto` files. Compile your protobuf schemas using:

```bash
protoc --descriptor_set_out=schemas/my_schemas.desc schemas/my_schemas.proto
```

### Function

Creates a user-defined function that references a function source:

```sql
{{ config(
    materialized='function',
    parameters={
        'args': [
            {'name': 'input_text', 'type': 'VARCHAR'}
        ],
        'returns': 'VARCHAR',
        'language': 'JAVA',
        'source.name': 'my_function_source',
        'class.name': 'com.example.TextProcessor'
    }
) }}

SELECT 1 as placeholder
```

**Note**: Functions, function sources, and descriptor sources are resources, not relations. They are managed independently and can be referenced by name in your streaming queries.

## Troubleshooting

### Function Source Readiness

If you encounter "function source is not ready" errors when creating functions:

1. **Automatic Retry**: The adapter automatically retries function creation with exponential backoff
2. **Timeout Configuration**: The default 30-second timeout can be extended if needed for large JAR files
3. **Dependency Order**: Ensure function sources are created before dependent functions
4. **Manual Retry**: If automatic retry fails, wait a few minutes and retry the operation

### File Attachment Issues

For problems with file attachments in function sources and descriptor sources:

1. **File Paths**: Use `@/path/to/file` syntax for project-relative paths
2. **File Types**:

- Function sources require `.jar` files
- Descriptor sources require compiled `.desc` files (not `.proto`)

3. **File Validation**: The adapter validates file existence before attempting attachment
4. **Compilation**: For descriptor sources, ensure protobuf files are compiled:

   ```bash
   protoc --descriptor_set_out=output.desc input.proto
   ```

## Query Macros

DeltaStream dbt adapter provides macros to help you manage and terminate running queries directly from dbt.

### Terminate a Specific Query

Use the `terminate_query` macro to terminate a query by its ID:

```bash
dbt run-operation terminate_query --args '{query_id: "<QUERY_ID>"}'
```

### Terminate All Running Queries

Use the `terminate_all_queries` macro to terminate all currently running queries:

```bash
dbt run-operation terminate_all_queries
```

These macros leverage DeltaStream's `LIST QUERIES;` and `TERMINATE QUERY <query_id>;` SQL commands to identify and terminate running queries. This is useful for cleaning up long-running or stuck jobs during development or operations.
Using this specific macro is not recommended in production environments as it will stop all queries including those that weren't created by the current user or in dbt.

### List All Queries

The `list_all_queries` macro displays all queries currently known to DeltaStream, including their state, owner, and SQL. It prints a formatted table to the dbt logs for easy inspection.

**Usage:**

```bash
dbt run-operation list_all_queries
```

**Example Output:**

```
ID | Name | Version | IntendedState | ActualState | Query | Owner | CreatedAt | UpdatedAt
-----------------------------------------------------------------------------------------
<query row 1>
<query row 2>
...
```

This macro is useful for debugging, monitoring, and operational tasks. It leverages DeltaStream's `LIST QUERIES;` SQL command and prints the results in a readable table format.

### Restart a Specific Query

Use the `restart_query` macro to restart a failed query by its ID:

```bash
dbt run-operation restart_query --args '{query_id: "<QUERY_ID>"}'
```

Before restarting a query, you can use the `describe_query` macro to check the logs and determine if it's worthwhile restarting:

```bash
dbt run-operation describe_query --args '{query_id: "<QUERY_ID>"}'
```

This will display the query's current state and any error information to help you understand why the query failed.

## Application Macro

### Execute Multiple Statements as a Unit

The `application` macro allows you to execute multiple DeltaStream SQL statements as a single unit of work with all-or-nothing semantics. This leverages DeltaStream's APPLICATION syntax for better efficiency and resource utilization.

**Usage:**

```bash
dbt run-operation application --args '{
  application_name: "my_data_pipeline",
  statements: [
    "USE DATABASE my_db",
    "CREATE STREAM user_events WITH (topic='"'"'events'"'"', value.format='"'"'json'"'"')",
    "CREATE MATERIALIZED VIEW user_counts AS SELECT user_id, COUNT(*) FROM user_events GROUP BY user_id"
  ]
}'
```

## Contributing

We welcome contributions! Please feel free to submit a Pull Request.

## License

[Apache License 2.0](LICENSE)
