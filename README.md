# dbt-deltastream

<p align="center">
  <a href="https://pypi.org/project/dbt-deltastream/"><img alt="PyPI version" src="https://img.shields.io/pypi/v/dbt-deltastream.svg"></a>
  <a href="https://pypi.org/project/dbt-deltastream/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/dbt-deltastream.svg"></a>
  <a href="https://github.com/deltastreaminc/dbt-deltastream/actions"><img alt="CI Status" src="https://github.com/deltastreaminc/dbt-deltastream/workflows/Testing%20Python%20package/badge.svg"></a>
  <a href="https://github.com/deltastreaminc/dbt-deltastream/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/deltastreaminc/dbt-deltastream.svg"></a>
  <a href="https://www.getdbt.com/"><img alt="dbt compatibility" src="https://img.shields.io/badge/dbt-≥1.10-orange.svg"></a>
  <a href="https://github.com/deltastreaminc/dbt-deltastream/blob/main/CONTRIBUTING.md"><img alt="PRs Welcome" src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg"></a>
</p>

**dbt-deltastream** is the official [dbt](https://www.getdbt.com/) adapter for [DeltaStream](https://deltastream.io), a unified streaming and batch processing engine built on Apache Flink. This adapter enables data teams to apply the power and simplicity of dbt's transformation workflows to real-time streaming data pipelines.

### Why dbt-deltastream?

- **🔄 Unified Analytics**: Combine batch and streaming transformations in a single dbt project
- **⚡ Real-time Insights**: Build continuously updating materialized views and streaming pipelines
- **🛠️ Developer Experience**: Leverage dbt's familiar SQL-first approach for stream processing
- **🔌 Ecosystem Integration**: Connect to Kafka, Kinesis, PostgreSQL, and other data systems
- **📊 Stream as Code**: Version control, test, and document your streaming transformations

DeltaStream bridges the gap between traditional data warehousing and real-time stream processing, and this adapter brings dbt's best practices to the streaming world.

## 🚀 Getting Started

### Prerequisites

Before installing dbt-deltastream, ensure you have:

- **Python 3.11 or higher** installed on your system
- **dbt-core 1.10 or higher** (will be installed automatically as a dependency)
- A **DeltaStream account** with:
  - Organization ID (found on settings page of your DeltaStream dashboard)
  - API Token (can be generated on the integration page in your DeltaStream dashboard)

> 📘 **New to DeltaStream?** Sign up at [deltastream.io](https://deltastream.io) to get started.

### Installation

Install dbt-deltastream using pip:

```bash
pip install dbt-deltastream
```

Or with [uv](https://github.com/astral-sh/uv) for faster installation:

```bash
uv init && uv add dbt-deltastream
```

### Quick Start

#### 1. Create a dbt Project

If you don't have a dbt project yet:

```bash
dbt init my_deltastream_project
```

When prompted for the database, select `deltastream`.

#### 2. Configure Your Profile

Edit your `~/.dbt/profiles.yml` (or create `profiles.yml` in your project directory):

```yaml
my_deltastream_project:
  target: dev
  outputs:
    dev:
      type: deltastream
      
      # Required Parameters
      token: "{{ env_var('DELTASTREAM_API_TOKEN') }}"  # Your API token
      organization_id: your-org-id                      # Your organization ID
      database: my_database                             # Target database name
      schema: my_schema                                 # Target schema name
      
      # Optional Parameters
      url: https://api.deltastream.io/v2                # API endpoint (default)
      timezone: UTC                                     # Timezone (default: UTC)
      role: AccountAdmin                                # User role
      compute_pool: default_pool                        # Compute pool name
```

> 🔐 **Security Tip**: Store sensitive credentials in environment variables rather than hardcoding them.

#### 3. Test Your Connection

```bash
dbt debug
```

You should see a successful connection to DeltaStream!

#### 4. Define Your Source Stream

Create `models/sources.yml` to define a source stream from the trial store:

```yaml
version: 2

sources:
  - name: kafka
    schema: public
    tables:
      - name: pageviews
        description: "Pageviews stream from trial store"
        config:
          materialized: stream
          parameters:
            store: trial_store
            topic: pageviews
            'value.format': JSON
        columns:
          - name: viewtime
            type: BIGINT
          - name: userid
            type: VARCHAR
          - name: pageid
            type: VARCHAR
```

#### 5. Create the Source Stream

Run the operation to create the source stream in DeltaStream:

```bash
dbt run-operation create_sources
```

This will create the `pageviews` stream in DeltaStream based on your YAML configuration.

#### 6. Create Your First Transformation Model

Create `models/user_pageviews.sql`:

```sql
{{ config(
    materialized='materialized_view'
) }}

SELECT
    userid,
    COUNT(*) as pageview_count,
    COUNT(DISTINCT pageid) as unique_pages_viewed
FROM {{ source('kafka', 'pageviews') }}
GROUP BY userid
```

#### 7. Run Your Project

```bash
dbt run
```

Congratulations! 🎉 You've created your first streaming transformation with dbt-deltastream. You've set up a source stream from Kafka and created a materialized view that continuously aggregates pageview data.

## ⚡ Features

### Supported dbt Capabilities

- ✅ **Materializations**: Table, View, Materialized View, Incremental, Stream, Changelog
- ✅ **Seeds**: Load CSV data into existing DeltaStream entities
- ✅ **Tests**: Data quality tests with dbt's testing framework
- ✅ **Documentation**: Auto-generate docs with `dbt docs generate`
- ✅ **Sources**: Define and test source data
- ✅ **Macros**: Full Jinja2 support for reusable SQL
- ⏳ **Snapshots**: Not yet supported (streaming context differs from batch)

### DeltaStream-Specific Materializations

This adapter extends dbt with streaming-first materializations:

- **`stream`**: Pure streaming transformation with continuous processing
- **`changelog`**: Change data capture (CDC) stream with primary keys
- **`materialized_view`**: Continuously updated aggregations
- **`table`**: Traditional batch table materialization
- **`store`**: External system connections (Kafka, Kinesis, PostgreSQL, etc.)
- **`entity`**: Entity definitions within stores
- **`database`**: Database resource definitions
- **`function`**: User-defined functions (UDFs) in Java
- **`function_source`**: JAR file sources for UDFs
- **`descriptor_source`**: Protocol buffer schema sources
- **`schema_registry`**: Schema registry connections (Confluent, etc.)
- **`compute_pool`**: Dedicated compute resource pools

### Advanced Features

- 🔄 **Automatic Retry Logic**: Smart retry for function creation with exponential backoff
- 📁 **File Attachment**: Seamless JAR and Protocol Buffer file handling
- 🎯 **Query Management**: Macros to list, terminate, and restart queries
- 🔗 **Multi-statement Applications**: Execute multiple statements as atomic units
- 🏗️ **Infrastructure as Code**: Define stores, databases, and compute pools in YAML

## 📋 Configuration

### Profile Parameters

Detailed configuration options for `profiles.yml`:

#### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `token` | Authentication token for DeltaStream API | `your-api-token` |
| `database` | Target default database name | `my_database` |
| `schema` | Target default schema name | `public` |
| `organization_id` | Organization identifier | `org-12345` |

#### Optional Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `url` | DeltaStream API URL | `https://api.deltastream.io/v2` |
| `timezone` | Timezone for operations | `UTC` |
| `session_id` | Custom session identifier for debugging | Auto-generated |
| `compute_pool` | Compute pool name for models requiring one | Default pool |
| `role` | User role | - |
| `store` | Target default store name | - |

### Best Practices

- **Use environment variables** for sensitive credentials:

  ```yaml
  your_profile_name:
    target: prod
    outputs:
      prod:
        type: deltastream
        token: "{{ env_var('DELTASTREAM_API_TOKEN') }}"
        organization_id: "{{ env_var('DELTASTREAM_ORG_ID') }}"
        # ... other parameters
  ```

- **Separate profiles** for different environments (dev, staging, prod)
- **Document required environment variables** in your project README
- **Use dbt Cloud** for secure credential management in production

## 🔧 Materializations

DeltaStream supports two types of model definitions:

1. YAML-only resources for defining infrastructure components
2. SQL models for data transformations

### YAML-Only Resources

These models don't contain SQL SELECT statements but define infrastructure components using YAML configuration.
YAML-only resources can be used to define external system connections such as streams, changelogs, and stores.
They can be either: managed or unmanaged by dbt DAG.

#### Should you use managed or unmanaged resources?

If you plan to be able to recreate all the infrastructure in different environments and/or use graph operators to execute only the creation of specific resources and downstream transformations, you should use managed resources.
Else, it might be simpler to use unmanaged resources to avoid placeholder files.

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

For example, you may create `my_kafka_stream.sql` with the following content:

```sql
-- Placeholder
```

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

## 🌱 Seeds

Load CSV data into existing DeltaStream entities using the `seed` materialization. Unlike traditional dbt seeds that create new tables, DeltaStream seeds insert data into pre-existing entities.

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

## ⚙️ Function and Source Materializations

DeltaStream supports user-defined functions (UDFs) and their dependencies through specialized materializations.

### File Attachment Support

The adapter provides seamless file attachment for function sources and descriptor sources:

- **Standardized Interface**: Common file handling logic for both function sources and descriptor sources
- **Path Resolution**: Supports both absolute paths and relative paths (including `@` syntax for project-relative paths)
- **Automatic Validation**: Files are validated for existence and accessibility before attachment

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

## 🎯 Query Management Macros

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

### List All Queries

The `list_all_queries` macro displays all queries currently known to DeltaStream, including their state, owner, and SQL. It prints a formatted table to the dbt logs for easy inspection.

**Usage:**

```bash
dbt run-operation list_all_queries
```

**Example Output:**

```text
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

## 📦 Application Macro

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

## 🔍 Troubleshooting

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

## 📚 Resources & Documentation

### Official Documentation

- **[DeltaStream Documentation](https://docs.deltastream.io)** - Complete DeltaStream platform documentation
- **[dbt Documentation](https://docs.getdbt.com)** - Official dbt documentation

### Examples

Check out the `/examples` directory for complete working examples:

- `snowflake_with_deltastream/` - Integration with Snowflake
- `databricks_with_deltastream/` - Integration with Databricks

## 🤝 Contributing

Contributions are welcome and encouraged! Whether you're fixing bugs, adding features, improving documentation, or creating examples, your help makes this adapter better for everyone.

**Ways to Contribute:**

- 🐛 **Report bugs** via [GitHub Issues](https://github.com/deltastreaminc/dbt-deltastream/issues)
- 💡 **Suggest features** or enhancements
- 📖 **Improve documentation** - even small fixes help!
- 🧪 **Add tests** to increase coverage
- ⭐ **Star the repository** to show your support

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines on:

- Setting up your development environment
- Running tests and quality checks
- Submitting pull requests
- Using `changie` for changelog management
