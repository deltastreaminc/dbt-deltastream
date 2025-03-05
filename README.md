# dbt-deltastream

A [dbt](https://www.getdbt.com/) adapter for [DeltaStream](https://deltastream.io) - a streaming processing engine based on Apache Flink.

## Features

- Seamless integration with DeltaStream's streaming capabilities
- Support for DeltaStream core concepts through dbt materialization types:
  - `table`: Traditional batch table materialization
  - `materialized_view`: Continuously updated view
  - `stream`: Pure streaming transformation
  - `changelog`: Change data capture (CDC) stream
  - `store`: External system connection (Kafka, PostgreSQL, etc.)
  - `entity`: Entity definition in a store
  - `database`: Database definition

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
      token: your-api-token            # Authentication token
      database: your-database          # Target database name
      schema: your-schema              # Target schema name
      organization_id: your-org-id     # Organization identifier
      
      # Optional Parameters
      url: https://api.deltastream.io/v2  # DeltaStream API URL
      timezone: UTC                       # Timezone for operations
      session_id: your-session-id         # Custom session identifier for debugging purpose
      role: your-role                     # User role
      store: your-store                   # Target store name
```

The following parameters are supported in the profile configuration:

### Required Parameters
- `token`: Authentication token for DeltaStream API
- `database`: Target default database name
- `schema`: Target default schema name
- `organization_id`: Organization identifier

### Optional Parameters
- `url`: DeltaStream API URL (default: https://api.deltastream.io/v2)
- `timezone`: Timezone for operations (default: UTC)
- `session_id`: Custom session identifier for debugging

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
When a YAML-only resource is managed by dbt DAG, it is automatically included in the DAG by creating them as *models*, for instance:

```yaml
version: 2
models:
  - name: my_kafka_stream
    config:
      materialized: stream
      parameters:
        topic: 'user_events'
        value.format: 'json'
        store: 'my_kafka_store'
```

In that case, we're running into a dbt limitation where we need to create a placeholder .sql file for the model to be detected. That .sql file would contain any content as long as it doesn't contain a "SELECT". We expect that limitation to be lifted in future dbt versions but it's still part of discussions.

Then it can be referenced in downstream model using the regular `ref` function:

```sql
SELECT * FROM {{ ref('my_kafka_stream') }}
```

#### Unmanaged
When a YAML-only resource is not managed by dbt DAG, it has to be created as *sources*, for instance:

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
          store: 'my_kafka_store'
          'value.format': JSON
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

#### Store

Creates a connection to external systems:

```yaml
version: 2
models:
  - name: my_kafka_store
    config:
      materialized: store
      parameters:
      type: KAFKA        # required
        access_region: "AWS us-east-1"
        uris: "kafka.broker1.url:9092,kafka.broker2.url:9092"
        tls.ca_cert_file: "@/certs/us-east-1/self-signed-kafka-ca.crt"
```

PostgreSQL store example:
```yaml
version: 2
models:
  - name: ps_store
    config:
      materialized: store
      parameters:
        type: POSTGRESQL        # required
        access_region: "AWS us-east-1"
        uris: "postgresql://mystore.com:5432/demo"
        postgres.username: "user"
        postgres.password: "password"
```

#### Stream (YAML-only)

Defines a stream with explicit column definitions:

```yaml
version: 2
models:
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
```

#### Changelog (YAML-only)

Defines a changelog with explicit column definitions and primary key:

```yaml
version: 2
models:
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
```

#### Entity (YAML-only)

Defines an entity in a store:

```yaml
version: 2
models:
  - name: pv_kinesis
    config:
      materialized: entity
      store: kinesis_store
      parameters:
        'kinesis.shards' = 3
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

## Contributing

We welcome contributions! Please feel free to submit a Pull Request.

## License

[Apache License 2.0](LICENSE)