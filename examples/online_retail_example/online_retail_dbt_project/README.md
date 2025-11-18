# Online Retail Example with dbt-deltastream

This example demonstrates how to use dbt-deltastream to build a complete data pipeline for online retail analytics, combining batch and streaming transformations.

## 🚀 Getting Started

### Prerequisites

Before running this example, ensure you have:

- **Python 3.11 or higher** installed on your system
- **dbt-core 1.10 or higher** (will be installed automatically as a dependency)
- A **DeltaStream account** with:
  - Organization ID (found on settings page of your DeltaStream dashboard)
  - API Token (can be generated on the integration page in your DeltaStream dashboard)

> 📘 **New to DeltaStream?** Sign up at [deltastream.io](https://deltastream.io) to get started.

### Installation

1. Install dbt-deltastream using pip:

```bash
pip install dbt-deltastream
```

Or with [uv](https://github.com/astral-sh/uv) for faster installation:

```bash
uv init && uv add dbt-deltastream
```

2. Clone or download this example project.

### Configuration

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Edit `.env` and add your DeltaStream credentials:

```bash
DELTASTREAM_API_TOKEN=your-api-token-here
DELTASTREAM_ORG_ID=your-org-id-here
```

3. export the enviroment variables
```bash
set -a
source .env
set +a
```  

The `profiles.yml` is already configured to use environment variables for security.

### Running the Example

1. Install dependencies:

```bash
dbt deps
```

2. Test your connection:

```bash
dbt debug
```

3. Create sources:

```bash
dbt run-operation create_sources
```

4
. Run the models:

```bash
dbt run-operations
dbt run
```

This will create the bronze, silver, and gold layer transformations for the online retail data pipeline.

## 📊 Pipeline Overview

This example implements a medallion architecture:

- **Bronze Layer**: Raw data ingestion from sources
- **Silver Layer**: Cleaned and enriched data
- **Gold Layer**: Business-ready aggregations and analytics

### Key Features Demonstrated

- Streaming data ingestion from Kafka
- Materialized views for real-time aggregations
- Batch processing for historical data
- Data quality tests
- Documentation generation

## 🔧 Customization

To adapt this example for your own data:

1. Update the source configurations in `models/sources.yml`
2. Modify the transformation logic in the model files
3. Adjust the profiles.yml for your DeltaStream environment
4. Add your own tests and documentation

## 📚 Resources

- [dbt-deltastream Documentation](https://github.com/deltastreaminc/dbt-deltastream)
- [DeltaStream Platform](https://deltastream.io)
- [dbt Documentation](https://docs.getdbt.com)
