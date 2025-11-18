# Online Retail Example (WIP)

This example demonstrates the integration of dbt with DeltaStream for processing streaming data in an online retail scenario. It showcases how to use the dbt-deltastream adapter to build, transform, and analyze real-time retail transaction data using DeltaStream's streaming SQL capabilities.

## Overview

The online retail example consists of two main components:

1. **Data Generator** (`online_retail_datagen/`): A Python application that simulates streaming online retail transactions, generating realistic data including customer purchases, product information, and timestamps.

2. **dbt Project** (`online_retail_dbt_project/`): A complete dbt project that processes the streaming data through bronze, silver, and gold layers, demonstrating various dbt patterns for streaming data transformation.

## Architecture

The example follows a typical streaming data architecture:

- **Bronze Layer**: Raw ingestion of streaming sales transaction data
- **Silver Layer**: Cleansed and enriched data with dimesion data
- **Gold Layer**: Aggregated analytics and business intelligence views

## Prerequisites

Before running this example, ensure you have:

- The dbt-deltastream adapter installed (`pip install dbt-deltastream`)
- Python 3.8+ with required dependencies
- DeltaStream API token

## Setup Instructions

1. **Clone and navigate to the example directory:**

   ```bash
   cd examples/online_retail_example
   ```

2. **Install dependencies for the data generator:**

   ```bash
   cd online_retail_datagen
   uv sync
   cd ..
   ```

3. **Configure DeltaStream connection:**
   - rename .env.example file to .env and edit it.
   - Update the connection details for your DeltaStream cluster
   - Ensure the profile name matches your dbt configuration

4. **Install dbt dependencies:**

   ```bash
   cd online_retail_dbt_project
   uv sync
   ```

## Running the Example

1. **Start the data generator:**

   ```bash
   cd online_retail_datagen
   source .venv/bin/activate
   uv run python src/data_generator.py
   ```

   This will begin generating streaming transaction data.

2. **Run the dbt project:**

   ```bash
   cd ../online_retail_dbt_project
   source .venv/bin/activate
   uv run dbt run
   ```

   This executes all models in the project, processing the streaming data.

3. **Monitor and analyze:**
   - Check the `target/` directory for run results
   - View logs in `logs/` for execution details
   - Use DeltaStream's web UI to monitor streaming jobs

## What This Example Demonstrates

- **Streaming Data Ingestion**: How to ingest real-time data streams into DeltaStream
- **dbt Materializations**: Using dbt's materialization strategies with streaming data
- **Layered Architecture**: Implementing bronze/silver/gold data layers for streaming analytics
- **Real-time Transformations**: Applying business logic to streaming data
- **Error Handling**: Managing streaming data quality and exceptions
- **Performance Optimization**: Best practices for streaming dbt models

## Key Models

- `bronze/raw_transactions`: Raw transaction data ingestion
- `silver/customer_orders`: Enriched order data with customer information
- `silver/product_analytics`: Product performance metrics
- `gold/daily_sales_summary`: Aggregated daily sales analytics
- `gold/customer_lifetime_value`: Customer segmentation and CLV calculations

## Customization

You can customize this example by:

- Modifying the data generator to simulate different retail scenarios
- Adding new models for additional analytics
- Adjusting streaming window functions for different time aggregations
- Integrating with external data sources or sinks

## Troubleshooting

- Ensure your DeltaStream cluster is running and accessible
- Check connection strings in `profiles.yml`
- Verify Python dependencies are installed correctly
- Review DeltaStream logs for streaming job errors
- Use `dbt debug` to troubleshoot dbt configuration issues

For more information about DeltaStream and dbt integration, refer to the main project documentation.
