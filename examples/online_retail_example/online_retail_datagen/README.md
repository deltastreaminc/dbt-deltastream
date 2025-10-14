# Online Retail Data Generator

A Python application that uses the DeltaStream SDK to insert sample retail sales transactions into DeltaStream entities. This demonstrates how to push data into Kafka topics via DeltaStream for testing and development.

## 📋 Prerequisites

- Python 3.11 or higher
- DeltaStream account with:
  - Organization ID
  - API Token
- Access to a DeltaStream Kafka store (or use DeltaStream's `trial_store`)

## 🚀 Quick Start

### 1. Install Dependencies

Using uv (recommended):

```bash
uv sync
```

Or using pip:

```bash
pip install -e .
```

### 2. Configure Environment

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` with your DeltaStream credentials:

```bash
# DeltaStream Authentication
DELTASTREAM_TOKEN=your_api_token_here
DELTASTREAM_ORG_ID=your_org_id_here
DELTASTREAM_SERVER_URL=https://api.deltastream.io/v2

# Database and Schema
DELTASTREAM_DATABASE_NAME=online_retail_dev
DELTASTREAM_SCHEMA_NAME=public

# Entity Configuration
ENTITY_NAME=fact_sales_raw
STORE_NAME=kafka_store  # or trial_store for testing
```

### 3. Run the Data Generator

Using uv:

```bash
uv run data-generator
```

Or with Python:

```bash
python -m src.data_generator
```

## 📊 What It Does

The data generator will:

1. **Connect** to DeltaStream using your credentials
2. **Discover** the configured Kafka store
3. **Create** the entity if it doesn't exist
4. **Insert** sample sales transaction data into the entity

### Sample Data

The generator inserts sales transaction events with this structure:

```json
{
  "transaction_id": "TXN_001",
  "event_time": 1753311018649,
  "product_id": "PROD_LAPTOP_001",
  "customer_id": "CUST_12345",
  "store_id": "STORE_NYC_01",
  "quantity": 1,
  "unit_price": 1299.99,
  "discount": 0.10,
  "sales_amount": 1169.99
}
```

## 📖 Expected Output

```text
Starting insert example: entity='fact_sales_raw', store='kafka_store'

=== STORE DISCOVERY ===
Available stores:
  - kafka_store
  - trial_store

✅ Found configured store: kafka_store

=== ENTITY MANAGEMENT ===
  - fact_sales_raw
✅ Found existing entity 'fact_sales_raw'
Entity 'fact_sales_raw' exists: True

=== DATA INSERTION ===
Inserting 3 sales transaction records into entity 'fact_sales_raw'...
Insert completed successfully.

✅ Successfully completed insert example for entity 'fact_sales_raw'
```

## 🔧 Customization

To customize the data being inserted, edit `src/data_generator.py` and modify the `sample_data` in the `insert_data()` function:

```python
sample_data = [
    {
        "transaction_id": "TXN_001",
        "event_time": 1753311018649,
        "product_id": "PROD_LAPTOP_001",
        "customer_id": "CUST_12345",
        "store_id": "STORE_NYC_01",
        "quantity": 1,
        "unit_price": 1299.99,
        "discount": 0.10,
        "sales_amount": 1169.99
    },
    # Add more transactions here...
]
```

## 🐛 Troubleshooting

### Store Not Found

If you see `❌ Configured store 'kafka_store' not found`:

1. Check available stores in the output
2. Update `STORE_NAME` in `.env` to match an existing store
3. Or use `trial_store` for testing

### Authentication Failed

If you see `DeltaStream SDK error: Unauthorized`:

1. Verify `DELTASTREAM_TOKEN` is correct
2. Check `DELTASTREAM_ORG_ID` matches your organization
3. Generate a new token from DeltaStream dashboard → Integrations

### Missing Environment Variables

If you see `Missing required environment variable`:

1. Ensure `.env` file exists
2. Verify all required variables are set
3. Check variable names match exactly

## 📚 Resources

- [DeltaStream SDK Documentation](https://github.com/deltastreaminc/deltastream-sdk-python)
- [DeltaStream Platform Docs](https://docs.deltastream.io)

## 📝 License

Apache License 2.0

