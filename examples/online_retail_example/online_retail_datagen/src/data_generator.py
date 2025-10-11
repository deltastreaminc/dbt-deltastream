#!/usr/bin/env python3
"""
DeltaStream SDK Example: Insert into Entity

This script inserts sample pageview data into a DeltaStream entity.
"""

import asyncio
import os
import sys
from typing import Any

from deltastream_sdk import DeltaStreamClient
from deltastream_sdk.exceptions import DeltaStreamSDKError
from deltastream_sdk.models.entities import EntityCreateParams
from dotenv import find_dotenv, load_dotenv


def get_env(name: str) -> str:
    """Get required environment variable or raise error."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


async def find_store(client: DeltaStreamClient, store_name: str) -> Any:
    """Find and validate the configured store."""
    print("=== STORE DISCOVERY ===")
    stores = await client.stores.list()
    print("Available stores:")
    for store in stores:
        print(f"  - {store.name}")

    target_store = None
    for store in stores:
        if store.name == store_name:
            target_store = store
            break

    if not target_store:
        print(f"\n❌ Configured store '{store_name}' not found.")
        print("Please update STORE_NAME in .env to match one of the available stores.")
        sys.exit(1)

    print(f"\n✅ Found configured store: {target_store.name}")
    await client.use_store(target_store.name)
    return target_store


async def ensure_entity(
    client: DeltaStreamClient, entity_name: str, target_store: Any
) -> None:
    """Check if entity exists and create it if needed."""
    print("\n=== ENTITY MANAGEMENT ===")

    try:
        entities = await client.entities.list()
        entity_exists = False

        for entity in entities:
            print(f"  - {entity.name}")
            if entity.name == entity_name:
                entity_exists = True
                print(f"✅ Found existing entity '{entity_name}'")
                break

        print(f"Entity '{entity_name}' exists: {entity_exists}")
    except Exception as e:
        print(f"Could not check entities: {e}")
        entity_exists = False

    if not entity_exists:
        print(f"Creating entity '{entity_name}' in store '{target_store.name}'...")
        try:
            create_params = EntityCreateParams(
                name=entity_name,
                store=target_store.name,
                params={
                    "kafka.topic.partitions": 1,
                    "kafka.topic.replicas": 1,
                    "kafka.topic.retention.ms": "604800000",  # 7 days
                    "kafka.topic.segment.ms": "86400000",  # 1 day
                },
            )
            await client.entities.create(params=create_params)
            print(f"Entity '{entity_name}' created successfully")
        except Exception as e:
            error_msg = str(e)
            if "already exists" in error_msg:
                print(f"Entity '{entity_name}' already exists, continuing...")
            else:
                print(f"Could not create entity: {e}")
                sys.exit(1)


async def insert_data(
    client: DeltaStreamClient, entity_name: str, store_name: str
) -> None:
    """Insert sample sales transaction data into the entity."""
    print("\n=== DATA INSERTION ===")

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
            "sales_amount": 1169.99,
        },
        {
            "transaction_id": "TXN_002",
            "event_time": 1753311018650,
            "product_id": "PROD_PHONE_002",
            "customer_id": "CUST_67890",
            "store_id": "STORE_LAX_02",
            "quantity": 2,
            "unit_price": 899.99,
            "discount": 0.05,
            "sales_amount": 1709.98,
        },
        {
            "transaction_id": "TXN_003",
            "event_time": 1753311018651,
            "product_id": "PROD_HEADPHONES_003",
            "customer_id": "CUST_11111",
            "store_id": "STORE_CHI_03",
            "quantity": 3,
            "unit_price": 199.99,
            "discount": 0.00,
            "sales_amount": 599.97,
        },
    ]

    print(
        f"Inserting {len(sample_data)} sales transaction records into entity '{entity_name}'..."
    )

    await client.entities.insert_values(
        name=entity_name, values=sample_data, store=store_name
    )
    print("Insert completed successfully.")


async def run_example() -> None:
    """Main function to run the data generator example."""
    load_dotenv(find_dotenv())

    token = get_env("DELTASTREAM_TOKEN")
    org_id = get_env("DELTASTREAM_ORG_ID")
    server_url = os.getenv("DELTASTREAM_SERVER_URL")

    async def token_provider() -> str:
        return token

    client_kwargs = {"token_provider": token_provider, "organization_id": org_id}
    if server_url:
        client_kwargs["server_url"] = server_url

    client = DeltaStreamClient(**client_kwargs)

    db_name = os.getenv("DELTASTREAM_DATABASE_NAME")
    if db_name:
        await client.use_database(db_name)

    entity_name = os.getenv("ENTITY_NAME", "fact_sales_raw")
    store_name = os.getenv("STORE_NAME", "kafka_store")

    if not entity_name or not store_name:
        print("❌ Missing required environment variables: ENTITY_NAME and STORE_NAME")
        sys.exit(1)

    print(f"Starting insert example: entity='{entity_name}', store='{store_name}'")

    try:
        target_store = await find_store(client, store_name)
        await ensure_entity(client, entity_name, target_store)
        await insert_data(client, entity_name, store_name)

        print(f"\n✅ Successfully completed insert example for entity '{entity_name}'")

    except DeltaStreamSDKError as e:
        print(f"DeltaStream SDK error: {e}")
        sys.exit(1)


def main() -> None:
    """Entry point for the data generator."""
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
