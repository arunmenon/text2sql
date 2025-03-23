#!/usr/bin/env python3
"""
Schema Loading Demonstration

This script demonstrates how to dynamically load different schema domains
using the schema_loader module.

Usage:
    python load_schema_demo.py --schema-type [walmart|supply-chain|merchandising] --tenant-id YOUR_TENANT_ID
"""
import argparse
import asyncio
import logging
import os
from dotenv import load_dotenv

from src.schema_extraction.simulation.schema_loader import SchemaSimulator
from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Dictionary mapping domain types to their respective schema files
SCHEMA_DOMAINS = {
    "walmart": "schemas/walmart_retail.sql",
    "supply-chain": "schemas/supply_chain.sql",
    "merchandising": None  # Uses the built-in MerchandisingSchemaSimulator
}

async def load_schema(tenant_id, schema_type):
    """Load a schema from the specified domain."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)

        # Create schema constraints
        neo4j_client.create_schema_constraints()

        # Create tenant if not exists
        description = f"{schema_type.title()} domain tenant"
        neo4j_client.create_tenant(tenant_id, tenant_id, description)

        # Get the schema file for the domain type
        schema_file = SCHEMA_DOMAINS.get(schema_type.lower())
        
        if schema_file:
            # Use the external schema file
            logger.info(f"Loading schema from file: {schema_file}")
            simulator = SchemaSimulator(tenant_id, schema_file, schema_type)
            schema = simulator.generate_schema()
        else:
            # Use the default schema simulator based on schema_type
            logger.info(f"Loading built-in {schema_type} schema for tenant {tenant_id}")
            from simulate_schema import simulate_schema
            await simulate_schema(tenant_id, schema_type)
            return

        # Store schema in Neo4j
        dataset_id = schema["dataset_id"]

        # Create dataset
        neo4j_client.create_dataset(tenant_id, dataset_id, f"{schema_type.title()} dataset")

        # Create tables and columns
        table_count = 0
        column_count = 0

        for table in schema["tables"]:
            table_name = table["table_name"]
            logger.info(f"Processing table: {table_name}")

            # Create table
            neo4j_client.create_table(tenant_id, dataset_id, table)
            table_count += 1

            # Create columns
            for column in table["columns"]:
                neo4j_client.create_column(
                    tenant_id,
                    dataset_id,
                    table_name,
                    column
                )
                column_count += 1

        logger.info(f"Schema loading complete. Added {table_count} tables and {column_count} columns.")

        # Close Neo4j client
        neo4j_client.close()

    except Exception as e:
        logger.error(f"Error loading schema: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Schema Loading Demonstration")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--schema-type", required=True, 
                      choices=["walmart", "supply-chain", "merchandising"],
                      help="Type of schema to load")
    
    args = parser.parse_args()
    
    asyncio.run(load_schema(args.tenant_id, args.schema_type))