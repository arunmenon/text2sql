"""
Simulate schema extraction from a database system and store in Neo4j.

This script generates a schema based on SQL definition files or built-in simulators
and loads it into Neo4j.

Usage:
    python simulate_schema.py --tenant-id YOUR_TENANT_ID [--schema-type TYPE] [--schema-file PATH]
"""
import argparse
import logging
import os
from dotenv import load_dotenv
import asyncio

from src.schema_extraction.simulation.merchandising_schema import MerchandisingSchemaSimulator
from src.schema_extraction.simulation.walmart_schema import WalmartSchemaSimulator
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

async def simulate_schema(tenant_id, schema_type="merchandising", schema_file=None):
    """Simulate schema extraction and store in Neo4j."""
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
        
        # Generate simulated schema based on type or file
        schema = None
        
        if schema_file:
            # Use the external schema file
            logger.info(f"Loading schema from file: {schema_file}")
            simulator = SchemaSimulator(tenant_id, schema_file, schema_type)
            schema = simulator.generate_schema()
        else:
            # Use built-in simulators based on schema_type
            logger.info(f"Generating simulated {schema_type} schema for tenant {tenant_id}")
            
            if schema_type == "walmart":
                simulator = WalmartSchemaSimulator(tenant_id)
            else:
                simulator = MerchandisingSchemaSimulator(tenant_id)
                
            schema = simulator.generate_schema()
        
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
        
        logger.info(f"Schema simulation complete. Added {table_count} tables and {column_count} columns.")
        
        # Close Neo4j client
        neo4j_client.close()
        
    except Exception as e:
        logger.error(f"Error in schema simulation: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate schema extraction")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--schema-type", default="merchandising", 
                      help="Type of schema to simulate or name for imported schema")
    parser.add_argument("--schema-file", help="Path to SQL schema file to load")
    
    args = parser.parse_args()
    
    asyncio.run(simulate_schema(args.tenant_id, args.schema_type, args.schema_file))