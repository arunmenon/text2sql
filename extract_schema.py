"""
Extract schema from BigQuery and store in Neo4j.

Usage:
    python extract_schema.py --tenant-id YOUR_TENANT_ID --project-id YOUR_GCP_PROJECT --dataset-id YOUR_DATASET
"""
import argparse
import asyncio
import logging
import os
from dotenv import load_dotenv

from src.schema_extraction.bigquery.extractor import BigQuerySchemaExtractor
from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def extract_schema(tenant_id, project_id, dataset_id):
    """Extract schema from BigQuery and store in Neo4j."""
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
        neo4j_client.create_tenant(tenant_id, tenant_id)
        
        # Extract schema
        logger.info(f"Extracting schema for {project_id}.{dataset_id}")
        extractor = BigQuerySchemaExtractor(project_id)
        schema = await extractor.extract_full_schema(dataset_id)
        
        # Create dataset
        neo4j_client.create_dataset(tenant_id, dataset_id)
        
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
        
        logger.info(f"Schema extraction complete. Added {table_count} tables and {column_count} columns.")
        
        # Close Neo4j client
        neo4j_client.close()
        
    except Exception as e:
        logger.error(f"Error extracting schema: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract schema from BigQuery")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--project-id", required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset-id", required=True, help="BigQuery dataset ID")
    
    args = parser.parse_args()
    
    asyncio.run(extract_schema(args.tenant_id, args.project_id, args.dataset_id))