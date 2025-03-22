#!/usr/bin/env python3
"""
Run Schema Enhancement Workflow

This script runs the schema enhancement workflow with LLM:
1. Loads schema and relationship information from Neo4j
2. Enhances metadata using LLM
3. Generates business glossary

Usage:
    python run_enhancement.py --tenant-id YOUR_TENANT_ID [--api-key YOUR_API_KEY]
"""
import argparse
import asyncio
import logging
import os
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient
from src.text2sql.metadata_enhancement import SchemaEnhancementWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def run_enhancement(tenant_id, api_key=None):
    """Run the metadata enhancement workflow."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = api_key or os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            logger.error("No LLM API key found. Please provide an API key.")
            return False
        
        logger.info(f"Using LLM API key: {llm_api_key[:5]}...{llm_api_key[-5:]}")
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Get dataset_id from Neo4j
        datasets = neo4j_client.get_datasets_for_tenant(tenant_id)
        if not datasets:
            logger.error(f"No datasets found for tenant {tenant_id}")
            return False
        
        dataset_id = datasets[0]["name"]
        logger.info(f"Found dataset: {dataset_id}")
        
        # Run enhancement workflow
        workflow = SchemaEnhancementWorkflow(neo4j_client, llm_client)
        success = await workflow.run(tenant_id, dataset_id)
        
        # Close clients
        neo4j_client.close()
        await llm_client.close()
        
        if success:
            logger.info("Metadata enhancement workflow completed successfully")
        else:
            logger.error("Metadata enhancement workflow failed")
        
        return success
        
    except Exception as e:
        logger.error(f"Error in metadata enhancement: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run schema enhancement workflow")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--api-key", help="OpenAI API key")
    
    args = parser.parse_args()
    
    # Run the process
    asyncio.run(run_enhancement(args.tenant_id, args.api_key))