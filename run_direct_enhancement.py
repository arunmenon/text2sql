#!/usr/bin/env python3
"""
Run Direct Enhancement Workflow

This script runs the direct enhancement workflow that creates a business glossary
with a proper graph structure.

Usage:
    python run_direct_enhancement.py --tenant-id YOUR_TENANT_ID
"""
import argparse
import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient
from src.text2sql.metadata_enhancement_direct import DirectEnhancementWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def run_direct_enhancement(tenant_id):
    """Run the direct metadata enhancement workflow."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            logger.error("No LLM API key found in environment variables")
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
        
        # Create and run the DirectEnhancementWorkflow
        workflow = DirectEnhancementWorkflow(neo4j_client, llm_client)
        success = await workflow.run(tenant_id, dataset_id)
        
        # Close clients
        neo4j_client.close()
        await llm_client.close()
        
        if success:
            logger.info("Direct enhancement workflow completed successfully")
        else:
            logger.error("Direct enhancement workflow failed")
        
        return success
        
    except Exception as e:
        logger.error(f"Error in direct enhancement workflow: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run direct enhancement workflow")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    
    args = parser.parse_args()
    
    # Make script executable
    os.chmod(__file__, 0o755)
    
    # Run the process
    success = asyncio.run(run_direct_enhancement(args.tenant_id))
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)