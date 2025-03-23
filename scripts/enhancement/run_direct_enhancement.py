#!/usr/bin/env python3
"""
Run Direct Enhancement Workflow

This script runs the direct enhancement workflow that creates a business glossary
with a proper graph structure. It supports the enhanced business glossary generator
with weighted term mapping and composite concept resolution.

Usage:
    python run_direct_enhancement.py --tenant-id YOUR_TENANT_ID [--use-enhanced-glossary]
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

async def run_direct_enhancement(tenant_id, use_enhanced_glossary=True):
    """Run the direct metadata enhancement workflow."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        # Check environment variable for enhanced glossary
        env_enhanced_glossary = os.getenv("USE_ENHANCED_GLOSSARY")
        if env_enhanced_glossary is not None:
            use_enhanced_glossary = env_enhanced_glossary.lower() in ("true", "1", "yes")
        
        if not llm_api_key:
            logger.error("No LLM API key found in environment variables")
            return False
        
        logger.info(f"Using LLM API key: {llm_api_key[:5]}...{llm_api_key[-5:]}")
        logger.info(f"Enhanced business glossary: {'Enabled' if use_enhanced_glossary else 'Disabled'}")
        
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
        workflow = DirectEnhancementWorkflow(
            neo4j_client, 
            llm_client, 
            use_enhanced_glossary=use_enhanced_glossary
        )
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
    parser.add_argument("--use-enhanced-glossary", action="store_true", 
                        help="Use enhanced business glossary generator with multi-agent approach")
    parser.add_argument("--use-legacy-glossary", action="store_true", 
                        help="Use legacy business glossary generator")
    
    args = parser.parse_args()
    
    # Determine whether to use enhanced glossary
    use_enhanced = True  # Default to enhanced
    if args.use_legacy_glossary:
        use_enhanced = False
    elif args.use_enhanced_glossary:
        use_enhanced = True
    
    # Make script executable
    os.chmod(__file__, 0o755)
    
    # Run the process
    success = asyncio.run(run_direct_enhancement(args.tenant_id, use_enhanced_glossary=use_enhanced))
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)