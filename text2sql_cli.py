#!/usr/bin/env python3
"""
Text2SQL CLI Tool

Command-line interface for running natural language to SQL conversions.
"""
import os
import sys
import json
import logging
import asyncio
import argparse
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient
from src.text2sql.engine import TextToSQLEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def process_query(query: str, tenant_id: str, output_file: str = None):
    """
    Process a natural language query and convert to SQL.
    
    Args:
        query: Natural language query
        tenant_id: Tenant ID
        output_file: Optional path to write results to
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "claude-3-opus-20240229")
        
        if not llm_api_key:
            logger.error("LLM API key not configured")
            return False
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Initialize Text2SQL engine
        engine = TextToSQLEngine(neo4j_client, llm_client)
        
        # Process the query
        result = await engine.process_query(query, tenant_id)
        
        # Convert to dictionary for output
        result_dict = result.dict()
        
        # Print results
        if result.primary_interpretation:
            print("\nGenerated SQL:")
            print("=============")
            print(result.primary_interpretation.sql)
            
            if result.primary_interpretation.explanation:
                print("\nExplanation:")
                print("============")
                print(result.primary_interpretation.explanation)
        
        # Write to file if requested
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(result_dict, f, indent=2)
            logger.info(f"Results written to {output_file}")
        
        # Clean up
        neo4j_client.close()
        await llm_client.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        return False

def main():
    """Main entry point for the CLI tool."""
    parser = argparse.ArgumentParser(description="Text2SQL CLI Tool")
    parser.add_argument("--query", help="Natural language query to process")
    parser.add_argument("--file", help="File containing query (one query per line)")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--output", help="Output file to write results to (JSON format)")
    
    args = parser.parse_args()
    
    if not args.query and not args.file:
        parser.error("Either --query or --file must be provided")
    
    if args.query and args.file:
        parser.error("Only one of --query or --file can be provided")
    
    # Get the query
    if args.file:
        try:
            with open(args.file, 'r') as f:
                query = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading query file: {e}")
            return 1
    else:
        query = args.query
    
    # Run the query processing
    success = asyncio.run(process_query(query, args.tenant_id, args.output))
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())