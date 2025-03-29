#!/usr/bin/env python3
"""
Client Example for Metadata Registry MCP Server

This example demonstrates how to connect to the Metadata Registry MCP Server
and use the Knowledge Graph service to query the Neo4j database.
"""
import os
import sys
import asyncio
import logging
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = str(Path(__file__).parent.parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from mcp.client import Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Run the client example."""
    try:
        # Connect to the MCP server
        client = Client("http://localhost:8234")
        
        # Get available resources
        resources = await client.get_resources()
        logger.info(f"Available resources: {[r.name for r in resources]}")
        
        # Find the Knowledge Graph resource
        kg_resource = None
        for resource in resources:
            if resource.name == "knowledge_graph":
                kg_resource = resource
                break
        
        if not kg_resource:
            logger.error("Knowledge Graph resource not found")
            return
        
        logger.info(f"Found Knowledge Graph resource: {kg_resource.name}")
        
        # List available operations
        operations = kg_resource.operations
        logger.info(f"Available operations: {[op.name for op in operations]}")
        
        # Example 1: Run a simple query
        logger.info("Running a simple Cypher query...")
        query_result = await kg_resource.query(
            query="MATCH (t:Table) RETURN t.name AS table_name LIMIT 5"
        )
        
        if query_result.error:
            logger.error(f"Query failed: {query_result.error}")
        else:
            logger.info("Available tables:")
            for item in query_result.data:
                logger.info(f"  - {item['table_name']}")
        
        # Example 2: Get schema for a table
        if query_result.data and len(query_result.data) > 0:
            table_name = query_result.data[0]['table_name']
            logger.info(f"Getting schema for table: {table_name}")
            
            schema_result = await kg_resource.get_table_schema(
                table_name=table_name,
                include_relationships=True
            )
            
            if schema_result.error:
                logger.error(f"Schema query failed: {schema_result.error}")
            else:
                logger.info(f"Table schema:")
                table = schema_result.data.get('table', {})
                columns = schema_result.data.get('columns', [])
                
                logger.info(f"  Table: {table.get('name')}")
                logger.info(f"  Description: {table.get('description', 'No description')}")
                logger.info(f"  Columns: {len(columns)}")
                
                for col in columns[:3]:  # Show first 3 columns
                    logger.info(f"    - {col.get('name')}: {col.get('data_type', 'unknown')}")
                
                if len(columns) > 3:
                    logger.info(f"    ... and {len(columns) - 3} more columns")
        
        # Example 3: Search for business terms
        logger.info("Searching for business terms with 'asset'...")
        terms_result = await kg_resource.search_business_terms(
            keyword="asset"
        )
        
        if terms_result.error:
            logger.error(f"Term search failed: {terms_result.error}")
        else:
            logger.info("Matching business terms:")
            for term in terms_result.data[:5]:  # Show up to 5 terms
                logger.info(f"  - {term.get('name')}: {term.get('definition', 'No definition')}")
        
    except Exception as e:
        logger.error(f"Error in client example: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())