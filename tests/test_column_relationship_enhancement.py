"""
Test Column Description Enhancement with Relationship Context

This script tests how relationship-aware column descriptions compare to 
standard descriptions without relationship context.
"""
import os
import json
import asyncio
import logging
from typing import Dict, List, Any

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient
from src.semantic_graph_builder.utils.prompt_loader import PromptLoader
from src.semantic_graph_builder.table_context_gen.utils.column_relationship_provider import ColumnRelationshipProvider

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_column_description_enhancement():
    """Test column description enhancement with and without relationship context."""
    try:
        # Get configuration from environment or use defaults
        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            logger.error("LLM API key not configured")
            return
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Initialize the relationship provider
        relationship_provider = ColumnRelationshipProvider(neo4j_client)
        
        # Initialize prompt loader
        prompt_loader = PromptLoader()
        
        # Test with a specific tenant and table
        tenant_id = "walmart-stores"  # Replace with your actual tenant ID
        
        # Selected columns to enhance
        test_cases = [
            # Table name, column name
            ("Asset", "location_id"),     # Foreign key
            ("Locations", "location_id"),  # Primary key
            ("Asset", "store_type"),      # Column with many relationships
        ]
        
        results = []
        
        for table_name, column_name in test_cases:
            logger.info(f"\nTesting column enhancement for {table_name}.{column_name}")
            
            # Get the column data
            columns = neo4j_client.get_columns_for_table(tenant_id, table_name)
            
            # Find our target column
            column_to_enhance = None
            for col in columns:
                if col.get('name') == column_name or col.get('column_name') == column_name:
                    column_to_enhance = col
                    break
            
            if not column_to_enhance:
                logger.warning(f"Column {column_name} not found in {table_name}")
                continue
            
            # Get relationship context
            rel_context = relationship_provider.get_column_relationships(
                tenant_id=tenant_id,
                table_name=table_name,
                column_name=column_name,
                include_table_info=True
            )
            
            # Create prompt variables for standard enhancement (no relationships)
            standard_prompt_vars = {
                "table_name": table_name,
                "columns_to_enhance_text": f"- {column_name}: {column_to_enhance.get('data_type')} - {column_to_enhance.get('description', 'No description')}",
                "all_columns_text": "\n".join([
                    f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')}"
                    for col in columns if col.get('name') or col.get('column_name')
                ]),
                "sample_data": "",  # No sample data for this test
                "column_examples": "",  # No column examples for this test
                "column_relationships": ""  # Empty relationship context
            }
            
            # Create prompt variables for relationship-enhanced description
            relationship_prompt_vars = standard_prompt_vars.copy()
            relationship_prompt_vars["column_relationships"] = f"RELATIONSHIP CONTEXT FOR {column_name}:\n{rel_context}\n"
            
            # Generate standard description
            standard_prompt = prompt_loader.format_prompt("column_description_enhancement", **standard_prompt_vars)
            standard_response = await llm_client.generate_structured_output(standard_prompt, {
                "column_descriptions": [
                    {
                        "column_name": "string",
                        "description": "string",
                        "business_purpose": "string",
                        "data_constraints": "string",
                        "potential_joins": "string"
                    }
                ]
            })
            
            # Generate relationship-enhanced description
            relationship_prompt = prompt_loader.format_prompt("column_description_enhancement", **relationship_prompt_vars)
            relationship_response = await llm_client.generate_structured_output(relationship_prompt, {
                "column_descriptions": [
                    {
                        "column_name": "string",
                        "description": "string",
                        "business_purpose": "string",
                        "data_constraints": "string",
                        "potential_joins": "string"
                    }
                ]
            })
            
            # Extract descriptions
            standard_desc = None
            if standard_response and "column_descriptions" in standard_response:
                for col_desc in standard_response["column_descriptions"]:
                    if col_desc.get("column_name") == column_name:
                        standard_desc = col_desc
                        break
            
            enhanced_desc = None
            if relationship_response and "column_descriptions" in relationship_response:
                for col_desc in relationship_response["column_descriptions"]:
                    if col_desc.get("column_name") == column_name:
                        enhanced_desc = col_desc
                        break
            
            # Compare descriptions
            if standard_desc and enhanced_desc:
                logger.info(f"Standard description for {table_name}.{column_name}:")
                logger.info(f"Description: {standard_desc.get('description')}")
                logger.info(f"Business purpose: {standard_desc.get('business_purpose')}")
                logger.info(f"Potential joins: {standard_desc.get('potential_joins')}")
                
                logger.info(f"\nRelationship-enhanced description for {table_name}.{column_name}:")
                logger.info(f"Description: {enhanced_desc.get('description')}")
                logger.info(f"Business purpose: {enhanced_desc.get('business_purpose')}")
                logger.info(f"Potential joins: {enhanced_desc.get('potential_joins')}")
                
                # Store results for comparison
                results.append({
                    "table_name": table_name,
                    "column_name": column_name,
                    "standard_description": standard_desc,
                    "enhanced_description": enhanced_desc,
                    "relationship_context": rel_context
                })
            
        # Save results to a file
        timestamp = asyncio.get_event_loop().time()
        with open(f"results/column_enhancement_comparison_{int(timestamp)}.json", "w") as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Saved comparison results to results/column_enhancement_comparison_{int(timestamp)}.json")
        
        # Close clients
        neo4j_client.close()
        await llm_client.close()
        
    except Exception as e:
        logger.error(f"Error testing column description enhancement: {e}", exc_info=True)

if __name__ == "__main__":
    # Run the async test function
    asyncio.run(test_column_description_enhancement())