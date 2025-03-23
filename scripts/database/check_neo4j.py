#!/usr/bin/env python3
"""
Check Neo4j Graph Database

This script checks the content of the Neo4j database after enhancement:
1. Shows table descriptions
2. Shows business glossary
3. Shows concept tags
4. Shows semantic relationships

Usage:
    python check_neo4j.py --tenant-id YOUR_TENANT_ID
"""
import argparse
import logging
import os
import sys
import json
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def check_enhanced_tables(neo4j_client, tenant_id):
    """Check for enhanced table descriptions."""
    print("\n=== Enhanced Table Descriptions ===")
    
    query = """
    MATCH (t:Table {tenant_id: $tenant_id})
    WHERE t.description_enhanced = true OR t.description IS NOT NULL
    RETURN t.name, t.description
    """
    
    results = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
    
    if not results:
        print("No enhanced table descriptions found.")
        return
    
    for result in results:
        print(f"\nTable: {result['t.name']}")
        print(f"Description: {result['t.description']}")

def check_business_glossary(neo4j_client, tenant_id):
    """Check for business glossary."""
    print("\n=== Business Glossary ===")
    
    query = """
    MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
    RETURN g.content
    """
    
    results = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
    
    if not results:
        print("No business glossary found.")
        return
    
    for result in results:
        print(result["g.content"])

def check_concept_tags(neo4j_client, tenant_id):
    """Check for concept tags."""
    print("\n=== Concept Tags ===")
    
    query = """
    MATCH (t:Table {tenant_id: $tenant_id})
    WHERE t.concept_tags IS NOT NULL
    RETURN t.name, t.concept_tags
    """
    
    results = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
    
    if not results:
        print("No concept tags found.")
        return
    
    for result in results:
        print(f"\nTable: {result['t.name']}")
        print(f"Tags: {result['t.concept_tags']}")

def check_column_enrichment(neo4j_client, tenant_id):
    """Check for enhanced column descriptions."""
    print("\n=== Enhanced Column Descriptions ===")
    
    query = """
    MATCH (c:Column {tenant_id: $tenant_id})
    WHERE c.description_enhanced = true OR c.business_purpose IS NOT NULL
    RETURN c.table_name, c.name, c.description, c.business_purpose
    LIMIT 5
    """
    
    results = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
    
    if not results:
        print("No enhanced column descriptions found.")
        return
    
    for result in results:
        print(f"\nTable: {result['c.table_name']}")
        print(f"Column: {result['c.name']}")
        print(f"Description: {result['c.description']}")
        print(f"Business Purpose: {result['c.business_purpose']}")

def check_llm_relationships(neo4j_client, tenant_id):
    """Check for LLM-inferred relationships."""
    print("\n=== LLM-inferred Relationships ===")
    
    query = """
    MATCH (source:Column {tenant_id: $tenant_id})-[r:LIKELY_REFERENCES]->(target:Column {tenant_id: $tenant_id})
    WHERE r.detection_method = 'llm_semantic'
    RETURN source.table_name, source.name, target.table_name, target.name, r.confidence, r.metadata_str
    """
    
    results = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
    
    if not results:
        print("No LLM-inferred relationships found.")
        return
    
    for result in results:
        metadata = result.get('r.metadata_str', '{}')
        try:
            if metadata:
                metadata_obj = json.loads(metadata)
                explanation = metadata_obj.get('explanation', 'No explanation')
            else:
                explanation = 'No metadata'
        except:
            explanation = 'Invalid metadata format'
            
        print(f"\n{result['source.table_name']}.{result['source.name']} → "
              f"{result['target.table_name']}.{result['target.name']}")
        print(f"Confidence: {result['r.confidence']}")
        print(f"Explanation: {explanation}")

def check_database_content(tenant_id):
    """Check database content after enhancement."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        # Check database content
        check_enhanced_tables(neo4j_client, tenant_id)
        check_business_glossary(neo4j_client, tenant_id)
        check_concept_tags(neo4j_client, tenant_id)
        check_column_enrichment(neo4j_client, tenant_id)
        check_llm_relationships(neo4j_client, tenant_id)
        
        # Close Neo4j client
        neo4j_client.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking database content: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check Neo4j database content")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    
    args = parser.parse_args()
    
    # Run the check
    success = check_database_content(args.tenant_id)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)