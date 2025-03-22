#!/usr/bin/env python3
"""
Normalize Business Glossary

This script converts the existing text-based business glossary into
a properly normalized graph structure in Neo4j.

Usage:
    python normalize_glossary.py --tenant-id YOUR_TENANT_ID
"""
import argparse
import logging
import os
import sys
import re
import json
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Output to stdout
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def normalize_glossary(tenant_id):
    """Convert text-based glossary to a normalized graph structure."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        # Get dataset_id from Neo4j
        datasets = neo4j_client.get_datasets_for_tenant(tenant_id)
        if not datasets:
            logger.error(f"No datasets found for tenant {tenant_id}")
            return False
        
        dataset_id = datasets[0]["name"]
        logger.info(f"Found dataset: {dataset_id}")
        
        # Get the existing business glossary content
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
        RETURN g.content
        """
        
        result = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
        
        if not result or not result[0].get("g.content"):
            logger.error("No business glossary content found")
            return False
        
        glossary_content = result[0]["g.content"]
        logger.info("Retrieved business glossary content")
        
        # Parse the glossary content
        terms = parse_terms(glossary_content)
        metrics = parse_metrics(glossary_content)
        
        logger.info(f"Parsed {len(terms)} terms and {len(metrics)} metrics")
        
        # Create the business glossary structure
        logger.info("Creating business glossary structure...")
        
        # First, clear existing business glossary structure if any
        clear_query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
        OPTIONAL MATCH (g)-[:HAS_TERM]->(t:GlossaryTerm)
        OPTIONAL MATCH (g)-[:HAS_METRIC]->(m:BusinessMetric)
        DETACH DELETE t, m, g
        """
        
        neo4j_client._execute_query(clear_query, {"tenant_id": tenant_id})
        logger.info("Cleared any existing business glossary structure")
        
        # Create root glossary node
        root_query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        CREATE (g:BusinessGlossary {
            tenant_id: $tenant_id,
            dataset_id: $dataset_id,
            created_at: datetime()
        })
        CREATE (d)-[:HAS_GLOSSARY]->(g)
        RETURN g
        """
        
        result = neo4j_client._execute_query(root_query, {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id
        })
        
        logger.info("Created business glossary root node")
        
        # Create term nodes
        for term in terms:
            term_query = """
            MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
            
            // Create term node
            CREATE (t:GlossaryTerm {
                tenant_id: $tenant_id,
                name: $name,
                definition: $definition,
                related_terms: $related_terms,
                synonyms: $synonyms
            })
            
            // Connect to glossary
            CREATE (g)-[:HAS_TERM]->(t)
            
            // Connect to mapped tables
            WITH t
            UNWIND $table_mappings AS mapping
            MATCH (table:Table {tenant_id: $tenant_id, name: mapping})
            CREATE (t)-[:MAPS_TO]->(table)
            
            // Connect to mapped columns
            WITH t
            UNWIND $column_mappings AS mapping
            MATCH (table:Table {tenant_id: $tenant_id, name: mapping.table})
            MATCH (column:Column {tenant_id: $tenant_id, table_name: mapping.table, name: mapping.column})
            CREATE (t)-[:MAPS_TO]->(column)
            
            RETURN t
            """
            
            neo4j_client._execute_query(term_query, {
                "tenant_id": tenant_id,
                "name": term["name"],
                "definition": term["definition"],
                "related_terms": term.get("related_terms", []),
                "synonyms": term.get("synonyms", []),
                "table_mappings": term.get("table_mappings", []),
                "column_mappings": term.get("column_mappings", [])
            })
            
            logger.info(f"Created term node: {term['name']}")
        
        # Create metric nodes
        for metric in metrics:
            metric_query = """
            MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
            
            // Create metric node
            CREATE (m:BusinessMetric {
                tenant_id: $tenant_id,
                name: $name,
                definition: $definition
            })
            
            // Connect to glossary
            CREATE (g)-[:HAS_METRIC]->(m)
            
            // Connect to derived tables
            WITH m
            UNWIND $derived_tables AS table_name
            MATCH (table:Table {tenant_id: $tenant_id, name: table_name})
            CREATE (m)-[:DERIVED_FROM]->(table)
            
            RETURN m
            """
            
            neo4j_client._execute_query(metric_query, {
                "tenant_id": tenant_id,
                "name": metric["name"],
                "definition": metric["definition"],
                "derived_tables": metric.get("derived_from", [])
            })
            
            logger.info(f"Created metric node: {metric['name']}")
        
        # Create term relationships
        create_term_relationships(neo4j_client, tenant_id, terms)
        
        # Close Neo4j client
        neo4j_client.close()
        
        logger.info("Business glossary structure created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error normalizing glossary: {e}")
        return False

def parse_terms(content):
    """Parse business terms from the glossary content."""
    terms = []
    
    # Extract the terms section
    term_pattern = r'## (\d+)\. (.+?)\n(.*?)(?=\n## \d+\.|## Common Business Metrics|$)'
    term_matches = re.finditer(term_pattern, content, re.DOTALL)
    
    for match in term_matches:
        term_name = match.group(2).strip()
        term_content = match.group(3).strip()
        
        # Extract definition
        definition_match = re.search(r'^\s*\*\*Definition\*\*:\s*(.+?)$', term_content, re.MULTILINE)
        definition = definition_match.group(1).strip() if definition_match else ""
        
        # Extract technical mapping
        table_mappings = []
        column_mappings = []
        mapping_section = re.search(r'\*\*Technical Mapping\*\*:(.*?)(?=\*\*Related Terms|\*\*Synonyms|$)', term_content, re.DOTALL)
        
        if mapping_section:
            # Extract table
            table_match = re.search(r'Table:\s*`(.+?)`', mapping_section.group(1))
            if table_match:
                table_name = table_match.group(1).strip()
                table_mappings.append(table_name)
            
            # Extract columns
            column_matches = re.findall(r'`(.+?)`', mapping_section.group(1))
            for col in column_matches[1:]:  # Skip the first one which is the table
                if table_name:
                    column_mappings.append({"table": table_name, "column": col})
        
        # Extract related terms
        related_terms = []
        related_match = re.search(r'\*\*Related Terms\*\*:\s*(.+?)$', term_content, re.MULTILINE)
        if related_match:
            related_terms = [term.strip() for term in related_match.group(1).split(',')]
        
        # Extract synonyms
        synonyms = []
        synonyms_match = re.search(r'\*\*Synonyms\*\*:\s*(.+?)$', term_content, re.MULTILINE)
        if synonyms_match:
            synonyms = [term.strip() for term in synonyms_match.group(1).split(',')]
        
        terms.append({
            "name": term_name,
            "definition": definition,
            "table_mappings": table_mappings,
            "column_mappings": column_mappings,
            "related_terms": related_terms,
            "synonyms": synonyms
        })
    
    return terms

def parse_metrics(content):
    """Parse business metrics from the glossary content."""
    metrics = []
    
    # Extract the metrics section
    metrics_section_match = re.search(r'## Common Business Metrics and KPIs(.*?)$', content, re.DOTALL)
    if not metrics_section_match:
        return metrics
    
    metrics_section = metrics_section_match.group(1)
    
    # Extract individual metrics
    metric_pattern = r'(\d+)\.\s+\*\*(.+?)\*\*\n\s+- \*\*Definition\*\*:\s*(.+?)\n\s+- \*\*Derivable From\*\*:\s*(.+?)$'
    metric_matches = re.finditer(metric_pattern, metrics_section, re.MULTILINE)
    
    for match in metric_matches:
        metric_name = match.group(2).strip()
        definition = match.group(3).strip()
        derivable_from = match.group(4).strip()
        
        # Extract tables
        tables = []
        if '`' in derivable_from:
            tables = re.findall(r'`(.+?)`', derivable_from)
        
        metrics.append({
            "name": metric_name,
            "definition": definition,
            "derived_from": tables
        })
    
    return metrics

def create_term_relationships(neo4j_client, tenant_id, terms):
    """Create relationships between term nodes."""
    # Create a mapping of term names to related terms and synonyms
    term_relationships = {}
    
    for term in terms:
        term_relationships[term["name"]] = {
            "related_terms": term.get("related_terms", []),
            "synonyms": term.get("synonyms", [])
        }
    
    # Create the relationships in Neo4j
    for term_name, relationships in term_relationships.items():
        # Create related_term relationships
        for related_term in relationships["related_terms"]:
            related_query = """
            MATCH (t1:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})
            MATCH (t2:GlossaryTerm {tenant_id: $tenant_id, name: $related_term})
            CREATE (t1)-[:RELATED_TO {type: "related_term"}]->(t2)
            """
            
            try:
                neo4j_client._execute_query(related_query, {
                    "tenant_id": tenant_id,
                    "term_name": term_name,
                    "related_term": related_term
                })
            except Exception as e:
                logger.warning(f"Could not create related_term relationship: {term_name} -> {related_term}: {e}")
        
        # Create synonym relationships
        for synonym in relationships["synonyms"]:
            synonym_query = """
            MATCH (t:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})
            MERGE (s:GlossaryTerm {tenant_id: $tenant_id, name: $synonym})
            CREATE (t)-[:RELATED_TO {type: "synonym"}]->(s)
            """
            
            try:
                neo4j_client._execute_query(synonym_query, {
                    "tenant_id": tenant_id,
                    "term_name": term_name,
                    "synonym": synonym
                })
            except Exception as e:
                logger.warning(f"Could not create synonym relationship: {term_name} -> {synonym}: {e}")
    
    logger.info("Created term relationships")

def show_example_queries():
    """Print example Cypher queries for the business glossary."""
    print("\nExample Cypher Queries for Business Glossary:")
    print("-" * 50)
    
    print("\n// Get all terms in the glossary")
    print("""
    MATCH (g:BusinessGlossary)-[:HAS_TERM]->(t:GlossaryTerm)
    RETURN t.name, t.definition
    """)
    
    print("\n// Find all tables related to a specific business term")
    print("""
    MATCH (t:GlossaryTerm {name: "Customer"})-[:MAPS_TO]->(table:Table)
    RETURN table.name
    """)
    
    print("\n// Find all business terms related to a specific table")
    print("""
    MATCH (t:GlossaryTerm)-[:MAPS_TO]->(table:Table {name: "customers"})
    RETURN t.name, t.definition
    """)
    
    print("\n// Find all metrics derived from a specific table")
    print("""
    MATCH (m:BusinessMetric)-[:DERIVED_FROM]->(table:Table {name: "sales"})
    RETURN m.name, m.definition
    """)
    
    print("\n// Find synonyms for a term")
    print("""
    MATCH (t:GlossaryTerm {name: "Customer"})-[:RELATED_TO {type: "synonym"}]->(s)
    RETURN s.name
    """)
    
    print("\n// Find all terms and their related terms")
    print("""
    MATCH (t:GlossaryTerm)-[r:RELATED_TO {type: "related_term"}]->(related)
    RETURN t.name, related.name
    """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize business glossary")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    
    args = parser.parse_args()
    
    # Run the normalization
    success = normalize_glossary(args.tenant_id)
    
    # Show example queries
    if success:
        show_example_queries()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)