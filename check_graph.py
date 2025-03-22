#!/usr/bin/env python3
"""
Check Neo4j Graph Structure for Business Glossary

This script checks the business glossary graph structure in Neo4j.
"""
import os
from dotenv import load_dotenv
from src.graph_storage.neo4j_client import Neo4jClient

# Load environment variables
load_dotenv()

# Get Neo4j connection details
neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

# Initialize Neo4j client
client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)

# Check BusinessGlossary node and its relationships
print("\n=== Business Glossary Structure ===")
glossary_result = client._execute_query("""
MATCH (g:BusinessGlossary)
RETURN COUNT(g) AS glossary_count
""")
glossary_count = glossary_result[0]["glossary_count"] if glossary_result else 0
print(f"Number of BusinessGlossary nodes: {glossary_count}")

# Check GlossaryTerm nodes
term_result = client._execute_query("""
MATCH (t:GlossaryTerm)
RETURN COUNT(t) AS term_count
""")
term_count = term_result[0]["term_count"] if term_result else 0
print(f"Number of GlossaryTerm nodes: {term_count}")

# Show some terms
if term_count > 0:
    print("\nSample GlossaryTerms:")
    terms_result = client._execute_query("""
    MATCH (g:BusinessGlossary)-[:HAS_TERM]->(t:GlossaryTerm)
    RETURN t.name, t.definition
    LIMIT 5
    """)
    for record in terms_result:
        name = record.get("t.name", "Unknown")
        definition = record.get("t.definition", "No definition") 
        print(f"  - {name}: {definition[:100]}...")

# Check term relationships
term_rel_result = client._execute_query("""
MATCH (t1:GlossaryTerm)-[r:RELATED_TO]->(t2:GlossaryTerm)
RETURN COUNT(r) AS relationship_count
""")
term_rel_count = term_rel_result[0]["relationship_count"] if term_rel_result else 0
print(f"\nNumber of term relationships: {term_rel_count}")

# Check term-to-table mappings
mapping_result = client._execute_query("""
MATCH (t:GlossaryTerm)-[r:MAPS_TO]->(entity)
RETURN COUNT(r) AS mapping_count
""")
mapping_count = mapping_result[0]["mapping_count"] if mapping_result else 0
print(f"Number of term-to-entity mappings: {mapping_count}")

# Show sample mappings
if mapping_count > 0:
    print("\nSample Mappings:")
    mappings_result = client._execute_query("""
    MATCH (t:GlossaryTerm)-[r:MAPS_TO]->(entity)
    RETURN t.name, labels(entity) AS entity_type, entity.name
    LIMIT 5
    """)
    for record in mappings_result:
        term_name = record.get("t.name", "Unknown")
        entity_type = record.get("entity_type", ["Unknown"])[0]
        entity_name = record.get("entity.name", "Unknown")
        print(f"  - {term_name} → {entity_type}: {entity_name}")

# Close Neo4j client
client.close()