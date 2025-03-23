# Database Utility Scripts

This directory contains scripts for working with the Neo4j database in the GraphAlchemy system, including checking the database state, examining graph structure, and clearing/reloading data.

## Available Scripts

### check_neo4j.py
Checks the content of the Neo4j database after enhancement, showing tables, business glossary, concept tags, and relationships.

**Usage:**
```bash
python check_neo4j.py --tenant-id YOUR_TENANT_ID
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.

**Features:**
- Shows enhanced table descriptions
- Displays business glossary content
- Lists concept tags for tables and columns
- Shows enhanced column descriptions with business purpose
- Displays LLM-inferred relationships with confidence scores and explanations

**Prerequisites:**
- Neo4j running with credentials in .env file
- Database populated with schema and metadata

### check_graph.py
Checks the business glossary graph structure in Neo4j, showing node counts and relationships.

**Usage:**
```bash
python check_graph.py
```

**Features:**
- Counts BusinessGlossary, GlossaryTerm, and BusinessMetric nodes
- Shows sample glossary terms and their definitions
- Counts term relationships (RELATED_TO)
- Counts term-to-entity mappings (MAPS_TO)
- Shows sample mappings between business terms and technical entities

**Prerequisites:**
- Neo4j running with credentials in .env file
- Business glossary already generated and normalized

### clear_and_reload.py
Clears all data from the Neo4j database and reloads a schema with LLM enhancements.

**Usage:**
```bash
python clear_and_reload.py --tenant-id YOUR_TENANT_ID
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.

**Features:**
- Clears all data from the Neo4j database
- Loads the merchandising schema (using simulate_schema.py)
- Infers relationships using both pattern matching and LLM
- Runs the metadata enhancement workflow
- Shows useful Neo4j queries for inspection

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file
- simulate_schema.py and simulate_relationships.py scripts available

## Common Cypher Queries

For working with the Neo4j database directly, here are some useful Cypher queries:

```cypher
// View all nodes
MATCH (n) RETURN n LIMIT 100

// View business glossary
MATCH (g:BusinessGlossary) RETURN g.content

// View enhanced table descriptions
MATCH (t:Table) WHERE t.description_enhanced = true 
RETURN t.name, t.description

// View concept tags for tables
MATCH (t:Table) WHERE t.concept_tags IS NOT NULL 
RETURN t.name, t.concept_tags

// View semantic relationships discovered by LLM
MATCH (source:Column)-[r:LIKELY_REFERENCES]->(target:Column) 
WHERE r.detection_method = 'llm_semantic'
RETURN source.table_name, source.name, target.table_name, target.name, r.confidence, r.metadata_str

// View all relationships
MATCH (source:Column)-[r:LIKELY_REFERENCES]->(target:Column)
RETURN source.table_name, source.name, target.table_name, target.name, r.confidence, r.detection_method
```

## Troubleshooting

- **Connection Issues**: If you can't connect to Neo4j, ensure the service is running and your .env file has the correct URI, username, and password
- **Empty Query Results**: If queries return empty results, ensure the specified tenant ID is correct and that the database contains enhanced metadata
- **Permission Errors**: Ensure your Neo4j user has write permissions if you're trying to clear and reload data