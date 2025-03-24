# Semantic Graph Builder

A framework for constructing rich semantic graph representations of database schemas,
including metadata enhancement, relationship discovery, and business glossary construction.

## Structure

### schema_extraction

Tools for extracting database schema metadata from various sources including SQL DDL, BigQuery, and simulation data.

### relationship_discovery

Detects relationships between database tables and columns using multiple methods:
- LLM-based semantic analysis
- Name pattern matching
- Statistical data analysis

### table_context_gen

Enhances database schema metadata with rich context information:
- Table descriptions
- Column descriptions
- Business purpose
- Data characteristics

### enhanced_glossary

Builds and manages business glossary terms:
- Term generation
- Term refinement
- Term validation
- Schema mapping

### prompts

Centralized prompt templates for LLM interactions:
- Relationship analysis
- Term generation
- Term refinement
- Term validation

### utils

Shared utilities:
- Prompt loading
- Schema processing
- Formatting

## Usage

### Single Entry Point

The semantic graph builder now has a single, comprehensive entry point script:

```bash
python build_semantic_graph.py --tenant-id YOUR_TENANT_ID --schema-file schemas/walmart_facilities_complete.sql --csv-dir data/datasets
```

This script handles the complete workflow:
1. Clearing the Neo4j database
2. Loading schema from SQL or simulation
3. Inferring relationships using pattern, statistical, and LLM methods
4. Enhancing metadata with descriptions
5. Generating business glossary and concepts
6. Verifying the graph structure

For full usage details, see the [scripts/README.md](./scripts/README.md) file.

### Purpose

The semantic graph builder constructs a rich semantic graph in Neo4j that represents:
1. Database schema (tables, columns, relationships)
2. Business context (descriptions, purpose)
3. Business glossary (terms, definitions, mappings)

This graph is then used by the text2sql engine to translate natural language to SQL.