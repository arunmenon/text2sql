# Business Glossary Scripts

This directory contains scripts for generating and managing business glossaries and concept taxonomies in the GraphAlchemy system.

## Available Scripts

### generate_glossary.py
Generates a comprehensive business glossary from database schema using LLM and stores it in Neo4j.

**Usage:**
```bash
python generate_glossary.py --tenant-id YOUR_TENANT_ID [--api-key YOUR_API_KEY]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier for multi-tenancy support.
- `--api-key`: Optional. Your OpenAI API key. If not provided, it will use the key from your .env file.

**Features:**
- Analyzes all tables and columns to create business terms
- Provides definitions in plain business language
- Includes technical mappings to tables/columns
- Identifies business metrics and KPIs derivable from data
- Includes synonyms and related terms
- Outputs glossary to terminal and stores in Neo4j

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file or provided as parameter

### normalize_glossary.py
Converts a text-based business glossary into a normalized graph structure in Neo4j with proper term relationships.

**Usage:**
```bash
python normalize_glossary.py --tenant-id YOUR_TENANT_ID
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.

**Features:**
- Parses existing text glossary into structured data
- Creates GlossaryTerm nodes for each business term
- Creates BusinessMetric nodes for KPIs
- Establishes proper relationships between terms and technical entities
- Creates relationships between related terms and synonyms
- Provides example Cypher queries for working with the glossary

**Prerequisites:**
- Neo4j running with credentials in .env file
- Existing business glossary content already in Neo4j

### generate_concept_tags.py
Generates concept tags for tables and columns to enhance semantic understanding.

**Usage:**
```bash
python generate_concept_tags.py --tenant-id YOUR_TENANT_ID [--api-key YOUR_API_KEY]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.
- `--api-key`: Optional. Your OpenAI API key. If not provided, it will use the key from your .env file.

**Features:**
- Generates semantic concept tags for tables (e.g., Transaction, Customer Profile)
- Generates semantic concept tags for columns (e.g., Personal Identifier, Timestamp)
- Returns structured JSON with tagging information
- Stores tags in Neo4j for later querying and visualization
- Enhances natural language understanding capabilities

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file or provided as parameter

## Running the Complete Glossary Workflow

For best results, run these scripts in sequence:

1. First, generate the initial business glossary:
   ```bash
   python generate_glossary.py --tenant-id YOUR_TENANT_ID
   ```

2. Then, normalize it into a proper graph structure:
   ```bash
   python normalize_glossary.py --tenant-id YOUR_TENANT_ID
   ```

3. Finally, enhance with concept tags:
   ```bash
   python generate_concept_tags.py --tenant-id YOUR_TENANT_ID
   ```

## Common Issues

- **API Rate Limiting**: If you encounter OpenAI API rate limits, add delays between requests or use a different API key
- **JSON Parsing Errors**: The LLM might occasionally generate malformed JSON; the scripts have error handling to retry
- **Missing Mappings**: If tables or columns can't be found, check that the names match exactly with what's in Neo4j