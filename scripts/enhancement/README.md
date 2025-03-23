# Metadata Enhancement Scripts

This directory contains scripts for enhancing database metadata using LLMs in the GraphAlchemy system, including business glossary generation and relationship inference.

## Available Scripts

### run_direct_enhancement.py
Runs the direct enhancement workflow that creates a business glossary with a proper graph structure, supporting weighted term mapping and composite concept resolution.

**Usage:**
```bash
python run_direct_enhancement.py --tenant-id YOUR_TENANT_ID [--use-enhanced-glossary] [--use-legacy-glossary]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.
- `--use-enhanced-glossary`: Optional flag. Use enhanced business glossary generator with multi-agent approach (default if neither flag is specified).
- `--use-legacy-glossary`: Optional flag. Use legacy business glossary generator.

**Features:**
- Creates a business glossary with proper graph structure
- Supports enhanced multi-agent glossary generation with term generator, refiner, and validator
- Enables weighted term mapping for better ambiguity resolution
- Provides composite concept resolution for complex business terms
- Configurable via CLI flags or environment variables

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file
- Schema already loaded in Neo4j

### run_enhancement.py
Runs the schema enhancement workflow with LLM to enhance schema metadata and generate a business glossary.

**Usage:**
```bash
python run_enhancement.py --tenant-id YOUR_TENANT_ID [--api-key YOUR_API_KEY]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.
- `--api-key`: Optional. Your OpenAI API key. If not provided, uses the key from .env file.

**Features:**
- Loads schema and relationship information from Neo4j
- Enhances table and column metadata using LLM
- Generates business glossary
- Adds detailed descriptions and business purposes
- Stores enhanced metadata back in Neo4j

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file or provided as parameter
- Schema already loaded in Neo4j

### run_walmart_discovery.py
Runs the full schema discovery process for the Walmart retail schema, including extraction, relationship inference, and join path exploration.

**Usage:**
```bash
python run_walmart_discovery.py --tenant-id YOUR_TENANT_ID [--use-llm]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.
- `--use-llm`: Optional flag. Enable LLM-based relationship inference.

**Features:**
- Performs schema extraction from the Walmart retail schema
- Runs relationship inference using pattern matching and optionally LLM
- Analyzes relationship quality against expected key relationships
- Explores join paths between tables using different strategies
- Provides detailed analytics on discovery rate and path quality

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file (if using LLM)
- simulate_schema.py and simulate_relationships.py scripts available

## Common Issues

- **API Rate Limiting**: If you encounter OpenAI API rate limits, add delays between requests or use a different API key
- **Memory Usage**: When working with large schemas, ensure Neo4j has sufficient memory allocated
- **Missing Tables**: If tables are not found, check that the schema was properly loaded for the specified tenant ID
- **Mixed Enhancement Results**: Different LLM models produce varying quality of enhancements; GPT-4 generally gives best results