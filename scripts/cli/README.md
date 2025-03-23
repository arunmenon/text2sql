# Command Line Interface Scripts

This directory contains command-line interfaces for interacting with the GraphAlchemy system.

## Available Scripts

### cli.py
The main command-line interface for GraphAlchemy, providing a comprehensive CLI for all operations.

**Usage:**
```bash
# Schema management
./cli.py schema load --tenant-id YOUR_TENANT_ID [--schema-type merchandising]
./cli.py schema relationships --tenant-id YOUR_TENANT_ID [--use-llm]

# Enhancement workflows
./cli.py enhance run --tenant-id YOUR_TENANT_ID [--direct] [--use-enhanced-glossary]
./cli.py enhance glossary --tenant-id YOUR_TENANT_ID
./cli.py enhance normalize --tenant-id YOUR_TENANT_ID
./cli.py enhance concepts --tenant-id YOUR_TENANT_ID

# Database checking
./cli.py check database --tenant-id YOUR_TENANT_ID
./cli.py check graph --tenant-id YOUR_TENANT_ID

# Utilities
./cli.py utils clear --tenant-id YOUR_TENANT_ID
```

**Features:**
- Unified CLI for all operations in the GraphAlchemy system
- Hierarchical command structure with intuitive syntax
- Support for all schema management operations
- Support for enhancement workflows with configurable options
- Database checking and utility commands
- Consistent argument format across all commands

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file for LLM operations
- Python dependencies installed

### text2sql_cli.py
A specialized CLI tool for running natural language to SQL conversions.

**Usage:**
```bash
./text2sql_cli.py --query "Show me all products with low inventory" --tenant-id YOUR_TENANT_ID
./text2sql_cli.py --file query.txt --tenant-id YOUR_TENANT_ID [--output results.json]
```

**Parameters:**
- `--query`: The natural language query to process.
- `--file`: Path to a file containing the query (alternative to --query).
- `--tenant-id`: Required. The tenant identifier.
- `--output`: Optional. Output file to write results to (JSON format).

**Features:**
- Processes natural language queries and converts them to SQL
- Takes input from command line or a file
- Displays generated SQL and explanations in the terminal
- Optionally writes detailed results to a JSON file
- Uses the TextToSQLEngine from the core library

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file
- Schema and business glossary already loaded for the tenant

## Example Workflows

### Basic Schema and Enhancement

```bash
# Load a schema
./cli.py schema load --tenant-id demo_tenant --schema-type merchandising

# Infer relationships with LLM
./cli.py schema relationships --tenant-id demo_tenant --use-llm

# Run the direct enhancement with enhanced glossary
./cli.py enhance run --tenant-id demo_tenant --direct --use-enhanced-glossary

# Check the results
./cli.py check database --tenant-id demo_tenant
./cli.py check graph --tenant-id demo_tenant
```

### Testing Text2SQL

```bash
# After setting up the schema and glossary
./text2sql_cli.py --query "Show me all products with low inventory" --tenant-id demo_tenant

# Process multiple queries from a file
./text2sql_cli.py --file test_queries.txt --tenant-id demo_tenant --output results.json
```

## Troubleshooting

- **Command Not Found**: Ensure that the script is executable (`chmod +x cli.py`)
- **Module Not Found**: Make sure you run the commands from the project root directory
- **Authentication Errors**: Check that your .env file has the correct Neo4j and API credentials
- **Query Processing Errors**: Ensure that your schema is properly loaded for the specified tenant