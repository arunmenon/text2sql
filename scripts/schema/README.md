# Schema Management Scripts

This directory contains scripts for extracting, loading, and simulating database schemas in the GraphAlchemy system.

## Available Scripts

### extract_schema.py
Extracts a schema from BigQuery and stores it in Neo4j.

**Usage:**
```bash
python extract_schema.py --tenant-id YOUR_TENANT_ID --project-id YOUR_GCP_PROJECT --dataset-id YOUR_DATASET
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier for multi-tenancy support.
- `--project-id`: Required. Your Google Cloud project ID.
- `--dataset-id`: Required. The BigQuery dataset ID to extract.

**Prerequisites:**
- Google Cloud credentials configured (auth should be set up)
- Neo4j running with credentials in .env file
- BigQuery access permissions

### simulate_schema.py
Simulates a schema of specified type and loads it into Neo4j. Useful for testing and development.

**Usage:**
```bash
python simulate_schema.py --tenant-id YOUR_TENANT_ID --schema-type [merchandising|supply_chain|walmart] --schema-file [optional_path_to_schema_file]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.
- `--schema-type`: The type of schema to simulate (merchandising, supply_chain, or walmart).
- `--schema-file`: Optional. Path to a custom schema file (SQL DDL).

**Example:**
```bash
# Load built-in merchandising schema
python simulate_schema.py --tenant-id test_tenant --schema-type merchandising

# Load from custom schema file
python simulate_schema.py --tenant-id test_tenant --schema-type supply_chain --schema-file ./schemas/supply_chain_simple.sql
```

### load_schema_demo.py
Loads a demo schema for demonstration purposes.

**Usage:**
```bash
python load_schema_demo.py --tenant-id YOUR_TENANT_ID
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.

## Common Issues

- **Authentication Errors**: Ensure Neo4j credentials are correctly set in your .env file
- **Schema Not Found**: Verify the schema file path or ensure the built-in schema type is spelled correctly
- **BigQuery Access**: Ensure you have proper access to the specified BigQuery dataset