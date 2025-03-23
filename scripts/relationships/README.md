# Relationship Management Scripts

This directory contains scripts for inferring and simulating relationships between database entities in the GraphAlchemy system.

## Available Scripts

### infer_relationships.py
Infers relationships between tables in a BigQuery dataset and stores them in Neo4j.

**Usage:**
```bash
python infer_relationships.py --tenant-id YOUR_TENANT_ID --project-id YOUR_GCP_PROJECT --dataset-id YOUR_DATASET [--min-confidence 0.7]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier for multi-tenancy support.
- `--project-id`: Required. Your Google Cloud project ID.
- `--dataset-id`: Required. The BigQuery dataset ID to analyze.
- `--min-confidence`: Optional. Minimum confidence threshold (0.0-1.0) for relationship detection. Default: 0.7

**Features:**
- Detects relationships using column name pattern matching
- Analyzes statistical column value overlaps
- Assigns confidence scores to detected relationships
- Filters relationships based on confidence threshold

**Prerequisites:**
- Google Cloud credentials configured
- Neo4j running with credentials in .env file
- BigQuery access permissions

### simulate_relationships.py
Simulates relationship inference for schema already loaded in Neo4j. Useful for testing and development with simulated schemas.

**Usage:**
```bash
python simulate_relationships.py --tenant-id YOUR_TENANT_ID [--schema-type walmart] [--min-confidence 0.7] [--use-llm]
```

**Parameters:**
- `--tenant-id`: Required. The tenant identifier.
- `--schema-type`: Optional. Type of schema to use (merchandising, walmart). Default: merchandising
- `--min-confidence`: Optional. Minimum confidence threshold (0.0-1.0). Default: 0.7
- `--use-llm`: Optional flag. Enable LLM-based relationship inference.

**Features:**
- Detects relationships using column name pattern matching
- Optionally uses LLM for semantic relationship inference
- Assigns confidence scores to detected relationships
- Provides detailed explanations for LLM-inferred relationships

**Prerequisites:**
- Neo4j running with credentials in .env file
- When using `--use-llm`, OpenAI API key configured in .env

## Common Issues

- **Authentication Errors**: Ensure Neo4j credentials are correctly set in your .env file
- **No Relationships Found**: Check that your schema has recognizable naming patterns (e.g., id fields with table prefix)
- **Low Confidence Scores**: Adjust the min-confidence threshold to include more potential relationships
- **LLM API Errors**: Check that your OpenAI API key is valid and has sufficient quota