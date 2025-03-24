# Table Context Generation

This module provides tools for enriching database table and column metadata with LLM-generated context.

## Components

### Workflows

- `enhancement_workflow.py` - Main workflow for enhancing schema metadata
- `direct_enhancement.py` - Direct enhancement of schema metadata

### Models

Data models for table context generation.

### Utils

Utility functions for table context generation.

## Usage

### From Command Line

```bash
python -m src.table-context-gen.cli --tenant-id YOUR_TENANT_ID --api-key YOUR_API_KEY
```

### From Python

```python
from src.table-context-gen.workflows.enhancement_workflow import SchemaEnhancementWorkflow
from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient

# Initialize clients
neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
llm_client = LLMClient(api_key=api_key, model=model)

# Create and run workflow
workflow = SchemaEnhancementWorkflow(neo4j_client, llm_client)
await workflow.run(tenant_id, dataset_id)
```