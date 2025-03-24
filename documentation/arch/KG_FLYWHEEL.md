# Knowledge Graph Flywheel Implementation

## Overview

The Knowledge Graph Flywheel approach transforms our text2sql semantic graph builder from a linear enhancement pipeline into a cyclical system where each component enriches the others. This creates a self-reinforcing cycle of metadata improvement, leading to better SQL generation results.

## Implementation Status

### Step 1: Neighborhood-Aware Table Enhancement ✅

- Implemented the `TableNeighborhoodProvider` class to extract relationship context for tables
- Enhanced table description prompts to include relationship information
- Added table descriptions from related tables to provide richer context
- Integrated with the DirectEnhancementWorkflow for table description enhancement

### Step 2: Relationship-Enriched Column Descriptions ✅

- Created the `ColumnRelationshipProvider` class to provide relationship context for columns
- Enhanced column description prompts to leverage relationship information
- Added directionality awareness (outgoing vs. incoming references)
- Integrated with DirectEnhancementWorkflow for column description enhancement
- Added role classification (primary key, foreign key, or both)

### Step 3: Graph-Aware Business Glossary 🔜

- Next step in the Knowledge Graph Flywheel implementation
- Will enhance business glossary generation with awareness of the relationship graph
- Will create more accurate mappings between business terms and technical elements
- Will improve term relationship modeling based on the underlying data model

## Key Components

### TableNeighborhoodProvider

```python
class TableNeighborhoodProvider:
    """Provides relationship context for tables to enhance descriptions."""
    
    def __init__(self, neo4j_client):
        self.neo4j_client = neo4j_client
        
    def get_table_neighborhood(
        self, 
        tenant_id: str, 
        table_name: str,
        max_tables: int = 5,
        max_relationships_per_table: int = 3,
        include_descriptions: bool = True
    ) -> str:
        """Get a formatted summary of tables related to this one."""
        # Implementation details...
```

### ColumnRelationshipProvider

```python
class ColumnRelationshipProvider:
    """Provides relationship context for columns participating in relationships."""
    
    def __init__(self, neo4j_client):
        self.neo4j_client = neo4j_client
        
    def get_column_relationships(
        self, 
        tenant_id: str, 
        table_name: str, 
        column_name: str,
        include_table_info: bool = True,
        max_relationships: int = 5
    ) -> str:
        """Get a summary of relationships in which this column participates."""
        # Implementation details...
```

## Integration with DirectEnhancementWorkflow

The providers are integrated with the `DirectEnhancementWorkflow` class:

```python
def __init__(self, neo4j_client: Neo4jClient, llm_client: LLMClient, ...):
    # Initialize the table neighborhood provider for relationship context
    self.neighborhood_provider = TableNeighborhoodProvider(neo4j_client)
    
    # Initialize the column relationship provider for column context
    self.column_relationship_provider = ColumnRelationshipProvider(neo4j_client)
```

The workflow uses these providers in the description enhancement methods:

```python
def _build_description_enhancement_prompt(self, ...):
    # Get table relationship context
    table_relationships = self.neighborhood_provider.get_table_neighborhood(...)
    
    # Format variables for the prompt template
    prompt_vars = {
        # ...
        "table_relationships": table_relationships
    }

def _build_column_enhancement_prompt(self, ...):
    # Get column relationship context
    rel_context = self.column_relationship_provider.get_column_relationships(...)
    
    # Format variables for the prompt template
    prompt_vars = {
        # ...
        "column_relationships": column_relationships_text
    }
```

## Benefits

1. **Contextually Richer Descriptions**: Both table and column descriptions now incorporate relationship information, making them more useful for SQL generation.

2. **Improved Join Awareness**: Columns descriptions explicitly mention tables they can join with, improving accuracy of join path selection.

3. **Role Classification**: Columns are now aware of their role in the data model (primary key, foreign key, or both), leading to better query structure.

4. **Business Context Integration**: Table descriptions from related tables provide business context to improve semantic understanding.

5. **Cyclical Enhancement**: Each improved component feeds into the others, creating a self-reinforcing cycle of metadata improvement.

## Next Steps

1. **Complete Step 3**: Implement Graph-Aware Business Glossary generation
2. **Measure Improvement**: Conduct evaluation to quantify the impact on SQL generation accuracy
3. **Optimize Performance**: Profile and optimize relationship context retrieval for large schemas