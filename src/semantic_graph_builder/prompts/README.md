# Centralized Prompt Templates

This directory contains all the prompt templates used in the semantic graph builder.

## Categories

### Relationship Discovery
- `relationship_analyzer.txt` - Used to analyze potential relationships between tables

### Table Context Generation
- `table_description_enhancement.txt` - Enhances table descriptions
- `column_description_enhancement.txt` - Enhances column descriptions
- `semantic_relationship_analysis.txt` - Analyzes semantic relationships between tables
- `business_glossary_generation.txt` - Generates a business glossary
- `domain_analysis.txt` - Analyzes the business domain
- `concept_tagging.txt` - Generates concept tags for tables and columns
- `sample_query_generation.txt` - Generates sample SQL queries

### Enhanced Glossary
- `term_generator.txt` - Generates business glossary terms
- `term_refiner.txt` - Refines business glossary terms
- `term_validator.txt` - Validates business glossary terms

## Usage

Prompts are loaded using the centralized `PromptLoader` utility:

```python
from src.semantic_graph_builder.utils.prompt_loader import PromptLoader

# Initialize the prompt loader
prompt_loader = PromptLoader()

# Format a prompt with variables
prompt = prompt_loader.format_prompt(
    "table_description_enhancement",
    table_name="customers",
    original_description="Customer data",
    columns_text="- id: INT\n- name: VARCHAR\n- email: VARCHAR"
)
```