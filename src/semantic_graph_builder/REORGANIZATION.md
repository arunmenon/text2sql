# Code Reorganization Documentation

## Overview

This document describes the major code reorganization that moved several key components into the `semantic_graph_builder` module, creating a more cohesive structure for the semantic graph components.

## Changes Made

1. Created a new `semantic_graph_builder` module to house all semantic graph components

2. Moved the following modules under `semantic_graph_builder`:
   - `schema_extraction` (from `src/schema_extraction`)
   - `relationship_discovery` (renamed from `src/relationship_inference`)
   - `table_context_gen` (from `src/table-context-gen` and `src/text2sql/metadata_enhancement.py`)
   - `enhanced_glossary` (from `src/text2sql/enhanced_glossary`)

3. Consolidated prompt templates:
   - Moved all prompts to `semantic_graph_builder/prompts/`
   - Updated the prompt loader to use this centralized location

4. Created a centralized `utils` module with common functionality:
   - `prompt_loader.py` for loading prompt templates

## Import Path Changes

When migrating code that depends on these modules, update imports as follows:

```python
# Old imports
from src.schema_extraction.bigquery.extractor import BigQueryExtractor
from src.relationship_inference.llm_inference.relationship_analyzer import LLMRelationshipAnalyzer
from src.text2sql.metadata_enhancement import SchemaEnhancementWorkflow
from src.text2sql.enhanced_glossary.agents.term_generator import TermGeneratorAgent

# New imports
from src.semantic_graph_builder.schema_extraction.bigquery.extractor import BigQueryExtractor
from src.semantic_graph_builder.relationship_discovery.llm_inference.relationship_analyzer import LLMRelationshipAnalyzer
from src.semantic_graph_builder.table_context_gen.workflows.enhancement_workflow import SchemaEnhancementWorkflow
from src.semantic_graph_builder.enhanced_glossary.agents.term_generator import TermGeneratorAgent
```

## Module Structure

```
src/semantic_graph_builder/
├── enhanced_glossary/       # Business glossary generation
├── prompts/                 # Centralized prompt templates
├── relationship_discovery/  # Relationship detection
├── schema_extraction/       # Schema extraction
├── table_context_gen/       # Table context enhancement
└── utils/                   # Shared utilities
```

## Rationale

These components collectively build the semantic graph that powers the text2sql engine. By organizing them into a cohesive module:

1. The data flow between components is more explicit
2. Shared utilities can be centralized
3. Prompt templates are consistently organized
4. The codebase better reflects the architecture
5. Future development can maintain consistent patterns