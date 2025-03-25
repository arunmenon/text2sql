# Text2SQL Project

A system that enhances database schema metadata using LLMs and builds a business glossary with proper graph structure.

## Overview

The Text2SQL project creates a powerful database schema documentation system that uses:
- Natural language queries to explore database schema
- LLM-enhanced metadata for tables and columns
- Enhanced business glossary with multi-agent generation approach
- Weighted term mapping for better ambiguity resolution
- Composite concept resolution for complex business terminology
- Concept tagging for semantic understanding
- Relationship inference

## Installation

1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the environment: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`

## Configuration

Create a `.env` file with the following variables:

```
NEO4J_URI=neo4j://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=password
OPENAI_API_KEY=your-api-key
LLM_MODEL=gpt-4o
USE_ENHANCED_GLOSSARY=true  # Enable enhanced business glossary generation
```

## Usage

The project provides a unified CLI for all operations:

```bash
# Load schema into Neo4j
./cli.py schema load --tenant-id test_tenant --schema-type merchandising

# Detect relationships between tables
./cli.py schema relationships --tenant-id test_tenant --use-llm

# Run the enhancement workflow (with graph structure)
./cli.py enhance run --tenant-id test_tenant --direct

# Generate enhanced business glossary with multi-agent approach
./cli.py enhance run --tenant-id test_tenant --direct --use-enhanced-glossary

# Generate and normalize business glossary (legacy approach)
./cli.py enhance glossary --tenant-id test_tenant
./cli.py enhance normalize --tenant-id test_tenant

# Generate concept tags
./cli.py enhance concepts --tenant-id test_tenant

# Check database content and graph structure
./cli.py check database --tenant-id test_tenant
./cli.py check graph --tenant-id test_tenant

# Clear database and reload everything
./cli.py utils clear --tenant-id test_tenant
```

## Project Structure

```
/text2sql/
  ├── src/                  # Core source code
  │   ├── text2sql/         # Main application code
  │   │   ├── components/   # Core components
  │   │   ├── enhanced_glossary/ # Enhanced business glossary generation
  │   │   │   ├── agents/   # Specialized agents for term generation
  │   │   │   ├── config/   # Externalized prompts and schemas
  │   │   │   └── utils/    # Utilities for prompt loading, etc.
  │   ├── schema_extraction/ # Schema extraction modules
  │   ├── graph_storage/    # Neo4j integration
  ├── tools/                # Command-line tools
  │   ├── schema/           # Schema-related tools
  │   ├── enhance/          # Enhancement tools
  │   ├── check/            # Checking tools
  │   └── utils/            # Utility tools
  ├── cli.py                # Main CLI entry point
  ├── setup.py              # Package setup
  └── requirements.txt      # Dependencies
```

## Enhanced Business Glossary

The enhanced business glossary generator uses a multi-agent approach:

1. **Term Generator Agent** - Creates initial business terms from schema
2. **Term Refiner Agent** - Improves definitions and adds synonyms
3. **Term Validator Agent** - Validates technical mappings and adds confidence scores

Key features:
- Weighted term mapping for resolving ambiguities
- Composite concept resolution for complex business terms
- Externalized prompts and schemas for better maintainability
- Proper fallback mechanisms in case of failures

## Transparent Agentic Architecture

The text2sql system uses a fully transparent, agentic architecture that provides complete visibility into the reasoning process:

1. **ReasoningStream**: Captures the entire query processing journey with stages, steps, evidence, and alternatives.
2. **Knowledge Boundaries**: Explicitly handles system limitations with clear communication about what the system doesn't know.
3. **Multi-Agent Pipeline**: Specialized agents for different aspects of query understanding:

   - **IntentAgent**: Determines query purpose and detects ambiguous/multiple intents
     - Pattern-based intent recognition
     - LLM-based classification
     - Multiple intent detection
     - Alternative generation

   - **EntityAgent**: Identifies and resolves database entities with multiple resolution strategies
     - DirectTableMatch: Matches names directly to tables
     - GlossaryTermMatch: Resolves via business glossary terms
     - SemanticConceptMatch: Resolves via knowledge graph concepts
     - LLMBasedResolution: Uses LLM for complex entity resolution

   - **RelationshipAgent**: Discovers relationships and join paths between entities
     - DirectForeignKeyStrategy: Uses schema-defined relationships
     - CommonColumnStrategy: Finds joins through column naming patterns
     - ConceptBasedJoinStrategy: Uses semantic concepts for complex joins
     - LLMBasedJoinStrategy: LLM-based join path discovery

   - **SQLAgent**: Generates SQL queries using LLM with comprehensive context
     - Context-aware generation with all collected information
     - SQL validation against schema
     - Alternative generation for ambiguous queries
     - Fallback mechanisms for complex scenarios

This architecture enables:
- Full transparency into query processing
- Explicit handling of edge cases 
- Clear communication of system limitations
- Improved query understanding through multiple specialized agents
- LLM-based SQL generation with complete context

For more details, see [Transparent Agentic Architecture](documentation/TRANSPARENT_AGENTIC_ARCHITECTURE.md)