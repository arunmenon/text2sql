# Transparent Agentic Architecture for Text2SQL

This document outlines the agentic architecture used in the transparent text2sql system, which provides full visibility into the reasoning process with explicit handling of knowledge boundaries.

## Architecture Overview

The architecture follows a multi-agent pipeline approach with specialized agents for different aspects of query processing:

```
┌────────────┐    ┌────────────┐    ┌─────────────────┐    ┌───────────┐
│IntentAgent │ -> │EntityAgent │ -> │RelationshipAgent│ -> │ SQLAgent  │
└────────────┘    └────────────┘    └─────────────────┘    └───────────┘
       │                │                   │                    │
       │                │                   │                    │
       ▼                ▼                   ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ReasoningStream                              │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                   ┌───────────────────────────┐
                   │     BoundaryRegistry      │
                   └───────────────────────────┘
```

Each agent is responsible for a specific aspect of query processing, and all agents contribute to a shared reasoning stream that captures the entire chain of thought.

## Core Components

### 1. ReasoningStream

The ReasoningStream captures the entire query processing journey as a series of stages and steps, with detailed evidence and confidence scores.

- **ReasoningStage**: A logical phase in the query processing (Intent Analysis, Entity Recognition, etc.)
- **ReasoningStep**: An individual step within a stage, with evidence and confidence
- **Alternative**: Alternative interpretations with confidence scores and reasons

### 2. Knowledge Boundaries

The system explicitly handles what it doesn't know through Knowledge Boundaries:

- **BoundaryType**: Different types of boundaries (UNKNOWN_ENTITY, AMBIGUOUS_INTENT, etc.)
- **KnowledgeBoundary**: Represents a specific limitation with confidence and suggestions
- **BoundaryRegistry**: Central registry for all boundaries encountered during processing

### 3. Agent Infrastructure

The agents follow a common interface for consistent behavior:

- **Agent**: Base class for all agents with common functionality
- **ResolutionStrategy**: Strategy pattern for different resolution approaches

## Specialized Agents

### 1. IntentAgent

Determines the primary purpose of the query.

- **Pattern Analysis**: Identifies intent signals in the query text
- **Multiple Intent Detection**: Checks for complex, multi-part queries
- **LLM-Based Classification**: Uses a language model for robust intent detection
- **Alternative Generation**: Proposes alternative interpretations for ambiguous queries

### 2. EntityAgent

Identifies and resolves entities mentioned in the query.

- **Resolution Strategies**:
  - **DirectTableMatch**: Matches entity names directly to tables
  - **GlossaryTermMatch**: Resolves via business glossary terms
  - **SemanticConceptMatch**: Uses knowledge graph concepts
  - **LLMBasedResolution**: Uses LLM for complex resolution

- **Entity Extraction**: Uses multiple methods to extract potential entities
  - Capitalization-based extraction
  - SQL keyword-based extraction
  - Noun phrase pattern recognition

- **Entity Filtering**: Intent-aware filtering of candidate entities

### 3. RelationshipAgent

Discovers relationships and join paths between entities.

- **Join Path Strategies**:
  - **DirectForeignKeyStrategy**: Uses direct FK relationships
  - **CommonColumnStrategy**: Identifies joins through common column patterns
  - **ConceptBasedJoinStrategy**: Uses semantic concepts for complex joins
  - **LLMBasedJoinStrategy**: LLM-based join path discovery

- **Join Tree Optimization**: Determines optimal join order for multi-table queries
- **Relationship Hint Extraction**: Analyzes query text for relationship clues

### 4. SQLAgent

Generates SQL based on the collected information.

- **Context-Aware Generation**: Uses intent, entities, and relationships
- **SQL Validation**: Validates generated SQL against the schema
- **Alternative Generation**: Creates alternative interpretations for ambiguous queries
- **Fallback Mechanisms**: Provides simplified SQL when complex generation fails

## Information Flow

1. **Query Input**: Natural language query enters the system
2. **Intent Analysis**: IntentAgent determines query purpose and ambiguities
3. **Entity Recognition**: EntityAgent identifies and resolves database entities
4. **Relationship Discovery**: RelationshipAgent finds join paths between entities
5. **SQL Generation**: SQLAgent creates the final SQL query
6. **Response Assembly**: TransparentQueryEngine assembles the response with all reasoning

## Knowledge Boundary Handling

Throughout the process, the system explicitly identifies limitations:

1. **Unknown Entities**: Terms that can't be mapped to the database
2. **Ambiguous Intents**: Queries with multiple possible interpretations
3. **Missing Relationships**: Tables that can't be joined reliably
4. **Invalid SQL**: Generated SQL that doesn't validate against the schema

For each boundary, the system:
- Records confidence scores and explanations
- Provides suggestions for resolution
- Documents alternative interpretations
- Indicates when clarification is required

## Benefits of the Architecture

1. **Full Transparency**: Every step of the reasoning is captured and exposed
2. **Explicit Limitations**: The system clearly communicates what it knows and doesn't know
3. **Multiple Resolution Approaches**: Each agent uses multiple strategies for robust processing
4. **Alternative Interpretations**: The system proposes alternatives for ambiguous queries
5. **Detailed Confidence Scoring**: Every decision includes confidence metrics
6. **Modular Design**: Easy to extend with new agents or strategies

## Future Extensions

The architecture is designed for easy extension:

1. **AttributeAgent**: For more detailed handling of filters, aggregations, and attributes
2. **ExplanationAgent**: For generating natural language explanations of SQL
3. **FeedbackAgent**: For learning from user feedback
4. **OptimizationAgent**: For query optimization and performance tuning