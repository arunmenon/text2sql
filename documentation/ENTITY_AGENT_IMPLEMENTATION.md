# Entity Agent Implementation

The EntityAgent is a key component of the transparent text2sql reasoning architecture, responsible for identifying and resolving entities mentioned in natural language queries into database tables and concepts.

## Architecture

The EntityAgent follows a Strategy pattern with multiple resolution approaches:

```
           ┌─────────────┐
           │ EntityAgent │
           └──────┬──────┘
                  │ uses
                  ▼
       ┌────────────────────┐
       │EntityResolutionStrategy│
       └────────────┬───────┘
                    │
     ┌──────────────┼──────────────┐
     │              │              │
┌────▼────┐    ┌────▼────┐    ┌────▼────┐
│DirectTable│   │Glossary │   │Semantic │
│Match     │   │TermMatch│   │Concept  │
└──────────┘   └─────────┘   └─────────┘
```

## Key Features

1. **Multiple Resolution Strategies**:
   - **DirectTableMatchStrategy**: Tries to match entity names directly to database tables
   - **GlossaryTermMatchStrategy**: Resolves entities via business glossary terms
   - **SemanticConceptMatchStrategy**: Resolves entities using semantic concepts in the knowledge graph
   - **LLMBasedResolutionStrategy**: Uses a language model to resolve complex entities when other strategies fail

2. **Transparent Reasoning Process**:
   - Records each step of the entity recognition process
   - Captures the extraction methods, filtering, resolution strategies
   - Documents alternatives and confidence levels

3. **Knowledge Boundary Handling**:
   - Explicitly identifies unknown entities
   - Creates knowledge boundaries for the system to acknowledge limitations
   - Provides explanations and suggestions for each boundary

4. **Entity Extraction Techniques**:
   - Capitalization-based extraction
   - SQL keyword-based extraction
   - Noun phrase pattern recognition

5. **Intent-Aware Entity Filtering**:
   - Prioritizes entities based on the query intent
   - Special handling for aggregation intents

## Processing Flow

1. **Candidate Extraction**:
   - Extract potential entity mentions using multiple heuristic methods
   - Calculate initial confidence based on extraction method overlap

2. **Intent-Based Filtering**:
   - Filter candidates based on query intent (e.g., prioritize aggregatable entities for COUNT queries)
   - Adjust candidate set based on intent context

3. **Multi-Strategy Resolution**:
   - Apply all resolution strategies in parallel
   - Select best resolution based on confidence
   - Record all resolution attempts for transparency

4. **Knowledge Boundary Creation**:
   - Create explicit boundaries for unknown entities
   - Add explanation and suggestions for clarification

5. **Alternative Generation**:
   - Generate alternatives for ambiguous entities
   - Capture reasoning for each alternative

## Implementation Details

### Entity Extraction

The entity extraction uses pattern-based approaches:

- **Capitalization**: Extracts words that begin with capital letters
- **SQL Keywords**: Extracts terms following SQL-like keywords (e.g., "from", "select")
- **Noun Phrases**: Extracts combinations of adjectives and entity-like nouns

### Resolution Strategies

Each strategy implements a common interface but uses different techniques:

- **Direct Table Match**: Uses exact and fuzzy matching with table names
- **Glossary Term Match**: Resolves through business glossary terms with table mappings
- **Semantic Concept Match**: Resolves through concept nodes in the semantic graph
- **LLM-Based Resolution**: Uses LLM with schema context for complex resolution

### Knowledge Boundaries

When an entity can't be resolved with sufficient confidence:

1. A knowledge boundary is created with `BoundaryType.UNKNOWN_ENTITY`
2. Confidence level is set to reflect uncertainty (typically < 0.4)
3. Explanation details why the entity couldn't be resolved
4. Suggestions are added for potential clarification

## Integration with Transparent Engine

The EntityAgent integrates with the `TransparentQueryEngine`:

1. The engine initializes the EntityAgent with necessary dependencies
2. During query processing, the engine calls the agent's `process` method
3. Results are captured in the reasoning stream and context
4. Knowledge boundaries are incorporated into the response metadata
5. Entity resolution results are used to generate appropriate SQL

## Future Enhancements

Planned improvements to the EntityAgent:

1. **Enhanced NLP Techniques**: Incorporate part-of-speech tagging and dependency parsing
2. **Contextual Entity Resolution**: Consider surrounding entities for better disambiguation
3. **Learning from Feedback**: Improve resolution based on user feedback
4. **Cross-Entity Relationships**: Consider relationships between entities during resolution