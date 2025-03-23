# Semantic Graph-Based Text2SQL Architecture

## Comprehensive Flow Diagram

```mermaid
flowchart TB
    subgraph "Data Sources"
        DS1[SQL DDL Files]
        DS2[BigQuery Schemas]
        DS3[Existing Databases]
    end

    subgraph "Schema Extraction Layer"
        SE1[DDL Parser]
        SE2[BigQuery Extractor]
        SE3[Schema Loader]
    end

    DS1 --> SE1
    DS2 --> SE2
    DS3 --> SE3

    subgraph "Knowledge Graph Construction"
        KG1[Schema Representation]
        KG2[Relationship Inference]
        KG3[Enhanced Metadata]
        
        subgraph "Multi-Agent Glossary Generation" 
            MAGG1[Term Generator Agent]
            MAGG2[Term Refiner Agent]
            MAGG3[Term Validator Agent]
            
            MAGG1 --> MAGG2
            MAGG2 --> MAGG3
        end
        
        KG1 --> KG2
        KG2 --> KG3
        MAGG3 --> KG3
    end

    SE1 --> KG1
    SE2 --> KG1 
    SE3 --> KG1

    subgraph "Graph Storage"
        GS1[Neo4j Graph Database]
        
        subgraph "Graph Entities"
            GE1[Tables]
            GE2[Columns]
            GE3[Relationships]
            GE4[Business Terms]
            GE5[Composite Concepts]
        end
        
        GS1 --- GE1
        GS1 --- GE2
        GS1 --- GE3
        GS1 --- GE4
        GS1 --- GE5
    end

    KG3 --> GS1

    subgraph "Natural Language Understanding"
        NLU1[Entity Recognition]
        NLU2[Term Mapping]
        NLU3[Concept Resolution]
        NLU4[Intent Classification]
        NLU5[Query Analysis]
        
        NLU1 --> NLU2
        NLU2 --> NLU3
        NLU3 --> NLU4
        NLU4 --> NLU5
    end

    GS1 --> NLU1

    subgraph "Text2SQL Transformation"
        TS1[SQL Structure Generator]
        TS2[JOIN Path Calculator]
        TS3[Filter Builder]
        TS4[Aggregation Handler]
        TS5[SQL Validator]
        
        TS1 --> TS2
        TS2 --> TS3
        TS3 --> TS4
        TS4 --> TS5
    end

    NLU5 --> TS1

    subgraph "Results & Interpretations"
        RI1[SQL Query]
        RI2[Explanation]
        RI3[Alternative Interpretations]
        RI4[Query Confidence]
    end

    TS5 --> RI1
    TS5 --> RI2
    TS5 --> RI3
    TS5 --> RI4

    subgraph "Enhanced Features"
        EF1["Weighted Term Mapping
        (Resolves Ambiguity)"]
        
        EF2["Composite Concept Resolution
        (e.g., 'slow-moving inventory')"]
        
        EF3["Domain-Specific Business Metrics
        (e.g., 'inventory turnover')"]
        
        EF4["Semantic Relationship Discovery
        (Beyond FK/PK)"]
        
        EF5["Automatic Schema Enhancement
        (Enrich metadata)"]
    end
    
    EF1 -.-> NLU2
    EF2 -.-> NLU3
    EF3 -.-> TS3
    EF4 -.-> KG2
    EF5 -.-> KG3

    class EF1,EF2,EF3,EF4,EF5 novelty
    class MAGG1,MAGG2,MAGG3 highlight
    
    classDef novelty fill:#f9a,stroke:#333,stroke-width:4px
    classDef highlight fill:#af9,stroke:#333,stroke-width:4px
```

## Key Value Propositions

### 1. Multi-Agent Business Glossary Generation
- **Problem Solved**: Traditional systems struggle with business terminology that differs from technical schema names
- **Novel Approach**: Uses specialized agents (Generator → Refiner → Validator) working in sequence to:
  - Generate initial business terms from schema
  - Refine and enrich definitions and mappings
  - Validate technical mappings with confidence scores
- **Business Value**: Enables business users to query using familiar terminology without technical knowledge

### 2. Composite Concept Resolution
- **Problem Solved**: Simple keyword mapping fails to handle complex business concepts
- **Novel Approach**: Recognizes and resolves composite business concepts like "slow-moving inventory" or "reliable suppliers"
- **Business Value**: Users can express queries naturally using domain concepts rather than technical specifications

### 3. Graph-Based Schema Understanding
- **Problem Solved**: Traditional text2sql misses implicit relationships and semantic meaning
- **Novel Approach**: Stores schema as rich knowledge graph with semantic relationship inference
- **Business Value**: Enables complex multi-table joins without explicit relationship modeling

### 4. Domain-Specific Intelligence
- **Problem Solved**: Generic SQL generation lacks domain context for meaningful queries
- **Novel Approach**: Encodes domain knowledge as part of the business glossary and concept resolution
- **Business Value**: Generates more relevant and accurate SQL for specific business domains

### 5. Weighted Term Mapping
- **Problem Solved**: Ambiguity when business terms map to multiple tables/columns
- **Novel Approach**: Assigns confidence scores to term mappings, enabling probabilistic resolution
- **Business Value**: More accurate query resolution, especially for terms with different meanings across contexts

## System Unique Differentiators

1. **Transparent Processing Pipeline**: Each step from NL understanding to SQL generation is explainable and traceable

2. **Self-Enriching Knowledge Graph**: System automatically enhances metadata through usage patterns

3. **Hybrid LLM/Graph Approach**: Combines semantic understanding of LLMs with structured knowledge in graph database

4. **Domain Adaptation**: Framework easily adapts to new business domains through glossary generation

5. **Confidence Scoring**: Provides confidence levels for mappings and interpretations, enabling better decision support