# GraphAlchemy Architecture Guide

GraphAlchemy transforms how users interact with databases by enabling natural language querying through semantic understanding. This document details the architecture, components, and flows that make this possible.

## System Overview

GraphAlchemy consists of two primary flows that work together seamlessly:

1. **Semantic Graph Construction**: Building an enriched knowledge graph of database schemas and business glossaries
2. **Text-to-SQL Transformation**: Converting natural language queries into precise SQL based on the semantic graph

## Semantic Graph Construction Flow

The Semantic Graph represents your database schema enriched with business context, relationships, and terminology. The construction flow involves:

### 1. Schema Extraction

The process begins by extracting schema information from one of three sources:

- **SQL DDL Files**: Parsed using our specialized DDL parser
- **BigQuery Schemas**: Extracted directly via BigQuery's metadata APIs
- **Existing Databases**: Schema introspection for already-deployed databases

The extractor captures tables, columns, data types, primary keys, and explicit foreign keys from these sources.

### 2. Knowledge Graph Construction

The raw schema is transformed into a rich knowledge graph through three key processes:

#### 2.1 Schema Representation

Tables, columns, and explicit relationships are represented as connected entities in the graph:
- Tables become nodes with their metadata (name, description)
- Columns become nodes connected to their tables
- Primary/foreign key relationships form explicit connections

#### 2.2 Relationship Inference

The system discovers implicit relationships between tables that aren't explicitly defined:

- **Pattern Matching**: Identifies relationships based on naming conventions (e.g., `order_id` in `order_items` likely references the `id` in `orders`)
- **Statistical Analysis**: Examines value overlaps to discover potential join paths
- **Semantic Inference**: Uses LLMs to detect relationships based on semantic understanding of columns and tables

#### 2.3 Enhanced Metadata Generation

Raw technical metadata is enhanced with business context:

- **Automatic Description Generation**: Creates human-readable descriptions for tables and columns
- **Business Purpose Identification**: Determines the business purpose of each entity
- **Concept Tagging**: Tags entities with business concepts (e.g., "Customer Information", "Financial Metric")

#### 2.4 Multi-Agent Business Glossary Generation

A sophisticated multi-agent system creates a comprehensive business glossary:

- **Term Generator Agent**: Creates initial business terms and definitions from schema analysis
- **Term Refiner Agent**: Enhances definitions, adds synonyms, and identifies relationships between terms
- **Term Validator Agent**: Validates technical mappings and assigns confidence scores

This staged approach ensures high-quality business terms with proper technical mappings.

### 3. Graph Storage

The knowledge graph is stored with the following entity types:

- **Tables**: Technical name, business description, and concept tags
- **Columns**: Names, data types, constraints, and business purpose
- **Relationships**: Both explicit and inferred connections with confidence scores
- **Business Terms**: Definitions, synonyms, and technical mappings
- **Composite Concepts**: Complex business concepts that span multiple entities

## Text-to-SQL Transformation Flow

When users issue natural language queries, the transformation process handles the conversion to SQL:

### 1. Natural Language Understanding

#### 1.1 Entity Recognition

Identifies potential entities, attributes, values, and operations in the natural language query. This includes:
- Technical terms that match schema elements
- Business terms that match glossary entries
- Values and operators for filtering
- Aggregation and grouping indicators

#### 1.2 Term Mapping

Maps identified terms to the semantic graph with sophisticated resolution:

- **Business-to-Technical Translation**: Maps business terms like "customer" to technical entities like "users" table
- **Weighted Term Mapping**: Assigns confidence scores when terms could map to multiple entities
- **Contextual Resolution**: Uses query context to disambiguate terms with multiple possible mappings

#### 1.3 Concept Resolution

Handles complex business concepts beyond simple term mapping:

- **Composite Concept Resolution**: Transforms concepts like "slow-moving inventory" into technical query components
- **Domain-Specific Metrics**: Translates business metrics like "inventory turnover" into calculation formulas
- **Implicit Filter Resolution**: Understands implicit filters like "reliable suppliers" (suppliers with high reliability scores)

#### 1.4 Intent Classification

Determines the fundamental query type:
- Selection queries (data retrieval)
- Aggregation queries (summaries, averages, counts)
- Comparison queries (filtering, ranking)
- Trend analysis (time-based patterns)

#### 1.5 Query Analysis

Deconstructs the query into structural elements:
- Selected attributes
- Filtering conditions
- Join requirements
- Sorting criteria
- Limit/offset parameters

### 2. SQL Transformation

#### 2.1 SQL Structure Generator

Creates the base SQL structure based on identified intent:
- SELECT clause with appropriate columns or expressions
- FROM clause with initial table
- Basic structure for more complex elements

#### 2.2 JOIN Path Calculator

Determines optimal join paths when multiple tables are involved:

- **Path Discovery**: Identifies possible paths between required tables
- **Path Optimization**: Selects the most efficient join path
- **Automatic JOIN Generation**: Constructs appropriate join clauses

#### 2.3 Filter Builder

Constructs WHERE clauses based on recognized conditions:

- **Condition Translation**: Converts natural language conditions to SQL predicates
- **Operator Selection**: Chooses appropriate operators (=, >, LIKE, IN, etc.)
- **Value Formatting**: Formats values properly for the SQL dialect

#### 2.4 Aggregation Handler

Manages aggregations and groupings:

- **Function Selection**: Chooses appropriate SQL functions (SUM, AVG, COUNT, etc.)
- **GROUP BY Generation**: Creates proper grouping clauses
- **HAVING Clause Construction**: Adds filtering for aggregated results

#### 2.5 SQL Validator

Ensures the generated SQL is valid and optimized:

- **Syntax Validation**: Checks for SQL syntax correctness
- **Semantic Validation**: Ensures query logic matches user intent
- **Performance Optimization**: Suggests improvements to query structure

### 3. Results & Interpretations

The system provides rich results beyond just the SQL:

- **SQL Query**: The primary SQL interpretation of the natural language input
- **Explanation**: A natural language explanation of what the query does
- **Alternative Interpretations**: Different SQL queries for ambiguous natural language
- **Query Confidence**: Confidence scores for the interpretation

## Enhanced Features

GraphAlchemy includes several innovative features that elevate its capabilities:

### 1. Weighted Term Mapping

When business terms could map to multiple technical entities, the system:
- Assigns probability-based confidence scores to each potential mapping
- Uses contextual clues to disambiguate
- Selects highest-confidence mappings for primary interpretation
- Provides alternative interpretations using other mappings

### 2. Composite Concept Resolution

For complex business concepts, the system:
- Maintains a dictionary of composite concepts and their technical implementations
- Recognizes when user queries include these concepts
- Expands concepts into their technical components
- Inserts appropriate joins, filters, and calculations

### 3. Domain-Specific Intelligence

The system adapts to specific business domains by:
- Encoding domain knowledge in the business glossary
- Recognizing domain-specific metrics and calculations
- Understanding industry-specific terminology
- Providing domain-appropriate query interpretations

### 4. Semantic Relationship Discovery

Beyond explicit database relationships, the system:
- Discovers implicit relationships through semantic understanding
- Identifies join paths that span multiple tables
- Recognizes relationships based on business logic
- Enables complex queries that traditional systems can't handle

### 5. Automatic Schema Enhancement

The system continuously improves metadata by:
- Learning from query patterns and user feedback
- Suggesting new business terms based on usage
- Refining relationship confidence scores
- Expanding composite concept definitions

## Component Interactions

The key to GraphAlchemy's effectiveness is the seamless interaction between components:

1. **Graph ↔ NLU**: The semantic graph provides the foundation for natural language understanding by mapping terms to entities

2. **Business Glossary ↔ Term Mapping**: Business glossary terms enable accurate mapping of natural language to technical components

3. **Relationship Inference ↔ JOIN Paths**: Inferred relationships enable discovering optimal join paths between tables

4. **Composite Concepts ↔ SQL Generation**: Composite concept resolution feeds directly into SQL structure generation

5. **LLM ↔ Graph Database**: LLM semantic understanding combines with structured graph data for best results

## Deployment Architecture

For production deployment, GraphAlchemy uses:

- **Graph Database**: Stores the semantic knowledge graph with relationships
- **API Layer**: Provides RESTful endpoints for queries and schema operations
- **Worker Pool**: Handles asynchronous operations like schema enhancement
- **LLM Integration**: Connects to LLM providers through secure API channels
- **Caching Layer**: Optimizes performance for repeated queries

By integrating these components and flows, GraphAlchemy delivers an intuitive, powerful system for translating natural language into database queries.