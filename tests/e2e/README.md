# End-to-End Integration Tests

This directory contains scripts for running end-to-end integration tests of the GraphAlchemy system. These scripts test all components working together in complete workflows, from schema extraction to natural language query execution.

## Available Test Scripts

### `run_supplychain_test.sh`

This script runs a complete end-to-end test of the GraphAlchemy system using the supply chain domain:

1. **Database Initialization**: Clears any existing data and prepares the graph database
2. **Schema Loading**: Loads the supply chain schema from SQL DDL
3. **Relationship Inference**: Detects and establishes relationships between tables using both pattern matching and LLM
4. **Metadata Enhancement**: Runs the direct enhancement workflow with the enhanced business glossary
5. **Database Verification**: Checks the database content and graph structure
6. **Query Testing**: Executes a suite of test queries specific to the supply chain domain

#### Running the Supply Chain Test

```bash
cd /path/to/graphalchemy
chmod +x tests/e2e/run_supplychain_test.sh
./tests/e2e/run_supplychain_test.sh
```

The test script will execute six steps sequentially, providing output at each stage. Results from the query tests are saved in the `results/` directory.

## Test Queries

The supply chain tests include several families of test queries:

1. **Inventory Basics**: Simple queries for selecting, filtering, and ordering inventory data
2. **Inventory Analytics**: Aggregations and business metric calculations for inventory
3. **Purchasing Operations**: Queries that examine purchase orders and supplier performance
4. **Shipping Logistics**: Analysis of shipments, carriers, and transit metrics
5. **Business Term Translation**: Tests for translating business terms into technical schema elements
6. **Composite Concept Queries**: Complex business concepts like "slow-moving inventory" and "reliable suppliers"

## System Architecture and Flow

The GraphAlchemy system integrates two primary components:

### Semantic Graph Flow

1. **Schema Extraction**: DDL parsing, schema loading, or BigQuery extraction
2. **Knowledge Graph Construction**:
   - Schema representation as graph nodes and relationships
   - Relationship inference through pattern matching and semantic analysis
   - Metadata enhancement with business-friendly descriptions
   - Multi-agent glossary generation:
     - Term Generator: Creates initial business terms from schema
     - Term Refiner: Improves definitions and adds synonyms
     - Term Validator: Validates mappings with confidence scores
3. **Graph Storage**: Stores the enhanced schema as an interconnected graph including:
   - Tables and columns with technical details
   - Inferred relationships
   - Business terms with definitions
   - Composite concepts that span multiple entities

### Text2SQL Flow

1. **Natural Language Understanding**:
   - Entity Recognition: Identifies business and technical terms in the query
   - Term Mapping: Maps business terms to technical entities with weighted confidence
   - Concept Resolution: Resolves composite concepts like "slow-moving inventory"
   - Intent Classification: Determines query type (selection, aggregation, etc.)
   - Query Analysis: Breaks down the query into structured components
2. **SQL Transformation**:
   - SQL Structure Generator: Creates basic structure based on intent
   - JOIN Path Calculator: Determines optimal join paths between relevant tables
   - Filter Builder: Creates WHERE clauses based on recognized conditions
   - Aggregation Handler: Manages GROUP BY, HAVING, and aggregation functions
   - SQL Validator: Ensures SQL correctness and optimization
3. **Results & Interpretations**:
   - Returns primary SQL interpretation with confidence score
   - Provides natural language explanation of the query
   - Offers alternative interpretations when appropriate
   - Includes confidence scores for decision support

## Enhanced Features

GraphAlchemy incorporates several innovative features tested in these end-to-end flows:

1. **Weighted Term Mapping**: Resolves ambiguity when terms could map to multiple entities
2. **Composite Concept Resolution**: Understands complex business concepts like "slow-moving inventory"
3. **Domain-Specific Business Metrics**: Handles domain-specific calculations like "inventory turnover"
4. **Semantic Relationship Discovery**: Infers relationships beyond explicit foreign keys
5. **Automatic Schema Enhancement**: Enriches technical metadata with business context

## Test Results Analysis

After running the tests, review the JSON files in the `results/` directory to assess:

1. SQL correctness for each query
2. Business term translation accuracy
3. Composite concept resolution effectiveness
4. Confidence scores for term mappings
5. Alternative interpretations quality

Each test result contains:
- The original natural language query
- The generated SQL
- Explanation of the interpretation
- Confidence scores
- Alternative interpretations (if any)