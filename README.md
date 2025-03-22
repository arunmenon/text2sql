# Text2SQL

A natural language to SQL conversion system with advanced schema discovery and relationship inference.

## Overview

Text2SQL is a system designed to convert natural language queries into SQL statements. It extracts schema information from databases, infers relationships between tables, and uses this graph representation to generate accurate SQL queries from natural language.

## Architecture

The system has the following components:

1. **Schema Extraction** - Extracts schema information from databases or SQL files
2. **Relationship Inference** - Uses pattern matching and LLM-based techniques to discover relationships
3. **Graph Representation** - Stores schema and relationship information in Neo4j
4. **Text2SQL Engine** - Converts natural language to SQL using the graph representation

## Supported Schema Domains

The system can load schemas from different domains:

1. **Retail (Walmart)** - A comprehensive retail schema with 15 tables (Departments, Categories, Products, etc.)
2. **Supply Chain** - A supply chain management schema with inventory, logistics, and warehouse operations
3. **Merchandising** - A merchandising management schema focusing on products and promotions

## Getting Started

### Prerequisites

- Python 3.8+
- Neo4j Database
- Optional: OpenAI API key for LLM-based relationship inference

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables in `.env`:
   ```
   NEO4J_URI=neo4j://localhost:7687
   NEO4J_USERNAME=neo4j
   NEO4J_PASSWORD=password
   # Optional for LLM relationship inference
   OPENAI_API_KEY=your-api-key-here
   ```

### Loading a Schema

Load a schema from a supported domain with:

```bash
python load_schema_demo.py --schema-type [walmart|supply-chain|merchandising] --tenant-id my-tenant-id
```

### Discovering Relationships

After loading a schema, discover relationships with:

```bash
python simulate_relationships.py --tenant-id my-tenant-id [--schema-type walmart] [--use-llm]
```

### Running the Full Discovery Pipeline

For the Walmart retail schema, run the complete discovery pipeline:

```bash
python run_walmart_discovery.py --tenant-id my-tenant-id [--use-llm]
```

### Using the API

Start the API server with:

```bash
python run_api.py
```

## Schema Domains

### Walmart Retail Schema

A retail schema with 15 tables:
- Departments
- Categories
- SubCategories
- Suppliers
- Products
- LegacyProducts
- DistributionCenters
- DCInventory
- Stores
- StoreEmployees
- Inventory
- Sales
- Promotions
- Pricing
- Returns

### Supply Chain Schema

A supply chain management schema with tables such as:
- Warehouses
- Suppliers
- Items
- PurchaseOrders
- Inventory
- ShippingCarriers
- Shipments
- ReplenishmentPlanning
- Forecasting