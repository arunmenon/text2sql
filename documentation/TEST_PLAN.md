# Enhanced Business Glossary Test Plan

This document outlines the testing approach for the enhanced business glossary implementation, focusing on the supply chain schema.

## Test Environment Setup

1. **Clean Database**
   - Clear all data from Neo4j
   - Verify database is empty

2. **Schema Loading**
   - Load supply chain schema from SQL file
   - Verify all tables and columns are properly created
   - Expected tables: Warehouses, Suppliers, Items, PurchaseOrders, PurchaseOrderItems, Inventory, InventoryTransactions, ShippingCarriers, Shipments, ShipmentItems

3. **Relationship Inference**
   - Run relationship detection with LLM assistance
   - Verify key relationships are detected:
     - PurchaseOrders.supplier_id → Suppliers.supplier_id
     - PurchaseOrders.warehouse_id → Warehouses.warehouse_id
     - PurchaseOrderItems.po_id → PurchaseOrders.po_id
     - PurchaseOrderItems.item_id → Items.item_id
     - Inventory.warehouse_id → Warehouses.warehouse_id
     - Inventory.item_id → Items.item_id
     - Other logical relationships

## Enhanced Business Glossary Generation Test

1. **Business Glossary Generation**
   - Run enhanced business glossary generation
   - Verify each agent executes properly:
     - Term Generator
     - Term Refiner
     - Term Validator

2. **Generated Terms Quality Checks**
   - Verify business terms are created for key concepts:
     - Warehouse
     - Supplier/Vendor
     - Inventory/Stock
     - Purchase Order
     - Shipment
     - Stock Keeping Unit (SKU)
   - Verify each term has:
     - Clear definition
     - Proper technical mappings
     - Synonyms where appropriate
     - Confidence scores for mappings

3. **Complex Business Concept Validation**
   - Verify composite concepts are properly identified:
     - "Slow-moving inventory"
     - "Critical supply shortages"
     - "Reliable suppliers"
     - "High-value inventory"

## Weighted Term Mapping Test

1. **Ambiguity Resolution**
   - Test queries with ambiguous terms:
     - "Show me all stock" (should map to Inventory)
     - "List vendors" (should map to Suppliers)
     - "Show all orders" (should map to PurchaseOrders)

2. **Synonym Resolution**
   - Test queries using synonyms:
     - "Show me all SKUs" (should map to Items)
     - "List materials" (should map to Items)
     - "Show all incoming shipments" (should map to Shipments with appropriate status filter)

## Text2SQL Integration Test

1. **Basic SQL Generation Test**
   - Run predefined query families:
     - Inventory basics
     - Inventory analytics
     - Purchasing operations
     - Shipping logistics
   - Verify SQL correctness and execution

2. **Business Term Translation Test**
   - Test translation of business terms to technical entities
   - Verify handling of business concepts in natural language queries

3. **Composite Concept Test**
   - Test queries that include composite business concepts
   - Verify proper SQL generation for complex business concepts

## Performance Test

1. **Generation Speed**
   - Measure time to generate enhanced business glossary
   - Compare with legacy approach

2. **Query Resolution Speed**
   - Measure average time to resolve queries with business terms
   - Compare resolution time for simple vs. complex queries

## Evaluation Metrics

1. **Term Generation Quality**
   - Number of generated terms
   - Coverage of domain concepts
   - Quality of definitions
   - Accuracy of technical mappings

2. **SQL Generation Quality**
   - Syntax correctness
   - Semantic correctness
   - Query efficiency
   - Business term resolution accuracy

3. **Overall System Performance**
   - End-to-end query processing time
   - Resource utilization

## Test Analysis

After running all tests, analyze:

1. Strengths and weaknesses of the enhanced approach
2. Areas for improvement
3. Performance bottlenecks
4. Accuracy of business term mappings
5. Handling of complex business concepts

## Expected Outcomes

1. Enhanced business glossary should provide better term definitions and mappings
2. Weighted term mapping should improve ambiguity resolution
3. Composite concept resolution should handle complex business terms correctly
4. Overall query accuracy should improve
5. The system should handle supply chain domain-specific queries effectively