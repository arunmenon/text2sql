# Schema-to-Semantic Graph Mapping: Automated Intelligence Layer

**Generation Date:** 2025-03-25  
**Generation Method:** Fully Automated, Zero Human Intervention

## Executive Overview: Automated Semantic Intelligence

This document explains how our system **automatically transforms raw database schema** into a rich semantic graph that provides business context and meaning. All entities and relationships described in this report were generated **without human intervention** using our graph-aware machine learning approach.

## Automated Generation Process

```
Raw Schema → Relationship Inference → Metadata Enrichment → Graph-Aware Entities → Semantic Graph
```

**Key metrics of automation:**
- Schema coverage: 100% of tables, 93% of columns semantically enhanced
- Entity generation confidence: 87% average across all entity types
- Zero human intervention or editing in the final output

## Glossary Graph: Entity Types and Schema Mapping

The semantic graph automatically generates six distinct entity types, each derived from specific elements of the underlying schema:

| Entity Type | Count | Schema Source | Business Value |
|-------------|-------|---------------|----------------|
| **Business Terms** | 79 | Table and column names, descriptions | Standardized vocabulary for database elements |
| **Business Metrics** | 19 | Numerical/date columns with aggregation potential | Pre-defined KPIs and measurements |
| **Composite Concepts** | 9 | Multi-table relationships with related entities | Cross-table business entities |
| **Business Processes** | 11 | Sequential table relationships with flow patterns | Workflow and procedural knowledge |
| **Relationship Concepts** | 13 | Foreign key relationships with business context | Semantic meaning of data connections |
| **Hierarchical Concepts** | 9 | Parent-child table patterns | Organizational and classification structures |

### Entity Type: Business Terms (79)
Business terms provide standardized vocabulary for technical schema elements, enabling consistent communication and natural language querying.

**Example Mapping:**
```
Schema Element: Table "sc_walmart_locations" with columns "address", "city", "state"
↓ AUTOMATED TRANSFORMATION ↓
Business Term: "Store"
Definition: "A retail location identified by a unique store number, type, and address."
Mapped To: sc_walmart_locations (table), location_id, address, city, state (columns)
```

### Entity Type: Business Metrics (19)
Business metrics represent important measurements derived from numerical or temporal data in the schema, providing pre-calculated KPIs.

**Example Mapping:**
```
Schema Element: Column "labor_cost" (DECIMAL) in "ServiceChannelInvoice" table
Schema Element: Column "materials_cost" (DECIMAL) in "ServiceChannelInvoice" table
↓ AUTOMATED TRANSFORMATION ↓
Business Metric: "Total Invoice Amount"
Definition: "The complete monetary value of the invoice, including all charges such as labor, materials, travel, and taxes."
Calculation: "SUM(labor_cost + materials_cost + travel_cost + other_costs)"
```

### Entity Type: Composite Concepts (9)
Composite concepts represent business entities that span multiple tables, capturing complex real-world objects that cannot be represented by a single table.

**Example Mapping:**
```
Schema Elements:
- Table "Asset" (contains asset_id, asset_type, status)
- Table "AssetLeak" (contains asset_id, inspection_id, result)
- Foreign Key: AssetLeak.asset_id → Asset.id
↓ AUTOMATED TRANSFORMATION ↓
Composite Concept: "Asset Management System"
Definition: "A system that integrates various tables to manage and track assets, their locations, and statuses."
Component Tables: Asset, AssetLeak, WorkOrders
Business Importance: "Critical for facilities maintenance and cost management"
```

### Entity Type: Business Processes (11)
Business processes capture operational workflows that span multiple related tables, providing insight into business activities and their sequence.

**Example Mapping:**
```
Schema Pattern: Sequential Table Relationship
- Table "Proposals" (initial state)
- Table "ProposalLineItems" (detail stage)
- Table "WorkOrders" (execution stage)
- Table "ServiceChannelInvoice" (completion stage)
↓ AUTOMATED TRANSFORMATION ↓
Business Process: "Work Order Lifecycle Management"
Definition: "The end-to-end process of managing work orders from initiation to completion"
Workflow Stages: "Proposal Creation → Line Item Definition → Work Order Generation → Invoice Processing"
Tables Involved: Proposals, ProposalLineItems, WorkOrders, ServiceChannelInvoice
```

### Entity Type: Relationship Concepts (13)
Relationship concepts provide business-meaningful context to technical relationships between tables, explaining the semantic meaning of foreign key connections.

**Example Mapping:**
```
Schema Elements:
- Foreign Key: WorkOrders.asset_id → Asset.id
- Foreign Key: WorkOrders.location_id → Locations.id
↓ AUTOMATED TRANSFORMATION ↓
Relationship Concept: "Asset to Location Mapping"
Definition: "The association between an asset and its physical location within the organization."
Business Rules: "Each asset must be assigned to a valid location"
Tables Connected: WorkOrders, Asset, Locations
```

### Entity Type: Hierarchical Concepts (9)
Hierarchical concepts identify and explain organizational structures and classifications in the data model through parent-child relationships.

**Example Mapping:**
```
Schema Elements:
- Table "Locations" with self-referential key parent_location_id
- Column "location_type" with values like "Region", "District", "Store"
↓ AUTOMATED TRANSFORMATION ↓
Hierarchical Concept: "Location Hierarchy"
Definition: "The hierarchical structure of locations, including parent and child relationships."
Levels: "Region → District → Store"
Primary Table: "Locations"
Hierarchy Type: "Geographic Organization"
```

## Table and Column Metadata Enrichment

The system automatically enriches table and column metadata with business context, descriptions, and usage information:

### Table Metadata Enrichment Example
```
Raw Table: "sc_walmart_proposals"
Initial Metadata: name="sc_walmart_proposals", columns=11
↓ AUTOMATED ENHANCEMENT ↓
Enhanced Table Metadata:
- Business Name: "Walmart Proposals"
- Business Description: "Contains information about maintenance and service proposals submitted for Walmart facilities"
- Primary Business Domain: "Facilities Management"
- Key Business Stakeholders: "Facilities Management Team, Service Providers"
- Data Sensitivity Level: "Medium - contains financial information"
- Update Frequency: "Daily"
- Related Business Processes: "Proposal Approval Workflow, Work Order Creation"
```

### Column Metadata Enrichment Example
```
Raw Column: "sc_walmart_proposals.status"
Initial Metadata: name="status", type="VARCHAR(50)", nullable=false
↓ AUTOMATED ENHANCEMENT ↓
Enhanced Column Metadata:
- Business Name: "Proposal Status"
- Business Description: "The current approval status of the proposal in its lifecycle"
- Allowed Values: "Draft, Submitted, Under Review, Approved, Rejected, Expired"
- Business Rules: "Proposals must be in 'Approved' status before work orders can be created"
- Data Quality Rules: "Must contain only valid status values; Cannot be NULL"
- Example Valid Values: "Approved", "Under Review"
- Related Business Terms: "Proposal Approval Process", "Service Request Workflow"
- Used In: "Proposal Approval Reporting", "Service Provider Performance Analytics"
```

## Schema Coverage Analysis

The semantic layer provides comprehensive coverage of the underlying schema:

| Schema Component | Total Count | Semantically Enhanced | Coverage % |
|------------------|-------------|------------------------|------------|
| Tables | 10 | 10 | 100% |
| Columns | 310 | 289 | 93.2% |
| Foreign Keys | 27 | 27 | 100% |
| Table Groups | 5 | 5 | 100% |
| Column Descriptions | 310 | 289 | 93.2% |
| Business Rules | 52 | 52 | 100% |
| Data Quality Rules | 127 | 127 | 100% |

## Natural Language Query Translation Examples

The enhanced semantic layer enables natural language queries by mapping business terminology to technical schema elements:

| Natural Language Query | Technical SQL Translation |
|------------------------|---------------------------|
| "Show me all stores with work orders in pending status" | `SELECT l.store_name, l.address FROM sc_walmart_locations l JOIN sc_walmart_workorder w ON l.location_id = w.location_id WHERE w.status = 'Pending'` |
| "What is the total invoice amount by asset category last month?" | `SELECT a.asset_category, SUM(i.labor_cost + i.materials_cost) as total_amount FROM ServiceChannelInvoice i JOIN WorkOrders w ON i.workorder_id = w.id JOIN Asset a ON w.asset_id = a.id WHERE i.invoice_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND i.invoice_date < DATE_TRUNC('month', CURRENT_DATE) GROUP BY a.asset_category ORDER BY total_amount DESC` |
| "List all assets with leak detection inspections that failed" | `SELECT a.asset_id, a.asset_name, a.location_id FROM Asset a JOIN AssetLeak al ON a.asset_id = al.asset_id WHERE al.inspection_result = 'Failed'` |

## Automation Value Proposition

The fully automated approach delivers significant advantages:

1. **Consistency**: Uniform application of semantic patterns across the entire schema
2. **Comprehensiveness**: 93-100% coverage across all schema elements
3. **Adaptability**: Automatic updates when schema changes, no manual maintenance required
4. **Intelligence**: Cross-table semantic insights that human documentation often misses
5. **Discoverability**: Business users can find and understand data using familiar terminology

## Conclusion: Automated Intelligence in Action

This report demonstrates how raw technical schema is automatically transformed into business-meaningful semantic entities without human intervention. The system bridges the gap between technical database structures and business understanding, enabling self-service analytics and natural language data access.

The semantic graph with its six entity types provides comprehensive business context across all levels of the data model:
- **Business Terms (79)**: Standardized vocabulary for database elements
- **Business Metrics (19)**: Pre-defined KPIs and measurements
- **Composite Concepts (9)**: Cross-table business entities
- **Business Processes (11)**: Workflow and procedural knowledge
- **Relationship Concepts (13)**: Semantic meaning of data connections
- **Hierarchical Concepts (9)**: Organizational and classification structures

This automated approach ensures that all data assets have comprehensive business context, dramatically improving data discovery, analytics, and governance while eliminating the need for manual documentation efforts.