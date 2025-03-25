# Semantic Graph Visualizations: Connecting Schema to Business Meaning

This document provides visual representations of how the semantic graph connects to the underlying schema, illustrating the automatic transformation of technical database elements into business-meaningful concepts.

## 1. Architectural Overview

The following diagram shows the layers of transformation from raw database schema to semantic graph:

```mermaid
flowchart TD
    subgraph "Raw Database Schema"
        Tables["Tables (10)"]
        Columns["Columns (310)"]
        FKs["Foreign Keys (27)"]
    end

    subgraph "Relationship Layer"
        Joined["Joined Tables"]
        Constraints["Constraints"]
        FlowPatterns["Flow Patterns"]
    end

    subgraph "Metadata Enhancement Layer"
        TableMD["Table Metadata"]
        ColumnMD["Column Metadata"]
        RelMD["Relationship Metadata"]
    end

    subgraph "Semantic Graph Layer"
        BT["Business Terms (79)"]
        BM["Business Metrics (19)"]
        CC["Composite Concepts (9)"]
        BP["Business Processes (11)"]
        RC["Relationship Concepts (13)"]
        HC["Hierarchical Concepts (9)"]
    end

    Tables --> Joined
    Columns --> Joined
    FKs --> Joined
    Tables --> Constraints
    FKs --> Constraints
    Joined --> FlowPatterns

    Joined --> TableMD
    Columns --> ColumnMD
    Constraints --> RelMD

    TableMD --> BT
    ColumnMD --> BT
    ColumnMD --> BM
    
    TableMD --> CC
    RelMD --> CC
    
    FlowPatterns --> BP
    
    RelMD --> RC
    
    TableMD --> HC
    RelMD --> HC

    style Tables fill:#d0e0ff
    style Columns fill:#d0e0ff
    style FKs fill:#d0e0ff
    
    style BT fill:#c0f0c0
    style BM fill:#c0f0c0
    style CC fill:#c0f0c0 
    style BP fill:#c0f0c0
    style RC fill:#c0f0c0
    style HC fill:#c0f0c0
```

## 2. Entity-Schema Mapping Details

This diagram illustrates how specific schema elements map to semantic graph entities:

```mermaid
flowchart LR
    subgraph "Schema Elements"
        direction TB
        T["Table Name"]
        CD["Column Definition"]
        FK["Foreign Key"]
        ST["Sequential Tables"]
        PC["Parent-Child Pattern"]
        NC["Numeric Columns"]
    end

    subgraph "Semantic Entities"
        direction TB
        BT["Business Term"]
        BM["Business Metric"]
        CC["Composite Concept"]
        BP["Business Process"]
        RC["Relationship Concept"]
        HC["Hierarchical Concept"]
    end

    T --> BT
    CD --> BT
    NC --> BM
    FK --> CC
    ST --> BP
    FK --> RC
    PC --> HC

    style BT fill:#c0f0c0
    style BM fill:#c0f0c0
    style CC fill:#c0f0c0
    style BP fill:#c0f0c0
    style RC fill:#c0f0c0
    style HC fill:#c0f0c0
```

## 3. Business Term Generation Example

```mermaid
flowchart TD
    Table["Table: sc_walmart_locations"]
    Cols["Columns: 
    - location_id
    - store_name
    - address
    - city
    - state
    - zip_code"]
    
    Meta["Metadata Analysis"]
    
    Term["Business Term: Store
    Definition: A retail location identified by a 
    unique store number, type, and address."]
    
    Table --> Meta
    Cols --> Meta
    Meta --> Term
    
    style Table fill:#d0e0ff
    style Cols fill:#d0e0ff
    style Term fill:#c0f0c0
```

## 4. Business Metric Generation Example

```mermaid
flowchart TD
    Cols["Columns: 
    - labor_cost (DECIMAL)
    - materials_cost (DECIMAL)
    - travel_cost (DECIMAL)
    - other_costs (DECIMAL)"]
    
    Analysis["Numeric Column Analysis
    - Identifies aggregation potential
    - Detects related columns"]
    
    Metric["Business Metric: Total Invoice Amount
    Definition: The complete monetary value of the invoice,
    including all charges such as labor, materials, 
    travel, and taxes
    Calculation: SUM(labor_cost + materials_cost + 
    travel_cost + other_costs)"]
    
    Cols --> Analysis
    Analysis --> Metric
    
    style Cols fill:#d0e0ff
    style Metric fill:#c0f0c0
```

## 5. Composite Concept Generation Example

```mermaid
flowchart TD
    Asset["Table: Asset
    - asset_id
    - asset_type
    - status"]
    
    AssetLeak["Table: AssetLeak
    - asset_id
    - inspection_id
    - result"]
    
    WorkOrders["Table: WorkOrders
    - id
    - asset_id
    - location_id
    - status"]
    
    FK1["Foreign Key:
    AssetLeak.asset_id → Asset.id"]
    
    FK2["Foreign Key:
    WorkOrders.asset_id → Asset.id"]
    
    Analysis["Multi-Table Relationship Analysis
    - Identifies central entities
    - Maps cross-table connections"]
    
    Concept["Composite Concept: Asset Management System
    Definition: A system that integrates various tables 
    to manage and track assets, their locations, and statuses.
    Component Tables: Asset, AssetLeak, WorkOrders
    Business Importance: Critical for facilities 
    maintenance and cost management"]
    
    Asset --> FK1
    AssetLeak --> FK1
    Asset --> FK2
    WorkOrders --> FK2
    
    FK1 --> Analysis
    FK2 --> Analysis
    
    Analysis --> Concept
    
    style Asset fill:#d0e0ff
    style AssetLeak fill:#d0e0ff
    style WorkOrders fill:#d0e0ff
    style FK1 fill:#d0e0ff
    style FK2 fill:#d0e0ff
    style Concept fill:#c0f0c0
```

## 6. Business Process Generation Example

```mermaid
flowchart LR
    P["Table: Proposals"]
    PL["Table: ProposalLineItems"]
    WO["Table: WorkOrders"]
    INV["Table: ServiceChannelInvoice"]
    
    P -- "1:n" --> PL
    PL -- "creates" --> WO
    WO -- "generates" --> INV
    
    FlowAnalysis["Sequence Analysis:
    - Detects transaction flow
    - Identifies create/update sequences
    - Maps status transitions"]
    
    Process["Business Process: Work Order Lifecycle Management
    Definition: The end-to-end process of managing work 
    orders from initiation to completion
    Workflow Stages: Proposal Creation → Line Item Definition 
    → Work Order Generation → Invoice Processing"]
    
    P --> FlowAnalysis
    PL --> FlowAnalysis
    WO --> FlowAnalysis
    INV --> FlowAnalysis
    
    FlowAnalysis --> Process
    
    style P fill:#d0e0ff
    style PL fill:#d0e0ff
    style WO fill:#d0e0ff
    style INV fill:#d0e0ff
    style Process fill:#c0f0c0
```

## 7. Cross-Entity Semantic Graph

This diagram shows how different entity types form a semantic network:

```mermaid
flowchart TD
    BT1["Business Term:
    Store"]
    
    BT2["Business Term:
    Work Order"]
    
    BT3["Business Term:
    Asset"]
    
    BM["Business Metric:
    Total Invoice Amount"]
    
    CC["Composite Concept:
    Asset Management System"]
    
    BP["Business Process:
    Work Order Lifecycle"]
    
    RC["Relationship Concept:
    Asset to Location Mapping"]
    
    HC["Hierarchical Concept:
    Location Hierarchy"]
    
    BT1 -- "part of" --> HC
    BT1 -- "involved in" --> RC
    BT2 -- "part of" --> BP
    BT2 -- "generates" --> BM
    BT3 -- "part of" --> CC
    BT3 -- "connected via" --> RC
    CC -- "follows" --> BP
    
    style BT1 fill:#c0f0c0
    style BT2 fill:#c0f0c0
    style BT3 fill:#c0f0c0
    style BM fill:#c0f0c0
    style CC fill:#c0f0c0
    style BP fill:#c0f0c0
    style RC fill:#c0f0c0
    style HC fill:#c0f0c0
```

## 8. Natural Language Query Processing Flow

```mermaid
flowchart TD
    Query["Natural Language Query:
    'Show me all stores with pending work orders'"]
    
    Parsing["Query Parsing"]
    
    BT1["Business Term:
    Store → sc_walmart_locations"]
    
    BT2["Business Term:
    Work Order → sc_walmart_workorder"]
    
    BT3["Business Term:
    Pending Status → status='Pending'"]
    
    RC["Relationship Concept:
    Store to Work Order Relationship →
    JOIN condition"]
    
    SQL["SQL Query:
    SELECT l.store_name, l.address 
    FROM sc_walmart_locations l
    JOIN sc_walmart_workorder w 
    ON l.location_id = w.location_id
    WHERE w.status = 'Pending'"]
    
    Query --> Parsing
    Parsing --> BT1
    Parsing --> BT2
    Parsing --> BT3
    
    BT1 --> RC
    BT2 --> RC
    
    BT1 --> SQL
    BT2 --> SQL
    BT3 --> SQL
    RC --> SQL
    
    style Query fill:#f9f9f9
    style BT1 fill:#c0f0c0
    style BT2 fill:#c0f0c0
    style BT3 fill:#c0f0c0
    style RC fill:#c0f0c0
    style SQL fill:#f9d0d0
```

## 9. Metadata Enrichment Visualization

```mermaid
flowchart TD
    RawTable["Raw Table:
    sc_walmart_proposals
    - No business context
    - Technical field names
    - No usage information"]
    
    Enhancement["Metadata Enhancement
    - Analyzes patterns
    - Infers business meaning
    - Detects relationships"]
    
    EnhancedTable["Enhanced Table:
    Business Name: Walmart Proposals
    Business Domain: Facilities Management
    Update Frequency: Daily
    Data Sensitivity: Medium
    Key Stakeholders: Facilities Management Team"]
    
    RawTable --> Enhancement
    Enhancement --> EnhancedTable
    
    style RawTable fill:#d0e0ff
    style EnhancedTable fill:#c0f0c0
```

## 10. Schema to Semantic Integration View

```mermaid
flowchart TD
    Schema["Database Schema Layer"]
    Metadata["Metadata Layer"]
    Semantic["Semantic Layer"]
    Business["Business Layer"]
    
    Schema -- "Enriches" --> Metadata
    Metadata -- "Generates" --> Semantic
    Semantic -- "Enables" --> Business
    
    subgraph "Schema Layer Elements"
        Tables["Tables"]
        Columns["Columns"]
        Keys["Keys & Constraints"]
    end
    
    subgraph "Semantic Layer Elements"
        BT["Business Terms"]
        BM["Business Metrics"]
        CC["Composite Concepts"]
        BP["Business Processes"]
        RC["Relationship Concepts"]
        HC["Hierarchical Concepts"]
    end
    
    subgraph "Business Layer Capabilities"
        NLQ["Natural Language Querying"]
        SD["Semantic Data Discovery"]
        DA["Domain-Aware Analytics"]
        SS["Self-Service BI"]
    end
    
    Tables --> Schema
    Columns --> Schema
    Keys --> Schema
    
    BT --> Semantic
    BM --> Semantic
    CC --> Semantic
    BP --> Semantic
    RC --> Semantic
    HC --> Semantic
    
    NLQ --> Business
    SD --> Business
    DA --> Business
    SS --> Business

    style Schema fill:#d0e0ff
    style Tables fill:#d0e0ff
    style Columns fill:#d0e0ff
    style Keys fill:#d0e0ff
    
    style Semantic fill:#c0f0c0
    style BT fill:#c0f0c0
    style BM fill:#c0f0c0
    style CC fill:#c0f0c0
    style BP fill:#c0f0c0
    style RC fill:#c0f0c0
    style HC fill:#c0f0c0
    
    style Business fill:#ffb3bc
    style NLQ fill:#ffb3bc
    style SD fill:#ffb3bc
    style DA fill:#ffb3bc
    style SS fill:#ffb3bc
```

These diagrams are created using Mermaid syntax and should render in markdown viewers that support it (including GitHub). The visualizations provide a clear picture of how the semantic graph connects to and enhances the underlying schema, automatically transforming technical database elements into business-meaningful entities.