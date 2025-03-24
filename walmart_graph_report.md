# Semantic Graph Report for Walmart Stores
Generated on: 2025-03-25 00:41:43

## Executive Summary
This report provides an overview of the semantic graph for Walmart Stores, showcasing the rich metadata and business context captured in the knowledge graph.

## Graph Statistics
### Core Graph Metrics
| Metric                |   Count |
|:----------------------|--------:|
| Tables                |      10 |
| Columns               |     310 |
| Business Terms        |      79 |
| Business Metrics      |      19 |
| Composite Concepts    |       9 |
| Business Processes    |      11 |
| Relationship Concepts |      13 |
| Hierarchical Concepts |       9 |


## Business Glossary
### Business Terms
The semantic graph contains 79 business terms providing business context for technical data elements.

#### Sample Business Terms:
| Term                         | Definition                                                                                              |
|:-----------------------------|:--------------------------------------------------------------------------------------------------------|
| Supplier Type                | Synonym of Vendor Type                                                                                  |
| Rack ID                      | Synonym of Rack Index                                                                                   |
| Rack Label                   | Synonym of Rack Name                                                                                    |
| Store                        | A retail location identified by a unique store number, type, and address.                               |
| Facilities Management Region | A geographical area designated for managing facilities operations.                                      |
| Retail Store                 | Synonym of Store                                                                                        |
| Outlet                       | Synonym of Store                                                                                        |
| FM Region                    | Synonym of Facilities Management Region                                                                 |
| Invoice                      | A document that itemizes and records a transaction between a buyer and a seller, detailing the produ... |
| Work Order                   | A task or a job for a customer that can be scheduled or assigned to someone.                            |


### Business Metrics
The semantic graph contains 19 business metrics that define KPIs and measurements.

#### Sample Business Metrics:
| Metric                         | Definition                                                                                              |
|:-------------------------------|:--------------------------------------------------------------------------------------------------------|
| Time in Target                 | The duration that a rack remains within a specified target condition or state.                          |
| Rack Run Time                  | The timestamp indicating when a rack operation was executed.                                            |
| Number of Stores per Region    | The total count of stores within each realty operations region.                                         |
| Facilities Management Coverage | The extent of facilities management operations across different regions.                                |
| Total Invoice Amount           | The complete monetary value of the invoice, including all charges such as labor, materials, travel, ... |
| Labor Cost                     | The monetary amount charged for labor services on the invoice.                                          |
| Work Order Completion Time     | The time taken from the creation to the completion of a work order.                                     |
| Service Provider Performance   | A measure of the efficiency and effectiveness of service providers in completing work orders.           |
| Active Asset Count             | The total number of assets currently marked as active.                                                  |
| Asset Utilization Rate         | The percentage of assets that are actively in use compared to the total number of assets.               |


## Graph-Aware Glossary Entities
The following entity types leverage the graph structure to provide rich semantic understanding that spans multiple tables and captures business meaning across the data model.

### Composite Concepts
Composite concepts represent business entities that span multiple tables, capturing complex real-world business objects that cannot be represented by a single table.

| Concept                              | Definition                                                                                              |
|:-------------------------------------|:--------------------------------------------------------------------------------------------------------|
| Rack Performance Analysis            | A comprehensive analysis of rack performance across different stores and vendors.                       |
| Store Alignment                      | The alignment of stores with their respective operational and management regions.                       |
| Service Invoice                      | A comprehensive record of financial transactions related to services rendered, including labor, mate... |
| Location-Based Work Order Management | The process of managing work orders based on their associated locations, including tracking and repo... |
| Asset Management System              | A system that integrates various tables to manage and track assets, their locations, and statuses.      |
| Store Performance Analysis           | An analysis of store performance metrics, including sales, inventory, and customer satisfaction.        |
| Store Operations Analysis            | A comprehensive analysis of store operations, including product loss, sales performance, and operati... |
| Proposal Management                  | The process of creating, tracking, and managing business proposals and their components.                |
| Proposal Financial Breakdown         | A detailed breakdown of the financial components of a proposal, including labor, material, tax, and ... |


### Business Processes
Business processes represent workflows and procedures that span multiple tables, capturing the operational flow of business activities.

| Process                         | Definition                                                                                              |
|:--------------------------------|:--------------------------------------------------------------------------------------------------------|
| Rack Performance Monitoring     | The process of tracking and analyzing the performance of racks in retail stores.                        |
| Store Alignment Management      | The process of creating, updating, and managing store alignment data.                                   |
| Invoice Management              | The process of creating, tracking, and managing invoices to ensure accurate billing and payment.        |
| Work Order Lifecycle Management | The end-to-end process of managing work orders from initiation to completion, including scheduling a... |
| Asset Tracking                  | The process of monitoring the location, status, and usage of assets within an organization.             |
| Maintenance Scheduling          | The process of planning and executing maintenance activities for assets based on their lifecycle and... |
| Asset Leak Inspection Process   | A workflow for conducting and documenting inspections of asset leaks.                                   |
| Location Management             | The process of managing the lifecycle of a location, from opening to closure.                           |
| Product Loss Management         | The process of tracking, analyzing, and mitigating product loss within the organization.                |
| Proposal Line Item Lifecycle    | The lifecycle of a proposal line item from creation to historical record, including updates and refi... |


### Relationship Concepts
Relationship concepts capture business-meaningful connections between tables, providing semantic context to technical foreign key relationships.

| Relationship                           | Definition                                                                                      |
|:---------------------------------------|:------------------------------------------------------------------------------------------------|
| Store to Rack Relationship             | The connection between store locations and the racks they contain.                              |
| Asset to Rack Relationship             | The linkage between assets and the racks they are associated with.                              |
| Store Type Reference                   | The relationship between store types in RealtyAlignment and other tables.                       |
| Invoice to Work Order Link             | The connection between an invoice and its associated work order, facilitating service tracking. |
| Work Order to ServiceChannelInvoice    | The relationship between work orders and their corresponding financial invoices.                |
| Asset to Location Mapping              | The association between an asset and its physical location within the organization.             |
| Asset to Supplier Relationship         | The connection between an asset and the supplier from which it was procured.                    |
| Asset to Inspection Relationship       | The connection between an asset and its associated leak inspections.                            |
| Location to Work Orders                | The relationship between a location and its associated work orders.                             |
| Store Number to ServiceChannel Invoice | The connection between a store number and its related service channel invoices.                 |


### Hierarchical Concepts
Hierarchical concepts represent parent-child structures in the data, providing insight into classification hierarchies and organizational structures.

| Hierarchy                                | Definition                                                                                              |
|:-----------------------------------------|:--------------------------------------------------------------------------------------------------------|
| Store Hierarchy                          | The hierarchical structure of stores and their associated racks.                                        |
| Facilities Management Hierarchy          | The hierarchical structure of facilities management roles and regions.                                  |
| Organizational Structure for Work Orders | The hierarchy of organizational units responsible for managing work orders, from global business uni... |
| Asset Type Hierarchy                     | A classification structure for assets based on their type and category.                                 |
| Asset Inspection Hierarchy               | A hierarchical structure representing the relationship between assets and their inspections.            |
| Location Hierarchy                       | The hierarchical structure of locations, including parent and child relationships.                      |
| Store and Department Hierarchy           | The hierarchical structure of stores and their departments.                                             |
| Proposal Hierarchy                       | A hierarchical structure where proposals are the parent entities and line items are the child entiti... |
| Proposal Approval Hierarchy              | The hierarchical structure representing the approval process of proposals.                              |


## Business Value
### Value Proposition of the Semantic Graph

The semantic graph provides significant business value through:

1. **Enhanced Data Discovery and Understanding**
   - Rich business context for technical data elements
   - Cross-table semantic connections
   - Domain-specific knowledge capture

2. **Natural Language Querying**
   - Business terminology for data access
   - Self-service analytics for non-technical users
   - More intuitive interaction with complex data

3. **Knowledge Management**
   - Preservation of institutional knowledge
   - Consistent business definitions
   - Reduced onboarding time for new analysts

4. **Cross-Table Business Insights**
   - Understanding of complex business entities
   - Visibility into business processes
   - Recognition of important data relationships

5. **Improved Data Governance**
   - Clear ownership of business terms
   - Consistent implementation of business rules
   - Better compliance documentation

## Conclusion
The semantic graph for Walmart Stores provides a comprehensive knowledge layer that bridges technical metadata with business understanding. By implementing the graph-aware approach with table-by-table processing, we're able to handle large data volumes while still capturing rich semantic relationships.

The graph-aware business glossary goes beyond traditional approaches by capturing composite concepts, business processes, relationship semantics, and hierarchical structures. This provides a more complete business context that enhances the value of the underlying data.

With continued enrichment and expansion, this semantic graph will serve as a foundational component of Walmart's data strategy, enabling more intuitive data exploration, better self-service analytics, and improved business intelligence.