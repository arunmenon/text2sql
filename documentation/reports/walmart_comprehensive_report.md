# Semantic Graph Report for walmart-stores (walmart-stores)
Generated on: 2025-03-25 00:45:02

## Executive Summary
Custom schema tenant

### Graph Statistics
| Metric | Count |
| --- | --- |
| Tables | 10 |
| Columns | 310 |
| Relationships | 311 |
| Business Terms | 26 |
| Business Metrics | 19 |
| Composite Concepts | 9 |
| Business Processes | 11 |
| Relationship Concepts | 13 |
| Hierarchical Concepts | 9 |


## Value Proposition of the Semantic Graph
The semantic graph represents a comprehensive knowledge model of your business data, connecting technical metadata with business concepts and relationships. This provides significant value in several key areas:

### Data Discovery and Understanding
- **Business Context**: Rich business descriptions of tables and columns make data discoverable using business terminology
- **Cross-Table Relationships**: Automatically detected relationships show how data connects across systems
- **Graph Navigation**: Intuitive exploration of data assets and their relationships

### Enhanced Business Intelligence
- **Business Metrics**: Pre-defined business KPIs with clear definitions and calculation methods
- **Composite Concepts**: Complex business entities spanning multiple tables are explicitly modeled
- **Business Processes**: End-to-end workflows captured as first-class entities

### Natural Language Data Access
- **Text-to-SQL Translation**: Business glossary empowers natural language queries over complex data
- **Consistent Terminology**: Standard business vocabulary across the organization
- **Self-Service Analytics**: Non-technical users can access data through natural language

## Technical Graph Structure
### Tables and Columns
The semantic graph contains 10 tables with 310 total columns.

#### Top Tables by Column Count
| Table                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |   Column Count |
|:----------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------:|
| WorkOrders            | The "WorkOrders" table is a comprehensive repository of data related to the management and tracking of work orders within an organization. It likely contains detailed information about each work order, including its creation, assignment, scheduling, and completion, as well as associated metadata such as location, priority, status, and involved personnel. The primary business purpose of this table is to facilitate efficient work order management, ensuring that maintenance and service tasks are tracked, prioritized, and completed in a timely manner, thereby supporting operational continuity and service quality. This table may relate to other tables through foreign keys like `location_id`, `asset_id`, `provider_id`, and `trade_id`, which link to detailed records of locations, assets, service providers, and trades, respectively. Typical use cases include generating reports on work order status, analyzing service provider performance, and ensuring compliance with service level agreements (SLAs). The entity lifecycle represented by this table spans from the initial call and creation of a work order, through its assignment and scheduling, to its eventual completion and any subsequent recall or modification, providing a full audit trail of the work order process.                                                |             76 |
| Proposals             | The "Proposals" table is a comprehensive repository of data related to business proposals within an organization, capturing essential details from creation to approval. It likely contains information about each proposal's unique identifiers, descriptions, associated work orders, locations, trades, service providers, and financial details, such as total amounts and specific cost breakdowns. The business purpose of this table is to facilitate the management and tracking of proposals, ensuring that all relevant information is centralized for efficient decision-making and workflow management. It is likely related to other tables such as "Work Orders," "Locations," "Trades," "Service Providers," and "Requests for Proposals" through foreign keys like tracking_number, location_id, trade_id, provider_id, and rfp_id, enabling a comprehensive view of the proposal's context and status. Typical use cases include generating reports on proposal statuses, analyzing financial commitments, and tracking the lifecycle of proposals from request to approval or rejection. The entity lifecycle represented by this table includes stages such as proposal creation, modification, approval, rejection, and archival, providing a complete audit trail of each proposal's journey within the organization.                                 |             38 |
| Asset                 | The "Asset" table is a comprehensive repository of information pertaining to the physical assets owned or managed by an organization. It likely contains detailed data about each asset, including identification details (such as asset ID, QR code, and serial number), specifications (like brand, model, and asset type), and lifecycle information (including install, manufactured, and deactivated dates). The business purpose of this table is to facilitate asset management, tracking, and maintenance by providing a centralized view of all assets, their statuses, and their locations. This table is crucial for inventory management, financial accounting, and operational efficiency, as it helps in tracking asset utilization, depreciation, and compliance with regulatory requirements. It may relate to other tables such as "Location" (via location_id), "Supplier" (via host_supplier_number and sap_supplier_number), and "Trade" (via primary_trade_id), indicating its integration with broader supply chain and operational systems. Typical use cases include asset tracking, maintenance scheduling, and reporting on asset performance and status. The entity lifecycle represented by this table spans from asset creation and installation to deactivation, capturing all relevant updates and changes throughout its operational life. |             36 |
| Locations             | The "Locations" table is a comprehensive repository of data pertaining to various physical locations within the organization, such as Walmart and Sam's Club stores. It serves a critical business function by providing detailed information about each location, including identifiers, geographical data, contact details, and operational status. This table is essential for managing and tracking the lifecycle of each location, from its opening to potential closure, and supports operational decisions, logistics, and strategic planning. It likely relates to other tables through shared identifiers like store numbers or subscriber IDs, facilitating integration with sales, inventory, or employee data. Typical use cases include analyzing store performance, planning resource allocation, and maintaining up-to-date contact and operational information for each location. The entity lifecycle captured in this table spans from the creation and opening of a location, through its operational phase, to its closure, with updates reflecting changes in status or contact information.                                                                                                                                                                                                                                                          |             34 |
| ServiceChannelInvoice | The `ServiceChannelInvoice` table is designed to store detailed financial and operational data related to service invoices within an organization. It likely contains information about invoices generated for work orders, including financial details such as labor, material, travel, freight, and tax amounts, as well as metadata like invoice status, currency, and vendor details. The business purpose of this table is to facilitate the tracking, auditing, and management of service-related financial transactions, ensuring accurate billing and payment processes. It may relate to other tables such as `WorkOrders` (via `tracking_nbr`), `Vendors` (via `vendor_payee_id`), and `Stores` (via `store_nbr`), providing a comprehensive view of service operations and financial accountability. Typical use cases include generating financial reports, auditing service transactions, and analyzing service costs across different stores or vendors. The entity lifecycle represented by this table includes the creation, modification, and auditing of service invoices, reflecting the financial interactions between the organization and its service providers.                                                                                                                                                                                     |             31 |
| RealtyAlignment       | The RealtyAlignment table is a critical component of the organization's data infrastructure, capturing detailed alignment information between real estate operations and facilities management. It contains data about store locations, including their types and addresses, and maps these to various operational regions and management hierarchies. This table serves the business purpose of facilitating efficient management and oversight of real estate assets by aligning them with the appropriate facilities management and regional management teams. It likely interacts with other tables that store detailed information about stores, regions, or personnel, providing a comprehensive view of the organizational structure and responsibilities. Typical use cases include generating reports for regional management, optimizing resource allocation, and supporting strategic planning initiatives. The entity lifecycle represented by this table involves the creation, updating, and management of store alignment data, ensuring that the information remains current and accurate for operational decision-making.                                                                                                                                                                                                                                 |             25 |
| ProductLoss           | The `ProductLoss` table is designed to capture and analyze various dimensions of product loss and associated costs within a retail organization. It likely contains data related to inventory discrepancies, sales losses, and operational costs incurred due to product unavailability or damage. The business purpose of this table is to provide insights into the financial impact of product loss, enabling the organization to identify areas for improvement in inventory management and operational efficiency. This table may relate to other tables such as `Inventory`, `Sales`, or `StoreOperations` through columns like `Store_Nbr` and `DEPT_NBR`, which can be used to join data across different aspects of the business. Typical use cases include analyzing the cost-effectiveness of loss prevention strategies, assessing the impact of alarms on reducing product loss, and optimizing resource allocation for store operations. The entity lifecycle represented by this table involves tracking product loss events over time, from initial detection to resolution, and evaluating the financial implications of these events on the business.                                                                                                                                                                                                    |             23 |
| ProposalLineItems     | The `ProposalLineItems` table is designed to store detailed information about individual line items associated with business proposals. Each record in this table represents a specific component or service included in a proposal, capturing essential details such as the type of work (e.g., craft, repair category, and repair type), material specifications (e.g., material thickness), and financial metrics (e.g., unit price, quantity, and total cost). This table plays a critical role in the proposal management process by enabling the organization to itemize and evaluate the cost and scope of work proposed to clients. It is likely linked to a `Proposals` table through the `proposal_id` column, allowing for comprehensive tracking and management of proposals and their components. Typical use cases include generating detailed cost estimates, analyzing proposal components for pricing strategies, and ensuring data quality and accuracy through the `ods_dataquality_status` column. The lifecycle of an entity in this table begins with its creation when a proposal is drafted and is updated as the proposal is refined, ultimately serving as a historical record of the proposal's line items.                                                                                                                                     |             16 |
| RackScore             | The RackScore table is designed to capture and track performance metrics related to vendor-managed racks within retail stores. It likely contains data on various racks, identified by their unique rack index and name, across different store locations, as indicated by the store number. The table serves a critical business purpose by enabling the organization to monitor and optimize the efficiency of rack operations, such as the time racks spend within target performance thresholds (time_in_target) and the overall run time of rack activities. This data can be cross-referenced with other tables containing vendor or store information, facilitating comprehensive performance analysis and vendor management. Typical use cases include assessing vendor performance, optimizing rack placement and usage, and ensuring compliance with operational standards. The entity lifecycle represented by this table includes the creation, monitoring, and updating of rack performance records, as evidenced by the timestamps for record creation and updates in the operational data store (ODS).                                                                                                                                                                                                                                                      |             16 |
| AssetLeak             | The AssetLeak table is designed to manage and track information related to asset leak inspections within an organization. It likely contains data about specific leak incidents, including the method of verification and the responsible parties, as indicated by columns such as `tracking_number`, `asset_id`, `actor`, and `leak_verification_method`. The business purpose of this table is to ensure the integrity and safety of assets by documenting leak inspections and facilitating follow-up actions. This table may relate to other tables such as an Asset table (via `asset_id`) or a User table (via `created_by_user_id` and `updated_by_user_id`), providing a comprehensive view of asset management and user interactions. Typical use cases include generating reports on asset leak incidents, auditing inspection processes, and ensuring compliance with safety regulations. The entity lifecycle represented by this table includes the creation, updating, and verification of asset leak records, with timestamps capturing the history of these activities for audit and analysis purposes.                                                                                                                                                                                                                                                    |             15 |


### Table Relationships
#### Top Tables by Relationship Count
| Table                 |   Outgoing Relationships |   Incoming Relationships |   Total |
|:----------------------|-------------------------:|-------------------------:|--------:|
| WorkOrders            |                       55 |                       53 |     108 |
| Proposals             |                       47 |                       48 |      95 |
| Locations             |                       37 |                       38 |      75 |
| Asset                 |                       35 |                       35 |      70 |
| AssetLeak             |                       31 |                       32 |      63 |
| ServiceChannelInvoice |                       29 |                       29 |      58 |
| ProposalLineItems     |                       26 |                       26 |      52 |
| RackScore             |                       25 |                       23 |      48 |
| RealtyAlignment       |                       21 |                       21 |      42 |
| ProductLoss           |                        4 |                        5 |       9 |


#### Top Relationships by Confidence
| Source                                   | Target                                   |   Confidence | Type   | Detection Method        |
|:-----------------------------------------|:-----------------------------------------|-------------:|:-------|:------------------------|
| Asset.store_type                         | AssetLeak.store_type                     |            1 |        | statistical_overlap_csv |
| Asset.store_type                         | WorkOrders.store_type                    |            1 |        | statistical_overlap_csv |
| AssetLeak.store_type                     | Asset.store_type                         |            1 |        | statistical_overlap_csv |
| AssetLeak.store_type                     | Locations.store_type                     |            1 |        | statistical_overlap_csv |
| AssetLeak.store_type                     | ProposalLineItems.store_type             |            1 |        | statistical_overlap_csv |
| AssetLeak.store_type                     | Proposals.store_type                     |            1 |        | statistical_overlap_csv |
| AssetLeak.store_type                     | ServiceChannelInvoice.store_type         |            1 |        | statistical_overlap_csv |
| Locations.store_type                     | AssetLeak.store_type                     |            1 |        | statistical_overlap_csv |
| Locations.ods_dataquality_status         | ProposalLineItems.ods_dataquality_status |            1 |        | statistical_overlap_csv |
| Locations.store_type                     | Proposals.store_type                     |            1 |        | statistical_overlap_csv |
| Locations.ods_dataquality_status         | Proposals.ods_dataquality_status         |            1 |        | statistical_overlap_csv |
| Locations.ods_dataquality_status         | WorkOrders.ods_dataquality_status        |            1 |        | statistical_overlap_csv |
| ProposalLineItems.store_type             | AssetLeak.store_type                     |            1 |        | statistical_overlap_csv |
| ProposalLineItems.ods_dataquality_status | Locations.ods_dataquality_status         |            1 |        | statistical_overlap_csv |
| ProposalLineItems.ods_dataquality_status | Proposals.ods_dataquality_status         |            1 |        | statistical_overlap_csv |


## Business Glossary
### Business Terms
The semantic graph contains 26 business terms providing business context for technical data elements.

#### Sample Business Terms
| Term               | Definition                                                                                              | Mapped To                                                  |
|:-------------------|:--------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------|
| Proposal Line Item | A specific component or service included in a business proposal, detailing type of work, material sp... | ProposalLineItems, ods_dataquality_status, store_type, ... |
| Asset              | A physical item owned or managed by an organization, tracked for management, maintenance, and financ... | Asset, id, serial_number, ...                              |
| Location           | A physical place where business operations occur, such as a store or warehouse.                         | Locations, location_full_name, location_name, ...          |
| Product Loss       | The financial impact of inventory discrepancies, sales losses, and operational costs due to product ... | ProductLoss, DEPT_NBR, Year_month_date, ...                |
| Proposal           | A formal offer or plan put forward for consideration or discussion by others, typically within a bus... | Proposals, proposal_number, proposal_description, ...      |
| Store              | A retail location identified by a unique store number, type, and address.                               | RealtyAlignment, store_no, store_address, ...              |
| Asset Lifecycle    | The stages an asset goes through from creation and installation to deactivation.                        | Asset, deactivated_date, install_date                      |
| Asset Status       | The current operational state of an asset, such as active, inactive, or under maintenance.              | Asset, asset_status, active                                |
| Invoice            | A document that itemizes and records a transaction between a buyer and a seller, detailing the produ... | ServiceChannelInvoice, invoice_id, invoice_nbr             |
| Service Provider   | An entity responsible for fulfilling work orders, tracked for performance and accountability.           | WorkOrders, provider_name, provider_id                     |


### Business Metrics
The semantic graph contains 19 business metrics that define KPIs and measurements.

#### Sample Business Metrics
| Metric                         | Definition                                                                                |
|:-------------------------------|:------------------------------------------------------------------------------------------|
| Active Asset Count             | The total number of assets currently marked as active.                                    |
| Active Asset Count             | The number of assets currently marked as active.                                          |
| Asset Utilization Rate         | The percentage of assets that are actively in use compared to the total number of assets. |
| Facilities Management Coverage | The extent of facilities management operations across different regions.                  |
| Labor Cost                     | The monetary amount charged for labor services on the invoice.                            |
| Location Lifecycle Duration    | The duration of time a location has been operational, from its opening to its closure.    |
| Number of Assignments          | The total number of tasks or assignments associated with a specific location.             |
| Number of Leak Inspections     | The total count of leak inspections conducted on assets.                                  |
| Number of Stores per Region    | The total count of stores within each realty operations region.                           |
| Proposal Approval Rate         | The percentage of proposals that are approved out of the total proposals submitted.       |


## Graph-Aware Glossary Entities
The following entity types leverage the graph structure to provide rich semantic understanding that spans multiple tables and captures business meaning across the data model.

### Composite Concepts
Composite concepts represent business entities that span multiple tables, capturing complex real-world business objects that cannot be represented by a single table.

The semantic graph contains 9 composite concepts.

| Concept                              | Definition                                                                                              | Business Importance                                                                                                              | Tables                                       |
|:-------------------------------------|:--------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------|
| Asset Management System              | A system that integrates various tables to manage and track assets, their locations, and statuses.      | Central to managing physical assets and ensuring operational efficiency.                                                         | Asset                                        |
| Location-Based Work Order Management | The process of managing work orders based on their associated locations, including tracking and repo... | Facilitates efficient resource allocation and service delivery based on geographic data.                                         | Locations, WorkOrders                        |
| Proposal Financial Breakdown         | A detailed breakdown of the financial components of a proposal, including labor, material, tax, and ... | Provides detailed financial insights for budgeting and cost management.                                                          | Proposals, ServiceChannelInvoice             |
| Proposal Management                  | The process of creating, tracking, and managing business proposals and their components.                | Enables detailed tracking and management of proposals and their components for accurate cost estimation and resource allocation. | Proposals, ProposalLineItems                 |
| Rack Performance Analysis            | A comprehensive analysis of rack performance across different stores and vendors.                       | Enables optimization of rack operations and vendor management.                                                                   | Asset, WorkOrders, RackScore                 |
| Service Invoice                      | A comprehensive record of financial transactions related to services rendered, including labor, mate... | Facilitates financial tracking and auditing of service-related transactions.                                                     | Proposals, WorkOrders, ServiceChannelInvoice |
| Store Alignment                      | The alignment of stores with their respective operational and management regions.                       | Ensures efficient management and oversight of real estate assets.                                                                | RealtyAlignment                              |
| Store Operations Analysis            | A comprehensive analysis of store operations, including product loss, sales performance, and operati... | Provides insights into operational efficiency and areas for improvement.                                                         | RackScore, ProductLoss, WorkOrders, ...      |
| Store Performance Analysis           | An analysis of store performance metrics, including sales, inventory, and customer satisfaction.        | Critical for strategic planning and operational improvements.                                                                    | RackScore, Locations, WorkOrders, ...        |


### Business Processes
Business processes represent workflows and procedures that span multiple tables, capturing the operational flow of business activities.

The semantic graph contains 11 business processes.

| Process                         | Definition                                                                                              | Tables                |
|:--------------------------------|:--------------------------------------------------------------------------------------------------------|:----------------------|
| Asset Leak Inspection Process   | A workflow for conducting and documenting inspections of asset leaks.                                   | AssetLeak             |
| Asset Tracking                  | The process of monitoring the location, status, and usage of assets within an organization.             | Asset                 |
| Invoice Management              | The process of creating, tracking, and managing invoices to ensure accurate billing and payment.        | ServiceChannelInvoice |
| Location Management             | The process of managing the lifecycle of a location, from opening to closure.                           | Locations             |
| Maintenance Scheduling          | The process of planning and executing maintenance activities for assets based on their lifecycle and... | Asset, WorkOrders     |
| Product Loss Management         | The process of tracking, analyzing, and mitigating product loss within the organization.                | ProductLoss           |
| Proposal Lifecycle Management   | The process of managing the lifecycle of a proposal from creation to approval or rejection.             | Proposals             |
| Proposal Line Item Lifecycle    | The lifecycle of a proposal line item from creation to historical record, including updates and refi... | ProposalLineItems     |
| Rack Performance Monitoring     | The process of tracking and analyzing the performance of racks in retail stores.                        | RackScore             |
| Store Alignment Management      | The process of creating, updating, and managing store alignment data.                                   | RealtyAlignment       |
| Work Order Lifecycle Management | The end-to-end process of managing work orders from initiation to completion, including scheduling a... | WorkOrders            |


### Relationship Concepts
Relationship concepts capture business-meaningful connections between tables, providing semantic context to technical foreign key relationships.

The semantic graph contains 13 relationship concepts.

| Relationship                           | Definition                                                                                              | Tables                |
|:---------------------------------------|:--------------------------------------------------------------------------------------------------------|:----------------------|
| Asset to Inspection Relationship       | The connection between an asset and its associated leak inspections.                                    | AssetLeak             |
| Asset to Location Mapping              | The association between an asset and its physical location within the organization.                     |                       |
| Asset to Rack Relationship             | The linkage between assets and the racks they are associated with.                                      | Asset                 |
| Asset to Supplier Relationship         | The connection between an asset and the supplier from which it was procured.                            |                       |
| Invoice to Work Order Link             | The connection between an invoice and its associated work order, facilitating service tracking.         | WorkOrders            |
| Location to Work Orders                | The relationship between a location and its associated work orders.                                     | WorkOrders            |
| Proposal to Line Item Association      | The relationship between a proposal and its associated line items, enabling detailed tracking of pro... | Proposals             |
| Proposal to Work Order Link            | The connection between a proposal and its associated work order, facilitating tracking and managemen... | WorkOrders            |
| Store Number to ServiceChannel Invoice | The connection between a store number and its related service channel invoices.                         | ServiceChannelInvoice |
| Store Type Reference                   | The relationship between store types in RealtyAlignment and other tables.                               | WorkOrders            |
| Store to Rack Relationship             | The connection between store locations and the racks they contain.                                      | Locations             |
| Store to Work Orders                   | The relationship between a store and its associated work orders.                                        | WorkOrders            |
| Work Order to ServiceChannelInvoice    | The relationship between work orders and their corresponding financial invoices.                        | ServiceChannelInvoice |


### Hierarchical Concepts
Hierarchical concepts represent parent-child structures in the data, providing insight into classification hierarchies and organizational structures.

The semantic graph contains 9 hierarchical concepts.

| Hierarchy                                | Definition                                                                                              | Tables          |
|:-----------------------------------------|:--------------------------------------------------------------------------------------------------------|:----------------|
| Asset Inspection Hierarchy               | A hierarchical structure representing the relationship between assets and their inspections.            | AssetLeak       |
| Asset Type Hierarchy                     | A classification structure for assets based on their type and category.                                 | Asset           |
| Facilities Management Hierarchy          | The hierarchical structure of facilities management roles and regions.                                  | RealtyAlignment |
| Location Hierarchy                       | The hierarchical structure of locations, including parent and child relationships.                      | Locations       |
| Organizational Structure for Work Orders | The hierarchy of organizational units responsible for managing work orders, from global business uni... | WorkOrders      |
| Proposal Approval Hierarchy              | The hierarchical structure representing the approval process of proposals.                              | Proposals       |
| Proposal Hierarchy                       | A hierarchical structure where proposals are the parent entities and line items are the child entiti... | Proposals       |
| Store Hierarchy                          | The hierarchical structure of stores and their associated racks.                                        | RackScore       |
| Store and Department Hierarchy           | The hierarchical structure of stores and their departments.                                             | ProductLoss     |


## Network Analysis
### Business Term Relationships
#### Most Connected Business Terms
| Term             |   Connection Count |
|:-----------------|-------------------:|
| Asset            |                 16 |
| Invoice          |                 12 |
| Work Order       |                 10 |
| Resource         |                  7 |
| Vendor           |                  7 |
| Store Number     |                  6 |
| Store ID         |                  6 |
| Equipment        |                  5 |
| Service Provider |                  5 |
| Property         |                  3 |


## Business Value in Action
### Example Natural Language Queries
The semantic graph enables natural language queries that leverage the business glossary. Here are some example queries that can be translated to SQL:

- "Show me all Asset"
- "What is the total number of Asset?"
- "List the top 10 Asset by count"
- "Calculate the Active Asset Count for each department"
- "Show me the trend of Active Asset Count over the last 12 months"


### Key Use Cases
The semantic graph enables various high-value business use cases:

1. **Integrated Business Intelligence**
   - Connect dashboards directly to the semantic layer
   - Consistent definitions across all reporting
   - Automatic business context for technical metrics

2. **Self-Service Analytics**
   - Non-technical users can query using familiar business language
   - Reduced dependency on data teams for simple queries
   - Faster time-to-insight for business users

3. **Data Lineage and Impact Analysis**
   - Trace data flows across systems and tables
   - Understand upstream and downstream dependencies
   - Assess potential impact of schema changes

4. **Knowledge Management**
   - Central repository for business definitions
   - Preserved institutional knowledge about data
   - Reduced onboarding time for new analysts

5. **Data Governance and Compliance**
   - Clear ownership and definitions of business terms
   - Consistent implementation of business rules
   - Audit trail of data understanding

## Conclusion
The semantic graph for the Walmart-stores tenant provides a comprehensive knowledge layer that bridges technical metadata with business understanding. By connecting tables, relationships, and rich business entities, it enables more intuitive data exploration, better self-service analytics, and improved business intelligence.

The graph-aware approach captures complex business concepts that span multiple tables, uncovering insights that would be difficult to discover through traditional approaches. This semantic layer continues to evolve as more data is added, relationships are discovered, and business understanding is enhanced.

Next steps include expanding the graph to additional datasets, enriching the business glossary with domain expertise, and integrating the semantic graph with existing business intelligence tools to maximize the value across the organization.