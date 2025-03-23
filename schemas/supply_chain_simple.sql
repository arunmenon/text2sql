-- Supply Chain Management Schema Definition

-- Warehouses table
CREATE TABLE Warehouses (
  warehouse_id STRING COMMENT 'Unique identifier for the warehouse',
  warehouse_code STRING COMMENT 'Warehouse code used in operational systems',
  warehouse_name STRING COMMENT 'Warehouse name',
  location_address STRING COMMENT 'Warehouse physical address',
  city STRING COMMENT 'Warehouse city',
  state STRING COMMENT 'Warehouse state/province',
  country STRING COMMENT 'Warehouse country',
  zip_code STRING COMMENT 'Warehouse ZIP/postal code',
  capacity_sqft FLOAT64 COMMENT 'Warehouse capacity in square feet',
  manager_id STRING COMMENT 'Warehouse manager employee ID',
  status STRING COMMENT 'Warehouse operational status',
  opening_date DATE COMMENT 'Date when warehouse became operational',
  is_automated BOOL COMMENT 'Whether warehouse uses automated systems',
  geo_location STRING COMMENT 'Geographic coordinates of the warehouse'
);

-- Suppliers table
CREATE TABLE Suppliers (
  supplier_id STRING COMMENT 'Unique identifier for the supplier',
  supplier_name STRING COMMENT 'Supplier company name',
  supplier_type STRING COMMENT 'Type of supplier (Manufacturer, Distributor, etc.)',
  contact_person STRING COMMENT 'Primary contact person name',
  email STRING COMMENT 'Contact email address',
  phone STRING COMMENT 'Contact phone number',
  address STRING COMMENT 'Supplier headquarters address',
  city STRING COMMENT 'Supplier city',
  state STRING COMMENT 'Supplier state/province',
  country STRING COMMENT 'Supplier country',
  postal_code STRING COMMENT 'Supplier postal/ZIP code',
  tax_id STRING COMMENT 'Supplier tax identification number',
  payment_terms STRING COMMENT 'Payment terms (Net30, Net60, etc.)',
  lead_time_days INT64 COMMENT 'Average lead time in days',
  reliability_score FLOAT64 COMMENT 'Supplier reliability score (0-100)',
  is_active BOOL COMMENT 'Whether supplier is currently active'
);

-- Items table
CREATE TABLE Items (
  item_id STRING COMMENT 'Unique identifier for the item',
  item_sku STRING COMMENT 'Stock keeping unit code',
  item_name STRING COMMENT 'Item name',
  description STRING COMMENT 'Item description',
  category STRING COMMENT 'Item category',
  subcategory STRING COMMENT 'Item subcategory',
  unit_of_measure STRING COMMENT 'Unit of measure (Each, Box, Pallet, etc.)',
  weight_kg FLOAT64 COMMENT 'Item weight in kilograms',
  volume_cubic_m FLOAT64 COMMENT 'Item volume in cubic meters',
  cost_price FLOAT64 COMMENT 'Cost price per unit',
  min_stock_level INT64 COMMENT 'Minimum stock level',
  max_stock_level INT64 COMMENT 'Maximum stock level',
  reorder_point INT64 COMMENT 'Reorder point quantity',
  lead_time_days INT64 COMMENT 'Average lead time for ordering',
  is_hazardous BOOL COMMENT 'Whether item contains hazardous materials',
  is_perishable BOOL COMMENT 'Whether item is perishable',
  created_date DATE COMMENT 'Date when item was created in system',
  discontinue_date DATE COMMENT 'Date when item was/will be discontinued'
);

-- PurchaseOrders table
CREATE TABLE PurchaseOrders (
  po_id STRING COMMENT 'Unique identifier for the purchase order',
  po_number STRING COMMENT 'Purchase order reference number',
  supplier_id STRING COMMENT 'Reference to supplier',
  warehouse_id STRING COMMENT 'Destination warehouse',
  order_date DATE COMMENT 'Date when order was placed',
  expected_delivery_date DATE COMMENT 'Expected delivery date',
  status STRING COMMENT 'Order status (Draft, Submitted, Delivered, etc.)',
  payment_status STRING COMMENT 'Payment status',
  total_amount FLOAT64 COMMENT 'Total order amount',
  currency STRING COMMENT 'Currency code',
  payment_terms STRING COMMENT 'Payment terms for this order',
  approval_date DATE COMMENT 'Date when order was approved',
  approved_by STRING COMMENT 'Person who approved the order',
  shipping_method STRING COMMENT 'Shipping method',
  notes STRING COMMENT 'Additional notes'
);

-- PurchaseOrderItems table
CREATE TABLE PurchaseOrderItems (
  po_item_id STRING COMMENT 'Unique identifier for PO line item',
  po_id STRING COMMENT 'Reference to purchase order',
  item_id STRING COMMENT 'Reference to item',
  quantity INT64 COMMENT 'Ordered quantity',
  unit_price FLOAT64 COMMENT 'Price per unit',
  total_price FLOAT64 COMMENT 'Total price for line item',
  discount_percent FLOAT64 COMMENT 'Discount percentage',
  tax_amount FLOAT64 COMMENT 'Tax amount',
  expected_delivery_date DATE COMMENT 'Expected delivery date for this item',
  status STRING COMMENT 'Line item status',
  received_quantity INT64 COMMENT 'Quantity received so far',
  received_date DATE COMMENT 'Date when items were received',
  quality_check_status STRING COMMENT 'Quality check status',
  notes STRING COMMENT 'Additional notes'
);

-- Inventory table
CREATE TABLE Inventory (
  inventory_id STRING COMMENT 'Unique identifier for inventory record',
  warehouse_id STRING COMMENT 'Reference to warehouse',
  item_id STRING COMMENT 'Reference to item',
  quantity_on_hand INT64 COMMENT 'Current available quantity',
  allocated_quantity INT64 COMMENT 'Quantity allocated to orders',
  available_quantity INT64 COMMENT 'Quantity available for allocation',
  bin_location STRING COMMENT 'Storage location identifier',
  value_on_hand FLOAT64 COMMENT 'Current inventory value',
  last_count_date DATE COMMENT 'Date of last inventory count',
  last_received_date DATE COMMENT 'Date when last shipment was received',
  last_issued_date DATE COMMENT 'Date when items were last issued',
  restock_level INT64 COMMENT 'Level at which restocking is triggered',
  optimal_stock INT64 COMMENT 'Optimal stock level',
  is_active BOOL COMMENT 'Whether inventory position is active'
);

-- InventoryTransactions table
CREATE TABLE InventoryTransactions (
  transaction_id STRING COMMENT 'Unique identifier for inventory transaction',
  warehouse_id STRING COMMENT 'Reference to warehouse',
  item_id STRING COMMENT 'Reference to item',
  transaction_type STRING COMMENT 'Type of transaction (Receipt, Issue, Transfer, Adjustment)',
  transaction_date TIMESTAMP COMMENT 'Date and time of transaction',
  quantity INT64 COMMENT 'Transaction quantity',
  reference_id STRING COMMENT 'Reference to source document',
  reference_type STRING COMMENT 'Type of reference document',
  from_location STRING COMMENT 'Source location for transfers',
  to_location STRING COMMENT 'Destination location for transfers',
  unit_value FLOAT64 COMMENT 'Value per unit',
  total_value FLOAT64 COMMENT 'Total transaction value',
  reason_code STRING COMMENT 'Reason code for transaction',
  notes STRING COMMENT 'Additional notes',
  created_by STRING COMMENT 'User who created the transaction',
  creation_timestamp TIMESTAMP COMMENT 'When transaction was created in system'
);

-- ShippingCarriers table
CREATE TABLE ShippingCarriers (
  carrier_id STRING COMMENT 'Unique identifier for shipping carrier',
  carrier_name STRING COMMENT 'Carrier company name',
  carrier_type STRING COMMENT 'Type of carrier (LTL, FTL, Parcel, etc.)',
  contact_person STRING COMMENT 'Primary contact person name',
  email STRING COMMENT 'Contact email address',
  phone STRING COMMENT 'Contact phone number',
  address STRING COMMENT 'Carrier headquarters address',
  city STRING COMMENT 'Carrier city',
  state STRING COMMENT 'Carrier state/province',
  country STRING COMMENT 'Carrier country',
  postal_code STRING COMMENT 'Carrier postal/ZIP code',
  contract_number STRING COMMENT 'Contract reference number',
  contract_start_date DATE COMMENT 'Contract start date',
  contract_end_date DATE COMMENT 'Contract end date',
  performance_rating FLOAT64 COMMENT 'Carrier performance rating',
  is_active BOOL COMMENT 'Whether carrier is currently active'
);

-- Shipments table
CREATE TABLE Shipments (
  shipment_id STRING COMMENT 'Unique identifier for shipment',
  shipment_number STRING COMMENT 'Shipment reference number',
  po_id STRING COMMENT 'Reference to purchase order',
  supplier_id STRING COMMENT 'Reference to supplier',
  carrier_id STRING COMMENT 'Reference to shipping carrier',
  origin_warehouse_id STRING COMMENT 'Origin warehouse for transfers',
  destination_warehouse_id STRING COMMENT 'Destination warehouse',
  ship_date DATE COMMENT 'Date when shipment was sent',
  expected_arrival_date DATE COMMENT 'Expected arrival date',
  actual_arrival_date DATE COMMENT 'Actual arrival date',
  status STRING COMMENT 'Shipment status',
  tracking_number STRING COMMENT 'Carrier tracking number',
  shipping_cost FLOAT64 COMMENT 'Total shipping cost',
  insurance_cost FLOAT64 COMMENT 'Insurance cost',
  total_weight_kg FLOAT64 COMMENT 'Total shipment weight in kilograms',
  shipping_method STRING COMMENT 'Shipping method used',
  customs_clearance_date DATE COMMENT 'Date of customs clearance for international shipments',
  notes STRING COMMENT 'Additional notes'
);

-- ShipmentItems table
CREATE TABLE ShipmentItems (
  shipment_item_id STRING COMMENT 'Unique identifier for shipment item',
  shipment_id STRING COMMENT 'Reference to shipment',
  po_item_id STRING COMMENT 'Reference to purchase order item',
  item_id STRING COMMENT 'Reference to item',
  quantity_shipped INT64 COMMENT 'Quantity shipped',
  quantity_received INT64 COMMENT 'Quantity received',
  lot_number STRING COMMENT 'Lot or batch number',
  expiration_date DATE COMMENT 'Expiration date for perishable items',
  condition_at_receipt STRING COMMENT 'Condition when received',
  is_damaged BOOL COMMENT 'Whether items were damaged during shipping',
  damage_notes STRING COMMENT 'Notes about any damage',
  received_by STRING COMMENT 'Person who received the items',
  receipt_date TIMESTAMP COMMENT 'Date and time of receipt'
);