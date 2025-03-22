-- Walmart Retail Schema Definition

-- Departments table
CREATE TABLE `project.dataset.Departments` (
  department_id STRING COMMENT 'Unique identifier for the department',
  department_code STRING COMMENT 'Department code used in operational systems',            
  dept_legacy_code STRING COMMENT 'Legacy department code from previous systems',           
  department_name STRING COMMENT 'Department name',
  department_manager STRING COMMENT 'Department manager name',
  location_code STRING COMMENT 'Physical location code',
  active_flag BOOL COMMENT 'Whether the department is active',
  budget FLOAT64 COMMENT 'Department budget',
  created_on DATE COMMENT 'Date when the department was created',
  last_modified DATE COMMENT 'Date when the department was last modified',
  legacy_notes STRING COMMENT 'Notes from legacy system migration'              
);

-- Categories table
CREATE TABLE `project.dataset.Categories` (
  category_id STRING COMMENT 'Unique identifier for the category',
  category_code STRING COMMENT 'Category code used in operational systems',
  category_legacy_id STRING COMMENT 'Legacy category ID from previous systems',        
  category_name STRING COMMENT 'Category name',
  department_id STRING COMMENT 'Reference to Departments table',             
  dept_legacy_code STRING COMMENT 'Legacy department code for integration with older systems',          
  category_description STRING COMMENT 'Category description',
  active_flag BOOL COMMENT 'Whether the category is active',
  create_date DATE COMMENT 'Date when the category was created',
  last_update DATE COMMENT 'Date when the category was last updated'
);

-- SubCategories table
CREATE TABLE `project.dataset.SubCategories` (
  sub_category_id STRING COMMENT 'Unique identifier for the subcategory',
  sub_cat_code STRING COMMENT 'Subcategory code used in operational systems',
  sub_category_name STRING COMMENT 'Subcategory name',
  category_id STRING COMMENT 'Reference to Categories table',
  department_id STRING COMMENT 'Reference to Departments table',
  sub_category_description STRING COMMENT 'Subcategory description',
  active_flag BOOL COMMENT 'Whether the subcategory is active',
  is_current BOOL COMMENT 'Whether the subcategory is currently in use',                 
  create_date DATE COMMENT 'Date when the subcategory was created',
  updated_dt DATE COMMENT 'Date when the subcategory was last updated'
);

-- Suppliers table
CREATE TABLE `project.dataset.Suppliers` (
  supplier_id STRING COMMENT 'Unique identifier for the supplier',
  supplier_code STRING COMMENT 'Supplier code used in operational systems',
  supplier_legacy_id STRING COMMENT 'Legacy supplier ID from previous systems',
  supplier_name STRING COMMENT 'Supplier name',
  contact_name STRING COMMENT 'Supplier contact person',
  contact_phone STRING COMMENT 'Supplier contact phone',
  address STRING COMMENT 'Supplier address',
  city STRING COMMENT 'Supplier city',
  state STRING COMMENT 'Supplier state/province',
  zip STRING COMMENT 'Supplier ZIP/postal code',
  country STRING COMMENT 'Supplier country',
  payment_terms STRING COMMENT 'Payment terms for the supplier',
  supplier_rating INT64 COMMENT 'Supplier rating (1-5)',
  legacy_supplier_rating FLOAT64 COMMENT 'Legacy supplier rating from previous system',   
  preferred_flag BOOL COMMENT 'Whether the supplier is preferred',
  create_date DATE COMMENT 'Date when the supplier was created',
  updated_on DATE COMMENT 'Date when the supplier was last updated'
);

-- Products table
CREATE TABLE `project.dataset.Products` (
  product_id STRING COMMENT 'Unique identifier for the product',
  sku_code STRING COMMENT 'Stock Keeping Unit code',
  vendor_id STRING COMMENT 'Reference to vendor/supplier',                
  primary_supplier_id STRING COMMENT 'Reference to primary supplier',      
  product_name STRING COMMENT 'Product name',
  department_id STRING COMMENT 'Reference to Departments table',
  category_id STRING COMMENT 'Reference to Categories table',
  sub_category_id STRING COMMENT 'Reference to SubCategories table',
  brand STRING COMMENT 'Product brand',
  product_description STRING COMMENT 'Product description',
  msrp FLOAT64 COMMENT 'Manufacturer\'s Suggested Retail Price',
  cost FLOAT64 COMMENT 'Product cost',
  profit_margin FLOAT64 COMMENT 'Profit margin percentage',
  legacy_product_flag BOOL COMMENT 'Whether this is a legacy product',
  creation_date DATE COMMENT 'Date when the product was created',
  discontinued_flag BOOL COMMENT 'Whether the product is discontinued',
  usual_lead_time_days INT64 COMMENT 'Usual lead time in days for ordering'
);

-- LegacyProducts table
CREATE TABLE `project.dataset.LegacyProducts` (
  legacy_prod_id STRING COMMENT 'Legacy product identifier',
  product_name STRING COMMENT 'Product name',
  vendor_id STRING COMMENT 'Reference to vendor/supplier',
  msrp FLOAT64 COMMENT 'Manufacturer\'s Suggested Retail Price',
  cost FLOAT64 COMMENT 'Product cost',
  old_margin_calc FLOAT64 COMMENT 'Profit margin calculation from old system',
  is_discontinued BOOL COMMENT 'Whether the product is discontinued',
  notes STRING COMMENT 'Additional notes'
);

-- DistributionCenters table
CREATE TABLE `project.dataset.DistributionCenters` (
  dc_id STRING COMMENT 'Unique identifier for the distribution center',
  dc_code STRING COMMENT 'Distribution center code used in operational systems', 
  dc_name STRING COMMENT 'Distribution center name',
  city STRING COMMENT 'Distribution center city',
  state STRING COMMENT 'Distribution center state/province',
  zip STRING COMMENT 'Distribution center ZIP/postal code',
  capacity INT64 COMMENT 'Distribution center capacity',
  manager_name STRING COMMENT 'Distribution center manager name',
  manager_email STRING COMMENT 'Distribution center manager email',
  phone STRING COMMENT 'Distribution center phone number',
  region STRING COMMENT 'Geographic region',
  open_date DATE COMMENT 'Date when the distribution center was opened',
  is_active BOOL COMMENT 'Whether the distribution center is active',
  notes TEXT COMMENT 'Additional notes'
);

-- DCInventory table
CREATE TABLE `project.dataset.DCInventory` (
  dc_inventory_id STRING COMMENT 'Unique identifier for the DC inventory record',
  dc_id STRING COMMENT 'Reference to DistributionCenters table',
  product_id STRING COMMENT 'Reference to Products table',
  dc_supplier_id STRING COMMENT 'Reference to Suppliers table',      
  quantity_on_hand INT64 COMMENT 'Current quantity in stock',
  pending_shipment INT64 COMMENT 'Quantity pending in shipments',
  msrp FLOAT64 COMMENT 'Manufacturer\'s Suggested Retail Price',               
  last_inbound_date DATE COMMENT 'Date of last inbound shipment',
  last_outbound_date DATE COMMENT 'Date of last outbound shipment',
  notes STRING COMMENT 'Additional notes'
);

-- Stores table
CREATE TABLE `project.dataset.Stores` (
  store_id STRING COMMENT 'Unique identifier for the store',
  store_code STRING COMMENT 'Store code used in operational systems',
  store_name STRING COMMENT 'Store name',
  store_manager STRING COMMENT 'Store manager name',
  address STRING COMMENT 'Store address',
  city STRING COMMENT 'Store city',
  state STRING COMMENT 'Store state/province',
  zip STRING COMMENT 'Store ZIP/postal code',
  region STRING COMMENT 'Geographic region',
  store_region_code STRING COMMENT 'Region code for the store',      
  open_date DATE COMMENT 'Date when the store was opened',
  store_type STRING COMMENT 'Type of store (Supercenter, Neighborhood, etc.)',
  store_legacy_type STRING COMMENT 'Legacy store type from previous system',      
  store_area_sqft INT64 COMMENT 'Store area in square feet',
  annual_revenue_estimate FLOAT64 COMMENT 'Estimated annual revenue',
  tax_rate FLOAT64 COMMENT 'Local tax rate',
  active_store BOOL COMMENT 'Whether the store is active'
);

-- StoreEmployees table
CREATE TABLE `project.dataset.StoreEmployees` (
  employee_id STRING COMMENT 'Unique identifier for the employee',
  store_id STRING COMMENT 'Reference to Stores table',
  first_name STRING COMMENT 'Employee first name',
  last_name STRING COMMENT 'Employee last name',
  hire_date DATE COMMENT 'Date when the employee was hired',
  role STRING COMMENT 'Employee role/position',
  salary FLOAT64 COMMENT 'Employee base salary',
  annual_compensation FLOAT64 COMMENT 'Total annual compensation including benefits',    
  shift_type STRING COMMENT 'Type of shift worked (Day, Night, Swing)',
  dept_code STRING COMMENT 'Department code for the employee',               
  active_flag BOOL COMMENT 'Whether the employee is active'
);

-- Inventory table
CREATE TABLE `project.dataset.Inventory` (
  inventory_id STRING COMMENT 'Unique identifier for the inventory record',
  store_id STRING COMMENT 'Reference to Stores table',
  product_id STRING COMMENT 'Reference to Products table',
  dept_id STRING COMMENT 'Reference to Departments table',              
  quantity_on_hand INT64 COMMENT 'Current quantity in stock',
  damaged_quantity INT64 COMMENT 'Quantity of damaged items',
  allocated_quantity INT64 COMMENT 'Quantity allocated for orders or holds',
  reorder_level INT64 COMMENT 'Level at which to reorder',
  reorder_quantity INT64 COMMENT 'Standard quantity to reorder',
  inventory_status STRING COMMENT 'Inventory status (In Stock, Low Stock, etc.)',
  stock_status STRING COMMENT 'Stock status code',         
  last_restock_date DATE COMMENT 'Date of last restock'
);

-- Sales table
CREATE TABLE `project.dataset.Sales` (
  sales_id STRING COMMENT 'Unique identifier for the sales record',
  store_id STRING COMMENT 'Reference to Stores table',
  product_id STRING COMMENT 'Reference to Products table',
  sale_date DATE COMMENT 'Date of sale',
  transaction_time TIMESTAMP COMMENT 'Timestamp of transaction',
  quantity_sold INT64 COMMENT 'Quantity sold',
  unit_price FLOAT64 COMMENT 'Unit price at time of sale',
  discount FLOAT64 COMMENT 'Discount amount',
  total_revenue FLOAT64 COMMENT 'Total revenue from the sale',
  taxes_applied FLOAT64 COMMENT 'Taxes applied to the sale',         
  promotion_id STRING COMMENT 'Reference to Promotions table',
  pos_terminal_id STRING COMMENT 'POS terminal ID',
  employee_id STRING COMMENT 'Reference to StoreEmployees table',
  pos_data STRING COMMENT 'Additional POS data'                
);

-- Promotions table
CREATE TABLE `project.dataset.Promotions` (
  promotion_id STRING COMMENT 'Unique identifier for the promotion',
  promotion_name STRING COMMENT 'Promotion name',
  start_date DATE COMMENT 'Promotion start date',
  end_date DATE COMMENT 'Promotion end date',
  discount_type STRING COMMENT 'Type of discount (Amount, Percent)',
  discount_value FLOAT64 COMMENT 'Discount value (amount or percentage)',
  promotion_channel STRING COMMENT 'Channel for the promotion (In-Store, Online, Both)',
  minimum_purchase FLOAT64 COMMENT 'Minimum purchase amount required',
  promo_scope STRING COMMENT 'Scope of the promotion (STORE, ONLINE, BOTH)',       
  min_order_value FLOAT64 COMMENT 'Minimum order value required'   
);

-- Pricing table
CREATE TABLE `project.dataset.Pricing` (
  pricing_id STRING COMMENT 'Unique identifier for the pricing record',
  product_id STRING COMMENT 'Reference to Products table',
  effective_date DATE COMMENT 'Date when the price becomes effective',
  list_price FLOAT64 COMMENT 'List price',
  unit_price FLOAT64 COMMENT 'Price per unit',
  special_price FLOAT64 COMMENT 'Special/sale price',
  markup_percentage FLOAT64 COMMENT 'Markup percentage',
  price_currency STRING COMMENT 'Currency code (USD, etc.)',
  comment STRING COMMENT 'Additional comments',
  override_flag BOOL COMMENT 'Whether this price is a manual override'
);

-- Returns table
CREATE TABLE `project.dataset.Returns` (
  return_id STRING COMMENT 'Unique identifier for the return record',
  store_id STRING COMMENT 'Reference to Stores table',
  product_id STRING COMMENT 'Reference to Products table',
  return_date DATE COMMENT 'Date of return',
  quantity_returned INT64 COMMENT 'Quantity returned',
  reason_code STRING COMMENT 'Reason code for the return',
  comments STRING COMMENT 'Additional comments',
  refund_amount FLOAT64 COMMENT 'Amount refunded',
  amount_refunded FLOAT64 COMMENT 'Amount actually refunded (may differ from refund_amount)',       
  return_channel STRING COMMENT 'Channel for the return (In-Store, Online, Mail-In)',
  return_mode STRING COMMENT 'Mode of return (STORE, ONLINE, MAIL)'             
);