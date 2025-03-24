"""
Walmart Schema Simulator

Generates mock BigQuery schema for a retail dataset based on provided schema.
"""
from typing import Dict, List, Any

class WalmartSchemaSimulator:
    """Simulates BigQuery schema for a Walmart retail dataset."""
    
    def __init__(self, tenant_id: str):
        """
        Initialize the Walmart schema simulator.
        
        Args:
            tenant_id: Tenant ID
        """
        self.tenant_id = tenant_id
        self.dataset_id = "walmart_retail"
    
    def generate_schema(self) -> Dict[str, Any]:
        """
        Generate a mock BigQuery schema for Walmart retail domain.
        
        Returns:
            Dictionary with mock schema information
        """
        tables = [
            self._create_departments_table(),
            self._create_categories_table(),
            self._create_subcategories_table(),
            self._create_suppliers_table(),
            self._create_products_table(),
            self._create_legacy_products_table(),
            self._create_distribution_centers_table(),
            self._create_dc_inventory_table(),
            self._create_stores_table(),
            self._create_store_employees_table(),
            self._create_inventory_table(),
            self._create_sales_table(),
            self._create_promotions_table(),
            self._create_pricing_table(),
            self._create_returns_table(),
        ]
        
        return {
            "dataset_id": self.dataset_id,
            "tables": tables
        }
    
    def _create_departments_table(self) -> Dict[str, Any]:
        """Create departments table schema."""
        return {
            "table_name": "Departments",
            "description": "Department information for Walmart retail stores",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Departments` (
  department_id STRING,
  department_code STRING,            
  dept_legacy_code STRING,           
  department_name STRING,
  department_manager STRING,
  location_code STRING,
  active_flag BOOL,
  budget FLOAT64,
  created_on DATE,
  last_modified DATE,
  legacy_notes STRING                
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "department_id", "data_type": "STRING", "description": "Unique identifier for the department", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "department_code", "data_type": "STRING", "description": "Department code used in operational systems", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "dept_legacy_code", "data_type": "STRING", "description": "Legacy department code from previous systems", "is_nullable": True, "ordinal_position": 3},
                {"column_name": "department_name", "data_type": "STRING", "description": "Department name", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "department_manager", "data_type": "STRING", "description": "Department manager name", "is_nullable": True, "ordinal_position": 5},
                {"column_name": "location_code", "data_type": "STRING", "description": "Physical location code", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "active_flag", "data_type": "BOOL", "description": "Whether the department is active", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "budget", "data_type": "FLOAT64", "description": "Department budget", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "created_on", "data_type": "DATE", "description": "Date when the department was created", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "last_modified", "data_type": "DATE", "description": "Date when the department was last modified", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "legacy_notes", "data_type": "STRING", "description": "Notes from legacy system migration", "is_nullable": True, "ordinal_position": 11}
            ]
        }
    
    def _create_categories_table(self) -> Dict[str, Any]:
        """Create categories table schema."""
        return {
            "table_name": "Categories",
            "description": "Product categories for Walmart retail stores",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Categories` (
  category_id STRING,
  category_code STRING,
  category_legacy_id STRING,        
  category_name STRING,
  department_id STRING,             
  dept_legacy_code STRING,          
  category_description STRING,
  active_flag BOOL,
  create_date DATE,
  last_update DATE
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "category_id", "data_type": "STRING", "description": "Unique identifier for the category", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "category_code", "data_type": "STRING", "description": "Category code used in operational systems", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "category_legacy_id", "data_type": "STRING", "description": "Legacy category ID from previous systems", "is_nullable": True, "ordinal_position": 3},
                {"column_name": "category_name", "data_type": "STRING", "description": "Category name", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "department_id", "data_type": "STRING", "description": "Reference to Departments table", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "dept_legacy_code", "data_type": "STRING", "description": "Legacy department code for integration with older systems", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "category_description", "data_type": "STRING", "description": "Category description", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "active_flag", "data_type": "BOOL", "description": "Whether the category is active", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "create_date", "data_type": "DATE", "description": "Date when the category was created", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "last_update", "data_type": "DATE", "description": "Date when the category was last updated", "is_nullable": True, "ordinal_position": 10}
            ]
        }

    def _create_subcategories_table(self) -> Dict[str, Any]:
        """Create subcategories table schema."""
        return {
            "table_name": "SubCategories",
            "description": "Product subcategories for Walmart retail stores",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.SubCategories` (
  sub_category_id STRING,
  sub_cat_code STRING,
  sub_category_name STRING,
  category_id STRING,
  department_id STRING,
  sub_category_description STRING,
  active_flag BOOL,
  is_current BOOL,                 
  create_date DATE,
  updated_dt DATE
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "sub_category_id", "data_type": "STRING", "description": "Unique identifier for the subcategory", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "sub_cat_code", "data_type": "STRING", "description": "Subcategory code used in operational systems", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "sub_category_name", "data_type": "STRING", "description": "Subcategory name", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "category_id", "data_type": "STRING", "description": "Reference to Categories table", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "department_id", "data_type": "STRING", "description": "Reference to Departments table", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "sub_category_description", "data_type": "STRING", "description": "Subcategory description", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "active_flag", "data_type": "BOOL", "description": "Whether the subcategory is active", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "is_current", "data_type": "BOOL", "description": "Whether the subcategory is currently in use", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "create_date", "data_type": "DATE", "description": "Date when the subcategory was created", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "updated_dt", "data_type": "DATE", "description": "Date when the subcategory was last updated", "is_nullable": True, "ordinal_position": 10}
            ]
        }

    def _create_suppliers_table(self) -> Dict[str, Any]:
        """Create suppliers table schema."""
        return {
            "table_name": "Suppliers",
            "description": "Supplier information for Walmart products",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Suppliers` (
  supplier_id STRING,
  supplier_code STRING,
  supplier_legacy_id STRING,
  supplier_name STRING,
  contact_name STRING,
  contact_phone STRING,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  country STRING,
  payment_terms STRING,
  supplier_rating INT64,
  legacy_supplier_rating FLOAT64,   
  preferred_flag BOOL,
  create_date DATE,
  updated_on DATE
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "supplier_id", "data_type": "STRING", "description": "Unique identifier for the supplier", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "supplier_code", "data_type": "STRING", "description": "Supplier code used in operational systems", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "supplier_legacy_id", "data_type": "STRING", "description": "Legacy supplier ID from previous systems", "is_nullable": True, "ordinal_position": 3},
                {"column_name": "supplier_name", "data_type": "STRING", "description": "Supplier name", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "contact_name", "data_type": "STRING", "description": "Supplier contact person", "is_nullable": True, "ordinal_position": 5},
                {"column_name": "contact_phone", "data_type": "STRING", "description": "Supplier contact phone", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "address", "data_type": "STRING", "description": "Supplier address", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "city", "data_type": "STRING", "description": "Supplier city", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "state", "data_type": "STRING", "description": "Supplier state/province", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "zip", "data_type": "STRING", "description": "Supplier ZIP/postal code", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "country", "data_type": "STRING", "description": "Supplier country", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "payment_terms", "data_type": "STRING", "description": "Payment terms for the supplier", "is_nullable": True, "ordinal_position": 12},
                {"column_name": "supplier_rating", "data_type": "INT64", "description": "Supplier rating (1-5)", "is_nullable": True, "ordinal_position": 13},
                {"column_name": "legacy_supplier_rating", "data_type": "FLOAT64", "description": "Legacy supplier rating from previous system", "is_nullable": True, "ordinal_position": 14},
                {"column_name": "preferred_flag", "data_type": "BOOL", "description": "Whether the supplier is preferred", "is_nullable": False, "ordinal_position": 15},
                {"column_name": "create_date", "data_type": "DATE", "description": "Date when the supplier was created", "is_nullable": False, "ordinal_position": 16},
                {"column_name": "updated_on", "data_type": "DATE", "description": "Date when the supplier was last updated", "is_nullable": True, "ordinal_position": 17}
            ]
        }

    def _create_products_table(self) -> Dict[str, Any]:
        """Create products table schema."""
        return {
            "table_name": "Products",
            "description": "Product information for Walmart retail stores",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Products` (
  product_id STRING,
  sku_code STRING,
  vendor_id STRING,                
  primary_supplier_id STRING,      
  product_name STRING,
  department_id STRING,
  category_id STRING,
  sub_category_id STRING,
  brand STRING,
  product_description STRING,
  msrp FLOAT64,
  cost FLOAT64,
  profit_margin FLOAT64,
  legacy_product_flag BOOL,
  creation_date DATE,
  discontinued_flag BOOL,
  usual_lead_time_days INT64
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "product_id", "data_type": "STRING", "description": "Unique identifier for the product", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "sku_code", "data_type": "STRING", "description": "Stock Keeping Unit code", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "vendor_id", "data_type": "STRING", "description": "Reference to vendor/supplier", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "primary_supplier_id", "data_type": "STRING", "description": "Reference to primary supplier", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "product_name", "data_type": "STRING", "description": "Product name", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "department_id", "data_type": "STRING", "description": "Reference to Departments table", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "category_id", "data_type": "STRING", "description": "Reference to Categories table", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "sub_category_id", "data_type": "STRING", "description": "Reference to SubCategories table", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "brand", "data_type": "STRING", "description": "Product brand", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "product_description", "data_type": "STRING", "description": "Product description", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "msrp", "data_type": "FLOAT64", "description": "Manufacturer's Suggested Retail Price", "is_nullable": False, "ordinal_position": 11},
                {"column_name": "cost", "data_type": "FLOAT64", "description": "Product cost", "is_nullable": False, "ordinal_position": 12},
                {"column_name": "profit_margin", "data_type": "FLOAT64", "description": "Profit margin percentage", "is_nullable": True, "ordinal_position": 13},
                {"column_name": "legacy_product_flag", "data_type": "BOOL", "description": "Whether this is a legacy product", "is_nullable": False, "ordinal_position": 14},
                {"column_name": "creation_date", "data_type": "DATE", "description": "Date when the product was created", "is_nullable": False, "ordinal_position": 15},
                {"column_name": "discontinued_flag", "data_type": "BOOL", "description": "Whether the product is discontinued", "is_nullable": False, "ordinal_position": 16},
                {"column_name": "usual_lead_time_days", "data_type": "INT64", "description": "Usual lead time in days for ordering", "is_nullable": True, "ordinal_position": 17}
            ]
        }

    def _create_legacy_products_table(self) -> Dict[str, Any]:
        """Create legacy products table schema."""
        return {
            "table_name": "LegacyProducts",
            "description": "Legacy product information from previous system",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.LegacyProducts` (
  legacy_prod_id STRING,
  product_name STRING,
  vendor_id STRING,
  msrp FLOAT64,
  cost FLOAT64,
  old_margin_calc FLOAT64,
  is_discontinued BOOL,
  notes STRING
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "legacy_prod_id", "data_type": "STRING", "description": "Legacy product identifier", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "product_name", "data_type": "STRING", "description": "Product name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "vendor_id", "data_type": "STRING", "description": "Reference to vendor/supplier", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "msrp", "data_type": "FLOAT64", "description": "Manufacturer's Suggested Retail Price", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "cost", "data_type": "FLOAT64", "description": "Product cost", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "old_margin_calc", "data_type": "FLOAT64", "description": "Profit margin calculation from old system", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "is_discontinued", "data_type": "BOOL", "description": "Whether the product is discontinued", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "notes", "data_type": "STRING", "description": "Additional notes", "is_nullable": True, "ordinal_position": 8}
            ]
        }

    def _create_distribution_centers_table(self) -> Dict[str, Any]:
        """Create distribution centers table schema."""
        return {
            "table_name": "DistributionCenters",
            "description": "Distribution center information for Walmart",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.DistributionCenters` (
  dc_id STRING,
  dc_code STRING, 
  dc_name STRING,
  city STRING,
  state STRING,
  zip STRING,
  capacity INT64,
  manager_name STRING,
  manager_email STRING,
  phone STRING,
  region STRING,
  open_date DATE,
  is_active BOOL,
  notes TEXT
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "dc_id", "data_type": "STRING", "description": "Unique identifier for the distribution center", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "dc_code", "data_type": "STRING", "description": "Distribution center code used in operational systems", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "dc_name", "data_type": "STRING", "description": "Distribution center name", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "city", "data_type": "STRING", "description": "Distribution center city", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "state", "data_type": "STRING", "description": "Distribution center state/province", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "zip", "data_type": "STRING", "description": "Distribution center ZIP/postal code", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "capacity", "data_type": "INT64", "description": "Distribution center capacity", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "manager_name", "data_type": "STRING", "description": "Distribution center manager name", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "manager_email", "data_type": "STRING", "description": "Distribution center manager email", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "phone", "data_type": "STRING", "description": "Distribution center phone number", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "region", "data_type": "STRING", "description": "Geographic region", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "open_date", "data_type": "DATE", "description": "Date when the distribution center was opened", "is_nullable": False, "ordinal_position": 12},
                {"column_name": "is_active", "data_type": "BOOL", "description": "Whether the distribution center is active", "is_nullable": False, "ordinal_position": 13},
                {"column_name": "notes", "data_type": "STRING", "description": "Additional notes", "is_nullable": True, "ordinal_position": 14}
            ]
        }

    def _create_dc_inventory_table(self) -> Dict[str, Any]:
        """Create DC inventory table schema."""
        return {
            "table_name": "DCInventory",
            "description": "Distribution center inventory for Walmart products",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.DCInventory` (
  dc_inventory_id STRING,
  dc_id STRING,
  product_id STRING,
  dc_supplier_id STRING,      
  quantity_on_hand INT64,
  pending_shipment INT64,
  msrp FLOAT64,               
  last_inbound_date DATE,
  last_outbound_date DATE,
  notes STRING
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "dc_inventory_id", "data_type": "STRING", "description": "Unique identifier for the DC inventory record", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "dc_id", "data_type": "STRING", "description": "Reference to DistributionCenters table", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to Products table", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "dc_supplier_id", "data_type": "STRING", "description": "Reference to Suppliers table", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "quantity_on_hand", "data_type": "INT64", "description": "Current quantity in stock", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "pending_shipment", "data_type": "INT64", "description": "Quantity pending in shipments", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "msrp", "data_type": "FLOAT64", "description": "Manufacturer's Suggested Retail Price", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "last_inbound_date", "data_type": "DATE", "description": "Date of last inbound shipment", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "last_outbound_date", "data_type": "DATE", "description": "Date of last outbound shipment", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "notes", "data_type": "STRING", "description": "Additional notes", "is_nullable": True, "ordinal_position": 10}
            ]
        }

    def _create_stores_table(self) -> Dict[str, Any]:
        """Create stores table schema."""
        return {
            "table_name": "Stores",
            "description": "Store information for Walmart retail locations",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Stores` (
  store_id STRING,
  store_code STRING,
  store_name STRING,
  store_manager STRING,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  region STRING,
  store_region_code STRING,      
  open_date DATE,
  store_type STRING,
  store_legacy_type STRING,      
  store_area_sqft INT64,
  annual_revenue_estimate FLOAT64,
  tax_rate FLOAT64,
  active_store BOOL
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "store_id", "data_type": "STRING", "description": "Unique identifier for the store", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "store_code", "data_type": "STRING", "description": "Store code used in operational systems", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "store_name", "data_type": "STRING", "description": "Store name", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "store_manager", "data_type": "STRING", "description": "Store manager name", "is_nullable": True, "ordinal_position": 4},
                {"column_name": "address", "data_type": "STRING", "description": "Store address", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "city", "data_type": "STRING", "description": "Store city", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "state", "data_type": "STRING", "description": "Store state/province", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "zip", "data_type": "STRING", "description": "Store ZIP/postal code", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "region", "data_type": "STRING", "description": "Geographic region", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "store_region_code", "data_type": "STRING", "description": "Region code for the store", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "open_date", "data_type": "DATE", "description": "Date when the store was opened", "is_nullable": False, "ordinal_position": 11},
                {"column_name": "store_type", "data_type": "STRING", "description": "Type of store (Supercenter, Neighborhood, etc.)", "is_nullable": False, "ordinal_position": 12},
                {"column_name": "store_legacy_type", "data_type": "STRING", "description": "Legacy store type from previous system", "is_nullable": True, "ordinal_position": 13},
                {"column_name": "store_area_sqft", "data_type": "INT64", "description": "Store area in square feet", "is_nullable": True, "ordinal_position": 14},
                {"column_name": "annual_revenue_estimate", "data_type": "FLOAT64", "description": "Estimated annual revenue", "is_nullable": True, "ordinal_position": 15},
                {"column_name": "tax_rate", "data_type": "FLOAT64", "description": "Local tax rate", "is_nullable": True, "ordinal_position": 16},
                {"column_name": "active_store", "data_type": "BOOL", "description": "Whether the store is active", "is_nullable": False, "ordinal_position": 17}
            ]
        }

    def _create_store_employees_table(self) -> Dict[str, Any]:
        """Create store employees table schema."""
        return {
            "table_name": "StoreEmployees",
            "description": "Employee information for Walmart retail stores",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.StoreEmployees` (
  employee_id STRING,
  store_id STRING,
  first_name STRING,
  last_name STRING,
  hire_date DATE,
  role STRING,
  salary FLOAT64,
  annual_compensation FLOAT64,    
  shift_type STRING,
  dept_code STRING,               
  active_flag BOOL
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "employee_id", "data_type": "STRING", "description": "Unique identifier for the employee", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "store_id", "data_type": "STRING", "description": "Reference to Stores table", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "first_name", "data_type": "STRING", "description": "Employee first name", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "last_name", "data_type": "STRING", "description": "Employee last name", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "hire_date", "data_type": "DATE", "description": "Date when the employee was hired", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "role", "data_type": "STRING", "description": "Employee role/position", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "salary", "data_type": "FLOAT64", "description": "Employee base salary", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "annual_compensation", "data_type": "FLOAT64", "description": "Total annual compensation including benefits", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "shift_type", "data_type": "STRING", "description": "Type of shift worked (Day, Night, Swing)", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "dept_code", "data_type": "STRING", "description": "Department code for the employee", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "active_flag", "data_type": "BOOL", "description": "Whether the employee is active", "is_nullable": False, "ordinal_position": 11}
            ]
        }

    def _create_inventory_table(self) -> Dict[str, Any]:
        """Create inventory table schema."""
        return {
            "table_name": "Inventory",
            "description": "Store inventory for Walmart products",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Inventory` (
  inventory_id STRING,
  store_id STRING,
  product_id STRING,
  dept_id STRING,              
  quantity_on_hand INT64,
  damaged_quantity INT64,
  allocated_quantity INT64,
  reorder_level INT64,
  reorder_quantity INT64,
  inventory_status STRING,
  stock_status STRING,         
  last_restock_date DATE
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "inventory_id", "data_type": "STRING", "description": "Unique identifier for the inventory record", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "store_id", "data_type": "STRING", "description": "Reference to Stores table", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to Products table", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "dept_id", "data_type": "STRING", "description": "Reference to Departments table", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "quantity_on_hand", "data_type": "INT64", "description": "Current quantity in stock", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "damaged_quantity", "data_type": "INT64", "description": "Quantity of damaged items", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "allocated_quantity", "data_type": "INT64", "description": "Quantity allocated for orders or holds", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "reorder_level", "data_type": "INT64", "description": "Level at which to reorder", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "reorder_quantity", "data_type": "INT64", "description": "Standard quantity to reorder", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "inventory_status", "data_type": "STRING", "description": "Inventory status (In Stock, Low Stock, etc.)", "is_nullable": False, "ordinal_position": 10},
                {"column_name": "stock_status", "data_type": "STRING", "description": "Stock status code", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "last_restock_date", "data_type": "DATE", "description": "Date of last restock", "is_nullable": True, "ordinal_position": 12}
            ]
        }

    def _create_sales_table(self) -> Dict[str, Any]:
        """Create sales table schema."""
        return {
            "table_name": "Sales",
            "description": "Sales transactions for Walmart products",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Sales` (
  sales_id STRING,
  store_id STRING,
  product_id STRING,
  sale_date DATE,
  transaction_time TIMESTAMP,
  quantity_sold INT64,
  unit_price FLOAT64,
  discount FLOAT64,
  total_revenue FLOAT64,
  taxes_applied FLOAT64,         
  promotion_id STRING,
  pos_terminal_id STRING,
  employee_id STRING,
  pos_data STRING                
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "sales_id", "data_type": "STRING", "description": "Unique identifier for the sales record", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "store_id", "data_type": "STRING", "description": "Reference to Stores table", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to Products table", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "sale_date", "data_type": "DATE", "description": "Date of sale", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "transaction_time", "data_type": "TIMESTAMP", "description": "Timestamp of transaction", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "quantity_sold", "data_type": "INT64", "description": "Quantity sold", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "unit_price", "data_type": "FLOAT64", "description": "Unit price at time of sale", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "discount", "data_type": "FLOAT64", "description": "Discount amount", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "total_revenue", "data_type": "FLOAT64", "description": "Total revenue from the sale", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "taxes_applied", "data_type": "FLOAT64", "description": "Taxes applied to the sale", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "promotion_id", "data_type": "STRING", "description": "Reference to Promotions table", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "pos_terminal_id", "data_type": "STRING", "description": "POS terminal ID", "is_nullable": True, "ordinal_position": 12},
                {"column_name": "employee_id", "data_type": "STRING", "description": "Reference to StoreEmployees table", "is_nullable": True, "ordinal_position": 13},
                {"column_name": "pos_data", "data_type": "STRING", "description": "Additional POS data", "is_nullable": True, "ordinal_position": 14}
            ]
        }

    def _create_promotions_table(self) -> Dict[str, Any]:
        """Create promotions table schema."""
        return {
            "table_name": "Promotions",
            "description": "Promotional campaigns for Walmart products",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Promotions` (
  promotion_id STRING,
  promotion_name STRING,
  start_date DATE,
  end_date DATE,
  discount_type STRING,
  discount_value FLOAT64,
  promotion_channel STRING,
  minimum_purchase FLOAT64,
  promo_scope STRING,       
  min_order_value FLOAT64   
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "promotion_id", "data_type": "STRING", "description": "Unique identifier for the promotion", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "promotion_name", "data_type": "STRING", "description": "Promotion name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "start_date", "data_type": "DATE", "description": "Promotion start date", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "end_date", "data_type": "DATE", "description": "Promotion end date", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "discount_type", "data_type": "STRING", "description": "Type of discount (Amount, Percent)", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "discount_value", "data_type": "FLOAT64", "description": "Discount value (amount or percentage)", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "promotion_channel", "data_type": "STRING", "description": "Channel for the promotion (In-Store, Online, Both)", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "minimum_purchase", "data_type": "FLOAT64", "description": "Minimum purchase amount required", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "promo_scope", "data_type": "STRING", "description": "Scope of the promotion (STORE, ONLINE, BOTH)", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "min_order_value", "data_type": "FLOAT64", "description": "Minimum order value required", "is_nullable": True, "ordinal_position": 10}
            ]
        }

    def _create_pricing_table(self) -> Dict[str, Any]:
        """Create pricing table schema."""
        return {
            "table_name": "Pricing",
            "description": "Pricing information for Walmart products",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Pricing` (
  pricing_id STRING,
  product_id STRING,
  effective_date DATE,
  list_price FLOAT64,
  unit_price FLOAT64,
  special_price FLOAT64,
  markup_percentage FLOAT64,
  price_currency STRING,
  comment STRING,
  override_flag BOOL
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "pricing_id", "data_type": "STRING", "description": "Unique identifier for the pricing record", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to Products table", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "effective_date", "data_type": "DATE", "description": "Date when the price becomes effective", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "list_price", "data_type": "FLOAT64", "description": "List price", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "unit_price", "data_type": "FLOAT64", "description": "Price per unit", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "special_price", "data_type": "FLOAT64", "description": "Special/sale price", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "markup_percentage", "data_type": "FLOAT64", "description": "Markup percentage", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "price_currency", "data_type": "STRING", "description": "Currency code (USD, etc.)", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "comment", "data_type": "STRING", "description": "Additional comments", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "override_flag", "data_type": "BOOL", "description": "Whether this price is a manual override", "is_nullable": False, "ordinal_position": 10}
            ]
        }

    def _create_returns_table(self) -> Dict[str, Any]:
        """Create returns table schema."""
        return {
            "table_name": "Returns",
            "description": "Product returns for Walmart stores",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": """CREATE TABLE `project.dataset.Returns` (
  return_id STRING,
  store_id STRING,
  product_id STRING,
  return_date DATE,
  quantity_returned INT64,
  reason_code STRING,
  comments STRING,
  refund_amount FLOAT64,
  amount_refunded FLOAT64,       
  return_channel STRING,
  return_mode STRING             
);""",
            "statistics": {"row_count": 10},
            "columns": [
                {"column_name": "return_id", "data_type": "STRING", "description": "Unique identifier for the return record", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "store_id", "data_type": "STRING", "description": "Reference to Stores table", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to Products table", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "return_date", "data_type": "DATE", "description": "Date of return", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "quantity_returned", "data_type": "INT64", "description": "Quantity returned", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "reason_code", "data_type": "STRING", "description": "Reason code for the return", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "comments", "data_type": "STRING", "description": "Additional comments", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "refund_amount", "data_type": "FLOAT64", "description": "Amount refunded", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "amount_refunded", "data_type": "FLOAT64", "description": "Amount actually refunded (may differ from refund_amount)", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "return_channel", "data_type": "STRING", "description": "Channel for the return (In-Store, Online, Mail-In)", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "return_mode", "data_type": "STRING", "description": "Mode of return (STORE, ONLINE, MAIL)", "is_nullable": True, "ordinal_position": 11}
            ]
        }