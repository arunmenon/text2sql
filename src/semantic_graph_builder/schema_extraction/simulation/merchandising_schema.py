"""
Merchandising Schema Simulator

Generates mock BigQuery schema for a merchandising domain.
"""
from typing import Dict, List, Any

class MerchandisingSchemaSimulator:
    """Simulates BigQuery schema for a merchandising domain."""
    
    def __init__(self, tenant_id: str):
        """
        Initialize the merchandising schema simulator.
        
        Args:
            tenant_id: Tenant ID
        """
        self.tenant_id = tenant_id
        self.dataset_id = "merchandising"
    
    def generate_schema(self) -> Dict[str, Any]:
        """
        Generate a mock BigQuery schema for merchandising domain.
        
        Returns:
            Dictionary with mock schema information
        """
        tables = [
            self._create_products_table(),
            self._create_categories_table(),
            self._create_inventory_table(),
            self._create_price_history_table(),
            self._create_promotions_table(),
            self._create_sales_table(),
            self._create_stores_table(),
            self._create_vendors_table(),
            self._create_customers_table(),
            self._create_reviews_table(),
        ]
        
        return {
            "dataset_id": self.dataset_id,
            "tables": tables
        }
    
    def _create_products_table(self) -> Dict[str, Any]:
        """Create products table schema."""
        return {
            "table_name": "products",
            "description": "Product catalog with detailed information about each product",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.products` (...)",
            "statistics": {"row_count": 15000},
            "columns": [
                {"column_name": "product_id", "data_type": "STRING", "description": "Unique identifier for the product", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "name", "data_type": "STRING", "description": "Product name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "description", "data_type": "STRING", "description": "Detailed product description", "is_nullable": True, "ordinal_position": 3},
                {"column_name": "category_id", "data_type": "STRING", "description": "Reference to product category", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "vendor_id", "data_type": "STRING", "description": "Reference to vendor/supplier", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "sku", "data_type": "STRING", "description": "Stock Keeping Unit", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "upc", "data_type": "STRING", "description": "Universal Product Code", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "base_price", "data_type": "NUMERIC", "description": "Standard retail price", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "cost", "data_type": "NUMERIC", "description": "Cost to acquire from vendor", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "weight", "data_type": "FLOAT64", "description": "Product weight in kg", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "dimensions", "data_type": "STRING", "description": "Product dimensions (L x W x H)", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "is_active", "data_type": "BOOL", "description": "Whether product is active in catalog", "is_nullable": False, "ordinal_position": 12},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When product was added to catalog", "is_nullable": False, "ordinal_position": 13},
                {"column_name": "updated_at", "data_type": "TIMESTAMP", "description": "When product was last updated", "is_nullable": False, "ordinal_position": 14},
            ]
        }
    
    def _create_categories_table(self) -> Dict[str, Any]:
        """Create categories table schema."""
        return {
            "table_name": "categories",
            "description": "Product categories and hierarchy",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.categories` (...)",
            "statistics": {"row_count": 200},
            "columns": [
                {"column_name": "category_id", "data_type": "STRING", "description": "Unique identifier for category", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "name", "data_type": "STRING", "description": "Category name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "description", "data_type": "STRING", "description": "Category description", "is_nullable": True, "ordinal_position": 3},
                {"column_name": "parent_category_id", "data_type": "STRING", "description": "Reference to parent category (for hierarchy)", "is_nullable": True, "ordinal_position": 4},
                {"column_name": "level", "data_type": "INT64", "description": "Hierarchy level (1=top level)", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "is_active", "data_type": "BOOL", "description": "Whether category is active", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When category was created", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "updated_at", "data_type": "TIMESTAMP", "description": "When category was last updated", "is_nullable": False, "ordinal_position": 8},
            ]
        }
    
    def _create_inventory_table(self) -> Dict[str, Any]:
        """Create inventory table schema."""
        return {
            "table_name": "inventory",
            "description": "Product inventory levels across stores",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.inventory` (...)",
            "statistics": {"row_count": 75000},
            "columns": [
                {"column_name": "inventory_id", "data_type": "STRING", "description": "Unique identifier for inventory record", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to product", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "store_id", "data_type": "STRING", "description": "Reference to store location", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "quantity", "data_type": "INT64", "description": "Current quantity in stock", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "reorder_level", "data_type": "INT64", "description": "Level at which reorder is triggered", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "reorder_quantity", "data_type": "INT64", "description": "Standard reorder quantity", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "last_counted_at", "data_type": "TIMESTAMP", "description": "Last physical inventory count date", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "updated_at", "data_type": "TIMESTAMP", "description": "When inventory was last updated", "is_nullable": False, "ordinal_position": 8},
            ]
        }
    
    def _create_price_history_table(self) -> Dict[str, Any]:
        """Create price_history table schema."""
        return {
            "table_name": "price_history",
            "description": "Historical record of product price changes",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.price_history` (...)",
            "statistics": {"row_count": 120000},
            "columns": [
                {"column_name": "price_history_id", "data_type": "STRING", "description": "Unique identifier for price change record", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to product", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "effective_date", "data_type": "DATE", "description": "When price change became effective", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "end_date", "data_type": "DATE", "description": "When price was superseded (NULL if current)", "is_nullable": True, "ordinal_position": 4},
                {"column_name": "price", "data_type": "NUMERIC", "description": "Product price during this period", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "reason_code", "data_type": "STRING", "description": "Reason for price change", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "created_by", "data_type": "STRING", "description": "User who created the price change", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When price change was recorded", "is_nullable": False, "ordinal_position": 8},
            ]
        }
    
    def _create_promotions_table(self) -> Dict[str, Any]:
        """Create promotions table schema."""
        return {
            "table_name": "promotions",
            "description": "Marketing promotions and discounts",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.promotions` (...)",
            "statistics": {"row_count": 500},
            "columns": [
                {"column_name": "promotion_id", "data_type": "STRING", "description": "Unique identifier for promotion", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "name", "data_type": "STRING", "description": "Promotion name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "description", "data_type": "STRING", "description": "Promotion description", "is_nullable": True, "ordinal_position": 3},
                {"column_name": "start_date", "data_type": "DATE", "description": "Promotion start date", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "end_date", "data_type": "DATE", "description": "Promotion end date", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "discount_type", "data_type": "STRING", "description": "Discount type (percentage, fixed amount, etc.)", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "discount_value", "data_type": "NUMERIC", "description": "Discount amount or percentage", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "applies_to", "data_type": "STRING", "description": "Scope of promotion (product, category, storewide)", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "min_purchase", "data_type": "NUMERIC", "description": "Minimum purchase amount to qualify", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "status", "data_type": "STRING", "description": "Status (active, scheduled, ended)", "is_nullable": False, "ordinal_position": 10},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When promotion was created", "is_nullable": False, "ordinal_position": 11},
                {"column_name": "updated_at", "data_type": "TIMESTAMP", "description": "When promotion was last updated", "is_nullable": False, "ordinal_position": 12},
            ]
        }
    
    def _create_sales_table(self) -> Dict[str, Any]:
        """Create sales table schema."""
        return {
            "table_name": "sales",
            "description": "Sales transactions at item level",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.sales` (...)",
            "statistics": {"row_count": 1500000},
            "columns": [
                {"column_name": "sale_id", "data_type": "STRING", "description": "Unique identifier for sale line item", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "order_id", "data_type": "STRING", "description": "Reference to order transaction", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to product sold", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "customer_id", "data_type": "STRING", "description": "Reference to customer", "is_nullable": True, "ordinal_position": 4},
                {"column_name": "store_id", "data_type": "STRING", "description": "Reference to store where sale occurred", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "sale_date", "data_type": "DATE", "description": "Date of sale", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "quantity", "data_type": "INT64", "description": "Quantity sold", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "unit_price", "data_type": "NUMERIC", "description": "Price per unit at time of sale", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "discount_amount", "data_type": "NUMERIC", "description": "Discount applied to line item", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "total_price", "data_type": "NUMERIC", "description": "Total price after discount", "is_nullable": False, "ordinal_position": 10},
                {"column_name": "promotion_id", "data_type": "STRING", "description": "Reference to promotion if applicable", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When sale record was created", "is_nullable": False, "ordinal_position": 12},
            ]
        }
    
    def _create_stores_table(self) -> Dict[str, Any]:
        """Create stores table schema."""
        return {
            "table_name": "stores",
            "description": "Retail store locations",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.stores` (...)",
            "statistics": {"row_count": 150},
            "columns": [
                {"column_name": "store_id", "data_type": "STRING", "description": "Unique identifier for store", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "name", "data_type": "STRING", "description": "Store name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "address", "data_type": "STRING", "description": "Street address", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "city", "data_type": "STRING", "description": "City", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "state", "data_type": "STRING", "description": "State/Province", "is_nullable": False, "ordinal_position": 5},
                {"column_name": "postal_code", "data_type": "STRING", "description": "Postal/ZIP code", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "country", "data_type": "STRING", "description": "Country", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "phone", "data_type": "STRING", "description": "Store phone number", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "store_type", "data_type": "STRING", "description": "Type of store (flagship, outlet, etc.)", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "square_feet", "data_type": "INT64", "description": "Store size in square feet", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "opening_date", "data_type": "DATE", "description": "Date store opened", "is_nullable": False, "ordinal_position": 11},
                {"column_name": "closure_date", "data_type": "DATE", "description": "Date store closed, if applicable", "is_nullable": True, "ordinal_position": 12},
                {"column_name": "is_active", "data_type": "BOOL", "description": "Whether store is currently active", "is_nullable": False, "ordinal_position": 13},
            ]
        }
    
    def _create_vendors_table(self) -> Dict[str, Any]:
        """Create vendors table schema."""
        return {
            "table_name": "vendors",
            "description": "Product suppliers and manufacturers",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.vendors` (...)",
            "statistics": {"row_count": 300},
            "columns": [
                {"column_name": "vendor_id", "data_type": "STRING", "description": "Unique identifier for vendor", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "name", "data_type": "STRING", "description": "Vendor name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "contact_name", "data_type": "STRING", "description": "Primary contact person", "is_nullable": True, "ordinal_position": 3},
                {"column_name": "email", "data_type": "STRING", "description": "Contact email address", "is_nullable": True, "ordinal_position": 4},
                {"column_name": "phone", "data_type": "STRING", "description": "Contact phone number", "is_nullable": True, "ordinal_position": 5},
                {"column_name": "address", "data_type": "STRING", "description": "Vendor address", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "city", "data_type": "STRING", "description": "City", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "state", "data_type": "STRING", "description": "State/Province", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "postal_code", "data_type": "STRING", "description": "Postal/ZIP code", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "country", "data_type": "STRING", "description": "Country", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "account_number", "data_type": "STRING", "description": "Vendor account number", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "payment_terms", "data_type": "STRING", "description": "Standard payment terms", "is_nullable": True, "ordinal_position": 12},
                {"column_name": "is_active", "data_type": "BOOL", "description": "Whether vendor is active", "is_nullable": False, "ordinal_position": 13},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When vendor was added", "is_nullable": False, "ordinal_position": 14},
                {"column_name": "updated_at", "data_type": "TIMESTAMP", "description": "When vendor was last updated", "is_nullable": False, "ordinal_position": 15},
            ]
        }
    
    def _create_customers_table(self) -> Dict[str, Any]:
        """Create customers table schema."""
        return {
            "table_name": "customers",
            "description": "Customer information and profiles",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.customers` (...)",
            "statistics": {"row_count": 50000},
            "columns": [
                {"column_name": "customer_id", "data_type": "STRING", "description": "Unique identifier for customer", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "first_name", "data_type": "STRING", "description": "Customer first name", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "last_name", "data_type": "STRING", "description": "Customer last name", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "email", "data_type": "STRING", "description": "Customer email address", "is_nullable": True, "ordinal_position": 4},
                {"column_name": "phone", "data_type": "STRING", "description": "Customer phone number", "is_nullable": True, "ordinal_position": 5},
                {"column_name": "address", "data_type": "STRING", "description": "Customer address", "is_nullable": True, "ordinal_position": 6},
                {"column_name": "city", "data_type": "STRING", "description": "City", "is_nullable": True, "ordinal_position": 7},
                {"column_name": "state", "data_type": "STRING", "description": "State/Province", "is_nullable": True, "ordinal_position": 8},
                {"column_name": "postal_code", "data_type": "STRING", "description": "Postal/ZIP code", "is_nullable": True, "ordinal_position": 9},
                {"column_name": "country", "data_type": "STRING", "description": "Country", "is_nullable": True, "ordinal_position": 10},
                {"column_name": "birth_date", "data_type": "DATE", "description": "Customer birth date", "is_nullable": True, "ordinal_position": 11},
                {"column_name": "joining_date", "data_type": "DATE", "description": "Date customer joined", "is_nullable": False, "ordinal_position": 12},
                {"column_name": "loyalty_tier", "data_type": "STRING", "description": "Loyalty program tier", "is_nullable": True, "ordinal_position": 13},
                {"column_name": "loyalty_points", "data_type": "INT64", "description": "Current loyalty points balance", "is_nullable": True, "ordinal_position": 14},
                {"column_name": "is_active", "data_type": "BOOL", "description": "Whether customer account is active", "is_nullable": False, "ordinal_position": 15},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When customer was added", "is_nullable": False, "ordinal_position": 16},
                {"column_name": "updated_at", "data_type": "TIMESTAMP", "description": "When customer was last updated", "is_nullable": False, "ordinal_position": 17},
            ]
        }
    
    def _create_reviews_table(self) -> Dict[str, Any]:
        """Create reviews table schema."""
        return {
            "table_name": "reviews",
            "description": "Product reviews by customers",
            "table_type": "TABLE",
            "source": "simulation",
            "ddl": "CREATE TABLE `merchandising.reviews` (...)",
            "statistics": {"row_count": 100000},
            "columns": [
                {"column_name": "review_id", "data_type": "STRING", "description": "Unique identifier for review", "is_nullable": False, "ordinal_position": 1},
                {"column_name": "product_id", "data_type": "STRING", "description": "Reference to product", "is_nullable": False, "ordinal_position": 2},
                {"column_name": "customer_id", "data_type": "STRING", "description": "Reference to customer", "is_nullable": False, "ordinal_position": 3},
                {"column_name": "rating", "data_type": "INT64", "description": "Rating from 1-5", "is_nullable": False, "ordinal_position": 4},
                {"column_name": "review_text", "data_type": "STRING", "description": "Text content of review", "is_nullable": True, "ordinal_position": 5},
                {"column_name": "review_date", "data_type": "DATE", "description": "Date review was submitted", "is_nullable": False, "ordinal_position": 6},
                {"column_name": "verified_purchase", "data_type": "BOOL", "description": "Whether reviewer purchased the product", "is_nullable": False, "ordinal_position": 7},
                {"column_name": "helpful_votes", "data_type": "INT64", "description": "Number of helpful votes received", "is_nullable": False, "ordinal_position": 8},
                {"column_name": "status", "data_type": "STRING", "description": "Review status (published, pending, rejected)", "is_nullable": False, "ordinal_position": 9},
                {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "When review was created", "is_nullable": False, "ordinal_position": 10},
                {"column_name": "updated_at", "data_type": "TIMESTAMP", "description": "When review was last updated", "is_nullable": False, "ordinal_position": 11},
            ]
        }