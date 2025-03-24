"""
Schema Loader

Loads database schema from SQL definition files and converts to simulation format.
"""
import re
import os
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SQLSchemaLoader:
    """Loads database schema from SQL CREATE TABLE statements."""
    
    def __init__(self, schema_file_path: str):
        """
        Initialize the schema loader.
        
        Args:
            schema_file_path: Path to SQL schema file
        """
        self.schema_file_path = schema_file_path
        
    def load_schema(self) -> List[Dict[str, Any]]:
        """
        Load and parse the schema from SQL file.
        
        Returns:
            List of table definitions
        """
        if not os.path.exists(self.schema_file_path):
            raise FileNotFoundError(f"Schema file not found: {self.schema_file_path}")
        
        with open(self.schema_file_path, 'r') as f:
            content = f.read()
        
        # Extract CREATE TABLE statements
        create_statements = self._extract_create_statements(content)
        
        # Parse each statement
        tables = []
        for statement in create_statements:
            table = self._parse_create_statement(statement)
            if table:
                tables.append(table)
        
        logger.info(f"Loaded {len(tables)} tables from schema file")
        return tables
    
    def _extract_create_statements(self, content: str) -> List[str]:
        """
        Extract CREATE TABLE statements from SQL content.
        
        Args:
            content: SQL content
            
        Returns:
            List of CREATE TABLE statements
        """
        # Normalize line endings and remove comments
        content = content.replace('\r\n', '\n')
        content = re.sub(r'--.*?\n', '\n', content)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        
        # Find all CREATE TABLE statements
        statements = []
        pattern = r'CREATE\s+TABLE\s+[`"\[]?([^`"\[\s]+)[`"\]]?\s*\((.*?)\);'
        matches = re.finditer(pattern, content, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            statements.append(match.group(0))
        
        return statements
    
    def _parse_create_statement(self, statement: str) -> Optional[Dict[str, Any]]:
        """
        Parse CREATE TABLE statement into table definition.
        
        Args:
            statement: CREATE TABLE statement
            
        Returns:
            Table definition dictionary or None if parsing failed
        """
        # Extract table name
        table_name_match = re.search(r'CREATE\s+TABLE\s+[`"\[]?([^`"\[\s.]+)[`"\]]?', statement, re.IGNORECASE)
        if not table_name_match:
            logger.warning(f"Failed to extract table name from statement: {statement[:100]}...")
            return None
        
        table_name = table_name_match.group(1)
        
        # Extract everything between the parentheses
        columns_section_match = re.search(r'\((.*)\)', statement, re.DOTALL)
        if not columns_section_match:
            logger.warning(f"Failed to extract columns section for table {table_name}")
            return None
        
        columns_section = columns_section_match.group(1)
        
        # Split by commas, but not those inside parentheses
        column_definitions = []
        paren_level = 0
        current_def = ""
        
        for char in columns_section:
            if char == '(':
                paren_level += 1
                current_def += char
            elif char == ')':
                paren_level -= 1
                current_def += char
            elif char == ',' and paren_level == 0:
                column_definitions.append(current_def.strip())
                current_def = ""
            else:
                current_def += char
        
        if current_def.strip():
            column_definitions.append(current_def.strip())
        
        # Filter out constraints and keys, keep only column definitions
        column_definitions = [col for col in column_definitions if not (
            col.upper().startswith('PRIMARY KEY') or
            col.upper().startswith('FOREIGN KEY') or
            col.upper().startswith('UNIQUE') or
            col.upper().startswith('CHECK') or
            col.upper().startswith('CONSTRAINT')
        )]
        
        # Parse each column definition
        columns = []
        for i, col_def in enumerate(column_definitions):
            column = self._parse_column_definition(col_def, i+1)
            if column:
                columns.append(column)
        
        # Build table definition
        table = {
            "table_name": table_name,
            "description": f"Table {table_name}",
            "table_type": "TABLE",
            "source": "imported_schema",
            "ddl": statement,
            "statistics": {"row_count": 100},  # Default value
            "columns": columns
        }
        
        return table
    
    def _parse_column_definition(self, column_def: str, position: int) -> Optional[Dict[str, Any]]:
        """
        Parse column definition into column metadata.
        
        Args:
            column_def: Column definition string
            position: Ordinal position of the column
            
        Returns:
            Column metadata dictionary or None if parsing failed
        """
        # Basic column definition pattern
        pattern = r'^\s*([`"\[]?[^`"\[\s]+[`"\]]?)\s+([A-Za-z0-9_]+(?:\([^)]+\))?)'
        match = re.search(pattern, column_def)
        
        if not match:
            logger.warning(f"Failed to parse column definition: {column_def}")
            return None
        
        column_name = match.group(1).strip('`"[]')
        data_type = match.group(2).upper()
        
        # Check for NULL/NOT NULL constraint
        is_nullable = True
        if 'NOT NULL' in column_def.upper():
            is_nullable = False
        
        # Check for column comment/description
        description = f"Column {column_name}"
        comment_match = re.search(r'COMMENT\s+[\'"](.+?)[\'"]', column_def, re.IGNORECASE)
        if comment_match:
            description = comment_match.group(1)
        
        return {
            "column_name": column_name,
            "data_type": data_type,
            "description": description,
            "is_nullable": is_nullable,
            "ordinal_position": position
        }


class SchemaSimulator:
    """Simulates database schema based on loaded SQL definitions."""
    
    def __init__(self, tenant_id: str, schema_file_path: str = None, schema_name: str = None):
        """
        Initialize schema simulator.
        
        Args:
            tenant_id: Tenant ID
            schema_file_path: Path to SQL schema file, optional
            schema_name: Name to use for the dataset, optional
        """
        self.tenant_id = tenant_id
        self.schema_file_path = schema_file_path
        
        # Use schema name from path if not provided
        if schema_name:
            self.dataset_id = schema_name
        elif schema_file_path:
            # Extract schema name from file path
            base_name = os.path.basename(schema_file_path)
            self.dataset_id = os.path.splitext(base_name)[0]
        else:
            self.dataset_id = "imported_schema"
    
    def generate_schema(self) -> Dict[str, Any]:
        """
        Generate a schema based on loaded SQL definitions or use default.
        
        Returns:
            Dictionary with schema information
        """
        if self.schema_file_path:
            # Load schema from SQL file
            loader = SQLSchemaLoader(self.schema_file_path)
            tables = loader.load_schema()
        else:
            # Use embedded demo schema as fallback
            logger.info("No schema file provided, using embedded demo schema")
            tables = self._get_demo_schema()
        
        return {
            "dataset_id": self.dataset_id,
            "tables": tables
        }
    
    def _get_demo_schema(self) -> List[Dict[str, Any]]:
        """
        Get a simple demo schema for testing.
        
        Returns:
            List of table definitions
        """
        return [
            {
                "table_name": "customers",
                "description": "Customer information",
                "table_type": "TABLE",
                "source": "demo",
                "ddl": "CREATE TABLE customers (...)",
                "statistics": {"row_count": 100},
                "columns": [
                    {"column_name": "customer_id", "data_type": "INTEGER", "description": "Unique customer identifier", "is_nullable": False, "ordinal_position": 1},
                    {"column_name": "name", "data_type": "VARCHAR(100)", "description": "Customer name", "is_nullable": False, "ordinal_position": 2},
                    {"column_name": "email", "data_type": "VARCHAR(200)", "description": "Customer email", "is_nullable": True, "ordinal_position": 3},
                    {"column_name": "created_at", "data_type": "TIMESTAMP", "description": "Account creation timestamp", "is_nullable": False, "ordinal_position": 4}
                ]
            },
            {
                "table_name": "orders",
                "description": "Customer orders",
                "table_type": "TABLE",
                "source": "demo",
                "ddl": "CREATE TABLE orders (...)",
                "statistics": {"row_count": 500},
                "columns": [
                    {"column_name": "order_id", "data_type": "INTEGER", "description": "Unique order identifier", "is_nullable": False, "ordinal_position": 1},
                    {"column_name": "customer_id", "data_type": "INTEGER", "description": "Reference to customers table", "is_nullable": False, "ordinal_position": 2},
                    {"column_name": "order_date", "data_type": "DATE", "description": "Date order was placed", "is_nullable": False, "ordinal_position": 3},
                    {"column_name": "total_amount", "data_type": "DECIMAL(10,2)", "description": "Order total amount", "is_nullable": False, "ordinal_position": 4},
                    {"column_name": "status", "data_type": "VARCHAR(20)", "description": "Order status", "is_nullable": False, "ordinal_position": 5}
                ]
            }
        ]