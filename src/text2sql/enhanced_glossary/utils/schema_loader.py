"""
Schema loader utility for loading JSON schemas from files.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

class SchemaLoader:
    """
    Loads and caches JSON schemas from files.
    
    This utility handles loading schema definitions from external files
    and caching them for performance.
    """
    
    def __init__(self, schemas_dir: str = None):
        """
        Initialize the schema loader.
        
        Args:
            schemas_dir: Directory containing schema files.
                         Defaults to the package's schemas directory.
        """
        if schemas_dir is None:
            # Default to package's schemas directory
            package_dir = Path(__file__).parent.parent
            self.schemas_dir = os.path.join(package_dir, 'config', 'schemas')
        else:
            self.schemas_dir = schemas_dir
            
        self._schema_cache = {}
        
    def load_schema(self, schema_name: str) -> Dict[str, Any]:
        """
        Load a JSON schema by name.
        
        Args:
            schema_name: Name of the schema file (without extension)
            
        Returns:
            JSON schema as a dictionary
            
        Raises:
            FileNotFoundError: If schema file doesn't exist
            json.JSONDecodeError: If schema file contains invalid JSON
        """
        # Check cache first
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]
            
        # Construct file path
        file_path = os.path.join(self.schemas_dir, f"{schema_name}.json")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
                
            # Cache the result
            self._schema_cache[schema_name] = schema
            return schema
        except FileNotFoundError:
            logger.error(f"Schema file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in schema file {file_path}: {e}")
            raise