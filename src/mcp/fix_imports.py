#!/usr/bin/env python3
"""
Script to fix MCP imports in all files.

This script updates the imports based on the correct MCP package structure.
"""
import os
import re
import sys

# Files to update
files = [
    "src/mcp/kg_resource.py",
    "src/mcp/enhanced_kg_resource.py",
    "src/mcp/server.py",
    "src/mcp/enhanced_server.py"
]

# Replacement mappings: old import -> new import
replacements = {
    # Replace mcp_server imports
    r'from\s+mcp_server\s+import\s+(.*?)': r'from mcp import \1',
    r'from\s+mcp\.server\s+import\s+(.*?)': r'from mcp import \1',
    
    # Replace specific class imports
    r'from\s+mcp(?:_server|\.server)?\s+import\s+Resource': r'from mcp.types import Resource',
    r'from\s+mcp(?:_server|\.server)?\s+import\s+(?:.*?)Response(?:.*?)': r'from mcp.types import Response',
    
    # Fix resolver function import
    r'from\s+mcp(?:_server|\.server)?\s+import\s+(?:.*?)resolver(?:.*?)': r'from mcp.server import resolver',
    
    # Fix JSONSchema import
    r'from\s+mcp(?:_server|\.server)?\s+import\s+(?:.*?)JSONSchema(?:.*?)': r'# JSONSchema is not defined in MCP package\n# Using pydantic for schema validation\nfrom pydantic import BaseModel',
    
    # Replace JSONSchema usage with BaseModel
    r'schema=JSONSchema\({': r'schema={',
}

def update_file(file_path):
    """Update a single file with the new imports."""
    print(f"Updating imports in {file_path}")
    
    # Read file
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Apply all replacements
    updated_content = content
    for pattern, replacement in replacements.items():
        updated_content = re.sub(pattern, replacement, updated_content)
    
    # Write updated content
    with open(file_path, 'w') as f:
        f.write(updated_content)
    
    print(f"  Updated {file_path}")

def main():
    """Main function."""
    for file_path in files:
        if os.path.exists(file_path):
            update_file(file_path)
        else:
            print(f"File not found: {file_path}")
    
    print("Import updates completed")

if __name__ == "__main__":
    main()