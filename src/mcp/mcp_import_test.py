#!/usr/bin/env python3
"""
Script to test MCP imports and find the correct import paths.
"""
import sys
import importlib
import inspect

def test_import(module_name, class_name):
    """Test importing a specific class from a module."""
    try:
        module = importlib.import_module(module_name)
        if hasattr(module, class_name):
            cls = getattr(module, class_name)
            print(f"✅ Successfully imported {class_name} from {module_name}")
            print(f"   Type: {type(cls)}")
            return True
        else:
            print(f"❌ {class_name} not found in {module_name}")
            
            # List available classes in the module
            print(f"   Available in {module_name}:")
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) or inspect.isfunction(obj):
                    print(f"   - {name}: {type(obj)}")
            return False
    except ImportError as e:
        print(f"❌ Failed to import {module_name}: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    print("Testing MCP imports...\n")
    
    # Test imports for Resource
    modules_to_try_resource = [
        "mcp.types",
        "mcp.server",
        "mcp"
    ]
    
    for module in modules_to_try_resource:
        if test_import(module, "Resource"):
            break
    
    print("\n")
    
    # Test imports for resolver
    modules_to_try_resolver = [
        "mcp.server",
        "mcp.server.fastmcp",
        "mcp"
    ]
    
    for module in modules_to_try_resolver:
        if test_import(module, "resolver"):
            break
    
    print("\n")
    
    # Test imports for Response
    modules_to_try_response = [
        "mcp.server",
        "mcp.types",
        "mcp"
    ]
    
    for module in modules_to_try_response:
        if test_import(module, "Response"):
            break
    
    print("\n")
    
    # Test imports for JSONSchema
    modules_to_try_schema = [
        "mcp.server",
        "mcp.types",
        "mcp"
    ]
    
    for module in modules_to_try_schema:
        if test_import(module, "JSONSchema"):
            break