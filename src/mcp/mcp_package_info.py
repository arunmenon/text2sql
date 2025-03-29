#!/usr/bin/env python3
"""
Script to examine the MCP package structure.

This script tries to import the MCP package and prints information about
its structure to help determine the correct import paths.
"""
import sys
import importlib
import pkgutil
import inspect

def explore_package(package_name):
    """
    Explore a package and print its structure.
    """
    print(f"Exploring package: {package_name}")
    
    try:
        # Import the package
        package = importlib.import_module(package_name)
        print(f"  Package imported successfully: {package.__name__}")
        
        # Check for __version__ if available
        if hasattr(package, '__version__'):
            print(f"  Version: {package.__version__}")
        
        # Show the package's file location
        print(f"  Location: {package.__file__}")
        
        # List all modules in the package
        print(f"  Modules in {package_name}:")
        for module_info in pkgutil.iter_modules(package.__path__, package.__name__ + '.'):
            print(f"    - {module_info.name}")
            
            # Try to import this module
            try:
                module = importlib.import_module(module_info.name)
                
                # List classes in the module
                print(f"      Classes:")
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if obj.__module__ == module_info.name:
                        print(f"        - {name}")
            except Exception as e:
                print(f"      Error importing module: {str(e)}")
        
    except ImportError as e:
        print(f"  Error: Could not import {package_name}: {str(e)}")
    except Exception as e:
        print(f"  Error exploring {package_name}: {str(e)}")
    
    print()

if __name__ == "__main__":
    # Explore the MCP package
    explore_package('mcp')
    
    # Try exploring potential server sub-package
    explore_package('mcp.server')