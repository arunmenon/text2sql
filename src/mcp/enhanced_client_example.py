"""
Enhanced client for the Knowledge Graph MCP Server.

This module demonstrates how to:
1. Discover available operations using MCP introspection
2. Use the discovered operations to query the knowledge graph
"""
import json
import sys
from typing import Any, Dict, List

from mcp_client import Client, Tool

def main():
    """
    Enhanced example of using MCP client with Knowledge Graph server.
    """
    # Connect to MCP server
    client = Client(base_url="http://localhost:8234")
    
    # First, discover available operations
    discover_operations(client)
    
    # Then demonstrate some example operations
    run_example_operations(client)


def discover_operations(client: Client):
    """
    Discover and display all available operations from the Knowledge Graph server.
    
    Args:
        client: Initialized MCP client
    """
    print("== DISCOVERING KNOWLEDGE GRAPH OPERATIONS ==\n")
    
    # List all available tools/operations
    tools = client.list_tools()
    
    # Group tools by resource
    resource_tools: Dict[str, List[Tool]] = {}
    for tool in tools:
        resource_name = tool.name.split('.')[0] if '.' in tool.name else 'unknown'
        if resource_name not in resource_tools:
            resource_tools[resource_name] = []
        resource_tools[resource_name].append(tool)
    
    # Print discovered operations by resource
    for resource_name, tools in resource_tools.items():
        print(f"\n=== {resource_name.upper()} RESOURCE ===\n")
        
        for tool in tools:
            operation_name = tool.name.split('.')[1] if '.' in tool.name else tool.name
            print(f"OPERATION: {operation_name}")
            print(f"DESCRIPTION: {tool.description}")
            
            # Pretty-print the schema
            if hasattr(tool, 'schema') and tool.schema:
                print("PARAMETERS:")
                if 'properties' in tool.schema:
                    for param_name, param_details in tool.schema['properties'].items():
                        param_type = param_details.get('type', 'unknown')
                        param_desc = param_details.get('description', 'No description')
                        print(f"  • {param_name} ({param_type}): {param_desc}")
                        
                if 'required' in tool.schema:
                    print("REQUIRED: " + ", ".join(tool.schema['required']))
            
            print("-" * 60)


def run_example_operations(client: Client):
    """
    Run example operations against the Knowledge Graph server.
    
    Args:
        client: Initialized MCP client
    """
    print("\n== EXAMPLE OPERATIONS ==\n")
    
    # Example: Get table schema
    response = client.invoke(
        "knowledge_graph.get_table_schema",
        {"table_name": "WorkOrders"}
    )
    
    print("== Table Schema ==\n")
    pretty_print(response)
    
    # Example: Search business terms
    response = client.invoke(
        "knowledge_graph.search_business_terms",
        {"keyword": "invoice"}
    )
    
    print("\n== Business Terms ==\n")
    pretty_print(response)
    
    # Example: Find relationships between entities
    response = client.invoke(
        "knowledge_graph.find_relationships",
        {
            "source": "WorkOrders",
            "target": "Locations",
            "max_depth": 2
        }
    )
    
    print("\n== Entity Relationships ==\n")
    pretty_print(response)


def pretty_print(obj: Any):
    """
    Pretty print an object as JSON.
    
    Args:
        obj: Object to print
    """
    if isinstance(obj, Dict):
        print(json.dumps(obj, indent=2))
    else:
        print(obj)


if __name__ == "__main__":
    main()