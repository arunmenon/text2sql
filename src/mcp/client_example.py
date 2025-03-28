"""
Example client for the Knowledge Graph MCP Server.

This module demonstrates how to use the MCP client to connect
to the Knowledge Graph server and access graph operations.
"""
import json
import sys
from typing import Any, Dict

from mcp_client import Client

# Example usage of the MCP client with Knowledge Graph server
def main():
    """
    Example of using MCP client with Knowledge Graph server.
    """
    # Connect to MCP server
    client = Client(base_url="http://localhost:8234")
    
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