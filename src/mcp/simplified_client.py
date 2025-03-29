"""
Simplified client for Knowledge Graph MCP Server.

This script demonstrates how to call the simplified MCP server endpoints
directly without requiring the MCP client package.
"""
import argparse
import json
import sys
import requests
from typing import Dict, Any, List, Optional

def pretty_print(obj: Any):
    """Pretty print JSON data."""
    if isinstance(obj, dict) or isinstance(obj, list):
        print(json.dumps(obj, indent=2))
    else:
        print(obj)

class SimplifiedClient:
    """Simplified client for Knowledge Graph MCP Server."""
    
    def __init__(self, base_url: str = "http://localhost:8234"):
        """Initialize the client with the server URL."""
        self.base_url = base_url
        self.session = requests.Session()
    
    def call_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Call a specific tool on the server."""
        url = f"{self.base_url}/tools/{tool_name}"
        response = self.session.post(url, json=params)
        
        if response.status_code != 200:
            print(f"Error calling {tool_name}: {response.status_code}")
            try:
                error = response.json()
                print(f"Error details: {error}")
            except:
                print(f"Error response: {response.text}")
            return {"error": response.text}
        
        return response.json()
    
    def list_tools(self) -> List[Dict[str, Any]]:
        """List available tools on the server."""
        url = f"{self.base_url}/mcp/tools"
        response = self.session.get(url)
        
        if response.status_code != 200:
            print(f"Error listing tools: {response.status_code}")
            return []
        
        return response.json().get("tools", [])
    
    def query(self, cypher_query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run a Cypher query."""
        params = params or {}
        return self.call_tool("knowledge_graph.query", {
            "query": cypher_query,
            "parameters": params
        })
    
    def get_table_schema(self, table_name: str, include_relationships: bool = False) -> Dict[str, Any]:
        """Get table schema."""
        return self.call_tool("knowledge_graph.get_table_schema", {
            "table_name": table_name,
            "include_relationships": include_relationships
        })
    
    def find_relationships(self, source: str, target: str, max_depth: int = 3) -> Dict[str, Any]:
        """Find relationships between entities."""
        return self.call_tool("knowledge_graph.find_relationships", {
            "source": source,
            "target": target,
            "max_depth": max_depth
        })
    
    def search_business_terms(self, keyword: str) -> Dict[str, Any]:
        """Search for business terms."""
        return self.call_tool("knowledge_graph.search_business_terms", {
            "keyword": keyword
        })
    
    def recommend_tables(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """Recommend tables for a query."""
        return self.call_tool("knowledge_graph.recommend_tables_for_query", {
            "query": query,
            "limit": limit
        })

def run_example_queries(client: SimplifiedClient):
    """Run example queries to demonstrate client usage."""
    print("=== LISTING AVAILABLE TOOLS ===")
    tools = client.list_tools()
    for tool in tools:
        print(f"Tool: {tool['name']}")
        print(f"Description: {tool['description']}")
        print("-" * 60)
    
    print("\n=== QUERYING TABLE SCHEMA ===")
    result = client.get_table_schema("WorkOrders", include_relationships=True)
    
    if "data" in result:
        table_data = result["data"]
        table_name = table_data.get("table", {}).get("name", "Unknown")
        column_count = len(table_data.get("columns", []))
        print(f"Table: {table_name}")
        print(f"Columns: {column_count}")
        
        # Print a few column names
        columns = table_data.get("columns", [])
        if columns:
            print("Sample columns:")
            for column in columns[:5]:
                print(f"  - {column.get('name')}: {column.get('data_type')}")
    else:
        print("Error fetching table schema")
        pretty_print(result)
    
    print("\n=== FINDING RELATIONSHIPS ===")
    result = client.find_relationships("WorkOrders", "Locations")
    pretty_print(result)
    
    print("\n=== SEARCHING BUSINESS TERMS ===")
    result = client.search_business_terms("invoice")
    pretty_print(result)
    
    print("\n=== RECOMMENDING TABLES FOR QUERY ===")
    result = client.recommend_tables("work orders completed in retail stores")
    pretty_print(result)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Simplified MCP Client")
    parser.add_argument("--url", default="http://localhost:8234", help="Server URL")
    args = parser.parse_args()
    
    client = SimplifiedClient(args.url)
    run_example_queries(client)

if __name__ == "__main__":
    main()