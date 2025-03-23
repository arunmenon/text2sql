#!/usr/bin/env python3
"""
Test Text2SQL with Supply Chain Schema

This script runs text-to-SQL queries specific to the supply chain domain
and saves the results for evaluation.
"""
import os
import sys
import json
import asyncio
import argparse
from datetime import datetime

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.text2sql.engine import TextToSQLEngine
from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient

# Define supply chain specific query test families
SUPPLY_CHAIN_QUERIES = {
    "inventory_basics": [
        {"query": "List all warehouses", "description": "Simple selection of warehouses"},
        {"query": "Show me all inventory items with quantity less than reorder point", "description": "Basic filtering for items needing restock"},
        {"query": "Get inventory levels for all items in warehouse W001", "description": "Filtering by specific warehouse"},
        {"query": "Show items ordered by quantity on hand", "description": "Simple ordering"},
        {"query": "List the top 5 items with highest inventory value", "description": "Limiting with ordering"}
    ],
    
    "inventory_analytics": [
        {"query": "Calculate total inventory value by warehouse", "description": "Aggregation with grouping"},
        {"query": "Find average days between orders for each item", "description": "Date-based calculation"},
        {"query": "Identify warehouses with more than 70% capacity utilization", "description": "Percentage-based calculation"},
        {"query": "Calculate inventory turnover ratio for each item", "description": "Business metric calculation"}
    ],
    
    "purchasing_operations": [
        {"query": "Show all purchase orders and their items", "description": "Join between POs and line items"},
        {"query": "Find suppliers with pending deliveries", "description": "Status-based filtering with join"},
        {"query": "List items that haven't been ordered in the last 30 days", "description": "Date-based filtering with NOT EXISTS"},
        {"query": "Show average lead time by supplier", "description": "Supplier performance metric"}
    ],
    
    "shipping_logistics": [
        {"query": "List all shipments in transit", "description": "Status-based filtering"},
        {"query": "Show shipments grouped by carrier with average transit time", "description": "Carrier performance analysis"},
        {"query": "Find delayed shipments that are past their expected delivery date", "description": "Date comparison for late deliveries"},
        {"query": "Calculate average shipping cost per kilogram by carrier", "description": "Cost efficiency analysis"}
    ],
    
    "business_term_translation": [
        {"query": "Show all stock", "description": "Business term (stock → inventory)"},
        {"query": "Find vendors with reliability score above 90", "description": "Business term (vendor → supplier)"},
        {"query": "List all incoming materials", "description": "Business concept (incoming materials → pending purchase orders)"},
        {"query": "Show me stock keeping units with low inventory", "description": "Business term (SKU → item)"}
    ],
    
    "composite_concept_queries": [
        {"query": "Find slow-moving inventory", "description": "Composite concept (slow-moving inventory)"},
        {"query": "List reliable suppliers", "description": "Business concept with threshold (reliable → reliability score > 85)"},
        {"query": "Show high-value inventory items", "description": "Business concept with threshold (high-value → value > $1000)"},
        {"query": "Identify critical supply shortages", "description": "Complex composite concept (critical supply shortages)"}
    ]
}

class SupplyChainQueryRunner:
    """Runner for supply chain specific text2sql queries"""
    
    def __init__(self, tenant_id: str):
        """
        Initialize query runner.
        
        Args:
            tenant_id: Tenant ID for queries
        """
        self.tenant_id = tenant_id
        self.results_dir = os.path.join(os.path.dirname(__file__), "../results")
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Initialize timestamp for result files
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize client connections
        self._init_clients()
    
    def _init_clients(self):
        """Initialize Neo4j and LLM clients"""
        # Get connection details from environment
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            raise ValueError("LLM API key not configured")
        
        # Initialize clients
        self.neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        self.llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Initialize Text2SQL engine
        self.engine = TextToSQLEngine(self.neo4j_client, self.llm_client)
    
    async def run_query(self, query: str):
        """Run a single query and return results"""
        try:
            print(f"Running query: {query}")
            # Process query
            result = await self.engine.process_query(query, self.tenant_id)
            
            # Convert to dictionary and add query
            result_dict = result.dict()
            result_dict["query"] = query
            
            return result_dict
        except Exception as e:
            print(f"Error processing query '{query}': {e}")
            return {
                "query": query,
                "error": str(e),
                "sql_results": [],
                "primary_interpretation": None
            }
    
    async def run_query_family(self, family_name: str, queries):
        """Run a family of queries and save results"""
        print(f"\nRunning {family_name} queries...")
        
        results = []
        for i, query_obj in enumerate(queries):
            query = query_obj["query"]
            description = query_obj["description"]
            
            print(f"  [{i+1}/{len(queries)}] {query}")
            result = await self.run_query(query)
            
            # Add description and family
            result["description"] = description
            result["family"] = family_name
            
            results.append(result)
        
        # Save results for this family
        self._save_results(family_name, results)
        
        return results
    
    def _save_results(self, family_name: str, results):
        """Save query results to file"""
        # Create filename with timestamp
        filename = f"supply_chain_{family_name}_{self.timestamp}.json"
        filepath = os.path.join(self.results_dir, filename)
        
        # Write results
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"  Results saved to {filepath}")
    
    async def run_all_query_families(self):
        """Run all query families and save results"""
        all_results = {}
        
        for family_name, queries in SUPPLY_CHAIN_QUERIES.items():
            results = await self.run_query_family(family_name, queries)
            all_results[family_name] = results
        
        # Save combined results
        combined_filename = f"supply_chain_all_results_{self.timestamp}.json"
        combined_filepath = os.path.join(self.results_dir, combined_filename)
        
        with open(combined_filepath, 'w') as f:
            json.dump(all_results, f, indent=2)
        
        print(f"\nAll results saved to {combined_filepath}")
    
    async def close(self):
        """Close client connections"""
        await self.llm_client.close()
        self.neo4j_client.close()


async def run_supply_chain_tests(tenant_id, family=None):
    """Run tests for the supply chain schema"""
    runner = SupplyChainQueryRunner(tenant_id)
    
    try:
        if family and family in SUPPLY_CHAIN_QUERIES:
            # Run specific family
            await runner.run_query_family(family, SUPPLY_CHAIN_QUERIES[family])
        else:
            # Run all families
            await runner.run_all_query_families()
    finally:
        # Ensure connections are closed
        await runner.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Supply Chain Text2SQL Tests")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--family", help="Optional specific query family to run")
    
    args = parser.parse_args()
    
    asyncio.run(run_supply_chain_tests(args.tenant_id, args.family))