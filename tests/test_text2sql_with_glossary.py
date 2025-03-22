#!/usr/bin/env python3
"""
Test Text2SQL with Business Glossary

Tests the text-to-SQL engine with business glossary integration.
"""
import os
import sys
import pytest
import asyncio
from unittest.mock import MagicMock, patch

# Add the parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.text2sql.engine import TextToSQLEngine
from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient
from src.text2sql.models import (
    SQLResult, Text2SQLResponse, StructuredQuery, StructuredQueryMeta,
    AmbiguityAssessment, ResolvedQuery, ResolvedEntity, ResolvedAttribute,
    ResolvedConcept, QueryInterpretation
)

# Sample test data
SAMPLE_GLOSSARY_TERMS = [
    {
        "name": "Customer",
        "definition": "Individual or entity that purchases products or services",
        "synonym": ["Client", "Patron"]
    },
    {
        "name": "Order",
        "definition": "A request from a customer to purchase products",
        "synonym": ["Purchase"]
    }
]

SAMPLE_GLOSSARY_TERM_DETAILS = {
    "Customer": {
        "name": "Customer",
        "definition": "Individual or entity that purchases products or services",
        "mapped_tables": ["customers"],
        "mapped_columns": [{"table": "customers", "column": "customer_id"}]
    },
    "Order": {
        "name": "Order",
        "definition": "A request from a customer to purchase products",
        "mapped_tables": ["orders"],
        "mapped_columns": [{"table": "orders", "column": "order_id"}]
    }
}

SAMPLE_TABLES = [
    {
        "name": "customers",
        "description": "Contains customer information"
    },
    {
        "name": "orders",
        "description": "Customer orders"
    }
]

SAMPLE_COLUMNS = {
    "customers": [
        {
            "name": "customer_id",
            "data_type": "INT",
            "description": "Unique identifier for customers"
        },
        {
            "name": "customer_name",
            "data_type": "VARCHAR",
            "description": "Customer's full name"
        }
    ],
    "orders": [
        {
            "name": "order_id",
            "data_type": "INT",
            "description": "Unique identifier for orders"
        },
        {
            "name": "customer_id",
            "data_type": "INT",
            "description": "Foreign key to customers table"
        }
    ]
}

SAMPLE_NL_PARSED = {
    "primary_intent": "selection",
    "main_entities": ["Customer"],
    "attributes": ["customer_name"],
    "filters": [],
    "grouping_dimensions": [],
    "sorting_criteria": [],
    "time_references": [],
    "aggregation_functions": [],
    "limit": None,
    "identified_ambiguities": []
}

SAMPLE_SQL = """
SELECT c.customer_name
FROM customers c
"""

SAMPLE_EXPLANATION = "This query retrieves all customer names from the customers table."


class TestText2SQLWithGlossary:
    """Test cases for Text2SQL with Business Glossary integration"""
    
    @pytest.fixture
    def mock_neo4j_client(self):
        """Create a mock Neo4j client for testing"""
        mock_client = MagicMock(spec=Neo4jClient)
        
        # Configure mocks for methods
        mock_client.get_tables_for_tenant.return_value = SAMPLE_TABLES
        mock_client.get_table_details.side_effect = lambda tenant_id, table_name: next(
            (t for t in SAMPLE_TABLES if t["name"] == table_name), None
        )
        mock_client.get_columns_for_table.side_effect = lambda tenant_id, table_name: SAMPLE_COLUMNS.get(table_name, [])
        mock_client.get_glossary_terms.return_value = SAMPLE_GLOSSARY_TERMS
        mock_client.get_glossary_term_details.side_effect = lambda tenant_id, term_name: SAMPLE_GLOSSARY_TERM_DETAILS.get(term_name)
        mock_client.get_schema_summary.return_value = {
            "table_count": len(SAMPLE_TABLES),
            "column_count": sum(len(cols) for cols in SAMPLE_COLUMNS.values()),
            "has_business_glossary": True,
            "glossary_term_count": len(SAMPLE_GLOSSARY_TERMS)
        }
        
        return mock_client
    
    @pytest.fixture
    def mock_llm_client(self):
        """Create a mock LLM client for testing"""
        mock_client = MagicMock(spec=LLMClient)
        
        # Configure mock responses
        async def mock_generate_structured(*args, **kwargs):
            # Simple mock that returns SAMPLE_NL_PARSED for parsing
            # and appropriate values for other structured outputs
            if "parsed query" in args[0].lower():
                return SAMPLE_NL_PARSED
            elif "match the entity" in args[0].lower():
                return {"table_name": "customers", "confidence": 0.9, "reasoning": "Matched to customers table"}
            elif "match the attribute" in args[0].lower():
                return {"table_name": "customers", "column_name": "customer_name", "confidence": 0.9, "reasoning": "Matched to customer_name column"}
            else:
                return {}
        
        async def mock_generate(*args, **kwargs):
            if "sql query" in args[0].lower():
                return SAMPLE_SQL
            elif "explanation" in args[0].lower():
                return SAMPLE_EXPLANATION
            else:
                return "Generated text response"
        
        mock_client.generate_structured.side_effect = mock_generate_structured
        mock_client.generate.side_effect = mock_generate
        
        return mock_client
    
    @pytest.fixture
    def text2sql_engine(self, mock_neo4j_client, mock_llm_client):
        """Create a Text2SQL engine instance with mock clients"""
        return TextToSQLEngine(mock_neo4j_client, mock_llm_client)
    
    @pytest.mark.asyncio
    async def test_glossary_term_resolution(self, text2sql_engine):
        """Test resolution of glossary terms to database entities"""
        # Create a structured query with a business glossary term
        structured_query = StructuredQuery(
            primary_intent="selection",
            main_entities=["Customer"],  # This should resolve to 'customers' table
            attributes=["name"],
            filters=[],
            grouping_dimensions=[],
            sorting_criteria=[],
            time_references=[],
            aggregation_functions=[],
            limit=None,
            identified_ambiguities=[],
            ambiguity_assessment=AmbiguityAssessment(
                score=0.1,
                factors=[]
            ),
            _meta=StructuredQueryMeta(
                raw_query="Get all customer names",
                parsing_timestamp="2023-01-01T00:00:00",
                parser_version="1.0"
            )
        )
        
        # Process the query
        with patch.object(text2sql_engine.nl_understanding, 'parse_query', return_value=structured_query):
            result = await text2sql_engine.process_query("Get all customer names", "test_tenant")
            
            # Verify the result
            assert isinstance(result, Text2SQLResponse)
            assert result.primary_interpretation is not None
            assert "customers" in result.primary_interpretation.sql.lower()
            assert "customer_name" in result.primary_interpretation.sql.lower() or "name" in result.primary_interpretation.sql.lower()
            
            # Check that Neo4j client was called to get glossary terms
            text2sql_engine.neo4j_client.get_glossary_terms.assert_called_once()
            
            # Verify glossary term details were requested
            text2sql_engine.neo4j_client.get_glossary_term_details.assert_called()
    
    @pytest.mark.asyncio
    async def test_end_to_end_with_glossary(self, text2sql_engine):
        """Test end-to-end query processing with glossary integration"""
        # Process an actual query
        result = await text2sql_engine.process_query("Get all customer names", "test_tenant")
        
        # Verify response structure
        assert isinstance(result, Text2SQLResponse)
        assert result.original_query == "Get all customer names"
        assert result.sql_results and len(result.sql_results) > 0
        assert result.primary_interpretation is not None
        
        # Verify that SQL was generated
        assert "select" in result.primary_interpretation.sql.lower()
        assert "customer" in result.primary_interpretation.sql.lower()
        
        # Verify explanation
        assert result.primary_interpretation.explanation
        
        # Verify entity resolution (should have used glossary)
        assert result.entities_resolved.get("Customer") == "customers"


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])