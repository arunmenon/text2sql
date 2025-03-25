"""
Tests for the TransparentQueryEngine.
"""

import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch

from src.text2sql.transparent_engine import TransparentQueryEngine
from src.text2sql.reasoning.knowledge_boundary import BoundaryType


@pytest.fixture
def llm_client():
    """Create a mock LLM client."""
    mock_client = AsyncMock()
    
    # Configure generate_structured to return different results based on input
    async def mock_generate_structured(prompt, schema):
        if "intent_classification" in str(prompt):
            return {
                "intent_type": "selection",
                "confidence": 0.95,
                "explanation": "Query is asking to retrieve information"
            }
        elif "entity_resolution" in str(prompt):
            if "Customer" in str(prompt):
                return {
                    "table_name": "customer",
                    "confidence": 0.9,
                    "reasoning": "Direct match with the customer table"
                }
            elif "Order" in str(prompt):
                return {
                    "table_name": "orders",
                    "confidence": 0.85,
                    "reasoning": "Direct match with the orders table"
                }
            else:
                return {
                    "table_name": None,
                    "confidence": 0.3,
                    "reasoning": "No matching table found"
                }
        elif "entity_alternatives" in str(prompt):
            return {
                "alternatives": [
                    {
                        "table_name": "alternative_table",
                        "description": "Alternative interpretation",
                        "confidence": 0.6,
                        "reason": "Possible semantic match"
                    }
                ]
            }
        elif "intent_alternatives" in str(prompt):
            return {
                "alternatives": [
                    {
                        "intent_type": "aggregation",
                        "description": "Query could be asking for aggregate counts",
                        "confidence": 0.4,
                        "reason": "Ambiguous phrasing"
                    }
                ]
            }
        return {}
    
    mock_client.generate_structured = AsyncMock(side_effect=mock_generate_structured)
    return mock_client


@pytest.fixture
def neo4j_client():
    """Create a mock Neo4j client."""
    mock_client = MagicMock()
    
    # Configure get_tables_for_tenant to return sample tables
    mock_client.get_tables_for_tenant = MagicMock(return_value=[
        {"name": "customer", "description": "Customer information"},
        {"name": "orders", "description": "Customer orders"}
    ])
    
    # Configure get_columns_for_table to return sample columns
    mock_client.get_columns_for_table = MagicMock(return_value=[
        {"name": "id", "data_type": "integer", "table_name": "customer"},
        {"name": "name", "data_type": "string", "table_name": "customer"}
    ])
    
    # Configure execute_query for graph operations
    mock_client._execute_query = MagicMock(return_value=[])
    
    return mock_client


@pytest.fixture
def transparent_engine(llm_client, neo4j_client):
    """Create a TransparentQueryEngine for testing."""
    return TransparentQueryEngine(llm_client, neo4j_client)


@pytest.mark.asyncio
async def test_transparent_engine_basic_query(transparent_engine):
    """Test the transparent engine with a basic query."""
    # Process a test query
    result = await transparent_engine.process_query(
        query="Show me all Customers and their Orders",
        tenant_id="default"
    )
    
    # Check high-level response
    assert result.original_query == "Show me all Customers and their Orders"
    assert "intent" in result.interpreted_as.lower()
    assert result.primary_interpretation is not None
    assert len(result.sql_results) > 0
    
    # Check entities were resolved
    assert len(result.entities_resolved) >= 1  # At least Customer should be resolved
    assert "Customer" in result.entities_resolved
    
    # Check metadata
    assert "reasoning_stream" in result.metadata
    assert "intent" in result.metadata
    assert "entities" in result.metadata
    assert "knowledge_boundaries" in result.metadata
    
    # Check reasoning stream
    reasoning_stream = result.metadata["reasoning_stream"]
    assert len(reasoning_stream["stages"]) >= 2  # Intent + Entity stages
    
    # Verify we have a valid SQL generated
    assert "SELECT" in result.primary_interpretation.sql
    assert "FROM" in result.primary_interpretation.sql
    
    # Print the full reasoning for debugging
    print(json.dumps(reasoning_stream, indent=2))


@pytest.mark.asyncio
async def test_transparent_engine_with_unknown_entity(transparent_engine):
    """Test the transparent engine with an unknown entity."""
    # Process a test query with an unknown entity
    result = await transparent_engine.process_query(
        query="Show me all UnknownEntity data",
        tenant_id="default"
    )
    
    # Check we got a response despite the unknown entity
    assert result.primary_interpretation is not None
    
    # Check knowledge boundaries
    boundaries = result.metadata["knowledge_boundaries"]["boundaries"]
    unknown_entity_boundaries = [b for b in boundaries 
                              if b["boundary_type"] == BoundaryType.UNKNOWN_ENTITY.value]
    
    # Should have at least one unknown entity boundary
    assert len(unknown_entity_boundaries) > 0
    
    # Check clarification flag is set
    assert result.metadata["requires_clarification"] is True


@pytest.mark.asyncio
async def test_entity_resolution_in_transparent_engine(transparent_engine):
    """Test entity resolution specifically in the transparent engine."""
    # Process a test query with known entities
    result = await transparent_engine.process_query(
        query="Show me Customer data with their Orders",
        tenant_id="default"
    )
    
    # Check entity recognition results
    entities = result.metadata["entities"]
    assert "Customer" in entities
    
    # Check entity details
    customer_entity = entities["Customer"]
    assert customer_entity["resolution_type"] == "table"  # Should be direct table match
    assert customer_entity["confidence"] >= 0.8  # High confidence
    
    # Check that the SQL references the resolved table
    assert "customer" in result.primary_interpretation.sql.lower()
    
    # Check that the reasoning stream includes entity recognition
    reasoning_stream = result.metadata["reasoning_stream"]
    entity_stage = next((s for s in reasoning_stream["stages"] 
                        if s["name"] == "Entity Recognition"), None)
    
    assert entity_stage is not None
    assert len(entity_stage["steps"]) >= 2