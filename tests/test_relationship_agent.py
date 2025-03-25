"""
Tests for RelationshipAgent in the transparent reasoning framework.
"""

import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from src.text2sql.reasoning.agents.relationship_agent import (
    RelationshipAgent, DirectForeignKeyStrategy, CommonColumnStrategy, 
    ConceptBasedJoinStrategy, LLMBasedJoinStrategy
)
from src.text2sql.reasoning.models import ReasoningStream
from src.text2sql.reasoning.knowledge_boundary import BoundaryRegistry, BoundaryType


@pytest.fixture
def llm_client():
    """Create a mock LLM client."""
    mock_client = AsyncMock()
    mock_client.generate_structured = AsyncMock()
    return mock_client


@pytest.fixture
def graph_context_provider():
    """Create a mock graph context provider."""
    mock_provider = MagicMock()
    
    # Configure get_join_paths to return different results based on strategy
    def mock_get_join_paths(tenant_id, source_table, target_table, min_confidence, strategy):
        if source_table == "customer" and target_table == "orders":
            if strategy == "direct_fk":
                return [{
                    "path": [
                        {"from_table": "customer", "from_column": "id", 
                         "to_table": "orders", "to_column": "customer_id"}
                    ],
                    "confidence": 0.9
                }]
            elif strategy == "common_columns":
                return [{
                    "path": [
                        {"from_table": "customer", "from_column": "id", 
                         "to_table": "orders", "to_column": "customer_id"}
                    ],
                    "confidence": 0.8
                }]
        elif source_table == "product" and target_table == "order_items":
            if strategy == "direct_fk":
                return [{
                    "path": [
                        {"from_table": "product", "from_column": "id", 
                         "to_table": "order_items", "to_column": "product_id"}
                    ],
                    "confidence": 0.9
                }]
        elif source_table == "customer" and target_table == "order_items":
            if strategy == "semantic_concepts":
                return [{
                    "path": [
                        {"from_table": "customer", "from_column": "id", 
                         "to_table": "orders", "to_column": "customer_id"},
                        {"from_table": "orders", "from_column": "id", 
                         "to_table": "order_items", "to_column": "order_id"}
                    ],
                    "confidence": 0.85,
                    "concept_info": {
                        "name": "CustomerOrderItems",
                        "type": "composite"
                    }
                }]
        return []
    
    mock_provider.get_join_paths = MagicMock(side_effect=mock_get_join_paths)
    
    # Configure get_schema_context
    mock_provider.get_schema_context = MagicMock(return_value={
        "tables": {
            "customer": {
                "description": "Customer information",
                "columns": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"},
                    {"name": "name", "data_type": "string", "description": "Customer name"}
                ]
            },
            "orders": {
                "description": "Customer orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"},
                    {"name": "customer_id", "data_type": "integer", "description": "Foreign key to customer"}
                ],
                "foreign_keys": [
                    {"name": "customer_id", "referenced_table": "customer", "referenced_column": "id"}
                ]
            },
            "product": {
                "description": "Product information",
                "columns": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"},
                    {"name": "name", "data_type": "string", "description": "Product name"}
                ]
            },
            "order_items": {
                "description": "Order line items",
                "columns": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"},
                    {"name": "order_id", "data_type": "integer", "description": "Foreign key to orders"},
                    {"name": "product_id", "data_type": "integer", "description": "Foreign key to product"}
                ],
                "foreign_keys": [
                    {"name": "order_id", "referenced_table": "orders", "referenced_column": "id"},
                    {"name": "product_id", "referenced_table": "product", "referenced_column": "id"}
                ]
            }
        }
    })
    
    return mock_provider


@pytest.fixture
def prompt_loader():
    """Create a mock prompt loader."""
    mock_loader = MagicMock()
    mock_loader.format_prompt = MagicMock(return_value="Mock prompt")
    return mock_loader


@pytest.fixture
def relationship_agent(llm_client, graph_context_provider, prompt_loader):
    """Create a RelationshipAgent for testing."""
    return RelationshipAgent(llm_client, graph_context_provider, prompt_loader)


@pytest.mark.asyncio
async def test_relationship_agent_direct_joins(relationship_agent, llm_client):
    """Test RelationshipAgent with direct joins."""
    # Configure LLM client to return structured results
    llm_client.generate_structured.return_value = {
        "join_path": [
            {
                "from_table": "customer", 
                "from_column": "id", 
                "to_table": "orders", 
                "to_column": "customer_id",
                "relationship_type": "one_to_many"
            }
        ],
        "confidence": 0.7,
        "reasoning": "Direct foreign key relationship"
    }
    
    # Setup test context with resolved entities
    context = {
        "query": "Show me all Customers and their Orders",
        "tenant_id": "default",
        "intent": {"intent_type": "selection", "confidence": 0.9},
        "entities": {
            "Customer": {
                "resolved_to": "customer",
                "confidence": 0.9,
                "resolution_type": "table"
            },
            "Orders": {
                "resolved_to": "orders",
                "confidence": 0.85,
                "resolution_type": "table"
            }
        }
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-1",
        query_text=context["query"]
    )
    
    # Process with relationship agent
    result = await relationship_agent.process(context, reasoning_stream)
    
    # Check that we have relationships
    assert "relationships" in result
    assert "requires_joins" in result
    assert result["requires_joins"] is True
    
    # Check for the specific relationship
    customer_orders_key = "Customer_to_Orders"
    assert customer_orders_key in result["relationships"]
    assert result["relationships"][customer_orders_key]["source_entity"] == "Customer"
    assert result["relationships"][customer_orders_key]["target_entity"] == "Orders"
    assert result["relationships"][customer_orders_key]["join_path"] is not None
    assert result["relationships"][customer_orders_key]["confidence"] >= 0.8
    
    # Check reasoning stream was properly updated
    assert len(reasoning_stream.stages) == 1
    assert reasoning_stream.stages[0].name == "Relationship Discovery"
    assert len(reasoning_stream.stages[0].steps) >= 2  # At least 2 steps


@pytest.mark.asyncio
async def test_relationship_agent_indirect_joins(relationship_agent, llm_client):
    """Test RelationshipAgent with indirect joins through intermediate tables."""
    # Setup test context with three entities requiring indirect joins
    context = {
        "query": "Show me Customers and the Products they ordered",
        "tenant_id": "default",
        "intent": {"intent_type": "selection", "confidence": 0.9},
        "entities": {
            "Customer": {
                "resolved_to": "customer",
                "confidence": 0.9,
                "resolution_type": "table"
            },
            "OrderItems": {
                "resolved_to": "order_items",
                "confidence": 0.85,
                "resolution_type": "table"
            },
            "Product": {
                "resolved_to": "product",
                "confidence": 0.8,
                "resolution_type": "table"
            }
        }
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-2",
        query_text=context["query"]
    )
    
    # Process with relationship agent
    result = await relationship_agent.process(context, reasoning_stream)
    
    # Check that we have relationships and a join tree for three tables
    assert "relationships" in result
    assert "requires_joins" in result
    assert result["requires_joins"] is True
    assert "join_tree" in result
    assert result["join_tree"] is not None
    
    # Check that we have proper paths for at least some of the relationships
    has_valid_paths = False
    for rel_key, rel_info in result["relationships"].items():
        if rel_info["join_path"] is not None:
            has_valid_paths = True
            break
    
    assert has_valid_paths is True
    
    # Check for missing relationships boundary
    assert "boundary_registry" in context
    boundary_registry = context["boundary_registry"]
    missing_rel_boundaries = boundary_registry.get_boundaries_by_type(BoundaryType.MISSING_RELATIONSHIP)
    
    # Since we didn't mock all possible join paths, we should have some missing relationships
    assert len(missing_rel_boundaries) > 0


@pytest.mark.asyncio
async def test_relationship_agent_single_entity(relationship_agent):
    """Test RelationshipAgent with a single entity (no joins needed)."""
    # Setup test context with just one entity
    context = {
        "query": "Show me all Customers",
        "tenant_id": "default",
        "intent": {"intent_type": "selection", "confidence": 0.9},
        "entities": {
            "Customer": {
                "resolved_to": "customer",
                "confidence": 0.9,
                "resolution_type": "table"
            }
        }
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-3",
        query_text=context["query"]
    )
    
    # Process with relationship agent
    result = await relationship_agent.process(context, reasoning_stream)
    
    # Should indicate no joins needed
    assert "relationships" in result
    assert "requires_joins" in result
    assert result["requires_joins"] is False
    assert len(result["relationships"]) == 0
    
    # Check reasoning stream
    assert len(reasoning_stream.stages) == 1
    assert reasoning_stream.stages[0].name == "Relationship Discovery"
    assert "single-entity" in reasoning_stream.stages[0].conclusion.lower()


@pytest.mark.asyncio
async def test_resolution_strategies(graph_context_provider, llm_client, prompt_loader):
    """Test individual join path resolution strategies."""
    # Test DirectForeignKeyStrategy
    direct_strategy = DirectForeignKeyStrategy(graph_context_provider)
    direct_result = await direct_strategy.resolve("customer", "orders", {"tenant_id": "default"})
    assert direct_result["join_path"] is not None
    assert direct_result["confidence"] == 0.9
    
    # Test CommonColumnStrategy
    common_strategy = CommonColumnStrategy(graph_context_provider)
    common_result = await common_strategy.resolve("customer", "orders", {"tenant_id": "default"})
    assert common_result["join_path"] is not None
    assert common_result["confidence"] == 0.8
    
    # Test ConceptBasedJoinStrategy
    concept_strategy = ConceptBasedJoinStrategy(graph_context_provider)
    concept_result = await concept_strategy.resolve("customer", "order_items", {"tenant_id": "default"})
    assert concept_result["join_path"] is not None
    assert concept_result["confidence"] <= 0.85
    assert "concept" in concept_result
    
    # Test LLMBasedJoinStrategy
    llm_client.generate_structured.return_value = {
        "join_path": [
            {
                "from_table": "customer", 
                "from_column": "id", 
                "to_table": "orders", 
                "to_column": "customer_id",
                "relationship_type": "one_to_many"
            }
        ],
        "confidence": 0.7,
        "reasoning": "Based on schema analysis"
    }
    
    llm_strategy = LLMBasedJoinStrategy(graph_context_provider, llm_client, prompt_loader)
    llm_result = await llm_strategy.resolve("customer", "orders", {
        "tenant_id": "default",
        "query": "Show me customers and their orders"
    })
    
    assert llm_result["join_path"] is not None
    assert llm_result["confidence"] == 0.7
    assert "reasoning" in llm_result


if __name__ == "__main__":
    asyncio.run(pytest.main(["-xvs", "test_relationship_agent.py"]))