"""
Tests for EntityAgent in the transparent reasoning framework.
"""

import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from src.text2sql.reasoning.agents.entity_agent import (
    EntityAgent, DirectTableMatchStrategy, GlossaryTermMatchStrategy, 
    SemanticConceptMatchStrategy, LLMBasedResolutionStrategy
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
    
    # Configure get_entity_resolution_context to return different results based on input
    def mock_get_entity_resolution(tenant_id, entity_name):
        if entity_name == "Customer":
            return {
                "type": "table",
                "resolution": [{"name": "customer", "description": "Customer table"}]
            }
        elif entity_name == "Proposal":
            return {
                "type": "business_term",
                "resolution": {
                    "name": "Proposal",
                    "definition": "A formal offer of services",
                    "mapped_tables": ["sc_walmart_proposals"]
                }
            }
        elif entity_name == "Inspection":
            return {
                "type": "semantic_concept",
                "resolution": {
                    "name": "Inspection",
                    "description": "Inspection of assets",
                    "labels": ["CompositeConcept"],
                    "implementation": {
                        "tables_involved": ["sc_walmart_asset_leak_inspections", "asset"]
                    }
                }
            }
        else:
            return {"type": "unknown", "resolution": None}
    
    mock_provider.get_entity_resolution_context = MagicMock(side_effect=mock_get_entity_resolution)
    mock_provider.get_graph_enhanced_context = MagicMock(return_value={
        "tables": {
            "customer": {
                "description": "Customer information",
                "columns": [{"name": "id", "data_type": "integer"}]
            },
            "sc_walmart_proposals": {
                "description": "Proposals for Walmart",
                "columns": [{"name": "proposal_id", "data_type": "integer"}]
            },
            "sc_walmart_asset_leak_inspections": {
                "description": "Asset leak inspections",
                "columns": [{"name": "inspection_id", "data_type": "integer"}]
            },
            "asset": {
                "description": "Asset information",
                "columns": [{"name": "asset_id", "data_type": "integer"}]
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
def entity_agent(llm_client, graph_context_provider, prompt_loader):
    """Create an EntityAgent for testing."""
    return EntityAgent(llm_client, graph_context_provider, prompt_loader)


@pytest.mark.asyncio
async def test_entity_agent_process_with_known_entities(entity_agent, llm_client):
    """Test EntityAgent with known entities."""
    # Configure LLM client to return structured results
    llm_client.generate_structured.return_value = {
        "alternatives": [
            {
                "table_name": "alternative_table",
                "description": "Alternative description",
                "confidence": 0.6,
                "reason": "Alternative reason"
            }
        ]
    }
    
    # Setup test context
    context = {
        "query": "Show me all Customers and their Proposals",
        "tenant_id": "default",
        "intent": {"intent_type": "selection", "confidence": 0.9}
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-1",
        query_text=context["query"]
    )
    
    # Process with entity agent
    result = await entity_agent.process(context, reasoning_stream)
    
    # Check the result has both entities
    assert "Customer" in result
    assert "Proposal" in result
    
    # Check each entity was resolved correctly
    assert result["Customer"]["resolved_to"] == "customer"
    assert result["Customer"]["resolution_type"] == "table"
    assert result["Customer"]["confidence"] >= 0.8
    
    assert result["Proposal"]["resolved_to"] == "sc_walmart_proposals"
    assert result["Proposal"]["resolution_type"] == "business_term"
    assert result["Proposal"]["confidence"] >= 0.8
    
    # Check reasoning stream was properly updated
    assert len(reasoning_stream.stages) == 1
    assert reasoning_stream.stages[0].name == "Entity Recognition"
    assert len(reasoning_stream.stages[0].steps) >= 3  # At least 3 steps
    
    # Check boundary registry is in context
    assert "boundary_registry" in context
    assert isinstance(context["boundary_registry"], BoundaryRegistry)


@pytest.mark.asyncio
async def test_entity_agent_with_unknown_entity(entity_agent, llm_client):
    """Test EntityAgent with an unknown entity."""
    # Configure LLM client to return a failed resolution
    llm_client.generate_structured.return_value = {
        "table_name": None,
        "confidence": 0.3,
        "reasoning": "This entity doesn't match any known table"
    }
    
    # Setup test context
    context = {
        "query": "Show me data about UnknownEntity",
        "tenant_id": "default",
        "intent": {"intent_type": "selection", "confidence": 0.9}
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-2",
        query_text=context["query"]
    )
    
    # Process with entity agent
    result = await entity_agent.process(context, reasoning_stream)
    
    # There should be no resolved entities
    assert len(result) == 0
    
    # Check that a knowledge boundary was added
    assert "boundary_registry" in context
    boundary_registry = context["boundary_registry"]
    
    # Check for unknown entity boundary
    unknown_boundaries = boundary_registry.get_boundaries_by_type(BoundaryType.UNKNOWN_ENTITY)
    assert len(unknown_boundaries) > 0
    
    # Check the explanation in the boundary
    assert "UnknownEntity" in unknown_boundaries[0].explanation


@pytest.mark.asyncio
async def test_entity_agent_with_composite_concept(entity_agent, llm_client):
    """Test EntityAgent with a composite concept entity."""
    # Setup test context
    context = {
        "query": "Count all Inspections",
        "tenant_id": "default",
        "intent": {"intent_type": "aggregation", "confidence": 0.9}
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-3",
        query_text=context["query"]
    )
    
    # Process with entity agent
    result = await entity_agent.process(context, reasoning_stream)
    
    # Check that Inspection was resolved
    assert "Inspection" in result
    
    # Check it was resolved to the appropriate table
    assert result["Inspection"]["resolved_to"] == "sc_walmart_asset_leak_inspections"
    assert result["Inspection"]["resolution_type"] == "semantic_concept"
    
    # Check that we have alternatives
    alternatives = [alt for alt in reasoning_stream.stages[0].alternatives 
                   if "sc_walmart_asset_leak_inspections" not in alt.description]
    
    # Should have at least one alternative (the second table in the composite concept)
    assert len(alternatives) > 0


@pytest.mark.asyncio
async def test_resolution_strategies(graph_context_provider, llm_client, prompt_loader):
    """Test individual resolution strategies."""
    # Test DirectTableMatchStrategy
    direct_strategy = DirectTableMatchStrategy(graph_context_provider)
    direct_result = await direct_strategy.resolve("Customer", {"tenant_id": "default"})
    assert direct_result["resolved_to"] == "customer"
    assert direct_result["confidence"] == 0.9
    
    # Test GlossaryTermMatchStrategy
    glossary_strategy = GlossaryTermMatchStrategy(graph_context_provider)
    glossary_result = await glossary_strategy.resolve("Proposal", {"tenant_id": "default"})
    assert glossary_result["resolved_to"] == "sc_walmart_proposals"
    assert glossary_result["confidence"] == 0.85
    
    # Test SemanticConceptMatchStrategy
    semantic_strategy = SemanticConceptMatchStrategy(graph_context_provider)
    semantic_result = await semantic_strategy.resolve("Inspection", {
        "tenant_id": "default",
        "intent": {"intent_type": "aggregation"}
    })
    assert semantic_result["resolved_to"] == "sc_walmart_asset_leak_inspections"
    assert semantic_result["confidence"] == 0.8
    
    # Test LLMBasedResolutionStrategy
    llm_client.generate_structured.return_value = {
        "table_name": "customer",
        "confidence": 0.7,
        "reasoning": "This appears to be referencing customer data"
    }
    
    llm_strategy = LLMBasedResolutionStrategy(graph_context_provider, llm_client, prompt_loader)
    llm_result = await llm_strategy.resolve("Shoppers", {
        "tenant_id": "default",
        "query": "Find all Shoppers",
        "intent": {"intent_type": "selection"}
    })
    
    assert llm_result["resolved_to"] == "customer"
    assert llm_result["confidence"] == 0.7
    assert "reasoning" in llm_result["metadata"]


if __name__ == "__main__":
    asyncio.run(pytest.main(["-xvs", "test_entity_agent.py"]))