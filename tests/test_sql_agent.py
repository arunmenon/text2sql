"""
Tests for SQLAgent in the transparent reasoning framework.
"""

import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from src.text2sql.reasoning.agents.sql_agent import SQLAgent
from src.text2sql.reasoning.models import ReasoningStream
from src.text2sql.reasoning.knowledge_boundary import BoundaryRegistry, BoundaryType


@pytest.fixture
def llm_client():
    """Create a mock LLM client."""
    mock_client = AsyncMock()
    
    # Configure generate_structured to return different results based on input
    async def mock_generate_structured(prompt, schema):
        if "sql_generation" in str(prompt):
            return {
                "sql": "SELECT c.name, COUNT(o.id) FROM customer c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY COUNT(o.id) DESC",
                "explanation": "Counts orders per customer and sorts by order count",
                "confidence": 0.85,
                "approach": "Join-based aggregation with grouping"
            }
        elif "sql_validation" in str(prompt):
            return {
                "is_valid": True,
                "confidence": 0.9,
                "issues": [],
                "suggestions": []
            }
        elif "sql_generation_alternative" in str(prompt):
            return {
                "sql": "SELECT c.name, o.order_date, o.total_amount FROM customer c JOIN orders o ON c.id = o.customer_id ORDER BY o.order_date DESC",
                "explanation": "This shows customer orders with details sorted by date",
                "confidence": 0.7,
                "approach": "Detailed selection instead of aggregation"
            }
        return {}
    
    mock_client.generate_structured = AsyncMock(side_effect=mock_generate_structured)
    return mock_client


@pytest.fixture
def graph_context_provider():
    """Create a mock graph context provider."""
    mock_provider = MagicMock()
    
    # Configure get_schema_context
    mock_provider.get_schema_context = MagicMock(return_value={
        "tables": {
            "customer": {
                "description": "Customer information",
                "columns": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"},
                    {"name": "name", "data_type": "string", "description": "Customer name"},
                    {"name": "email", "data_type": "string", "description": "Customer email"}
                ]
            },
            "orders": {
                "description": "Customer orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"},
                    {"name": "customer_id", "data_type": "integer", "description": "Foreign key to customer"},
                    {"name": "order_date", "data_type": "date", "description": "Date of order"},
                    {"name": "total_amount", "data_type": "decimal", "description": "Total order amount"}
                ]
            }
        },
        "glossary_terms": {
            "Customer": {
                "definition": "A person or company that buys goods or services",
                "mapped_tables": ["customer"]
            },
            "Order": {
                "definition": "A request to purchase something",
                "mapped_tables": ["orders"]
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
def sql_agent(llm_client, graph_context_provider, prompt_loader):
    """Create a SQLAgent for testing."""
    return SQLAgent(llm_client, graph_context_provider, prompt_loader)


@pytest.mark.asyncio
async def test_sql_agent_with_entities(sql_agent, llm_client):
    """Test SQLAgent with resolved entities."""
    # Setup test context with resolved entities
    context = {
        "query": "Show me the customers with the most orders",
        "tenant_id": "default",
        "intent": {"intent_type": "aggregation", "confidence": 0.9},
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
        },
        "relationships": {
            "requires_joins": True,
            "relationships": {
                "Customer_to_Orders": {
                    "source_entity": "Customer",
                    "target_entity": "Orders",
                    "source_table": "customer",
                    "target_table": "orders",
                    "join_path": {
                        "path": [
                            {"from_table": "customer", "from_column": "id", 
                             "to_table": "orders", "to_column": "customer_id"}
                        ],
                        "confidence": 0.9
                    },
                    "confidence": 0.9
                }
            }
        }
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-1",
        query_text=context["query"]
    )
    
    # Process with SQL agent
    result = await sql_agent.process(context, reasoning_stream)
    
    # Check the result has SQL
    assert "sql" in result
    assert result["sql"] != ""
    assert "confidence" in result
    assert result["confidence"] >= 0.8
    
    # Check reasoning stream was properly updated
    assert len(reasoning_stream.stages) == 1
    assert reasoning_stream.stages[0].name == "SQL Generation"
    assert len(reasoning_stream.stages[0].steps) >= 2  # At least 2 steps
    
    # Check SQL validates properly
    assert "is_valid" in result
    assert result["is_valid"] is True


@pytest.mark.asyncio
async def test_sql_agent_with_invalid_sql(sql_agent, llm_client):
    """Test SQLAgent with SQL that fails validation."""
    # Configure LLM client to return invalid SQL
    llm_client.generate_structured = AsyncMock()
    llm_client.generate_structured.side_effect = [
        # First call - SQL generation
        {
            "sql": "SELECT * FROM non_existent_table",
            "explanation": "Shows all records from the table",
            "confidence": 0.7,
            "approach": "Direct selection"
        },
        # Second call - SQL validation
        {
            "is_valid": False,
            "confidence": 0.9,
            "issues": ["Table 'non_existent_table' does not exist"],
            "suggestions": ["Check table names against schema"]
        },
        # Third call - Simplified SQL generation
        {
            "sql": "SELECT * FROM customer LIMIT 10",
            "explanation": "Shows sample customer data",
            "confidence": 0.6
        }
    ]
    
    # Setup test context
    context = {
        "query": "Show me all records",
        "tenant_id": "default",
        "intent": {"intent_type": "selection", "confidence": 0.9},
        "entities": {
            "Customer": {
                "resolved_to": "customer",
                "confidence": 0.9,
                "resolution_type": "table"
            }
        },
        "boundary_registry": BoundaryRegistry()
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-2",
        query_text=context["query"]
    )
    
    # Process with SQL agent
    result = await sql_agent.process(context, reasoning_stream)
    
    # Check the result has SQL
    assert "sql" in result
    assert result["sql"] != ""
    
    # Check that SQL validation failed
    assert "is_valid" in result
    assert result["is_valid"] is False
    assert "issues" in result
    assert len(result["issues"]) > 0
    
    # Check knowledge boundary was added
    assert "boundary_registry" in context
    boundary_registry = context["boundary_registry"]
    sql_boundaries = boundary_registry.get_boundaries_by_type(BoundaryType.COMPLEX_IMPLEMENTATION)
    assert len(sql_boundaries) > 0
    
    # Check alternative was generated
    assert "alternatives" in result
    assert len(result["alternatives"]) > 0


@pytest.mark.asyncio
async def test_sql_agent_no_entities(sql_agent):
    """Test SQLAgent with no resolved entities."""
    # Setup test context with no entities
    context = {
        "query": "Show me the data",
        "tenant_id": "default",
        "intent": {"intent_type": "selection", "confidence": 0.9},
        "entities": {},
        "boundary_registry": BoundaryRegistry()
    }
    
    # Setup reasoning stream
    reasoning_stream = ReasoningStream(
        query_id="test-3",
        query_text=context["query"]
    )
    
    # Process with SQL agent
    result = await sql_agent.process(context, reasoning_stream)
    
    # Check the result indicates failure
    assert "sql" in result
    assert result["sql"] == ""
    assert "explanation" in result
    assert "missing entities" in result["explanation"].lower()
    
    # Check knowledge boundary was added
    assert "boundary_registry" in context
    boundary_registry = context["boundary_registry"]
    unmappable_boundaries = boundary_registry.get_boundaries_by_type(BoundaryType.UNMAPPABLE_CONCEPT)
    assert len(unmappable_boundaries) > 0
    
    # Check reasoning stage was concluded
    assert reasoning_stream.stages[0].completed is True
    assert "missing entities" in reasoning_stream.stages[0].conclusion.lower()


@pytest.mark.asyncio
async def test_sql_agent_basic_validation(sql_agent):
    """Test SQL Agent's basic validation function."""
    # Test valid SQL
    valid_sql = "SELECT * FROM customer WHERE status = 'active'"
    validation = sql_agent._basic_sql_validation(valid_sql)
    assert validation["is_valid"] is True
    assert len(validation["issues"]) == 0
    
    # Test SQL missing SELECT
    invalid_sql1 = "* FROM customer"
    validation = sql_agent._basic_sql_validation(invalid_sql1)
    assert validation["is_valid"] is False
    assert len(validation["issues"]) > 0
    assert any("SELECT" in issue for issue in validation["issues"])
    
    # Test SQL with unbalanced parentheses
    invalid_sql2 = "SELECT * FROM customer WHERE (status = 'active'"
    validation = sql_agent._basic_sql_validation(invalid_sql2)
    assert validation["is_valid"] is False
    assert len(validation["issues"]) > 0
    assert any("parentheses" in issue for issue in validation["issues"])
    
    # Test SQL with potential injection patterns
    invalid_sql3 = "SELECT * FROM customer; DROP TABLE users;"
    validation = sql_agent._basic_sql_validation(invalid_sql3)
    assert validation["is_valid"] is False
    assert len(validation["issues"]) > 0
    assert any("injection" in issue.lower() for issue in validation["issues"])


if __name__ == "__main__":
    asyncio.run(pytest.main(["-xvs", "test_sql_agent.py"]))