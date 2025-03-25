"""
Tests for the AttributeAgent.

This module contains unit and integration tests for the AttributeAgent and its components.
"""

import unittest
import asyncio
from unittest.mock import MagicMock, patch

from src.text2sql.reasoning.agents.attribute.attribute_agent import AttributeAgent
from src.text2sql.reasoning.agents.attribute.base import AttributeType
from src.text2sql.reasoning.models import ReasoningStream


class TestAttributeAgent(unittest.TestCase):
    """Tests for the AttributeAgent."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock dependencies
        self.llm_client = MagicMock()
        self.graph_context = MagicMock()
        self.prompt_loader = MagicMock()
        
        # Set up return values for common graph context methods
        self.graph_context.get_table_schema.return_value = {
            "columns": [
                {"name": "id", "data_type": "integer", "description": "Primary key"},
                {"name": "name", "data_type": "string", "description": "Customer name"},
                {"name": "status", "data_type": "string", "description": "Customer status"},
                {"name": "created_at", "data_type": "date", "description": "Creation date"}
            ]
        }
        
        # Create agent instance
        self.agent = AttributeAgent(self.llm_client, self.graph_context, self.prompt_loader)
        
        # Add some test extractors and strategies
        self.agent.extractors = [MagicMock()]
        self.agent.resolution_strategies = [MagicMock()]
        
        # Set up mock extractor behavior
        self.agent.extractors[0].name = "TestExtractor"
        self.agent.extractors[0].extract.return_value = {
            AttributeType.FILTER: [
                {"attribute_value": "status is active", "operator": "equal_to", "value": {"text": "active"}, "column_hint": "status"}
            ],
            AttributeType.AGGREGATION: [
                {"attribute_value": "count of customers", "function": "count", "target": "customers"}
            ],
            AttributeType.GROUPING: [],
            AttributeType.SORTING: [],
            AttributeType.LIMIT: []
        }
        
        # Set up mock strategy behavior
        self.agent.resolution_strategies[0].strategy_name = "TestStrategy"
        self.agent.resolution_strategies[0].resolve.return_value = {
            "attribute_type": "filter",
            "attribute_value": "status is active",
            "resolved_to": "customer.status = 'active'",
            "confidence": 0.85,
            "strategy": "TestStrategy",
            "metadata": {}
        }
    
    def test_get_agent_type(self):
        """Test that the agent returns the correct type."""
        self.assertEqual(self.agent._get_agent_type(), "attribute")
    
    def test_async_process(self):
        """Test the async process method."""
        async def _run_test():
            # Create test context and reasoning stream
            context = {
                "query": "Show me active customers",
                "entities": {
                    "customers": {
                        "resolved_to": "customer",
                        "confidence": 0.9
                    }
                }
            }
            reasoning_stream = ReasoningStream(
                query_id="test_id",
                query_text="Show me active customers"
            )
            
            # Set up more specific mock behavior for the test
            self.agent.resolution_strategies[0].resolve.return_value = {
                "attribute_type": AttributeType.FILTER,
                "attribute_value": "status is active",
                "resolved_to": "customer.status = 'active'",
                "confidence": 0.85,
                "strategy": "TestStrategy",
                "metadata": {}
            }
            
            # Run the process method
            result = await self.agent.process(context, reasoning_stream)
            
            # Verify the results
            self.assertIn(AttributeType.FILTER, result)
            self.assertEqual(len(result[AttributeType.FILTER]), 1)
            self.assertEqual(result[AttributeType.FILTER][0]["resolved_to"], "customer.status = 'active'")
            self.assertIn("confidence", result)
            self.assertEqual(result["confidence"], 0.85)
            
            # Verify that the reasoning stream was updated
            self.assertEqual(len(reasoning_stream.stages), 1)
            self.assertEqual(reasoning_stream.stages[0].name, "Attribute Processing")
            self.assertTrue(reasoning_stream.stages[0].completed)
        
        # Run the async test
        asyncio.run(_run_test())
    
    def test_extract_attributes(self):
        """Test the attribute extraction method."""
        async def _run_test():
            # Test data
            query = "Show me active customers with more than 3 orders"
            entity_context = {
                "entities": {
                    "customers": {
                        "resolved_to": "customer"
                    }
                }
            }
            
            # Run the method
            result = await self.agent._extract_attributes(query, entity_context)
            
            # Verify results
            self.assertIn("attributes", result)
            self.assertIn("methods", result)
            self.assertIn("confidence", result)
            self.assertIn(AttributeType.FILTER, result["attributes"])
            self.assertIn("TestExtractor", result["methods"])
        
        # Run the async test
        asyncio.run(_run_test())
    
    def test_resolve_attributes(self):
        """Test the attribute resolution method."""
        async def _run_test():
            # Test data
            attributes = {
                AttributeType.FILTER: [
                    {"attribute_value": "status is active", "operator": "equal_to", "value": {"text": "active"}, "column_hint": "status"}
                ],
                AttributeType.AGGREGATION: [
                    {"attribute_value": "count of customers", "function": "count", "target": "customers"}
                ],
                AttributeType.GROUPING: [],
                AttributeType.SORTING: [],
                AttributeType.LIMIT: []
            }
            context = {
                "query": "Show me active customers",
                "entities": {
                    "customers": {
                        "resolved_to": "customer"
                    }
                }
            }
            
            # Run the method
            result = await self.agent._resolve_attributes(attributes, context)
            
            # Verify results
            self.assertIn(AttributeType.FILTER, result)
            self.assertEqual(len(result[AttributeType.FILTER]), 1)
            self.assertEqual(result[AttributeType.FILTER][0]["resolved_to"], "customer.status = 'active'")
        
        # Run the async test
        asyncio.run(_run_test())


class TestAttributeExtractors(unittest.TestCase):
    """Tests for attribute extractors."""
    
    def test_keyword_based_extractor(self):
        """Test the keyword-based attribute extractor."""
        from src.text2sql.reasoning.agents.attribute.extractors.keyword_based import KeywordBasedAttributeExtractor
        
        # Create extractor
        extractor = KeywordBasedAttributeExtractor()
        
        # Test query and context
        query = "Show me customers with status active, order by name, limit 10"
        entity_context = {
            "entities": {
                "customers": {
                    "resolved_to": "customer"
                }
            }
        }
        
        # Run the extractor
        result = extractor.extract(query, entity_context)
        
        # Verify results
        self.assertIn(AttributeType.FILTER, result)
        self.assertIn(AttributeType.SORTING, result)
        self.assertIn(AttributeType.LIMIT, result)
        
        # Check filter
        filters = result[AttributeType.FILTER]
        self.assertTrue(any(f.get("attribute_value") and "status active" in f.get("attribute_value") for f in filters))
        
        # Check sorting
        sortings = result[AttributeType.SORTING]
        self.assertTrue(any(s.get("attribute_value") and "order by name" in s.get("attribute_value") for s in sortings))
        
        # Check limit
        limits = result[AttributeType.LIMIT]
        self.assertTrue(any(l.get("attribute_value") and "limit 10" in l.get("attribute_value") for l in limits))


class TestAttributeResolutionStrategies(unittest.TestCase):
    """Tests for attribute resolution strategies."""
    
    def test_column_based_strategy(self):
        """Test the column-based resolution strategy."""
        from src.text2sql.reasoning.agents.attribute.strategies.column_based import ColumnBasedResolutionStrategy
        
        # Create mock graph context
        graph_context = MagicMock()
        graph_context.get_table_schema.return_value = {
            "columns": [
                {"name": "id", "data_type": "integer", "description": "Primary key"},
                {"name": "status", "data_type": "string", "description": "Customer status"},
                {"name": "created_at", "data_type": "date", "description": "Creation date"}
            ]
        }
        
        # Create strategy
        strategy = ColumnBasedResolutionStrategy(graph_context)
        
        # Test resolving a filter
        async def _run_test():
            # Test attribute and context
            attribute = {
                "attribute_value": "status is active",
                "operator": "equal_to",
                "value": {"text": "active"},
                "column_hint": "status"
            }
            context = {
                "entities": {
                    "customers": {
                        "resolved_to": "customer"
                    }
                }
            }
            
            # Run the strategy
            result = await strategy.resolve(AttributeType.FILTER, attribute, context)
            
            # Verify results
            self.assertEqual(result["attribute_type"], AttributeType.FILTER)
            self.assertIsNotNone(result["resolved_to"])
            self.assertIn("customer.status", result["resolved_to"])
            self.assertIn("active", result["resolved_to"])
            self.assertGreater(result["confidence"], 0.0)
        
        # Run the async test
        asyncio.run(_run_test())


if __name__ == "__main__":
    unittest.main()