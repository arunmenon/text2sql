"""
Intent Analysis Agent for transparent text2sql reasoning.

This agent is responsible for determining the primary intent of natural language queries
and identifying potential ambiguities or multiple intents.
"""

import logging
import re
from typing import Dict, Any, List, Optional

from src.llm.client import LLMClient
from src.text2sql.reasoning.agents.base import Agent
from src.text2sql.reasoning.models import ReasoningStream, Alternative
from src.text2sql.reasoning.knowledge_boundary import BoundaryType, KnowledgeBoundary, BoundaryRegistry
from src.text2sql.utils.prompt_loader import PromptLoader


class IntentAgent(Agent):
    """Agent for intent analysis with knowledge boundary awareness."""
    
    def __init__(self, llm_client: LLMClient, prompt_loader: PromptLoader):
        """
        Initialize intent agent.
        
        Args:
            llm_client: LLM client for text generation
            prompt_loader: Loader for prompt templates
        """
        super().__init__(prompt_loader)
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
    
    def _get_agent_type(self) -> str:
        """Return agent type for prompt naming."""
        return "intent"
    
    async def process(self, context: Dict[str, Any], reasoning_stream: ReasoningStream) -> Dict[str, Any]:
        """
        Process query for intent analysis.
        
        Args:
            context: Context with query information
            reasoning_stream: Reasoning stream to update
            
        Returns:
            Intent analysis results
        """
        query = context["query"]
        boundary_registry = context.get("boundary_registry", BoundaryRegistry())
        
        # Start intent analysis stage
        stage = reasoning_stream.start_stage(
            name="Intent Analysis",
            description="Determining the primary purpose of the query"
        )
        
        # Step 1: Pattern analysis for intent signals
        pattern_evidence = self._analyze_patterns(query)
        reasoning_stream.add_step(
            description="Analyzing query structure and patterns",
            evidence=pattern_evidence,
            confidence=pattern_evidence["confidence"]
        )
        
        # Step 2: Check for multiple intents (if needed)
        if self._might_have_multiple_intents(query):
            multi_intent_evidence = await self._check_multiple_intents(query)
            reasoning_stream.add_step(
                description="Checking for multiple or complex intents",
                evidence=multi_intent_evidence,
                confidence=multi_intent_evidence.get("confidence", 0.7)
            )
            
            # If multiple intents detected with ambiguity
            if multi_intent_evidence.get("has_multiple_intents") and multi_intent_evidence.get("requires_clarification"):
                boundary = KnowledgeBoundary(
                    boundary_type=BoundaryType.AMBIGUOUS_INTENT,
                    component="query_intent",
                    confidence=0.3,
                    explanation=multi_intent_evidence.get("explanation", "Query contains multiple conflicting intents"),
                    suggestions=multi_intent_evidence.get("clarification_questions", [])
                )
                boundary_registry.add_boundary(boundary)
        else:
            multi_intent_evidence = {"has_multiple_intents": False}
        
        # Step 3: LLM intent classification
        classification_evidence = await self._classify_intent(query)
        reasoning_stream.add_step(
            description="Classifying intent using language model",
            evidence=classification_evidence,
            confidence=classification_evidence.get("confidence", 0.8)
        )
        
        # Step 4: Determine final intent
        final_intent = self._determine_final_intent(
            pattern_evidence,
            classification_evidence,
            multi_intent_evidence if "has_multiple_intents" in multi_intent_evidence else None
        )
        
        # Step 5: Generate alternatives if confidence is not very high
        alternatives = []
        if final_intent["confidence"] < 0.8:
            alternatives = await self._generate_alternatives(query, final_intent["intent_type"])
            
            # If alternatives have high confidence, consider an ambiguity boundary
            if any(alt["confidence"] > 0.6 for alt in alternatives):
                boundary = KnowledgeBoundary(
                    boundary_type=BoundaryType.AMBIGUOUS_INTENT,
                    component="query_intent",
                    confidence=final_intent["confidence"],
                    explanation="Query has multiple possible interpretations with similar confidence",
                    alternatives=[{
                        "intent_type": alt["intent_type"] if "intent_type" in alt else "alternative",
                        "description": alt["description"],
                        "confidence": alt["confidence"]
                    } for alt in alternatives if alt["confidence"] > 0.4]
                )
                boundary_registry.add_boundary(boundary)
        
        # Conclude the reasoning stage
        reasoning_stream.conclude_stage(
            conclusion=f"Query intent determined to be {final_intent['intent_type']} with {final_intent['confidence']:.2f} confidence",
            final_output=final_intent,
            alternatives=[Alternative(
                description=alt["description"],
                confidence=alt["confidence"],
                reason=alt["reason"]
            ) for alt in alternatives]
        )
        
        # Update context with boundary registry
        context["boundary_registry"] = boundary_registry
        
        return final_intent
    
    def _analyze_patterns(self, query: str) -> Dict[str, Any]:
        """
        Analyze query patterns to detect intent signals.
        
        Args:
            query: Natural language query
            
        Returns:
            Pattern analysis evidence
        """
        query_lower = query.lower()
        
        # Define patterns for different intent types
        patterns = {
            "selection": [
                r"(show|list|get|find|display|give)(\s+me)?",
                r"what (are|is)",
                r"which",
                r"return"
            ],
            "aggregation": [
                r"how many",
                r"count",
                r"(sum|total)",
                r"average",
                r"(min|minimum|max|maximum)",
                r"mean|median"
            ],
            "comparison": [
                r"compare",
                r"(differ\w+|difference)",
                r"contrast",
                r"versus|vs",
                r"highest|lowest"
            ],
            "trend": [
                r"(trend|change) (of|in|over)",
                r"over time",
                r"(increas\w+|decreas\w+)",
                r"growth",
                r"time series"
            ]
        }
        
        # Count pattern matches for each intent type
        matches = {}
        for intent_type, intent_patterns in patterns.items():
            matches[intent_type] = sum(1 for pattern in intent_patterns if re.search(pattern, query_lower))
        
        # Find intent type with most pattern matches
        most_likely = max(matches.items(), key=lambda x: x[1]) if any(matches.values()) else ("selection", 0)
        
        # Calculate confidence based on match strength
        total_matches = sum(matches.values())
        if total_matches == 0:
            # No matches, default to selection with low confidence
            confidence = 0.5
        else:
            # Calculate confidence based on proportion of matches and absolute count
            proportion = most_likely[1] / total_matches
            strength = min(most_likely[1] / 3, 1.0)  # Normalize by expecting up to 3 matches
            confidence = 0.5 + (proportion * 0.2) + (strength * 0.3)  # Base 0.5 + up to 0.5 from signals
        
        return {
            "pattern_matches": matches,
            "most_likely_type": most_likely[0],
            "match_strength": most_likely[1],
            "confidence": confidence
        }
    
    def _might_have_multiple_intents(self, query: str) -> bool:
        """
        Check if query might have multiple intents based on heuristics.
        
        Args:
            query: Natural language query
            
        Returns:
            True if query might have multiple intents, False otherwise
        """
        # Check for conjunction indicators
        conjunction_patterns = [
            r"(and|also|additionally)",
            r"(as well as)",
            r"(along with)",
            r"(;|,\s*and)",
            r"(\.|\?)\s*[A-Z]"  # Multiple sentences
        ]
        
        # Check for multiple verb phrases
        verb_patterns = [
            r"(show|list|get|display).+(calculate|count|sum|average)",
            r"(compare|contrast).+(find|identify)",
            r"(how many).+(what is|which are)"
        ]
        
        # Check if any patterns match
        for pattern in conjunction_patterns + verb_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return True
        
        # Check if query is unusually long (might be multiple queries)
        if len(query.split()) > 20:
            return True
            
        return False
    
    async def _check_multiple_intents(self, query: str) -> Dict[str, Any]:
        """
        Check for multiple intents using LLM.
        
        Args:
            query: Natural language query
            
        Returns:
            Multiple intent analysis result
        """
        prompt = self.prompt_loader.format_prompt("intent_multiple_check", query=query)
        
        schema = {
            "type": "object",
            "properties": {
                "has_multiple_intents": {"type": "boolean"},
                "intents": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "intent_type": {"type": "string"},
                            "query_part": {"type": "string"},
                            "confidence": {"type": "number"}
                        }
                    }
                },
                "primary_intent": {
                    "type": "object",
                    "properties": {
                        "intent_type": {"type": "string"},
                        "confidence": {"type": "number"}
                    }
                },
                "requires_clarification": {"type": "boolean"},
                "clarification_questions": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "explanation": {"type": "string"},
                "confidence": {"type": "number"}
            }
        }
        
        try:
            result = await self.llm_client.generate_structured(prompt, schema)
            # Add overall confidence if not provided
            if "confidence" not in result:
                result["confidence"] = 0.8
            return result
        except Exception as e:
            self.logger.error(f"Error checking for multiple intents: {e}")
            return {
                "has_multiple_intents": False,
                "confidence": 0.5,
                "explanation": f"Error checking for multiple intents: {str(e)}"
            }
    
    async def _classify_intent(self, query: str) -> Dict[str, Any]:
        """
        Classify intent using LLM.
        
        Args:
            query: Natural language query
            
        Returns:
            Intent classification result
        """
        prompt = self.prompt_loader.format_prompt("intent_classification", query=query)
        
        schema = {
            "type": "object",
            "properties": {
                "intent_type": {
                    "type": "string",
                    "enum": ["selection", "aggregation", "comparison", "trend", "complex"]
                },
                "subtype": {"type": "string"},
                "confidence": {"type": "number"},
                "explanation": {"type": "string"}
            },
            "required": ["intent_type"]
        }
        
        try:
            return await self.llm_client.generate_structured(prompt, schema)
        except Exception as e:
            self.logger.error(f"Error classifying intent: {e}")
            return {
                "intent_type": "selection",  # Default to selection on error
                "confidence": 0.5,
                "explanation": f"Error classifying intent: {str(e)}"
            }
    
    def _determine_final_intent(self, pattern_evidence: Dict[str, Any], 
                              classification_evidence: Dict[str, Any],
                              multi_intent_evidence: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Determine final intent by combining evidence.
        
        Args:
            pattern_evidence: Evidence from pattern matching
            classification_evidence: Evidence from LLM classification
            multi_intent_evidence: Optional evidence from multiple intent check
            
        Returns:
            Final intent determination
        """
        # Extract intent types from evidence
        pattern_intent = pattern_evidence["most_likely_type"]
        pattern_confidence = pattern_evidence["confidence"]
        
        llm_intent = classification_evidence.get("intent_type", "selection")
        llm_confidence = classification_evidence.get("confidence", 0.8)
        
        # Handle multiple intents case
        if multi_intent_evidence and multi_intent_evidence.get("has_multiple_intents"):
            primary_intent = multi_intent_evidence.get("primary_intent", {})
            multi_intent_type = primary_intent.get("intent_type", llm_intent)
            multi_confidence = primary_intent.get("confidence", 0.7)
            
            # Use the primary intent from multiple intent analysis with appropriate confidence
            return {
                "intent_type": multi_intent_type,
                "subtype": classification_evidence.get("subtype", "multi_intent"),
                "confidence": multi_confidence,
                "explanation": classification_evidence.get("explanation", "Multiple intents detected"),
                "is_multi_intent": True,
                "intents": multi_intent_evidence.get("intents", [])
            }
        
        # Weighted combination of pattern and LLM evidence
        llm_weight = 0.7  # LLM gets higher weight
        pattern_weight = 0.3  # Pattern matching gets lower weight
        
        # If both agree, high confidence
        if llm_intent == pattern_intent:
            return {
                "intent_type": llm_intent,
                "subtype": classification_evidence.get("subtype", "general"),
                "confidence": min(llm_confidence + 0.1, 0.98),  # Boost confidence but cap at 0.98
                "explanation": classification_evidence.get("explanation", ""),
                "agreement": "full"
            }
        
        # If they disagree, weighted decision
        weighted_confidence = (llm_confidence * llm_weight) + (pattern_confidence * pattern_weight)
        
        # Use LLM intent as it's more sophisticated
        return {
            "intent_type": llm_intent,
            "subtype": classification_evidence.get("subtype", "general"),
            "confidence": weighted_confidence * 0.9,  # Reduce confidence due to disagreement
            "explanation": classification_evidence.get("explanation", ""),
            "agreement": "partial",
            "disagreement": {
                "pattern_intent": pattern_intent,
                "llm_intent": llm_intent
            }
        }
    
    async def _generate_alternatives(self, query: str, primary_intent: str) -> List[Dict[str, Any]]:
        """
        Generate alternative intent interpretations.
        
        Args:
            query: Natural language query
            primary_intent: Primary intent type
            
        Returns:
            List of alternative interpretations
        """
        prompt = self.prompt_loader.format_prompt(
            "intent_alternatives", 
            query=query,
            primary_intent=primary_intent
        )
        
        schema = {
            "type": "object",
            "properties": {
                "alternatives": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "intent_type": {"type": "string"},
                            "description": {"type": "string"},
                            "confidence": {"type": "number"},
                            "reason": {"type": "string"}
                        }
                    }
                }
            }
        }
        
        try:
            result = await self.llm_client.generate_structured(prompt, schema)
            return result.get("alternatives", [])
        except Exception as e:
            self.logger.error(f"Error generating alternatives: {e}")
            return []
"""