"""
Reasoning models for the transparent text2sql engine.

These models define the structure for capturing and exposing reasoning
during the query journey.
"""

from datetime import datetime
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field


class ReasoningStep(BaseModel):
    """Individual reasoning step with evidence and confidence."""
    description: str = Field(..., description="Description of this reasoning step")
    evidence: Dict[str, Any] = Field(..., description="Evidence supporting this step")
    confidence: float = Field(..., description="Confidence in this step (0.0-1.0)")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), 
                          description="When this step occurred")


class Alternative(BaseModel):
    """Alternative interpretation for a reasoning conclusion."""
    description: str = Field(..., description="Description of this alternative")
    confidence: float = Field(..., description="Confidence in this alternative (0.0-1.0)")
    reason: str = Field(..., description="Reason for considering this alternative")


class ReasoningStage(BaseModel):
    """A logical stage in the reasoning process."""
    name: str = Field(..., description="Name of this reasoning stage")
    description: str = Field(..., description="Description of what this stage does")
    steps: List[ReasoningStep] = Field(default_factory=list, 
                                     description="Reasoning steps in this stage")
    conclusion: str = Field(default="", description="Conclusion of this stage")
    final_output: Any = Field(default=None, description="Final output from this stage")
    alternatives: List[Alternative] = Field(default_factory=list, 
                                         description="Alternative interpretations")
    completed: bool = Field(default=False, description="Whether this stage is complete")
    
    def add_step(self, description: str, evidence: Dict[str, Any], confidence: float) -> 'ReasoningStage':
        """Add a reasoning step to this stage."""
        self.steps.append(ReasoningStep(
            description=description,
            evidence=evidence,
            confidence=confidence
        ))
        return self
    
    def conclude(self, conclusion: str, final_output: Any, 
               alternatives: Optional[List[Alternative]] = None) -> 'ReasoningStage':
        """Complete this reasoning stage with a conclusion."""
        self.conclusion = conclusion
        self.final_output = final_output
        if alternatives:
            self.alternatives = alternatives
        self.completed = True
        return self


class ReasoningStream(BaseModel):
    """Complete reasoning stream for a query."""
    query_id: str = Field(..., description="Unique ID for this query")
    query_text: str = Field(..., description="Original query text")
    stages: List[ReasoningStage] = Field(default_factory=list, 
                                       description="Reasoning stages")
    active_stage: Optional[ReasoningStage] = Field(default=None, 
                                                description="Currently active stage")
    complete: bool = Field(default=False, description="Whether reasoning is complete")
    user_intervention: bool = Field(default=False, 
                                  description="Whether user has intervened")
    user_feedback: Dict[str, Any] = Field(default_factory=dict, 
                                        description="Feedback from user")
    
    class Config:
        arbitrary_types_allowed = True
    
    def start_stage(self, name: str, description: str) -> ReasoningStage:
        """Start a new reasoning stage."""
        self.active_stage = ReasoningStage(name=name, description=description)
        self.stages.append(self.active_stage)
        return self.active_stage
    
    def add_step(self, description: str, evidence: Dict[str, Any], confidence: float) -> 'ReasoningStream':
        """Add a reasoning step to the active stage."""
        if not self.active_stage:
            raise ValueError("No active reasoning stage")
        
        self.active_stage.add_step(
            description=description,
            evidence=evidence,
            confidence=confidence
        )
        return self
    
    def conclude_stage(self, conclusion: str, final_output: Any, 
                     alternatives: Optional[List[Alternative]] = None) -> 'ReasoningStream':
        """Conclude the active reasoning stage."""
        if not self.active_stage:
            raise ValueError("No active reasoning stage")
        
        self.active_stage.conclude(conclusion, final_output, alternatives)
        self.active_stage = None
        return self
    
    def complete_reasoning(self) -> 'ReasoningStream':
        """Mark the reasoning stream as complete."""
        self.complete = True
        return self
"""