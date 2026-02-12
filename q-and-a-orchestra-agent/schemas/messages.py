"""
Message schemas for agent communication.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class MessageType(str, Enum):
    """Types of messages between agents."""
    
    # Requirements extraction
    QUESTION_ASKED = "question_asked"
    QUESTION_ANSWERED = "question_answered"
    REQUIREMENTS_EXTRACTED = "requirements_extracted"
    
    # Repository analysis
    REPO_ANALYSIS_REQUESTED = "repo_analysis_requested"
    REPO_ANALYSIS_COMPLETED = "repo_analysis_completed"
    PATTERNS_IDENTIFIED = "patterns_identified"
    
    # Architecture design
    DESIGN_REQUESTED = "design_requested"
    DESIGN_COMPLETED = "design_completed"
    AGENT_TOPOLOGY_DESIGNED = "agent_topology_designed"
    
    # Implementation planning
    PLAN_REQUESTED = "plan_requested"
    PLAN_COMPLETED = "plan_completed"
    COST_ESTIMATE_GENERATED = "cost_estimate_generated"
    
    # Validation
    VALIDATION_REQUESTED = "validation_requested"
    VALIDATION_COMPLETED = "validation_completed"
    SAFETY_CHECK_COMPLETED = "safety_check_completed"
    
    # System
    ERROR_OCCURRED = "error_occurred"
    SESSION_STARTED = "session_started"
    SESSION_ENDED = "session_ended"


class Priority(int, Enum):
    """Message priority levels."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class AgentMessage(BaseModel):
    """Base message schema for agent communication."""
    
    message_id: UUID = Field(default_factory=uuid4)
    correlation_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    agent_id: str
    intent: str
    message_type: MessageType
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.MEDIUM
    requires_approval: bool = False
    session_id: Optional[UUID] = None
    
    class Config:
        use_enum_values = True


class RequirementsPayload(BaseModel):
    """Payload for requirements extraction messages."""
    
    questions_asked: List[str] = Field(default_factory=list)
    answers_received: Dict[str, str] = Field(default_factory=dict)
    stack_requirements: Dict[str, Any] = Field(default_factory=dict)
    constraints: Dict[str, Any] = Field(default_factory=dict)
    must_haves: List[str] = Field(default_factory=list)
    nice_to_haves: List[str] = Field(default_factory=list)
    timeline: Optional[str] = None
    budget: Optional[str] = None


class RepositoryAnalysisPayload(BaseModel):
    """Payload for repository analysis messages."""
    
    repo_files: List[str] = Field(default_factory=list)
    patterns_identified: Dict[str, Any] = Field(default_factory=dict)
    architecture_principles: List[str] = Field(default_factory=list)
    best_practices: List[str] = Field(default_factory=list)
    technology_stack: Dict[str, Any] = Field(default_factory=dict)


class DesignPayload(BaseModel):
    """Payload for architecture design messages."""
    
    agent_count: int
    agent_roles: Dict[str, str] = Field(default_factory=dict)
    message_flow: List[Dict[str, Any]] = Field(default_factory=list)
    coordination_protocol: Dict[str, Any] = Field(default_factory=dict)
    mcp_integrations: List[str] = Field(default_factory=list)
    safety_mechanisms: List[str] = Field(default_factory=dict)
    observability_setup: Dict[str, Any] = Field(default_factory=dict)


class ImplementationPlanPayload(BaseModel):
    """Payload for implementation planning messages."""
    
    phases: List[Dict[str, Any]] = Field(default_factory=list)
    file_structure: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    timeline_estimate: Optional[str] = None
    cost_estimate: Dict[str, Any] = Field(default_factory=dict)
    resource_requirements: Dict[str, Any] = Field(default_factory=dict)
    risks: List[str] = Field(default_factory=list)


class ValidationPayload(BaseModel):
    """Payload for validation messages."""
    
    validation_results: Dict[str, bool] = Field(default_factory=dict)
    safety_check_results: Dict[str, bool] = Field(default_factory=dict)
    best_practices_check: Dict[str, bool] = Field(default_factory=dict)
    recommendations: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class ErrorPayload(BaseModel):
    """Payload for error messages."""
    
    error_type: str
    error_message: str
    stack_trace: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    retry_possible: bool = False


class SessionPayload(BaseModel):
    """Payload for session management messages."""
    
    session_id: UUID
    user_id: Optional[str] = None
    session_type: str = "design"
    metadata: Dict[str, Any] = Field(default_factory=dict)
