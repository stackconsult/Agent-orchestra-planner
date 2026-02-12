"""
Architecture design schemas for agent orchestras.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class AgentType(str, Enum):
    """Types of agents in an orchestra."""
    PLANNER = "planner"
    EXECUTOR = "executor"
    VALIDATOR = "validator"
    MONITOR = "monitor"
    ROUTER = "router"
    STORAGE = "storage"
    NOTIFICATION = "notification"
    API_GATEWAY = "api_gateway"
    DATA_PROCESSOR = "data_processor"
    CUSTOM = "custom"


class CommunicationPattern(str, Enum):
    """Communication patterns between agents."""
    EVENT_DRIVEN = "event_driven"
    REQUEST_RESPONSE = "request_response"
    PUBLISH_SUBSCRIBE = "publish_subscribe"
    PIPELINE = "pipeline"
    FAN_OUT = "fan_out"
    AGGREGATION = "aggregation"


class SafetyMechanism(str, Enum):
    """Safety mechanisms for agent orchestras."""
    APPROVAL_GATE = "approval_gate"
    RATE_LIMIT = "rate_limit"
    CIRCUIT_BREAKER = "circuit_breaker"
    TIMEOUT = "timeout"
    KILL_SWITCH = "kill_switch"
    ROLLBACK = "rollback"
    VALIDATION_CHECK = "validation_check"


class AgentDefinition(BaseModel):
    """Definition of a single agent in the orchestra."""
    
    agent_id: str
    agent_name: str
    agent_type: AgentType
    description: str
    responsibilities: List[str] = Field(default_factory=list)
    
    # Technical specs
    technology_stack: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    mcp_integrations: List[str] = Field(default_factory=list)
    
    # Communication
    input_message_types: List[str] = Field(default_factory=list)
    output_message_types: List[str] = Field(default_factory=list)
    communication_patterns: List[CommunicationPattern] = Field(default_factory=list)
    
    # Configuration
    max_concurrent_tasks: int = 1
    timeout_seconds: int = 30
    retry_attempts: int = 3
    
    # Safety
    safety_mechanisms: List[SafetyMechanism] = Field(default_factory=list)
    requires_approval_for: List[str] = Field(default_factory=list)


class MessageFlow(BaseModel):
    """Message flow between agents."""
    
    flow_id: str
    from_agent: str
    to_agent: str
    message_type: str
    communication_pattern: CommunicationPattern
    conditions: List[str] = Field(default_factory=list)
    transformations: List[str] = Field(default_factory=list)
    
    # Performance
    expected_frequency: Optional[str] = None
    latency_requirement_ms: Optional[int] = None
    reliability_requirement: Optional[float] = None


class OrchestraDesign(BaseModel):
    """Complete architecture design for an agent orchestra."""
    
    design_id: UUID
    session_id: UUID
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Basic info
    orchestra_name: str
    orchestra_description: str
    primary_goal: str
    
    # Architecture
    agents: Dict[str, AgentDefinition] = Field(default_factory=dict)
    message_flows: List[MessageFlow] = Field(default_factory=list)
    coordination_protocol: Dict[str, Any] = Field(default_factory=dict)
    
    # Infrastructure
    message_bus_type: str = "redis"
    database_type: str = "postgres"
    deployment_pattern: str = "microservices"
    
    # Safety & reliability
    safety_mechanisms: List[SafetyMechanism] = Field(default_factory=dict)
    error_handling_strategy: Dict[str, Any] = Field(default_factory=dict)
    monitoring_setup: Dict[str, Any] = Field(default_factory=dict)
    
    # Performance
    expected_load: Dict[str, Any] = Field(default_factory=dict)
    scalability_plan: Dict[str, Any] = Field(default_factory=dict)
    
    # Integration
    external_integrations: List[Dict[str, Any]] = Field(default_factory=list)
    api_endpoints: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Metadata
    design_patterns_used: List[str] = Field(default_factory=list)
    best_practices_applied: List[str] = Field(default_factory=list)
    risks_identified: List[str] = Field(default_factory=list)
    assumptions_made: List[str] = Field(default_factory=list)


class ValidationRule(BaseModel):
    """Rule for validating an orchestra design."""
    
    rule_id: str
    rule_name: str
    rule_type: str  # safety, performance, architecture, security
    description: str
    validation_logic: str  # Could be a function reference or expression
    severity: str  # error, warning, info
    
    # Conditions
    applies_when: List[str] = Field(default_factory=list)
    does_not_apply_when: List[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    """Result of validating an orchestra design."""
    
    validation_id: UUID
    design_id: UUID
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Results
    passed_rules: List[str] = Field(default_factory=list)
    failed_rules: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    
    # Details
    rule_results: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    recommendations: List[str] = Field(default_factory=list)
    blocking_issues: List[str] = Field(default_factory=list)
    
    # Overall assessment
    overall_score: float = Field(default=0.0, ge=0.0, le=1.0)
    is_production_ready: bool = False
    requires_changes: bool = False


# Predefined validation rules
ORCHESTRA_VALIDATION_RULES = [
    ValidationRule(
        rule_id="safety_approval_gates",
        rule_name="Production Approval Gates Required",
        rule_type="safety",
        description="Production deployments must have approval gates for critical operations",
        validation_logic="len([s for s in design.safety_mechanisms if s == SafetyMechanism.APPROVAL_GATE]) > 0",
        severity="error",
        applies_when=["deployment_target == 'production'"]
    ),
    
    ValidationRule(
        rule_id="agent_count_limit",
        rule_name="Agent Count Complexity Check",
        rule_type="architecture",
        description="Check if agent count is appropriate for complexity",
        validation_logic="len(design.agents) <= 10",
        severity="warning",
        applies_when=["design.complexity in ['simple', 'moderate']"]
    ),
    
    ValidationRule(
        rule_id="message_bus_redundancy",
        rule_name="Message Bus Redundancy",
        rule_type="reliability",
        description="Production systems should have redundant message bus",
        validation_logic="design.message_bus_redundancy == True",
        severity="warning",
        applies_when=["deployment_target == 'production'"]
    ),
    
    ValidationRule(
        rule_id="observability_coverage",
        rule_name="Observability Coverage",
        rule_type="monitoring",
        description="All agents should have proper observability",
        validation_logic="all(agent.monitoring_setup for agent in design.agents.values())",
        severity="error"
    ),
    
    ValidationRule(
        rule_id="timeout_configuration",
        rule_name="Timeout Configuration",
        rule_type="reliability",
        description="All agents should have timeout configuration",
        validation_logic="all(agent.timeout_seconds > 0 for agent in design.agents.values())",
        severity="error"
    ),
    
    ValidationRule(
        rule_id="retry_logic",
        rule_name="Retry Logic Configuration",
        rule_type="reliability",
        description="External service calls should have retry logic",
        validation_logic="all(agent.retry_attempts >= 1 for agent in design.agents.values() if agent.mcp_integrations)",
        severity="warning"
    )
]
