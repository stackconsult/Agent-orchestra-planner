"""
Requirements schema for user needs extraction.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class StackType(str, Enum):
    """Supported technology stacks."""
    PYTHON_FASTAPI = "python_fastapi"
    NODEJS_EXPRESS = "nodejs_express"
    TYPESCRIPT_REACT = "typescript_react"
    MULTI_STACK = "multi_stack"


class Complexity(str, Enum):
    """Project complexity levels."""
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    ENTERPRISE = "enterprise"


class UserRequirements(BaseModel):
    """Complete user requirements document."""
    
    requirements_id: UUID
    session_id: UUID
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Core requirements
    project_description: str
    primary_goal: str
    stack_type: StackType
    complexity: Complexity
    
    # Technical requirements
    technology_stack: Dict[str, Any] = Field(default_factory=dict)
    integrations_required: List[str] = Field(default_factory=list)
    performance_requirements: Dict[str, Any] = Field(default_factory=dict)
    
    # Constraints
    timeline: Optional[str] = None
    budget: Optional[str] = None
    team_size: Optional[int] = None
    must_have_features: List[str] = Field(default_factory=list)
    nice_to_have_features: List[str] = Field(default_factory=list)
    
    # Non-functional requirements
    security_requirements: List[str] = Field(default_factory=list)
    scalability_requirements: List[str] = Field(default_factory=list)
    reliability_requirements: List[str] = Field(default_factory=list)
    observability_requirements: List[str] = Field(default_factory=list)
    
    # Business context
    business_domain: Optional[str] = None
    user_count_estimate: Optional[str] = None
    deployment_environment: Optional[str] = None
    
    # Metadata
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0)
    missing_information: List[str] = Field(default_factory=list)
    assumptions_made: List[str] = Field(default_factory=list)


class QuestionTemplate(BaseModel):
    """Template for questions to ask users."""
    
    question_id: str
    question_text: str
    question_type: str  # open_ended, multiple_choice, yes_no, numeric
    required: bool = True
    follow_up_questions: List[str] = Field(default_factory=list)
    validation_rules: Dict[str, Any] = Field(default_factory=dict)
    examples: List[str] = Field(default_factory=list)


class RequirementsExtractionSession(BaseModel):
    """Session for extracting requirements from user."""
    
    session_id: UUID
    started_at: datetime = Field(default_factory=datetime.utcnow)
    current_question_index: int = 0
    questions_asked: List[str] = Field(default_factory=list)
    answers_received: Dict[str, str] = Field(default_factory=dict)
    
    # State tracking
    is_complete: bool = False
    confidence_threshold: float = 0.8
    current_confidence: float = 0.0
    
    # Strategy
    extraction_strategy: str = "comprehensive"  # comprehensive, quick, focused
    focus_areas: List[str] = Field(default_factory=list)


# Question templates for requirements extraction
REQUIREMENT_QUESTIONS = [
    QuestionTemplate(
        question_id="project_description",
        question_text="Can you describe what you want to build in one or two sentences?",
        question_type="open_ended",
        required=True,
        examples=[
            "I need an agent system to process real estate documents",
            "Build a customer support automation platform",
            "Create a data analysis pipeline for financial reports"
        ]
    ),
    
    QuestionTemplate(
        question_id="primary_goal",
        question_text="What is the primary goal or main problem this system will solve?",
        question_type="open_ended",
        required=True,
        examples=[
            "Automate document processing to save 20 hours/week",
            "Reduce customer response time from 24 hours to 1 hour",
            "Generate financial reports automatically instead of manual work"
        ]
    ),
    
    QuestionTemplate(
        question_id="technology_stack",
        question_text="What technology stack would you prefer to use?",
        question_type="multiple_choice",
        required=True,
        examples=["Python/FastAPI", "Node.js/Express", "TypeScript/React", "Mixed stack"]
    ),
    
    QuestionTemplate(
        question_id="timeline",
        question_text="What is your timeline for this project?",
        question_type="open_ended",
        required=False,
        examples=["2 weeks", "1 month", "3 months", "6 months", "Flexible"]
    ),
    
    QuestionTemplate(
        question_id="budget",
        question_text="Do you have a budget constraint I should consider?",
        question_type="open_ended",
        required=False,
        examples=["$100/month", "$500/month", "$1000/month", "No strict budget"]
    ),
    
    QuestionTemplate(
        question_id="must_haves",
        question_text="What are the absolute must-have features or capabilities?",
        question_type="open_ended",
        required=True,
        examples=[
            "Must integrate with our existing CRM",
            "Must handle PDF documents",
            "Must send email notifications",
            "Must have user authentication"
        ]
    ),
    
    QuestionTemplate(
        question_id="integrations",
        question_text="What external systems or services does this need to integrate with?",
        question_type="open_ended",
        required=False,
        examples=[
            "Database: Postgres, MongoDB",
            "Cloud: AWS, GCP, Azure", 
            "APIs: Stripe, SendGrid, Slack",
            "Authentication: OAuth, SAML"
        ]
    ),
    
    QuestionTemplate(
        question_id="user_scale",
        question_text="How many users do you expect to use this system?",
        question_type="multiple_choice",
        required=False,
        examples=["1-10 users", "10-100 users", "100-1000 users", "1000+ users"]
    ),
    
    QuestionTemplate(
        question_id="security_requirements",
        question_text="Are there any specific security requirements or compliance needs?",
        question_type="open_ended",
        required=False,
        examples=[
            "GDPR compliance",
            "HIPAA compliance",
            "SOC 2 compliance",
            "Data encryption at rest and in transit",
            "Role-based access control"
        ]
    )
]
