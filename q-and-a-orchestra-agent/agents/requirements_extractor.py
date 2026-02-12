"""
Requirements Extraction Agent - Conducts conversational Q&A to extract user requirements.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from anthropic import AsyncAnthropic
from schemas.messages import AgentMessage, MessageType, RequirementsPayload
from schemas.requirements import (
    UserRequirements, RequirementsExtractionSession, 
    REQUIREMENT_QUESTIONS, QuestionTemplate, StackType, Complexity
)

logger = logging.getLogger(__name__)


class RequirementsExtractorAgent:
    """Extracts requirements from users through conversational Q&A."""
    
    def __init__(self, anthropic_client: AsyncAnthropic):
        self.anthropic = anthropic_client
        self.agent_id = "requirements_extractor"
        
        # Active sessions
        self._active_sessions: Dict[UUID, RequirementsExtractionSession] = {}
        
        # Question templates
        self._question_templates = {q.question_id: q for q in REQUIREMENT_QUESTIONS}
    
    async def start_extraction_session(self, message: AgentMessage) -> AgentMessage:
        """
        Start a new requirements extraction session.
        
        Args:
            message: Message to start the session
            
        Returns:
            Message with first question
        """
        try:
            session_id = message.session_id or UUID()
            
            # Create new session
            session = RequirementsExtractionSession(
                session_id=session_id,
                extraction_strategy=message.payload.get("strategy", "comprehensive"),
                focus_areas=message.payload.get("focus_areas", [])
            )
            
            self._active_sessions[session_id] = session
            
            # Get first question
            first_question = await self._get_next_question(session)
            
            # Update session
            session.questions_asked.append(first_question.question_id)
            session.current_question_index += 1
            
            # Create response
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="question_asked",
                message_type=MessageType.QUESTION_ASKED,
                payload={
                    "question_id": first_question.question_id,
                    "question_text": first_question.question_text,
                    "question_type": first_question.question_type,
                    "examples": first_question.examples,
                    "session_progress": {
                        "current_question": session.current_question_index,
                        "total_questions": len(self._question_templates),
                        "completion_percentage": (session.current_question_index / len(self._question_templates)) * 100
                    }
                },
                session_id=session_id
            )
            
            logger.info(f"Started requirements extraction session: {session_id}")
            return response_message
            
        except Exception as e:
            logger.error(f"Failed to start extraction session: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def process_answer(self, message: AgentMessage) -> AgentMessage:
        """
        Process user's answer and ask next question or complete extraction.
        
        Args:
            message: Message containing user's answer
            
        Returns:
            Message with next question or completed requirements
        """
        try:
            session_id = message.session_id
            if session_id not in self._active_sessions:
                raise ValueError(f"Session {session_id} not found")
            
            session = self._active_sessions[session_id]
            
            # Extract answer data
            question_id = message.payload.get("question_id")
            answer = message.payload.get("answer")
            
            # Store answer
            session.answers_received[question_id] = answer
            
            # Check if extraction is complete
            if await self._is_extraction_complete(session):
                requirements = await self._generate_requirements_document(session)
                
                # Clean up session
                del self._active_sessions[session_id]
                
                response_message = AgentMessage(
                    correlation_id=message.correlation_id,
                    agent_id=self.agent_id,
                    intent="requirements_extracted",
                    message_type=MessageType.REQUIREMENTS_EXTRACTED,
                    payload=requirements.dict(),
                    session_id=session_id
                )
                
                logger.info(f"Requirements extraction completed for session: {session_id}")
                return response_message
            
            # Get next question
            next_question = await self._get_next_question(session)
            if not next_question:
                # No more questions, complete extraction
                requirements = await self._generate_requirements_document(session)
                del self._active_sessions[session_id]
                
                return AgentMessage(
                    correlation_id=message.correlation_id,
                    agent_id=self.agent_id,
                    intent="requirements_extracted",
                    message_type=MessageType.REQUIREMENTS_EXTRACTED,
                    payload=requirements.dict(),
                    session_id=session_id
                )
            
            # Update session
            session.questions_asked.append(next_question.question_id)
            session.current_question_index += 1
            
            # Create response with next question
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="question_asked",
                message_type=MessageType.QUESTION_ASKED,
                payload={
                    "question_id": next_question.question_id,
                    "question_text": next_question.question_text,
                    "question_type": next_question.question_type,
                    "examples": next_question.examples,
                    "session_progress": {
                        "current_question": session.current_question_index,
                        "total_questions": len(self._question_templates),
                        "completion_percentage": (session.current_question_index / len(self._question_templates)) * 100
                    }
                },
                session_id=session_id
            )
            
            return response_message
            
        except Exception as e:
            logger.error(f"Failed to process answer: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def clarify_requirements(self, message: AgentMessage) -> AgentMessage:
        """
        Ask clarifying questions based on existing requirements.
        
        Args:
            message: Message requesting clarification
            
        Returns:
            Message with clarifying questions
        """
        try:
            requirements = message.payload.get("requirements", {})
            unclear_areas = message.payload.get("unclear_areas", [])
            
            clarifying_questions = await self._generate_clarifying_questions(
                requirements, unclear_areas
            )
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="clarification_questions",
                message_type=MessageType.QUESTION_ASKED,
                payload={
                    "clarifying_questions": clarifying_questions,
                    "purpose": "clarify_ambiguous_requirements"
                },
                session_id=message.session_id
            )
            
            return response_message
            
        except Exception as e:
            logger.error(f"Failed to generate clarifying questions: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def _get_next_question(self, session: RequirementsExtractionSession) -> Optional[QuestionTemplate]:
        """Get the next question to ask based on session state."""
        
        # Get questions that haven't been asked yet
        remaining_questions = [
            q for q_id, q in self._question_templates.items()
            if q_id not in session.questions_asked
        ]
        
        if not remaining_questions:
            return None
        
        # Prioritize required questions first
        required_questions = [q for q in remaining_questions if q.required]
        if required_questions:
            return required_questions[0]
        
        # Return first optional question
        return remaining_questions[0] if remaining_questions else None
    
    async def _is_extraction_complete(self, session: RequirementsExtractionSession) -> bool:
        """Check if requirements extraction is complete."""
        
        # Check if all required questions have been answered
        required_questions = [q for q in self._question_templates.values() if q.required]
        answered_required = [
            q for q in required_questions 
            if q.question_id in session.answers_received
        ]
        
        if len(answered_required) < len(required_questions):
            return False
        
        # Check confidence threshold
        confidence = await self._calculate_confidence_score(session)
        session.current_confidence = confidence
        
        return confidence >= session.confidence_threshold
    
    async def _calculate_confidence_score(self, session: RequirementsExtractionSession) -> float:
        """Calculate confidence score based on answers received."""
        
        score = 0.0
        total_weight = 0.0
        
        # Weight required questions higher
        for question in self._question_templates.values():
            if question.question_id in session.answers_received:
                answer = session.answers_received[question.question_id]
                # Simple heuristic: longer, detailed answers get higher scores
                answer_quality = min(len(answer) / 100, 1.0)  # Normalize to 0-1
                weight = 2.0 if question.required else 1.0
                
                score += answer_quality * weight
                total_weight += weight
        
        return score / total_weight if total_weight > 0 else 0.0
    
    async def _generate_requirements_document(self, session: RequirementsExtractionSession) -> UserRequirements:
        """Generate a structured requirements document from session data."""
        
        # Use Claude to analyze and structure the answers
        answers_text = "\n".join([
            f"Q{qid}: {answer}" 
            for qid, answer in session.answers_received.items()
        ])
        
        prompt = f"""
        Analyze these Q&A answers and create a structured requirements document:
        
        {answers_text}
        
        Extract and structure:
        1. Project description and primary goal
        2. Technology stack preferences
        3. Timeline and budget constraints
        4. Must-have features
        5. Nice-to-have features
        6. Integration requirements
        7. Security requirements
        8. Scalability needs
        9. Any assumptions or missing information
        
        Return a JSON object with clear, structured information.
        """
        
        try:
            response = await self.anthropic.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse Claude's response and create requirements
            # In production, implement proper JSON parsing
            structured_data = {"analysis": response.content[0].text}
            
            # Create requirements document
            requirements = UserRequirements(
                requirements_id=UUID(),
                session_id=session.session_id,
                project_description=session.answers_received.get("project_description", ""),
                primary_goal=session.answers_received.get("primary_goal", ""),
                stack_type=self._infer_stack_type(session.answers_received),
                complexity=self._infer_complexity(session.answers_received),
                technology_stack=structured_data.get("technology_stack", {}),
                timeline=session.answers_received.get("timeline"),
                budget=session.answers_received.get("budget"),
                must_have_features=self._parse_list_field(session.answers_received.get("must_haves", "")),
                nice_to_have_features=self._parse_list_field(session.answers_received.get("nice_to_haves", "")),
                assumptions_made=structured_data.get("assumptions", []),
                missing_information=structured_data.get("missing_info", [])
            )
            
            return requirements
            
        except Exception as e:
            logger.error(f"Failed to generate requirements document: {str(e)}")
            # Fallback to basic requirements
            return UserRequirements(
                requirements_id=UUID(),
                session_id=session.session_id,
                project_description=session.answers_received.get("project_description", ""),
                primary_goal=session.answers_received.get("primary_goal", ""),
                stack_type=StackType.MULTI_STACK,
                complexity=Complexity.MODERATE
            )
    
    async def _generate_clarifying_questions(self, requirements: Dict[str, Any], unclear_areas: List[str]) -> List[str]:
        """Generate clarifying questions for ambiguous requirements."""
        
        prompt = f"""
        Based on these requirements and unclear areas, generate 3-5 clarifying questions:
        
        Requirements: {requirements}
        Unclear areas: {unclear_areas}
        
        Generate specific, actionable questions that will help clarify the requirements.
        Focus on technical details, constraints, and specific use cases.
        """
        
        try:
            response = await self.anthropic.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse questions from response
            content = response.content[0].text
            # Simple parsing - in production, use more sophisticated parsing
            questions = [q.strip() for q in content.split('\n') if q.strip() and '?' in q]
            return questions[:5]  # Limit to 5 questions
            
        except Exception as e:
            logger.error(f"Failed to generate clarifying questions: {str(e)}")
            return ["Could you provide more details about your requirements?"]
    
    def _infer_stack_type(self, answers: Dict[str, str]) -> StackType:
        """Infer technology stack from answers."""
        tech_answer = answers.get("technology_stack", "").lower()
        
        if "python" in tech_answer or "fastapi" in tech_answer:
            return StackType.PYTHON_FASTAPI
        elif "node" in tech_answer or "javascript" in tech_answer:
            return StackType.NODEJS_EXPRESS
        elif "typescript" in tech_answer or "react" in tech_answer:
            return StackType.TYPESCRIPT_REACT
        else:
            return StackType.MULTI_STACK
    
    def _infer_complexity(self, answers: Dict[str, str]) -> Complexity:
        """Infer project complexity from answers."""
        description = answers.get("project_description", "").lower()
        features = answers.get("must_haves", "").lower()
        
        # Simple heuristic based on keywords
        complexity_indicators = [
            "enterprise", "scalable", "multi-tenant", "microservices",
            "complex", "advanced", "multiple systems"
        ]
        
        indicator_count = sum(1 for indicator in complexity_indicators if indicator in description + features)
        
        if indicator_count >= 3:
            return Complexity.ENTERPRISE
        elif indicator_count >= 2:
            return Complexity.COMPLEX
        elif indicator_count >= 1:
            return Complexity.MODERATE
        else:
            return Complexity.SIMPLE
    
    def _parse_list_field(self, field_value: str) -> List[str]:
        """Parse a comma-separated or bullet-point list."""
        if not field_value:
            return []
        
        # Split by common delimiters
        items = []
        for delimiter in [',', ';', '\n']:
            if delimiter in field_value:
                items = [item.strip() for item in field_value.split(delimiter)]
                break
        
        # Clean up items
        cleaned_items = []
        for item in items:
            # Remove bullet points and numbering
            item = item.lstrip('-•*1234567890. ')
            if item:
                cleaned_items.append(item)
        
        return cleaned_items
    
    def _create_error_message(self, original_message: AgentMessage, error: str) -> AgentMessage:
        """Create an error message."""
        return AgentMessage(
            correlation_id=original_message.correlation_id,
            agent_id=self.agent_id,
            intent="error_occurred",
            message_type=MessageType.ERROR_OCCURRED,
            payload={
                "error_type": "RequirementsExtractionError",
                "error_message": error,
                "context": {"agent": self.agent_id}
            },
            session_id=original_message.session_id
        )
