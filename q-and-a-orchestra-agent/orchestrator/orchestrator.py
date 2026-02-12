"""
Main Orchestrator - Coordinates all agents and manages the overall workflow.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from anthropic import AsyncAnthropic

from agents.repository_analyzer import RepositoryAnalyzerAgent
from agents.requirements_extractor import RequirementsExtractorAgent
from agents.architecture_designer import ArchitectureDesignerAgent
from agents.implementation_planner import ImplementationPlannerAgent
from agents.validator import ValidatorAgent

from orchestrator.message_bus import MessageBus
from orchestrator.router import MessageRouter
from orchestrator.context_manager import ContextManager, ConversationMemory

from schemas.messages import AgentMessage, MessageType

logger = logging.getLogger(__name__)


class OrchestraOrchestrator:
    """Main orchestrator for the Q&A Orchestra Agent system."""
    
    def __init__(self, anthropic_client: AsyncAnthropic, redis_url: str = "redis://localhost:6379/0"):
        self.anthropic = anthropic_client
        
        # Core components
        self.message_bus = MessageBus(redis_url)
        self.router = MessageRouter(self.message_bus)
        self.context_manager = ContextManager()
        self.conversation_memory = ConversationMemory(self.context_manager)
        
        # Agents
        self.repository_analyzer = RepositoryAnalyzerAgent(anthropic_client, self)
        self.requirements_extractor = RequirementsExtractorAgent(anthropic_client)
        self.architecture_designer = ArchitectureDesignerAgent(anthropic_client)
        self.implementation_planner = ImplementationPlannerAgent(anthropic_client)
        self.validator = ValidatorAgent(anthropic_client)
        
        # System state
        self.is_running = False
        self.active_sessions: Dict[UUID, Dict[str, Any]] = {}
        
        # Register agents with router
        self._register_agents()
    
    async def start(self) -> None:
        """Start the orchestrator and all components."""
        try:
            # Connect message bus
            await self.message_bus.connect()
            
            # Set up message subscriptions
            await self._setup_message_subscriptions()
            
            # Register agents
            await self._register_agents_with_router()
            
            self.is_running = True
            logger.info("Orchestrator started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start orchestrator: {str(e)}")
            raise
    
    async def stop(self) -> None:
        """Stop the orchestrator and all components."""
        try:
            self.is_running = False
            
            # Disconnect message bus
            await self.message_bus.disconnect()
            
            logger.info("Orchestrator stopped")
            
        except Exception as e:
            logger.error(f"Error stopping orchestrator: {str(e)}")
    
    async def start_design_session(self, user_id: Optional[str] = None, metadata: Dict[str, Any] = None) -> UUID:
        """
        Start a new design session.
        
        Args:
            user_id: Optional user identifier
            metadata: Optional session metadata
            
        Returns:
            Session ID
        """
        if not self.is_running:
            raise RuntimeError("Orchestrator not running")
        
        session_id = await self.context_manager.create_session(user_id, metadata)
        
        # Track active session
        self.active_sessions[session_id] = {
            "started_at": asyncio.get_event_loop().time(),
            "user_id": user_id,
            "status": "active"
        }
        
        # Start the requirements extraction process
        await self._start_requirements_extraction(session_id)
        
        logger.info(f"Started design session {session_id}")
        return session_id
    
    async def process_user_input(self, session_id: UUID, user_input: str, input_type: str = "text") -> Dict[str, Any]:
        """
        Process user input in a session.
        
        Args:
            session_id: Session ID
            user_input: User's input
            input_type: Type of input
            
        Returns:
            Response data
        """
        if not self.is_running:
            raise RuntimeError("Orchestrator not running")
        
        if session_id not in self.active_sessions:
            raise ValueError(f"Session {session_id} not found")
        
        # Store user input
        await self.context_manager.add_user_input(session_id, user_input, input_type)
        
        # Get current session phase
        session = await self.context_manager.get_session(session_id)
        current_phase = session["current_phase"]
        
        # Route input based on current phase
        if current_phase == "requirements_extraction":
            return await self._handle_requirements_input(session_id, user_input)
        elif current_phase == "design_refinement":
            return await self._handle_design_refinement(session_id, user_input)
        elif current_phase == "implementation_planning":
            return await self._handle_implementation_input(session_id, user_input)
        else:
            return await self._handle_general_input(session_id, user_input)
    
    async def request_refinement(self, session_id: UUID, refinement_type: str, description: str) -> Dict[str, Any]:
        """
        Request a refinement to the current design.
        
        Args:
            session_id: Session ID
            refinement_type: Type of refinement
            description: Description of the refinement
            
        Returns:
            Response data
        """
        if not self.is_running:
            raise RuntimeError("Orchestrator not running")
        
        # Store refinement request
        await self.context_manager.add_refinement_request(session_id, refinement_type, description)
        
        # Update session phase
        await self.context_manager.update_session_phase(session_id, "design_refinement")
        
        # Route to architecture designer
        message = AgentMessage(
            correlation_id=UUID(),
            agent_id="orchestrator",
            intent="refine_design",
            message_type=MessageType.DESIGN_COMPLETED,
            payload={
                "refinements": [description],
                "design": (await self.context_manager.get_session(session_id))["context"]["orchestra_design"]
            },
            session_id=session_id
        )
        
        # Route message
        await self.router.route_message(message)
        
        return {"status": "refinement_requested", "refinement_type": refinement_type}
    
    async def get_session_status(self, session_id: UUID) -> Dict[str, Any]:
        """
        Get the status of a session.
        
        Args:
            session_id: Session ID
            
        Returns:
            Session status
        """
        if session_id not in self.active_sessions:
            return {"error": "Session not found"}
        
        session = await self.context_manager.get_session(session_id)
        summary = await self.context_manager.get_conversation_summary(session_id)
        
        return {
            "session_id": session_id,
            "status": session["status"],
            "current_phase": session["current_phase"],
            "summary": summary,
            "context_available": {
                "requirements": session["context"]["requirements"] is not None,
                "repository_analysis": session["context"]["repository_analysis"] is not None,
                "orchestra_design": session["context"]["orchestra_design"] is not None,
                "implementation_plan": session["context"]["implementation_plan"] is not None,
                "validation_results": session["context"]["validation_results"] is not None
            }
        }
    
    async def get_conversation_history(self, session_id: UUID, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get conversation history for a session.
        
        Args:
            session_id: Session ID
            limit: Maximum number of entries to return
            
        Returns:
            Conversation history
        """
        return await self.context_manager.get_conversation_history(session_id, limit)
    
    async def end_session(self, session_id: UUID) -> Dict[str, Any]:
        """
        End a session and perform cleanup.
        
        Args:
            session_id: Session ID
            
        Returns:
            Session summary
        """
        if session_id not in self.active_sessions:
            return {"error": "Session not found"}
        
        # Learn from session
        await self.conversation_memory.learn_from_session(session_id)
        
        # Get final summary
        summary = await self.context_manager.get_conversation_summary(session_id)
        
        # Clean up session
        del self.active_sessions[session_id]
        
        logger.info(f"Ended session {session_id}")
        return {"status": "session_ended", "summary": summary}
    
    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get overall system status.
        
        Returns:
            System status information
        """
        bus_stats = await self.message_bus.get_bus_stats()
        agent_status = await self.router.get_agent_status()
        session_stats = await self.context_manager.get_session_statistics()
        
        return {
            "orchestrator": {
                "is_running": self.is_running,
                "active_sessions": len(self.active_sessions)
            },
            "message_bus": bus_stats,
            "agents": agent_status,
            "sessions": session_stats
        }
    
    async def _register_agents(self) -> None:
        """Register agents with their configurations."""
        # This would be called during initialization
        pass
    
    async def _setup_message_subscriptions(self) -> None:
        """Set up message subscriptions for the orchestrator."""
        
        # Subscribe to all message types for monitoring
        for message_type in MessageType:
            await self.message_bus.subscribe_to_message_type(
                message_type, 
                self._handle_agent_message
            )
    
    async def _register_agents_with_router(self) -> None:
        """Register agents with the message router."""
        
        # Register each agent with its capabilities
        await self.router.register_agent("repository_analyzer", {
            "message_types": ["repo_analysis_requested", "repo_analysis_completed"],
            "capabilities": ["pattern_extraction", "architecture_analysis"],
            "max_concurrent_tasks": 3
        })
        
        await self.router.register_agent("requirements_extractor", {
            "message_types": ["question_asked", "question_answered", "requirements_extracted"],
            "capabilities": ["conversational_qa", "requirements_analysis"],
            "max_concurrent_tasks": 5
        })
        
        await self.router.register_agent("architecture_designer", {
            "message_types": ["design_requested", "design_completed"],
            "capabilities": ["orchestra_design", "agent_topology"],
            "max_concurrent_tasks": 2
        })
        
        await self.router.register_agent("implementation_planner", {
            "message_types": ["plan_requested", "plan_completed"],
            "capabilities": ["implementation_planning", "cost_estimation"],
            "max_concurrent_tasks": 2
        })
        
        await self.router.register_agent("validator", {
            "message_types": ["validation_requested", "validation_completed"],
            "capabilities": ["design_validation", "safety_checking"],
            "max_concurrent_tasks": 3
        })
    
    async def _start_requirements_extraction(self, session_id: UUID) -> None:
        """Start the requirements extraction process."""
        
        message = AgentMessage(
            correlation_id=UUID(),
            agent_id="orchestrator",
            intent="start_requirements_extraction",
            message_type=MessageType.QUESTION_ASKED,
            payload={"strategy": "comprehensive"},
            session_id=session_id
        )
        
        await self.router.route_message(message)
    
    async def _handle_requirements_input(self, session_id: UUID, user_input: str) -> Dict[str, Any]:
        """Handle user input during requirements extraction."""
        
        message = AgentMessage(
            correlation_id=UUID(),
            agent_id="orchestrator",
            intent="process_answer",
            message_type=MessageType.QUESTION_ANSWERED,
            payload={"answer": user_input},
            session_id=session_id
        )
        
        await self.router.route_message(message)
        
        return {"status": "processing_requirements"}
    
    async def _handle_design_refinement(self, session_id: UUID, user_input: str) -> Dict[str, Any]:
        """Handle user input during design refinement."""
        
        # Parse refinement request
        refinement_type = "custom"
        if "error handling" in user_input.lower():
            refinement_type = "add_error_handling"
        elif "monitoring" in user_input.lower():
            refinement_type = "add_monitoring"
        elif "security" in user_input.lower():
            refinement_type = "add_security"
        
        return await self.request_refinement(session_id, refinement_type, user_input)
    
    async def _handle_implementation_input(self, session_id: UUID, user_input: str) -> Dict[str, Any]:
        """Handle user input during implementation planning."""
        
        return {"status": "implementation_planning", "message": "Implementation planning in progress"}
    
    async def _handle_general_input(self, session_id: UUID, user_input: str) -> Dict[str, Any]:
        """Handle general user input."""
        
        return {"status": "processing", "message": "Processing your input"}
    
    async def _handle_agent_message(self, message: AgentMessage) -> None:
        """Handle messages from agents."""
        
        try:
            # Store agent response
            await self.context_manager.add_agent_response(
                message.session_id,
                message.agent_id,
                message.payload.get("response", ""),
                "agent_response",
                message.payload
            )
            
            # Update session phase based on message type
            if message.message_type == MessageType.REQUIREMENTS_EXTRACTED:
                await self.context_manager.update_session_phase(message.session_id, "repository_analysis")
                await self._start_repository_analysis(message.session_id)
            elif message.message_type == MessageType.REPO_ANALYSIS_COMPLETED:
                await self.context_manager.update_session_phase(message.session_id, "architecture_design")
                await self._start_architecture_design(message.session_id)
            elif message.message_type == MessageType.DESIGN_COMPLETED:
                await self.context_manager.update_session_phase(message.session_id, "implementation_planning")
                await self._start_implementation_planning(message.session_id)
            elif message.message_type == MessageType.PLAN_COMPLETED:
                await self.context_manager.update_session_phase(message.session_id, "validation")
                await self._start_validation(message.session_id)
            elif message.message_type == MessageType.VALIDATION_COMPLETED:
                await self.context_manager.update_session_phase(message.session_id, "complete")
            elif message.message_type == MessageType.ERROR_OCCURRED:
                logger.error(f"Agent error: {message.payload.get('error_message')}")
            
        except Exception as e:
            logger.error(f"Error handling agent message: {str(e)}")
    
    async def _start_repository_analysis(self, session_id: UUID) -> None:
        """Start repository analysis."""
        
        message = AgentMessage(
            correlation_id=UUID(),
            agent_id="orchestrator",
            intent="analyze_repository",
            message_type=MessageType.REPO_ANALYSIS_REQUESTED,
            payload={},
            session_id=session_id
        )
        
        await self.router.route_message(message)
    
    async def _start_architecture_design(self, session_id: UUID) -> None:
        """Start architecture design."""
        
        session = await self.context_manager.get_session(session_id)
        requirements = session["context"]["requirements"]
        patterns = session["context"]["repository_analysis"]
        
        message = AgentMessage(
            correlation_id=UUID(),
            agent_id="orchestrator",
            intent="design_orchestra",
            message_type=MessageType.DESIGN_REQUESTED,
            payload={
                "requirements": requirements,
                "patterns": patterns
            },
            session_id=session_id
        )
        
        await self.router.route_message(message)
    
    async def _start_implementation_planning(self, session_id: UUID) -> None:
        """Start implementation planning."""
        
        session = await self.context_manager.get_session(session_id)
        design = session["context"]["orchestra_design"]
        
        message = AgentMessage(
            correlation_id=UUID(),
            agent_id="orchestrator",
            intent="create_implementation_plan",
            message_type=MessageType.PLAN_REQUESTED,
            payload={"design": design},
            session_id=session_id
        )
        
        await self.router.route_message(message)
    
    async def _start_validation(self, session_id: UUID) -> None:
        """Start validation."""
        
        session = await self.context_manager.get_session(session_id)
        design = session["context"]["orchestra_design"]
        
        message = AgentMessage(
            correlation_id=UUID(),
            agent_id="orchestrator",
            intent="validate_design",
            message_type=MessageType.VALIDATION_REQUESTED,
            payload={"design": design},
            session_id=session_id
        )
        
        await self.router.route_message(message)
