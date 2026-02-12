"""
Integration tests for the Q&A Orchestra Agent system.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from orchestrator.orchestrator import OrchestraOrchestrator
from orchestrator.message_bus import MessageBus
from orchestrator.router import MessageRouter
from orchestrator.context_manager import ContextManager
from schemas.messages import AgentMessage, MessageType, Priority


@pytest.fixture
async def mock_orchestrator():
    """Mock orchestrator for testing."""
    with patch('orchestrator.orchestrator.AsyncAnthropic') as mock_anthropic:
        mock_client = AsyncMock()
        mock_anthropic.return_value = mock_client
        
        # Mock Claude responses
        mock_client.messages.create.return_value = AsyncMock(
            content=[AsyncMock(text="mock response")]
        )
        
        orchestrator = OrchestraOrchestrator(mock_client, "redis://localhost:6379/1")
        
        # Mock message bus connection
        with patch.object(orchestrator.message_bus, 'connect'):
            await orchestrator.start()
        
        yield orchestrator
        
        await orchestrator.stop()


@pytest.mark.integration
class TestOrchestratorIntegration:
    """Integration tests for the orchestrator."""
    
    @pytest.mark.asyncio
    async def test_full_design_session(self, mock_orchestrator):
        """Test complete design session workflow."""
        # Start a session
        session_id = await mock_orchestrator.start_design_session(
            user_id="test_user",
            metadata={"test": True}
        )
        
        assert session_id is not None
        assert session_id in mock_orchestrator.active_sessions
        
        # Process user input
        response = await mock_orchestrator.process_user_input(
            session_id=session_id,
            user_input="I need to build a document processing system",
            input_type="text"
        )
        
        assert response["status"] == "processing_requirements"
        
        # Get session status
        status = await mock_orchestrator.get_session_status(session_id)
        assert status["session_id"] == session_id
        assert status["status"] == "active"
        
        # End session
        summary = await mock_orchestrator.end_session(session_id)
        assert summary["status"] == "session_ended"
        assert session_id not in mock_orchestrator.active_sessions
    
    @pytest.mark.asyncio
    async def test_system_status(self, mock_orchestrator):
        """Test system status endpoint."""
        status = await mock_orchestrator.get_system_status()
        
        assert "orchestrator" in status
        assert "message_bus" in status
        assert "agents" in status
        assert "sessions" in status
        
        assert status["orchestrator"]["is_running"] is True
        assert status["orchestrator"]["active_sessions"] >= 0


@pytest.mark.integration
class TestMessageBusIntegration:
    """Integration tests for message bus."""
    
    @pytest.fixture
    async def message_bus(self):
        """Create message bus for testing."""
        bus = MessageBus("redis://localhost:6379/2")
        
        with patch.object(bus, 'connect'):
            await bus.connect()
        
        yield bus
        
        with patch.object(bus, 'disconnect'):
            await bus.disconnect()
    
    @pytest.mark.asyncio
    async def test_message_publishing(self, message_bus):
        """Test message publishing and subscription."""
        received_messages = []
        
        async def message_handler(message):
            received_messages.append(message)
        
        # Subscribe to agent messages
        await message_bus.subscribe_to_agent("test_agent", message_handler)
        
        # Publish a message
        test_message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="test_agent",
            intent="test",
            message_type=MessageType.QUESTION_ASKED,
            payload={"test": "data"},
            session_id=uuid4()
        )
        
        await message_bus.publish_message(test_message)
        
        # Give some time for async processing
        await asyncio.sleep(0.1)
        
        # Verify message was received
        assert len(received_messages) == 1
        assert received_messages[0].agent_id == "test_agent"
        assert received_messages[0].message_type == MessageType.QUESTION_ASKED
    
    @pytest.mark.asyncio
    async def test_message_history(self, message_bus):
        """Test message history functionality."""
        # Publish some test messages
        for i in range(3):
            message = AgentMessage(
                correlation_id=uuid4(),
                agent_id=f"agent_{i}",
                intent="test",
                message_type=MessageType.QUESTION_ASKED,
                payload={"index": i},
                session_id=uuid4()
            )
            await message_bus.publish_message(message)
        
        # Get message history
        history = await message_bus.get_message_history(limit=10)
        
        assert len(history) >= 3
        assert all(isinstance(msg, AgentMessage) for msg in history)


@pytest.mark.integration
class TestRouterIntegration:
    """Integration tests for message router."""
    
    @pytest.fixture
    async def router(self):
        """Create router for testing."""
        bus = AsyncMock()
        router = MessageRouter(bus)
        
        # Register test agents
        await router.register_agent("test_agent_1", {
            "message_types": ["question_asked", "question_answered"],
            "capabilities": ["test_capability"],
            "max_concurrent_tasks": 2
        })
        
        await router.register_agent("test_agent_2", {
            "message_types": ["design_requested", "design_completed"],
            "capabilities": ["design_capability"],
            "max_concurrent_tasks": 1
        })
        
        return router
    
    @pytest.mark.asyncio
    async def test_agent_registration(self, router):
        """Test agent registration."""
        status = await router.get_agent_status()
        
        assert "total_agents" in status
        assert status["total_agents"] == 2
        assert "test_agent_1" in status["agents"]
        assert "test_agent_2" in status["agents"]
    
    @pytest.mark.asyncio
    async def test_message_routing(self, router):
        """Test message routing."""
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="test",
            message_type=MessageType.QUESTION_ASKED,
            payload={"test": "data"},
            session_id=uuid4()
        )
        
        # Route message
        routed_agents = await router.route_message(message)
        
        # Should route to test_agent_1 based on message type
        assert "test_agent_1" in routed_agents
    
    @pytest.mark.asyncio
    async def test_workflow_state_tracking(self, router):
        """Test workflow state tracking."""
        correlation_id = uuid4()
        
        message = AgentMessage(
            correlation_id=correlation_id,
            agent_id="test_agent",
            intent="test",
            message_type=MessageType.QUESTION_ASKED,
            payload={"test": "data"},
            session_id=uuid4()
        )
        
        await router.route_message(message)
        
        # Check workflow state
        state = await router.get_workflow_state(correlation_id)
        
        assert state is not None
        assert state["correlation_id"] == correlation_id
        assert len(state["messages"]) == 1
        assert "test_agent" in state["agents_involved"]


@pytest.mark.integration
class TestContextManagerIntegration:
    """Integration tests for context manager."""
    
    @pytest.fixture
    def context_manager(self):
        """Create context manager for testing."""
        return ContextManager()
    
    @pytest.mark.asyncio
    async def test_session_lifecycle(self, context_manager):
        """Test complete session lifecycle."""
        # Create session
        session_id = await context_manager.create_session(
            user_id="test_user",
            metadata={"test": True}
        )
        
        assert session_id is not None
        
        # Add user input
        await context_manager.add_user_input(
            session_id=session_id,
            user_input="I need a document processing system",
            input_type="text"
        )
        
        # Add agent response
        await context_manager.add_agent_response(
            session_id=session_id,
            agent_id="requirements_extractor",
            response="What type of documents?",
            response_type="question"
        )
        
        # Add refinement request
        await context_manager.add_refinement_request(
            session_id=session_id,
            refinement_type="add_error_handling",
            description="Add comprehensive error handling"
        )
        
        # Add decision
        await context_manager.add_decision(
            session_id=session_id,
            decision_type="architecture",
            decision="Use event-driven architecture",
            rationale="Better for scalability"
        )
        
        # Get conversation summary
        summary = await context_manager.get_conversation_summary(session_id)
        
        assert summary["session_id"] == session_id
        assert summary["user_id"] == "test_user"
        assert summary["total_user_inputs"] == 1
        assert summary["total_agent_responses"] == 1
        assert summary["total_refinements"] == 1
        assert summary["total_decisions"] == 1
        
        # Get conversation history
        history = await context_manager.get_conversation_history(session_id)
        
        assert len(history) == 4  # input + response + refinement + decision
        
        # Search conversation
        search_results = await context_manager.search_conversation_history(
            session_id=session_id,
            query="document"
        )
        
        assert len(search_results) == 1
        assert "document" in search_results[0]["content"].lower()
    
    @pytest.mark.asyncio
    async def test_context_persistence(self, context_manager):
        """Test context data persistence."""
        session_id = await context_manager.create_session()
        
        # Store various context data
        from schemas.requirements import UserRequirements
        from schemas.design import OrchestraDesign
        
        requirements = UserRequirements(
            requirements_id=uuid4(),
            session_id=session_id,
            project_description="Test project",
            primary_goal="Test goal"
        )
        
        await context_manager.store_requirements(session_id, requirements)
        
        design = OrchestraDesign(
            design_id=uuid4(),
            session_id=session_id,
            orchestra_name="Test Orchestra",
            primary_goal="Test goal"
        )
        
        await context_manager.store_orchestra_design(session_id, design)
        
        # Get session context
        context = await context_manager.get_session_context(session_id)
        
        assert context["context"]["requirements"] is not None
        assert context["context"]["orchestra_design"] is not None
        assert context["context"]["requirements"]["project_description"] == "Test project"
        assert context["context"]["orchestra_design"]["orchestra_name"] == "Test Orchestra"
    
    @pytest.mark.asyncio
    async def test_session_cleanup(self, context_manager):
        """Test session cleanup functionality."""
        # Create multiple sessions
        session_ids = []
        for i in range(3):
            session_id = await context_manager.create_session(user_id=f"user_{i}")
            session_ids.append(session_id)
        
        assert len(context_manager.sessions) == 3
        
        # Clean up expired sessions (none should be expired yet)
        cleaned = await context_manager.cleanup_expired_sessions()
        assert cleaned == 0
        assert len(context_manager.sessions) == 3
        
        # Manually end one session
        await context_manager.end_session(session_ids[0])
        
        # Session should still be there (end_session doesn't remove by default)
        assert len(context_manager.sessions) == 3
    
    @pytest.mark.asyncio
    async def test_user_preferences(self, context_manager):
        """Test user preferences functionality."""
        user_id = "test_user"
        
        # Set preferences
        preferences = {
            "preferred_stack": "python",
            "max_agents": 5,
            "budget_limit": 1000
        }
        
        await context_manager.update_user_preferences(user_id, preferences)
        
        # Get preferences
        retrieved = await context_manager.get_user_preferences(user_id)
        
        assert retrieved["preferred_stack"] == "python"
        assert retrieved["max_agents"] == 5
        assert retrieved["budget_limit"] == 1000
        
        # Update preferences
        await context_manager.update_user_preferences(user_id, {"max_agents": 10})
        
        updated = await context_manager.get_user_preferences(user_id)
        assert updated["max_agents"] == 10
        assert updated["preferred_stack"] == "python"  # Should preserve existing


@pytest.mark.integration
class TestEndToEndWorkflow:
    """End-to-end workflow tests."""
    
    @pytest.mark.asyncio
    async def test_complete_design_workflow(self):
        """Test complete workflow from start to finish."""
        # This would test the full orchestrator workflow
        # For now, just verify the structure
        
        with patch('orchestrator.orchestrator.AsyncAnthropic') as mock_anthropic:
            mock_client = AsyncMock()
            mock_anthropic.return_value = mock_client
            
            # Mock all Claude responses
            mock_client.messages.create.return_value = AsyncMock(
                content=[AsyncMock(text="mock response")]
            )
            
            orchestrator = OrchestraOrchestrator(mock_client, "redis://localhost:6379/3")
            
            with patch.object(orchestrator.message_bus, 'connect'):
                await orchestrator.start()
            
            try:
                # Start session
                session_id = await orchestrator.start_design_session(
                    user_id="integration_test_user"
                )
                
                # Simulate user inputs through the workflow
                inputs = [
                    "I need a document processing system for real estate contracts",
                    "Python with FastAPI, PostgreSQL, and Claude",
                    "3 months timeline, $500/month budget",
                    "Must handle PDF extraction and validation",
                    "Deploy to AWS with monitoring"
                ]
                
                for user_input in inputs:
                    response = await orchestrator.process_user_input(
                        session_id=session_id,
                        user_input=user_input
                    )
                    
                    # Verify response structure
                    assert "status" in response
                    assert "message" in response
                
                # Request refinement
                await orchestrator.request_refinement(
                    session_id=session_id,
                    refinement_type="add_security",
                    description="Add authentication and authorization"
                )
                
                # Get final status
                final_status = await orchestrator.get_session_status(session_id)
                
                # Verify session completed successfully
                assert final_status["status"] == "active"
                assert final_status["current_phase"] in [
                    "requirements_extraction",
                    "repository_analysis", 
                    "architecture_design",
                    "implementation_planning",
                    "validation",
                    "complete"
                ]
                
                # End session
                summary = await orchestrator.end_session(session_id)
                assert summary["status"] == "session_ended"
                
            finally:
                await orchestrator.stop()


# Performance integration tests
@pytest.mark.integration
@pytest.mark.performance
class TestSystemPerformance:
    """Performance tests for the integrated system."""
    
    @pytest.mark.asyncio
    async def test_concurrent_sessions(self):
        """Test handling multiple concurrent sessions."""
        with patch('orchestrator.orchestrator.AsyncAnthropic') as mock_anthropic:
            mock_client = AsyncMock()
            mock_anthropic.return_value = mock_client
            
            mock_client.messages.create.return_value = AsyncMock(
                content=[AsyncMock(text="quick response")]
            )
            
            orchestrator = OrchestraOrchestrator(mock_client, "redis://localhost:6379/4")
            
            with patch.object(orchestrator.message_bus, 'connect'):
                await orchestrator.start()
            
            try:
                import time
                start_time = time.time()
                
                # Create multiple sessions concurrently
                session_tasks = []
                for i in range(10):
                    task = orchestrator.start_design_session(user_id=f"user_{i}")
                    session_tasks.append(task)
                
                session_ids = await asyncio.gather(*session_tasks)
                
                # Process inputs concurrently
                input_tasks = []
                for session_id in session_ids:
                    task = orchestrator.process_user_input(
                        session_id=session_id,
                        user_input=f"Test input for session {session_id}"
                    )
                    input_tasks.append(task)
                
                responses = await asyncio.gather(*input_tasks)
                
                end_time = time.time()
                duration = end_time - start_time
                
                # Verify performance
                assert len(session_ids) == 10
                assert len(responses) == 10
                assert duration < 10.0, f"Concurrent processing took too long: {duration}s"
                
                # Clean up
                cleanup_tasks = [
                    orchestrator.end_session(session_id) 
                    for session_id in session_ids
                ]
                await asyncio.gather(*cleanup_tasks)
                
            finally:
                await orchestrator.stop()
