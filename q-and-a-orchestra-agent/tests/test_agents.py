"""
Tests for the Q&A Orchestra Agent system.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from schemas.messages import AgentMessage, MessageType, Priority
from agents.repository_analyzer import RepositoryAnalyzerAgent
from agents.requirements_extractor import RequirementsExtractorAgent
from agents.architecture_designer import ArchitectureDesignerAgent
from agents.implementation_planner import ImplementationPlannerAgent
from agents.validator import ValidatorAgent


@pytest.fixture
def mock_anthropic_client():
    """Mock Anthropic client."""
    client = AsyncMock()
    client.messages.create = AsyncMock()
    return client


@pytest.fixture
def mock_repo_reader():
    """Mock repository reader."""
    reader = AsyncMock()
    reader.read_file = AsyncMock(return_value="mock file content")
    return reader


@pytest.fixture
def sample_message():
    """Sample agent message for testing."""
    return AgentMessage(
        correlation_id=uuid4(),
        agent_id="test_agent",
        intent="test_intent",
        message_type=MessageType.QUESTION_ASKED,
        payload={"test": "data"},
        session_id=uuid4()
    )


class TestRepositoryAnalyzerAgent:
    """Test cases for RepositoryAnalyzerAgent."""
    
    @pytest.fixture
    def agent(self, mock_anthropic_client, mock_repo_reader):
        return RepositoryAnalyzerAgent(mock_anthropic_client, mock_repo_reader)
    
    @pytest.mark.asyncio
    async def test_analyze_repository_success(self, agent, sample_message):
        """Test successful repository analysis."""
        # Mock Claude response
        agent.anthropic.messages.create.return_value = AsyncMock(
            content=[AsyncMock(text="mock pattern analysis")]
        )
        
        # Test analysis
        result = await agent.analyze_repository(sample_message)
        
        assert result.message_type == MessageType.REPO_ANALYSIS_COMPLETED
        assert result.agent_id == agent.agent_id
        assert "patterns_identified" in result.payload
        assert "architecture_principles" in result.payload
    
    @pytest.mark.asyncio
    async def test_analyze_repository_error(self, agent, sample_message):
        """Test repository analysis with error."""
        # Mock error
        agent.repo_reader.read_file.side_effect = Exception("Read error")
        
        result = await agent.analyze_repository(sample_message)
        
        assert result.message_type == MessageType.ERROR_OCCURRED
        assert result.intent == "error_occurred"
    
    def test_get_pattern_recommendations(self, agent):
        """Test pattern recommendations."""
        requirements = {"complexity": "complex", "scalability_requirements": ["high_scale"]}
        
        recommendations = agent.get_pattern_recommendations(requirements)
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0


class TestRequirementsExtractorAgent:
    """Test cases for RequirementsExtractorAgent."""
    
    @pytest.fixture
    def agent(self, mock_anthropic_client):
        return RequirementsExtractorAgent(mock_anthropic_client)
    
    @pytest.mark.asyncio
    async def test_start_extraction_session(self, agent):
        """Test starting requirements extraction session."""
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="start_requirements_extraction",
            message_type=MessageType.QUESTION_ASKED,
            payload={},
            session_id=uuid4()
        )
        
        result = await agent.start_extraction_session(message)
        
        assert result.message_type == MessageType.QUESTION_ASKED
        assert result.intent == "question_asked"
        assert "question_text" in result.payload
        assert "session_progress" in result.payload
    
    @pytest.mark.asyncio
    async def test_process_answer(self, agent):
        """Test processing user answer."""
        # First start a session
        session_id = uuid4()
        start_message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="start_requirements_extraction",
            message_type=MessageType.QUESTION_ASKED,
            payload={},
            session_id=session_id
        )
        await agent.start_extraction_session(start_message)
        
        # Process answer
        answer_message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="user",
            intent="process_answer",
            message_type=MessageType.QUESTION_ANSWERED,
            payload={
                "question_id": "project_description",
                "answer": "I need to build a document processing system"
            },
            session_id=session_id
        )
        
        result = await agent.process_answer(answer_message)
        
        assert result.message_type in [MessageType.QUESTION_ASKED, MessageType.REQUIREMENTS_EXTRACTED]
    
    @pytest.mark.asyncio
    async def test_clarify_requirements(self, agent):
        """Test requirements clarification."""
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="clarify_requirements",
            message_type=MessageType.QUESTION_ASKED,
            payload={
                "requirements": {"project_description": "vague description"},
                "unclear_areas": ["timeline", "budget"]
            },
            session_id=uuid4()
        )
        
        result = await agent.clarify_requirements(message)
        
        assert result.message_type == MessageType.QUESTION_ASKED
        assert result.intent == "clarification_questions"
        assert "clarifying_questions" in result.payload


class TestArchitectureDesignerAgent:
    """Test cases for ArchitectureDesignerAgent."""
    
    @pytest.fixture
    def agent(self, mock_anthropic_client):
        return ArchitectureDesignerAgent(mock_anthropic_client)
    
    @pytest.mark.asyncio
    async def test_design_orchestra_success(self, agent):
        """Test successful orchestra design."""
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="design_orchestra",
            message_type=MessageType.DESIGN_REQUESTED,
            payload={
                "requirements": {
                    "project_description": "Document processing system",
                    "complexity": "moderate",
                    "technology_stack": {"backend": "python"}
                },
                "patterns": {
                    "agent_patterns": ["event_driven"],
                    "best_practices": ["error_handling"]
                }
            },
            session_id=uuid4()
        )
        
        # Mock Claude response
        agent.anthropic.messages.create.return_value = AsyncMock(
            content=[AsyncMock(text="document_processing")]
        )
        
        result = await agent.design_orchestra(message)
        
        assert result.message_type == MessageType.DESIGN_COMPLETED
        assert result.intent == "design_completed"
        assert "design" in result.payload
        assert "design_summary" in result.payload
    
    @pytest.mark.asyncio
    async def test_refine_design(self, agent):
        """Test design refinement."""
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="refine_design",
            message_type=MessageType.DESIGN_COMPLETED,
            payload={
                "design": {
                    "orchestra_name": "Test Orchestra",
                    "agents": {"agent1": {"agent_id": "agent1"}}
                },
                "refinements": ["add error handling"]
            },
            session_id=uuid4()
        )
        
        result = await agent.refine_design(message)
        
        assert result.message_type == MessageType.DESIGN_COMPLETED
        assert result.intent == "design_refined"
    
    def test_determine_design_approach(self, agent):
        """Test design approach determination."""
        # This would need to be made async for actual testing
        pass


class TestImplementationPlannerAgent:
    """Test cases for ImplementationPlannerAgent."""
    
    @pytest.fixture
    def agent(self, mock_anthropic_client):
        return ImplementationPlannerAgent(mock_anthropic_client)
    
    @pytest.mark.asyncio
    async def test_create_implementation_plan(self, agent):
        """Test implementation plan creation."""
        design_data = {
            "design_id": str(uuid4()),
            "orchestra_name": "Test Orchestra",
            "agents": {"agent1": {"agent_id": "agent1", "agent_type": "executor"}},
            "message_flows": [],
            "external_integrations": []
        }
        
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="create_implementation_plan",
            message_type=MessageType.PLAN_REQUESTED,
            payload={"design": design_data},
            session_id=uuid4()
        )
        
        result = await agent.create_implementation_plan(message)
        
        assert result.message_type == MessageType.PLAN_COMPLETED
        assert result.intent == "plan_completed"
        assert "phases" in result.payload
        assert "file_structure" in result.payload
        assert "cost_estimate" in result.payload
    
    @pytest.mark.asyncio
    async def test_create_dry_run_plan(self, agent):
        """Test dry-run plan creation."""
        design_data = {
            "design_id": str(uuid4()),
            "agents": {"agent1": {"agent_id": "agent1"}},
            "message_flows": []
        }
        
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="create_dry_run_plan",
            message_type=MessageType.PLAN_REQUESTED,
            payload={"design": design_data},
            session_id=uuid4()
        )
        
        result = await agent.create_dry_run_plan(message)
        
        assert result.message_type == MessageType.PLAN_COMPLETED
        assert result.intent == "dry_run_plan_created"
        assert "dry_run_plan" in result.payload
    
    def test_calculate_costs(self, agent):
        """Test cost calculation."""
        # This would need to be made async for actual testing
        pass


class TestValidatorAgent:
    """Test cases for ValidatorAgent."""
    
    @pytest.fixture
    def agent(self, mock_anthropic_client):
        return ValidatorAgent(mock_anthropic_client)
    
    @pytest.mark.asyncio
    async def test_validate_design_success(self, agent):
        """Test successful design validation."""
        design_data = {
            "design_id": str(uuid4()),
            "orchestra_name": "Test Orchestra",
            "agents": {
                "agent1": {
                    "agent_id": "agent1",
                    "timeout_seconds": 30,
                    "retry_attempts": 3,
                    "monitoring_setup": {"logging": "enabled"}
                }
            },
            "safety_mechanisms": ["timeout", "retry_logic"],
            "monitoring_setup": {"logging": "structured", "metrics": "prometheus"}
        }
        
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="validate_design",
            message_type=MessageType.VALIDATION_REQUESTED,
            payload={"design": design_data},
            session_id=uuid4()
        )
        
        result = await agent.validate_design(message)
        
        assert result.message_type == MessageType.VALIDATION_COMPLETED
        assert result.intent == "validation_completed"
        assert "validation_result" in result.payload
        assert "validation_summary" in result.payload
    
    @pytest.mark.asyncio
    async def test_validate_implementation_plan(self, agent):
        """Test implementation plan validation."""
        plan_data = {
            "phases": [
                {
                    "phase_name": "Foundation",
                    "duration_weeks": 2,
                    "tasks": ["Setup infrastructure"]
                }
            ],
            "dependencies": ["fastapi", "pydantic"],
            "timeline_estimate": "4 weeks",
            "cost_estimate": {
                "development": {"one_time_total": 1000},
                "monthly_operational": {"total": 100}
            }
        }
        
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id="orchestrator",
            intent="validate_implementation_plan",
            message_type=MessageType.VALIDATION_REQUESTED,
            payload={"implementation_plan": plan_data},
            session_id=uuid4()
        )
        
        result = await agent.validate_implementation_plan(message)
        
        assert result.message_type == MessageType.VALIDATION_COMPLETED
        assert result.intent == "implementation_validation_completed"
        assert "implementation_validation" in result.payload
    
    def test_evaluate_rule_condition(self, agent):
        """Test rule condition evaluation."""
        # This would need to be made async for actual testing
        pass


# Integration tests
@pytest.mark.integration
class TestAgentIntegration:
    """Integration tests for agent interactions."""
    
    @pytest.mark.asyncio
    async def test_full_workflow(self, mock_anthropic_client, mock_repo_reader):
        """Test complete workflow from requirements to validation."""
        # This is a simplified integration test
        # In production, you'd test the full orchestrator workflow
        
        # Create agents
        repo_analyzer = RepositoryAnalyzerAgent(mock_anthropic_client, mock_repo_reader)
        requirements_extractor = RequirementsExtractorAgent(mock_anthropic_client)
        designer = ArchitectureDesignerAgent(mock_anthropic_client)
        planner = ImplementationPlannerAgent(mock_anthropic_client)
        validator = ValidatorAgent(mock_anthropic_client)
        
        # Mock responses
        mock_anthropic_client.messages.create.return_value = AsyncMock(
            content=[AsyncMock(text="mock response")]
        )
        
        session_id = uuid4()
        correlation_id = uuid4()
        
        # Step 1: Start requirements extraction
        start_message = AgentMessage(
            correlation_id=correlation_id,
            agent_id="orchestrator",
            intent="start_requirements_extraction",
            message_type=MessageType.QUESTION_ASKED,
            payload={},
            session_id=session_id
        )
        
        req_result = await requirements_extractor.start_extraction_session(start_message)
        assert req_result.message_type == MessageType.QUESTION_ASKED
        
        # Step 2: Process answer (simplified)
        answer_message = AgentMessage(
            correlation_id=correlation_id,
            agent_id="user",
            intent="process_answer",
            message_type=MessageType.QUESTION_ANSWERED,
            payload={
                "question_id": "project_description",
                "answer": "Document processing system"
            },
            session_id=session_id
        )
        
        # This would normally continue through the workflow
        # For now, just verify the message structure
        assert answer_message.session_id == session_id
        assert answer_message.correlation_id == correlation_id


# Mock MCP servers for testing
class MockMCPServer:
    """Mock MCP server for testing."""
    
    def __init__(self):
        self.responses = {}
        self.requests = []
    
    async def call_tool(self, tool_name, parameters):
        """Mock tool call."""
        self.requests.append({"tool": tool_name, "params": parameters})
        return self.responses.get(tool_name, {"result": "mock_result"})
    
    def set_response(self, tool_name, response):
        """Set mock response for a tool."""
        self.responses[tool_name] = response
    
    def get_requests(self):
        """Get all requests made to the mock server."""
        return self.requests


@pytest.fixture
def mock_mcp_server():
    """Mock MCP server fixture."""
    return MockMCPServer()


# Performance tests
@pytest.mark.performance
class TestAgentPerformance:
    """Performance tests for agents."""
    
    @pytest.mark.asyncio
    async def test_message_processing_performance(self, mock_anthropic_client, mock_repo_reader):
        """Test message processing performance."""
        agent = RepositoryAnalyzerAgent(mock_anthropic_client, mock_repo_reader)
        
        # Mock fast response
        mock_anthropic_client.messages.create.return_value = AsyncMock(
            content=[AsyncMock(text="quick response")]
        )
        
        import time
        start_time = time.time()
        
        # Process multiple messages
        messages = [
            AgentMessage(
                correlation_id=uuid4(),
                agent_id="test",
                intent="test",
                message_type=MessageType.REPO_ANALYSIS_REQUESTED,
                payload={},
                session_id=uuid4()
            )
            for _ in range(10)
        ]
        
        tasks = [agent.analyze_repository(msg) for msg in messages]
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Verify all messages processed
        assert len(results) == 10
        assert all(r.message_type == MessageType.REPO_ANALYSIS_COMPLETED for r in results)
        
        # Performance assertion (adjust as needed)
        assert duration < 5.0, f"Processing took too long: {duration}s"
