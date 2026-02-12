"""
Mock MCP servers for testing agent integrations.
"""

import asyncio
import json
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

from integrations.mcp_github import GitHubMCPClient
from integrations.repo_reader import RepositoryReader


class MockGitHubMCPClient(GitHubMCPClient):
    """Mock GitHub MCP client for testing."""
    
    def __init__(self):
        # Don't call parent constructor to avoid real GitHub API calls
        self.session = None
        self.repo_owner = "test_owner"
        self.repo_name = "test_repo"
        self.base_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}"
        
        # Mock data
        self.mock_files = {
            "SKILL.md": "# Test Skill\nThis is a test skill file.",
            "architecture-patterns.md": "# Architecture Patterns\nEvent-driven architecture...",
            "best-practices.md": "# Best Practices\n✅ Always include error handling",
            "multi-agent.md": "# Multi-Agent Systems\nVoice-driven DevOps assistant...",
            "full-stack.md": "# Full Stack\nReact + FastAPI + PostgreSQL",
            "tech-stack-guide.md": "# Tech Stack\nPython, FastAPI, PostgreSQL",
            "refactoring.md": "# Refactoring\nCode improvement strategies",
            "README.md": "# Test Project\nThis is a test project."
        }
        
        self.mock_repo_info = {
            "name": "test_repo",
            "description": "Test repository for Q&A Orchestra Agent",
            "stargazers_count": 42,
            "language": "Python",
            "updated_at": "2024-01-15T10:30:00Z"
        }
    
    async def connect(self) -> None:
        """Mock connection."""
        self.session = AsyncMock()
    
    async def disconnect(self) -> None:
        """Mock disconnection."""
        self.session = None
    
    async def get_repository_info(self) -> Dict[str, Any]:
        """Mock repository info."""
        return self.mock_repo_info.copy()
    
    async def list_files(self, path: str = "", ref: str = "main") -> List[Dict[str, Any]]:
        """Mock file listing."""
        files = []
        for filename in self.mock_files.keys():
            if path == "" or filename.startswith(path):
                files.append({
                    "name": filename,
                    "type": "file",
                    "path": filename,
                    "size": len(self.mock_files[filename])
                })
        return files
    
    async def get_file_content(self, file_path: str, ref: str = "main") -> str:
        """Mock file content."""
        return self.mock_files.get(file_path, "")
    
    async def get_architecture_files(self) -> Dict[str, str]:
        """Mock architecture files."""
        return self.mock_files.copy()
    
    async def search_files(self, query: str, file_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """Mock file search."""
        results = []
        query_lower = query.lower()
        
        for filename, content in self.mock_files.items():
            if query_lower in content.lower():
                if file_pattern is None or filename.endswith(file_pattern):
                    results.append({
                        "name": filename,
                        "path": filename,
                        "type": "file",
                        "score": 1.0
                    })
        
        return results
    
    async def get_file_history(self, file_path: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Mock file history."""
        return [
            {
                "sha": f"commit_hash_{i}",
                "commit": {
                    "author": {"name": f"Author {i}", "email": f"author{i}@test.com"},
                    "message": f"Commit message {i}",
                    "date": f"2024-01-{i+1:02d}T10:30:00Z"
                }
            }
            for i in range(min(limit, 5))
        ]
    
    async def get_branches(self) -> List[Dict[str, Any]]:
        """Mock branches."""
        return [
            {
                "name": "main",
                "commit": {"sha": "main_commit_hash"}
            },
            {
                "name": "develop",
                "commit": {"sha": "develop_commit_hash"}
            }
        ]
    
    async def get_tags(self) -> List[Dict[str, Any]]:
        """Mock tags."""
        return [
            {
                "name": "v1.0.0",
                "commit": {"sha": "v1.0.0_commit_hash"}
            },
            {
                "name": "v1.1.0",
                "commit": {"sha": "v1.1.0_commit_hash"}
            }
        ]


class MockRepositoryReader(RepositoryReader):
    """Mock repository reader for testing."""
    
    def __init__(self):
        # Don't call parent constructor
        self.github_client = MockGitHubMCPClient()
        self._file_cache = {}
        self._cache_ttl = 3600
        self._last_cache_update = 0
    
    async def read_file(self, file_path: str, use_cache: bool = True) -> str:
        """Mock file reading."""
        return await self.github_client.get_file_content(file_path)
    
    async def read_multiple_files(self, file_paths: List[str]) -> Dict[str, str]:
        """Mock multiple file reading."""
        results = {}
        for file_path in file_paths:
            results[file_path] = await self.read_file(file_path)
        return results
    
    async def get_architecture_patterns(self) -> Dict[str, Any]:
        """Mock architecture patterns extraction."""
        return {
            "multi_agent_patterns": [
                {
                    "name": "voice_driven_ops",
                    "description": "Voice-driven DevOps automation system",
                    "agents": ["intent_parser", "planner", "safety_checker"],
                    "communication": "event_driven"
                }
            ],
            "communication_patterns": [
                {
                    "name": "event_driven",
                    "description": "Loose coupling via message bus",
                    "benefits": ["async_processing", "parallelism"]
                }
            ],
            "safety_patterns": [
                {
                    "name": "retry_with_backoff",
                    "description": "Retry transient failures with exponential backoff"
                }
            ],
            "best_practices": [
                "✅ Always include error handling",
                "✅ Use structured logging",
                "✅ Implement timeouts"
            ],
            "technology_stack": {
                "languages": ["Python"],
                "frameworks": ["FastAPI"],
                "databases": ["PostgreSQL", "Redis"],
                "cloud": ["GCP"],
                "tools": ["Docker", "Kubernetes"]
            }
        }
    
    async def clear_cache(self) -> None:
        """Mock cache clearing."""
        self._file_cache.clear()
        self._last_cache_update = 0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Mock cache stats."""
        return {
            "cached_files": len(self._file_cache),
            "cache_age_seconds": 0,
            "cache_ttl_seconds": self._cache_ttl
        }


class MockMCPServer:
    """Generic mock MCP server for testing."""
    
    def __init__(self, server_name: str):
        self.server_name = server_name
        self.tools = {}
        self.requests = []
        self.responses = {}
    
    def register_tool(self, tool_name: str, handler):
        """Register a tool handler."""
        self.tools[tool_name] = handler
    
    def set_response(self, tool_name: str, response: Any):
        """Set a mock response for a tool."""
        self.responses[tool_name] = response
    
    async def call_tool(self, tool_name: str, parameters: Dict[str, Any]) -> Any:
        """Mock tool call."""
        self.requests.append({
            "tool": tool_name,
            "parameters": parameters,
            "timestamp": asyncio.get_event_loop().time()
        })
        
        if tool_name in self.responses:
            return self.responses[tool_name]
        elif tool_name in self.tools:
            return await self.tools[tool_name](parameters)
        else:
            return {"error": f"Unknown tool: {tool_name}"}
    
    def get_requests(self) -> List[Dict[str, Any]]:
        """Get all requests made to this server."""
        return self.requests
    
    def clear_requests(self) -> None:
        """Clear request history."""
        self.requests.clear()


class MockDatabaseMCP:
    """Mock database MCP server."""
    
    def __init__(self):
        self.data = {}
        self.tables = {
            "sessions": {},
            "messages": {},
            "designs": {},
            "requirements": {}
        }
    
    async def create_table(self, table_name: str, schema: Dict[str, Any]) -> None:
        """Mock table creation."""
        self.tables[table_name] = {}
    
    async def insert(self, table_name: str, record: Dict[str, Any]) -> str:
        """Mock record insertion."""
        import uuid
        record_id = str(uuid.uuid4())
        record["id"] = record_id
        self.tables[table_name][record_id] = record
        return record_id
    
    async def select(self, table_name: str, query: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Mock record selection."""
        if query is None:
            return list(self.tables[table_name].values())
        
        results = []
        for record in self.tables[table_name].values():
            match = True
            for key, value in query.items():
                if record.get(key) != value:
                    match = False
                    break
            if match:
                results.append(record)
        
        return results
    
    async def update(self, table_name: str, record_id: str, updates: Dict[str, Any]) -> bool:
        """Mock record update."""
        if record_id in self.tables[table_name]:
            self.tables[table_name][record_id].update(updates)
            return True
        return False
    
    async def delete(self, table_name: str, record_id: str) -> bool:
        """Mock record deletion."""
        if record_id in self.tables[table_name]:
            del self.tables[table_name][record_id]
            return True
        return False


class MockCloudMCP:
    """Mock cloud provider MCP server."""
    
    def __init__(self, provider: str = "gcp"):
        self.provider = provider
        self.resources = {}
        self.deployments = {}
    
    async def deploy_service(self, service_config: Dict[str, Any]) -> Dict[str, Any]:
        """Mock service deployment."""
        import uuid
        deployment_id = str(uuid.uuid4())
        
        deployment = {
            "id": deployment_id,
            "status": "deploying",
            "config": service_config,
            "created_at": asyncio.get_event_loop().time()
        }
        
        self.deployments[deployment_id] = deployment
        
        # Simulate deployment completion
        deployment["status"] = "deployed"
        deployment["url"] = f"https://{service_config.get('name', 'service')}-{deployment_id}.example.com"
        
        return deployment
    
    async def get_deployment_status(self, deployment_id: str) -> Dict[str, Any]:
        """Mock deployment status check."""
        return self.deployments.get(deployment_id, {"error": "Deployment not found"})
    
    async def scale_service(self, deployment_id: str, replicas: int) -> bool:
        """Mock service scaling."""
        if deployment_id in self.deployments:
            self.deployments[deployment_id]["replicas"] = replicas
            return True
        return False
    
    async def delete_deployment(self, deployment_id: str) -> bool:
        """Mock deployment deletion."""
        if deployment_id in self.deployments:
            del self.deployments[deployment_id]
            return True
        return False


class MockLLMMCP:
    """Mock LLM MCP server."""
    
    def __init__(self, model_name: str = "claude-3-sonnet"):
        self.model_name = model_name
        self.responses = {}
        self.call_count = 0
    
    def set_response(self, prompt: str, response: str):
        """Set a mock response for a prompt."""
        self.responses[prompt] = response
    
    async def complete(self, prompt: str, max_tokens: int = 1000) -> str:
        """Mock LLM completion."""
        self.call_count += 1
        
        if prompt in self.responses:
            return self.responses[prompt]
        
        # Default mock response
        return f"Mock response for: {prompt[:100]}..."
    
    async def complete_with_tools(self, prompt: str, tools: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Mock LLM completion with tool use."""
        self.call_count += 1
        
        return {
            "content": f"Mock response with tools for: {prompt[:100]}...",
            "tool_calls": [],
            "usage": {
                "prompt_tokens": len(prompt.split()),
                "completion_tokens": 50,
                "total_tokens": len(prompt.split()) + 50
            }
        }
    
    def get_call_count(self) -> int:
        """Get total call count."""
        return self.call_count
    
    def reset_call_count(self) -> None:
        """Reset call count."""
        self.call_count = 0


# Test fixtures
@pytest.fixture
def mock_github_client():
    """Mock GitHub MCP client fixture."""
    return MockGitHubMCPClient()


@pytest.fixture
def mock_repo_reader():
    """Mock repository reader fixture."""
    return MockRepositoryReader()


@pytest.fixture
def mock_database_mcp():
    """Mock database MCP fixture."""
    return MockDatabaseMCP()


@pytest.fixture
def mock_cloud_mcp():
    """Mock cloud MCP fixture."""
    return MockCloudMCP()


@pytest.fixture
def mock_llm_mcp():
    """Mock LLM MCP fixture."""
    return MockLLMMCP()


@pytest.fixture
def mock_mcp_servers():
    """Collection of mock MCP servers."""
    return {
        "github": MockGitHubMCPClient(),
        "database": MockDatabaseMCP(),
        "cloud": MockCloudMCP(),
        "llm": MockLLMMCP()
    }


# Utility functions for testing
def create_mock_session_data():
    """Create mock session data for testing."""
    from uuid import uuid4
    
    return {
        "session_id": uuid4(),
        "user_id": "test_user",
        "created_at": "2024-01-15T10:30:00Z",
        "status": "active",
        "current_phase": "requirements_extraction",
        "context": {
            "requirements": {
                "project_description": "Test project",
                "primary_goal": "Test goal"
            }
        }
    }


def create_mock_message(message_type: str = "question_asked"):
    """Create mock message for testing."""
    from schemas.messages import AgentMessage, MessageType, Priority
    from uuid import uuid4
    
    message_type_map = {
        "question_asked": MessageType.QUESTION_ASKED,
        "question_answered": MessageType.QUESTION_ANSWERED,
        "requirements_extracted": MessageType.REQUIREMENTS_EXTRACTED,
        "design_completed": MessageType.DESIGN_COMPLETED,
        "plan_completed": MessageType.PLAN_COMPLETED,
        "validation_completed": MessageType.VALIDATION_COMPLETED
    }
    
    return AgentMessage(
        correlation_id=uuid4(),
        agent_id="test_agent",
        intent="test_intent",
        message_type=message_type_map.get(message_type, MessageType.QUESTION_ASKED),
        payload={"test": "data"},
        session_id=uuid4()
    )


def create_mock_requirements():
    """Create mock requirements for testing."""
    from schemas.requirements import UserRequirements, StackType, Complexity
    from uuid import uuid4
    
    return UserRequirements(
        requirements_id=uuid4(),
        session_id=uuid4(),
        project_description="Document processing system",
        primary_goal="Automate contract analysis",
        stack_type=StackType.PYTHON_FASTAPI,
        complexity=Complexity.MODERATE,
        technology_stack={"backend": "python", "database": "postgresql"},
        timeline="3 months",
        budget="$500/month",
        must_have_features=["PDF processing", "data extraction"],
        nice_to_have_features=["web interface", "reporting"]
    )


def create_mock_orchestra_design():
    """Create mock orchestra design for testing."""
    from schemas.design import OrchestraDesign, AgentDefinition, AgentType
    from uuid import uuid4
    
    return OrchestraDesign(
        design_id=uuid4(),
        session_id=uuid4(),
        orchestra_name="Document Processing Orchestra",
        orchestra_description="Automated document processing system",
        primary_goal="Process real estate contracts",
        agents={
            "document_processor": AgentDefinition(
                agent_id="document_processor",
                agent_name="Document Processor",
                agent_type=AgentType.DATA_PROCESSOR,
                description="Processes incoming documents",
                responsibilities=["file_validation", "content_extraction"]
            )
        },
        message_flows=[],
        coordination_protocol={"type": "event_driven"},
        safety_mechanisms=["timeout", "retry_logic"],
        monitoring_setup={"logging": "enabled"}
    )
