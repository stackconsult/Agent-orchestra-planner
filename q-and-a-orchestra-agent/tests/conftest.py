"""
Pytest configuration and fixtures for the Q&A Orchestra Agent tests.
"""

import asyncio
import os
import pytest
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Set test environment variables
os.environ["TESTING"] = "true"
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["REDIS_URL"] = "redis://localhost:6379/15"  # Use different DB for tests


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
async def cleanup_test_data():
    """Cleanup test data after each test."""
    yield
    # Add any cleanup logic here
    pass


# Mock environment variables for testing
@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test_anthropic_key")
    monkeypatch.setenv("GITHUB_TOKEN", "test_github_token")
    monkeypatch.setenv("GITHUB_REPO_OWNER", "test_owner")
    monkeypatch.setenv("GITHUB_REPO_NAME", "test_repo")
    monkeypatch.setenv("SECRET_KEY", "test_secret_key")
    monkeypatch.setenv("JWT_SECRET_KEY", "test_jwt_secret")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/15")


# Test configuration
def pytest_configure(config):
    """Configure pytest."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


# Custom fixtures for common test scenarios
@pytest.fixture
def sample_user_input():
    """Sample user input for testing."""
    return {
        "project_description": "I need to build a document processing system for real estate contracts",
        "primary_goal": "Automate contract analysis and data extraction",
        "stack": "Python with FastAPI",
        "timeline": "3 months",
        "budget": "$500/month",
        "must_haves": ["PDF processing", "data validation", "storage"],
        "nice_to_haves": ["web dashboard", "email notifications"]
    }


@pytest.fixture
def sample_architecture_patterns():
    """Sample architecture patterns for testing."""
    return {
        "multi_agent_patterns": [
            {
                "name": "document_processing",
                "description": "Document processing workflow",
                "agents": ["ingester", "processor", "validator", "storage"],
                "communication": "pipeline"
            }
        ],
        "communication_patterns": [
            {
                "name": "event_driven",
                "description": "Event-based communication",
                "benefits": ["decoupling", "scalability"]
            }
        ],
        "best_practices": [
            "Always include error handling",
            "Use structured logging",
            "Implement timeouts and retries"
        ],
        "technology_stack": {
            "languages": ["Python"],
            "frameworks": ["FastAPI"],
            "databases": ["PostgreSQL", "Redis"],
            "cloud": ["GCP"]
        }
    }


@pytest.fixture
def sample_orchestra_design():
    """Sample orchestra design for testing."""
    return {
        "design_id": "test-design-123",
        "orchestra_name": "Document Processing Orchestra",
        "orchestra_description": "Automated document processing system",
        "primary_goal": "Process real estate contracts efficiently",
        "agents": {
            "document_ingester": {
                "agent_id": "document_ingester",
                "agent_name": "Document Ingester",
                "agent_type": "data_processor",
                "description": "Ingests and validates incoming documents",
                "responsibilities": ["file_validation", "format_detection"],
                "timeout_seconds": 30,
                "retry_attempts": 3
            },
            "content_extractor": {
                "agent_id": "content_extractor",
                "agent_name": "Content Extractor",
                "agent_type": "data_processor",
                "description": "Extracts structured content from documents",
                "responsibilities": ["text_extraction", "entity_recognition"],
                "timeout_seconds": 60,
                "retry_attempts": 3
            },
            "validation_agent": {
                "agent_id": "validation_agent",
                "agent_name": "Validation Agent",
                "agent_type": "validator",
                "description": "Validates extracted content",
                "responsibilities": ["business_rules", "compliance_check"],
                "timeout_seconds": 30,
                "retry_attempts": 2
            }
        },
        "message_flows": [
            {
                "from_agent": "document_ingester",
                "to_agent": "content_extractor",
                "message_type": "document_validated",
                "communication_pattern": "request_response"
            },
            {
                "from_agent": "content_extractor",
                "to_agent": "validation_agent",
                "message_type": "content_extracted",
                "communication_pattern": "event_driven"
            }
        ],
        "coordination_protocol": {
            "protocol_type": "event_driven",
            "message_bus": "redis",
            "correlation_ids": True
        },
        "safety_mechanisms": ["timeout", "retry_logic", "validation_check"],
        "monitoring_setup": {
            "logging": "structured_json",
            "metrics": "prometheus",
            "tracing": "opentelemetry"
        },
        "external_integrations": [
            {
                "name": "database",
                "type": "postgresql",
                "purpose": "Store processed documents"
            }
        ]
    }


@pytest.fixture
def sample_implementation_plan():
    """Sample implementation plan for testing."""
    return {
        "phases": [
            {
                "phase_id": "foundation",
                "phase_name": "Foundation Setup",
                "duration_weeks": 2,
                "tasks": [
                    "Set up project structure",
                    "Implement message bus",
                    "Create base agent classes"
                ],
                "deliverables": ["Project skeleton", "Message bus", "Base agents"]
            },
            {
                "phase_id": "core_agents",
                "phase_name": "Core Agent Implementation",
                "duration_weeks": 4,
                "tasks": [
                    "Implement document ingester",
                    "Implement content extractor",
                    "Implement validation agent"
                ],
                "deliverables": ["All core agents", "Message flows"]
            }
        ],
        "file_structure": {
            "agents/": ["document_ingester.py", "content_extractor.py"],
            "schemas/": ["messages.py", "agents.py"],
            "orchestrator/": ["message_bus.py", "router.py"]
        },
        "dependencies": [
            "fastapi>=0.104.0",
            "pydantic>=2.5.0",
            "redis>=5.0.0",
            "sqlalchemy>=2.0.0"
        ],
        "timeline_estimate": "6 weeks",
        "cost_estimate": {
            "development": {"one_time_total": 15000},
            "monthly_operational": {"total": 350}
        },
        "resource_requirements": {
            "team_composition": {
                "backend_developers": 2,
                "devops_engineer": 1
            },
            "infrastructure": {
                "cpu_cores": 4,
                "memory_gb": 8
            }
        }
    }


# Async test helpers
async def wait_for_condition(condition_func, timeout=5.0, interval=0.1):
    """Wait for a condition to become true."""
    import time
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if await condition_func():
            return True
        await asyncio.sleep(interval)
    
    return False


# Database test fixtures
@pytest.fixture
async def test_database():
    """Create test database."""
    # This would set up a test database
    # For now, just return a mock connection
    import asyncio
    mock_conn = AsyncMock()
    yield mock_conn
    # Cleanup would go here


# Redis test fixtures
@pytest.fixture
async def test_redis():
    """Create test Redis connection."""
    # This would set up a test Redis instance
    # For now, just return a mock connection
    mock_redis = AsyncMock()
    mock_redis.ping.return_value = True
    yield mock_redis
    # Cleanup would go here


# Performance testing utilities
class PerformanceTracker:
    """Track performance metrics during tests."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.checkpoints = {}
    
    def start(self):
        """Start tracking."""
        import time
        self.start_time = time.time()
    
    def stop(self):
        """Stop tracking."""
        import time
        self.end_time = time.time()
    
    def checkpoint(self, name):
        """Add a checkpoint."""
        import time
        self.checkpoints[name] = time.time()
    
    def get_duration(self):
        """Get total duration."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    def get_checkpoint_duration(self, name):
        """Get duration to a specific checkpoint."""
        if name in self.checkpoints and self.start_time:
            return self.checkpoints[name] - self.start_time
        return None


@pytest.fixture
def performance_tracker():
    """Performance tracker fixture."""
    return PerformanceTracker()


# Test data generators
def generate_test_messages(count=10):
    """Generate test messages."""
    from schemas.messages import AgentMessage, MessageType, Priority
    from uuid import uuid4
    
    messages = []
    for i in range(count):
        message = AgentMessage(
            correlation_id=uuid4(),
            agent_id=f"test_agent_{i}",
            intent=f"test_intent_{i}",
            message_type=MessageType.QUESTION_ASKED,
            payload={"test_data": f"data_{i}"},
            session_id=uuid4()
        )
        messages.append(message)
    
    return messages


def generate_test_requirements(count=5):
    """Generate test requirements."""
    from schemas.requirements import UserRequirements, StackType, Complexity
    from uuid import uuid4
    
    requirements = []
    for i in range(count):
        requirement = UserRequirements(
            requirements_id=uuid4(),
            session_id=uuid4(),
            project_description=f"Test project {i}",
            primary_goal=f"Test goal {i}",
            stack_type=StackType.PYTHON_FASTAPI,
            complexity=Complexity.MODERATE
        )
        requirements.append(requirement)
    
    return requirements


# Error simulation utilities
class ErrorSimulator:
    """Simulate various error conditions for testing."""
    
    @staticmethod
    def simulate_timeout():
        """Simulate a timeout error."""
        raise asyncio.TimeoutError("Simulated timeout")
    
    @staticmethod
    def simulate_connection_error():
        """Simulate a connection error."""
        raise ConnectionError("Simulated connection error")
    
    @staticmethod
    def simulate_validation_error():
        """Simulate a validation error."""
        raise ValueError("Simulated validation error")
    
    @staticmethod
    def simulate_rate_limit_error():
        """Simulate a rate limit error."""
        raise Exception("Rate limit exceeded")


@pytest.fixture
def error_simulator():
    """Error simulator fixture."""
    return ErrorSimulator()
