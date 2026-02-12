# Q&A Orchestra Agent

A production-grade meta-agent system that helps design and plan agent orchestras through conversational Q&A.

## Overview

The Q&A Orchestra agent consists of 5 specialized agents working together to help users design production-grade agent orchestras by:

1. **Repository Analysis Agent** - Reads and understands architecture patterns from the repo
2. **Requirements Extraction Agent** - Conducts conversational Q&A to extract user requirements  
3. **Architecture Designer Agent** - Applies patterns to design custom agent orchestras
4. **Implementation Planner Agent** - Generates phased implementation plans with cost estimates
5. **Validator Agent** - Reviews designs against best practices and safety mechanisms

## Technology Stack

- **Backend**: Python 3.11+ with FastAPI
- **Message Bus**: Redis for event-driven communication
- **Database**: Postgres on Neon for conversation history
- **MCP Integration**: GitHub MCP for reading repo files
- **LLM**: Claude (Anthropic) for natural language understanding
- **Observability**: Prometheus + Grafana

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up infrastructure
docker-compose up -d  # Redis, Postgres

# 3. Configure environment
cp .env.example .env
# Edit .env with your API keys

# 4. Run migrations
alembic upgrade head

# 5. Start the agent system
python -m q_and_a_orchestra_agent.main

# 6. Access the API
curl http://localhost:8000/health
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed system architecture.

## Usage

```python
from q_and_a_orchestra_agent import OrchestraPlanner

planner = OrchestraPlanner()

# Start a design session
session = await planner.start_session()
response = await planner.ask_question(session.id, "I need an agent system for real estate document processing")
print(response.response)
```

## Development

- Run tests: `pytest tests/`
- Format code: `black .`
- Lint: `flake8 .`
- Type check: `mypy .`

## Deployment

See [docs/deployment-guide.md](docs/deployment-guide.md) for production deployment instructions.

## License

MIT License - see LICENSE file for details.
