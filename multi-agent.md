# Multi-Agent System Example

## Voice-Driven DevOps Assistant

### User Request
> "Build a continuous voice-driven ops assistant that listens for commands like 'deploy to staging', 'check service status', 'rollback last deployment'. Use MCP servers for GitHub and GCP access. Include safety mechanisms and observability."

### What Claude Delivers

**1. Agent Architecture** (7 agents)
- Intent Parser: Parses voice commands into structured intents
- Planner: Creates execution plans from intents
- Safety Checker: Validates plans against safety rules
- Router: Dispatches tasks to appropriate executors
- GitHub Executor: Handles GitHub operations via MCP
- GCP Executor: Handles GCP operations via MCP
- Reporter: Generates status reports

**2. Communication Protocol**
- Event-driven message bus (Redis)
- Structured message schemas (Pydantic)
- Correlation IDs for request tracing
- Priority-based message queuing

**3. Safety Mechanisms**
- Approval gates for production deployments
- Rate limits (10 deploys/hour, 5 rollbacks/hour)
- Concurrent operation limits (max 3)
- Kill switch for emergency shutdown
- Operation timeouts

**4. Observability**
- Structured logging with correlation IDs
- Prometheus metrics (success rate, duration, counts)
- Distributed tracing across agents
- Agent health monitoring dashboard

**5. Files Created** (~1500 lines total)
```
outputs/
├── architecture.md        # Complete system design
├── schemas.py            # 11 Pydantic message models
├── intent_parser.py      # Intent parsing agent
├── planner_safety.py     # Planner + safety checker
├── router_executor.py    # Router + sample executor
└── deployment_config.md  # Production deployment guide
```

### Key Conversation Flow

```
User: "Deploy to staging"
  ↓
Intent Parser: {intent: "deploy", environment: "staging"}
  ↓
Planner: [validate_code, run_tests, build_artifact, deploy]
  ↓
Safety Checker: ✓ Low risk, no approval needed
  ↓
Router: Dispatches to GitHub & GCP executors
  ↓
Executors: Execute each step with retries
  ↓
Reporter: "Deployment to staging completed in 4m 32s"
```

### Cost Estimate
```
Monthly costs:
- GCP Cloud Run: $75/month (assuming 1000 deployments)
- Redis (managed): $15/month
- Database (Neon): $25/month
- LLM API calls: $30/month (voice parsing)
Total: ~$145/month
```

### Dry-Run Example

**User:** "Show me what would happen if I deploy to production"

**Claude generates:**
```yaml
Execution Plan (DRY RUN):
  Step 1: Validate code (30s)
    - Run linters
    - Check test coverage
    - Security scan
  
  Step 2: Run test suite (5m)
    - Unit tests: 450 tests
    - Integration tests: 120 tests
    
  Step 3: Build artifact (3m)
    - Docker build
    - Push to registry
    
  Step 4: Deploy to production (2m)
    - **APPROVAL REQUIRED**
    - Canary: 10% traffic for 5 minutes
    - Full rollout if metrics healthy
    
  Step 5: Notify team (5s)
    - Slack #deployments
    - Email to on-call
    
Total estimated time: 10m 35s
Estimated cost: $0.15 (compute) + $0.02 (LLM)
Risk level: HIGH (production deployment)
```

**User approves → Actual execution proceeds**

### Multi-Turn Context Example

```
User: Build the voice ops system

Claude: [Creates complete system]

User: Add rollback capability

Claude: [Adds rollback logic to planner, executor, and updates schemas - 
        remembers all previously created files and architecture]

User: Now add cost tracking for each deployment

Claude: [Adds cost estimation to planner, tracking to executor, 
        reporting to notifier - maintains full context]

User: Update the architecture doc to reflect these changes

Claude: [Updates architecture.md with rollback and cost tracking features]
```

### Production Deployment

```bash
# 1. Set up infrastructure
docker-compose up -d  # Redis, Postgres

# 2. Configure MCP servers
export MCP_GITHUB_URL="https://mcp.github.com"
export MCP_GCP_URL="https://mcp.gcp.com"
export GITHUB_TOKEN="ghp_..."
export GCP_TOKEN="..."

# 3. Run migrations
cd backend && alembic upgrade head

# 4. Start agents
python -m agents.orchestrator

# 5. Monitor
curl localhost:9090/metrics  # Prometheus metrics
```

### What Makes This Production-Ready

✅ **Error Handling**: Retry logic, circuit breakers, timeouts  
✅ **Safety**: Approval gates, rate limits, kill switch  
✅ **Observability**: Structured logs, metrics, tracing  
✅ **Scalability**: Event-driven, stateless agents  
✅ **Reliability**: Graceful degradation, fallback plans  
✅ **Security**: Secrets management, input validation  

This is a **real, deployable system**, not a demo.
