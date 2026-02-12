# Architecture Patterns Reference

Quick reference for common patterns used by this skill.

---

## Multi-Agent Systems

### Event-Driven Architecture
```
Producer Agent → Message Bus → Consumer Agent(s)
  
Benefits:
- Loose coupling between agents
- Async processing
- Natural parallelism
- Easy to add new agents

Tools: Redis Pub/Sub, RabbitMQ, Kafka
```

### Agent Communication Protocol
```python
class AgentMessage(BaseModel):
    message_id: UUID
    correlation_id: UUID
    from_agent: str
    to_agent: str
    message_type: str
    payload: Dict[str, Any]
    timestamp: datetime
```

### Agent Roles Pattern
```
Intent Parser → Planner → Safety Checker → Router → Executors → Reporter

Each agent has single responsibility
Communication via structured messages
State managed in database or message metadata
```

---

## Reliability Patterns

### Retry with Exponential Backoff
```python
@retry_with_backoff(
    max_attempts=3,
    initial_delay=1.0,
    exponential_base=2.0,
    exceptions=(ConnectionError, TimeoutError)
)
async def call_external_service():
    # Retries: 1s, 2s, 4s with jitter
    pass
```

### Circuit Breaker
```
States: CLOSED → OPEN → HALF_OPEN

CLOSED: Normal operation
  ↓ (5 failures)
OPEN: Reject requests for 60s
  ↓ (timeout elapsed)
HALF_OPEN: Test with limited requests
  ↓ (success)
CLOSED: Resume normal operation
```

### Timeout Management
```python
result = await asyncio.wait_for(
    operation(),
    timeout=30.0
)
```

---

## Data Patterns

### Database Schema Design
```sql
-- Proper indexes
CREATE INDEX idx_table_field ON table(field);
CREATE INDEX idx_table_composite ON table(field1, field2);
CREATE INDEX idx_table_jsonb ON table USING GIN(jsonb_field);

-- Foreign keys with cascades
ALTER TABLE child ADD CONSTRAINT fk_parent
  FOREIGN KEY (parent_id) REFERENCES parent(id)
  ON DELETE CASCADE;

-- Triggers for auto-update
CREATE TRIGGER update_timestamp
  BEFORE UPDATE ON table
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
```

### Migration Strategy
```
Alembic (Python) or TypeORM (TS)

Commands:
- alembic revision --autogenerate -m "description"
- alembic upgrade head
- alembic downgrade -1

Always test: upgrade → downgrade → upgrade
```

---

## API Design

### REST Endpoints
```
GET    /resources          List (with pagination, filters)
POST   /resources          Create
GET    /resources/{id}     Get details
PUT    /resources/{id}     Update (full)
PATCH  /resources/{id}     Update (partial)
DELETE /resources/{id}     Delete

POST   /resources/{id}/action  Actions (start, stop, etc)
```

### WebSocket Pattern
```python
class ConnectionManager:
    active_connections: List[WebSocket]
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)

# Broadcast on events
await manager.broadcast({
    "type": "workflow_updated",
    "data": workflow_dict
})
```

---

## Security Patterns

### Secrets Management
```
Development: Environment variables
Staging: AWS Secrets Manager / Vault
Production: AWS Secrets Manager / Vault + rotation

Never:
- Hard-code in source
- Commit to git
- Log secret values
```

### Authentication
```
JWT with refresh tokens:

Access token: 30 min expiry (in header)
Refresh token: 7 days expiry (HttpOnly cookie)

On access token expiry:
- Use refresh token to get new access token
- Continue without re-login
```

### Input Validation
```python
class RequestModel(BaseModel):
    field: str = Field(..., min_length=1, max_length=100)
    
    @validator('field')
    def validate_field(cls, v):
        if not v.strip():
            raise ValueError('Cannot be empty')
        return v.strip()
```

---

## Observability Patterns

### Structured Logging
```python
logger.info(
    "operation_completed",
    operation="deploy",
    duration_seconds=45.2,
    correlation_id=correlation_id,
    status="success"
)
```

### Metrics Collection
```python
# Counters
operations_total.inc()
operations_failed.inc()

# Histograms
operation_duration.observe(duration)

# Gauges
active_workers.set(count)
```

### Correlation IDs
```
User Request → correlation_id: abc-123

All logs/events include correlation_id
Trace request across all services
Essential for debugging distributed systems
```

---

## Deployment Patterns

### Canary Deployment
```
1. Deploy v2 to 10% of traffic
2. Monitor for 5 minutes
   - Error rate < 1%
   - P95 latency unchanged
   - No crashes
3. If healthy: full rollout
4. If unhealthy: automatic rollback
```

### Database Migration
```
1. Backup database
2. Run migration (with CONCURRENTLY for indexes)
3. Test critical queries
4. Monitor for 5 minutes
5. If issues: rollback (downgrade -1 or restore)
```

### Zero-Downtime Deployment
```
Rolling update:
- New pods start
- Health checks pass
- Traffic shifts gradually
- Old pods terminate

Readiness probe ensures no traffic to unhealthy pods
```

---

## Cost Optimization

### Estimate Before Execute
```python
cost_estimate = {
    "database_storage": calculate_storage_cost(),
    "compute_hours": estimate_compute_cost(),
    "api_calls": count_api_calls() * cost_per_call,
    "total": sum_costs()
}

if cost_estimate["total"] > budget:
    raise BudgetExceededError()
```

### Common Optimizations
- Cache LLM responses
- Use smaller models for simple tasks
- Batch operations
- Connection pooling
- Lazy loading
- Autoscaling (scale to zero when idle)

---

## Testing Patterns

### Unit Tests
```python
@pytest.fixture
def mock_llm_client():
    return Mock(spec=LLMClient)

async def test_planner_success(mock_llm_client):
    planner = Planner(llm_client=mock_llm_client)
    result = await planner.create_plan(request)
    assert result.status == "success"
```

### Integration Tests
```python
@pytest.mark.integration
async def test_workflow_execution(test_db):
    # Real database, mocked external services
    workflow = create_workflow(test_db)
    execution = await start_workflow(workflow.id)
    assert execution.status == "success"
```

### E2E Tests
```python
@pytest.mark.e2e
async def test_full_workflow():
    # Real everything
    response = await client.post("/api/v1/workflows", json=workflow_data)
    workflow_id = response.json()["id"]
    
    execution = await client.post(f"/api/v1/workflows/{workflow_id}/start")
    
    # Poll for completion
    await wait_for_completion(execution["id"])
```

---

These patterns appear consistently in production-grade systems built by this skill.
