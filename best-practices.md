# Best Practices

Guidelines for building production-grade systems.

---

## Code Quality

### Always Include

- ✅ Input validation (Pydantic, TypeScript interfaces)
- ✅ Error handling (try/catch, custom exceptions)
- ✅ Type hints/annotations
- ✅ Docstrings for public APIs
- ✅ Tests (unit + integration)

### Never Do

- ❌ Hard-code secrets
- ❌ Skip error handling
- ❌ Ignore type safety
- ❌ Leave TODO in production code
- ❌ Log sensitive data

---

## Error Handling

### Exception Hierarchy

```python
class AppException(Exception):
    """Base for all app exceptions"""
    pass

class ValidationError(AppException):
    """Invalid input"""
    pass

class ExternalServiceError(AppException):
    """External service failed"""
    pass

# Catch specific exceptions
try:
    result = await external_service()
except ValidationError as e:
    logger.error("validation_failed", error=str(e))
    raise HTTPException(400, detail=str(e))
except ExternalServiceError as e:
    logger.error("service_failed", error=str(e))
    raise HTTPException(503, detail="Service unavailable")
```

### Retry Logic

```python
# Always use retry for transient failures
@retry_with_backoff(
    max_attempts=3,
    initial_delay=1.0,
    exceptions=(ConnectionError, TimeoutError)
)
async def call_api():
    pass

# Don't retry for permanent failures
# (400 Bad Request, 401 Unauthorized, 404 Not Found)
```

### Timeouts

```python
# Always set timeouts
async with httpx.AsyncClient(timeout=30.0) as client:
    response = await client.get(url)

# Or per-request
result = await asyncio.wait_for(operation(), timeout=60.0)
```

---

## Security

### Secrets Management

```python
# ✅ Good: Use secret manager
config = SecretConfig(provider="aws")
api_key = await config.get("api-key")

# ❌ Bad: Hard-coded
api_key = "sk-hardcoded-secret"

# ❌ Bad: Environment variable in production
api_key = os.getenv("API_KEY")  # Development only!
```

### Input Validation

```python
# ✅ Good: Validate everything
class CreateUser(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    age: int = Field(ge=18, le=120)

# ❌ Bad: Trust user input
def create_user(email, password, age):
    # No validation!
    pass
```

### Authentication

```python
# ✅ Good: JWT with refresh tokens
access_token = create_token(user_id, expires=30*60)  # 30 min
refresh_token = create_token(user_id, expires=7*24*3600)  # 7 days

# ❌ Bad: Long-lived tokens
token = create_token(user_id, expires=365*24*3600)  # 1 year!
```

---

## Database

### Migrations

```python
# ✅ Good: Use migration tool
alembic revision --autogenerate -m "Add user table"
alembic upgrade head

# ❌ Bad: Manual SQL changes
# Running scripts directly on production DB
```

### Indexes

```sql
-- ✅ Good: Index frequently queried columns
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_logs_timestamp ON logs(timestamp DESC);

-- For JSONB
CREATE INDEX idx_config ON table USING GIN(config);

-- ❌ Bad: No indexes (slow queries)
```

### Queries

```python
# ✅ Good: Use ORM with proper joins
users = await session.execute(
    select(User)
    .join(Profile)
    .where(User.active == True)
    .limit(100)
)

# ❌ Bad: N+1 queries
users = await session.execute(select(User))
for user in users:
    profile = await session.get(Profile, user.profile_id)  # N+1!
```

---

## Logging

### Structured Logging

```python
# ✅ Good: Structured with context
logger.info(
    "user_created",
    user_id=user.id,
    email=user.email,
    correlation_id=correlation_id,
    duration_ms=45
)

# ❌ Bad: String interpolation
logger.info(f"User {user.id} created")  # Can't query/filter
```

### Log Levels

```text
DEBUG: Detailed diagnostic info (dev only)
INFO: General informational messages
WARNING: Warning messages (degraded but working)
ERROR: Error messages (operation failed)
CRITICAL: Critical errors (system failing)
```

### What to Log

```python
# ✅ Always log
- Operation start/complete with duration
- Errors with full context
- Security events (auth, access)
- State changes

# ❌ Never log
- Passwords or secrets
- Full credit card numbers
- PII without consent
- Excessive debug in production
```

---

## Testing

### Test Pyramid

```text
      E2E (few)
    /           \
   Integration (some)
  /                   \
 Unit Tests (many)
```

### Unit Tests

```python
# ✅ Good: Test one thing
def test_user_validation():
    with pytest.raises(ValidationError):
        User(email="invalid")

# ✅ Good: Use fixtures
@pytest.fixture
def user():
    return User(email="test@example.com")

def test_user_name(user):
    assert user.email == "test@example.com"
```

### Integration Tests

```python
# ✅ Good: Test with real DB
@pytest.mark.integration
async def test_create_user(test_db):
    user = await create_user(test_db, email="test@example.com")
    assert user.id is not None
```

### Mocking

```python
# ✅ Good: Mock external services
@pytest.fixture
def mock_llm():
    with patch('app.llm_client') as mock:
        mock.complete.return_value = "response"
        yield mock

async def test_with_mock(mock_llm):
    result = await my_function()
    mock_llm.complete.assert_called_once()
```

---

## Performance

### Async Operations

```python
# ✅ Good: Concurrent operations
results = await asyncio.gather(
    fetch_user(1),
    fetch_user(2),
    fetch_user(3)
)

# ❌ Bad: Sequential
results = []
for i in [1, 2, 3]:
    result = await fetch_user(i)  # Slow!
    results.append(result)
```

### Caching

```python
# ✅ Good: Cache expensive operations
@lru_cache(maxsize=1000)
def expensive_function(arg):
    return complex_calculation(arg)

# Or use Redis for distributed cache
cache_key = f"user:{user_id}"
cached = await redis.get(cache_key)
if not cached:
    data = await fetch_user(user_id)
    await redis.setex(cache_key, 300, json.dumps(data))
```

### Database Performance

```python
# ✅ Good: Connection pooling
engine = create_async_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=10
)

# ✅ Good: Pagination
SELECT * FROM users LIMIT 100 OFFSET 0;

# ❌ Bad: Load everything
SELECT * FROM users;  # Could be millions!
```

---

## Deployment

### Environment Config

```python
# ✅ Good: Environment-based config
class Settings(BaseSettings):
    database_url: str
    secret_key: SecretStr
    
    class Config:
        env_file = ".env"

settings = Settings()

# ❌ Bad: Hard-coded config
DATABASE_URL = "postgresql://localhost/db"
```

### Health Checks

```python
# ✅ Good: Implement health endpoint
@app.get("/health")
async def health_check():
    try:
        await db.execute("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(503, detail=str(e))
```

### Graceful Shutdown

```python
# ✅ Good: Handle signals
import signal

def signal_handler(signum, frame):
    logger.info("shutdown_requested")
    cleanup_resources()
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
```

---

## Monitoring

### Metrics to Track

```text
- Request rate (requests/second)
- Error rate (errors/total)
- Response time (P50, P95, P99)
- Active connections
- Queue depth
- Resource usage (CPU, memory, disk)
```

### Alerts to Set

```text
CRITICAL: Error rate > 5%
CRITICAL: P95 latency > 1000ms
WARNING: Error rate > 1%
WARNING: P95 latency > 500ms
INFO: Deployment completed
```

### Dashboards

```text
Operations Dashboard:
- Request rate over time
- Error rate over time
- P95 latency over time
- Active users

Business Dashboard:
- Daily active users
- Workflows created/executed
- Success rate
- Cost per execution
```

---

## Cost Optimization

### Before Building

- Estimate costs for expected load
- Set budget alerts
- Choose appropriate instance sizes

### During Development

- Use free tiers for dev/staging
- Scale to zero when not in use
- Mock expensive services in tests

### In Production

- Monitor actual costs daily
- Optimize expensive queries
- Cache aggressively
- Use spot instances where possible
- Auto-scale based on load

---

## Documentation

### Code Documentation

```python
# ✅ Good: Clear docstrings
async def create_user(email: str, password: str) -> User:
    """
    Create a new user account.
    
    Args:
        email: User's email address
        password: Plain text password (will be hashed)
    
    Returns:
        Created user object
    
    Raises:
        ValidationError: If email is invalid
        DuplicateError: If email already exists
    """
    pass
```

### README Template

```markdown
# Project Name

## Quick Start
1. Install dependencies
2. Set environment variables
3. Run migrations
4. Start server

## Development
- Run tests: `pytest tests/`
- Lint: `flake8 .`
- Format: `black .`

## Deployment
- Staging: `./deploy.sh staging`
- Production: `./deploy.sh production`

## Environment Variables
- DATABASE_URL: Postgres connection string
- SECRET_KEY: JWT secret
- API_KEY: External API key
```

---

These practices are applied consistently in all systems built by this skill.
