# Technology Stack Guide

Quick reference for technologies used by this skill.

---

## Python Backend

### FastAPI (Primary)
```python
from fastapi import FastAPI, Depends
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    name: str
    price: float

@app.post("/items")
async def create_item(item: Item):
    return {"id": 1, **item.dict()}
```

**When to use:** REST APIs, async workloads, type safety  
**Alternatives:** Flask (simpler), Django (full-featured)

### SQLAlchemy 2.0
```python
from sqlalchemy import Column, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

engine = create_async_engine("postgresql+asyncpg://...")

class User(Base):
    __tablename__ = "users"
    id = Column(UUID, primary_key=True)
    email = Column(String, unique=True)
```

**When to use:** Database ORM, complex queries  
**Key features:** Async support, migrations (Alembic), type hints

### Pydantic
```python
from pydantic import BaseModel, Field, validator

class Config(BaseModel):
    api_key: SecretStr
    timeout: int = Field(ge=1, le=300)
    
    @validator('api_key')
    def validate_key(cls, v):
        if not v.get_secret_value().startswith('sk-'):
            raise ValueError('Invalid API key')
        return v
```

**When to use:** Input validation, config management, API schemas

---

## TypeScript Frontend

### React + TypeScript
```typescript
interface Props {
  name: string;
  onUpdate: (value: string) => void;
}

export const Component: React.FC<Props> = ({ name, onUpdate }) => {
  const [value, setValue] = useState(name);
  
  return (
    <input 
      value={value}
      onChange={(e) => {
        setValue(e.target.value);
        onUpdate(e.target.value);
      }}
    />
  );
};
```

**When to use:** Web UIs, component-based architecture  
**Key libraries:** React Query, Zustand, Tailwind CSS

### Electron
```typescript
import { app, BrowserWindow } from 'electron';

function createWindow() {
  const win = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  });
  
  win.loadFile('index.html');
}

app.whenReady().then(createWindow);
```

**When to use:** Desktop applications, native OS integration  
**Combines:** Web technologies + native capabilities

---

## Databases

### Postgres
```sql
-- JSONB for flexible data
CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    config JSONB NOT NULL,
    tags TEXT[]
);

-- GIN index for JSONB queries
CREATE INDEX idx_config ON workflows USING GIN(config);

-- Query JSONB
SELECT * FROM workflows WHERE config @> '{"type": "deploy"}';
```

**When to use:** Primary database for most applications  
**Key features:** JSONB, arrays, full-text search, triggers

### Neon
```
Serverless Postgres
- Autoscaling (scale to zero)
- Branch per PR (like git)
- Connection pooling
- Generous free tier

Connection string:
postgresql://user:pass@ep-xyz.neon.tech/dbname
```

**When to use:** Postgres without managing servers  
**Alternative:** Supabase (Postgres + auth + storage)

---

## Cloud Platforms

### GCP
```bash
# Cloud Run (serverless containers)
gcloud run deploy api \
  --image gcr.io/project/api:latest \
  --platform managed \
  --region us-central1

# Vertex AI (LLMs)
from vertexai.preview.language_models import ChatModel
chat = ChatModel.from_pretrained("chat-bison")
```

**When to use:** Google's LLMs (Gemini), BigQuery, Cloud Run  
**Key services:** Cloud Run, Vertex AI, GCS, BigQuery

### AWS
```bash
# Lambda functions
aws lambda create-function \
  --function-name my-function \
  --runtime python3.11

# Secrets Manager
aws secretsmanager get-secret-value \
  --secret-id production/api-key
```

**When to use:** Lambda, S3, comprehensive services  
**Key services:** Lambda, S3, Secrets Manager, RDS

---

## LLM Providers

### Anthropic Claude
```python
import anthropic

client = anthropic.Anthropic(api_key="sk-ant-...")

response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1000,
    messages=[
        {"role": "user", "content": "Hello!"}
    ]
)
```

**When to use:** Best reasoning, long context, function calling  
**Models:** Opus (most capable), Sonnet (balanced), Haiku (fast)

### OpenAI
```python
from openai import OpenAI

client = OpenAI(api_key="sk-...")

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "user", "content": "Hello!"}
    ]
)
```

**When to use:** Embeddings, vision, broad ecosystem  
**Models:** GPT-4o (multimodal), GPT-4 (reasoning), GPT-3.5 (fast)

---

## MCP Integration

### MCP Client
```python
import httpx

class MCPClient:
    def __init__(self, server_url: str):
        self.server_url = server_url
        self.client = httpx.AsyncClient()
    
    async def list_tools(self):
        response = await self.client.get(f"{self.server_url}/tools")
        return response.json()["tools"]
    
    async def call_tool(self, name: str, params: dict):
        response = await self.client.post(
            f"{self.server_url}/tools/{name}",
            json=params
        )
        return response.json()
```

**When to use:** External tool access (GitHub, GCP, Slack, etc)  
**Common servers:** GitHub, GCP, Asana, Salesforce

---

## Development Tools

### Testing
```python
# pytest
pip install pytest pytest-asyncio pytest-cov

# Run tests
pytest tests/ -v --cov=app

# Integration tests
pytest tests/integration/ --db=postgresql://test
```

### Linting
```bash
# Python
black .           # Formatter
isort .           # Import sorting
flake8 .          # Linter
mypy .            # Type checker

# TypeScript
eslint src/       # Linter
tsc --noEmit      # Type checker
```

### CI/CD
```yaml
# GitHub Actions
- uses: actions/setup-python@v5
  with:
    python-version: '3.11'

- run: pytest tests/

# GitLab CI
test:
  script:
    - pytest tests/
  coverage: '/TOTAL.*\s+(\d+%)$/'
```

---

## Monitoring

### Structured Logging
```python
import structlog

logger = structlog.get_logger()

logger.info(
    "user_login",
    user_id="123",
    ip_address="1.2.3.4",
    duration_ms=45
)
```

### Prometheus Metrics
```python
from prometheus_client import Counter, Histogram

requests_total = Counter('requests_total', 'Total requests')
request_duration = Histogram('request_duration_seconds', 'Request duration')

requests_total.inc()
request_duration.observe(0.234)
```

---

## Quick Stack Recommendations

### Startup MVP
```
Frontend: React + Vite
Backend: FastAPI
Database: Neon (free tier)
Hosting: Vercel (frontend) + Cloud Run (backend)
LLM: Claude Sonnet
```

### Production SaaS
```
Frontend: React + TypeScript + Electron
Backend: FastAPI + async workers
Database: Neon Pro or RDS Postgres
Cache: Redis
Hosting: GCP Cloud Run or AWS ECS
LLM: Claude Opus (primary) + GPT-4 (fallback)
Monitoring: Prometheus + Grafana
```

### Enterprise
```
Frontend: React + TypeScript
Backend: FastAPI + microservices
Database: Aurora Postgres (multi-region)
Message Bus: Kafka or RabbitMQ
Cache: Redis Cluster
Hosting: Kubernetes (GKE or EKS)
LLM: Self-hosted + API fallback
Monitoring: Datadog or New Relic
```

---

All recommendations based on production deployments by this skill.
