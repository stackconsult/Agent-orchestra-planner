# Omni-Stack Agentic Systems Architect

**Build production-grade automation systems with an autonomous AI engineer.**

This skill transforms Claude into an expert full-stack engineer specializing in multi-agent systems, complete applications, and production-ready codebases.

---

## 🚀 Quick Start

### Installation

1. **Download this skill**:
   ```bash
   # Clone or download the skill directory
   git clone <repo-url> omni-stack-agentic-architect
   ```

2. **Install to Claude**:
   ```bash
   # Copy to your Claude skills directory
   cp -r omni-stack-agentic-architect ~/.claude/skills/user/
   ```

3. **Start using**:
   Just chat with Claude! The skill activates automatically when you discuss:
   - Building multi-agent systems
   - Creating full-stack applications
   - Refactoring or hardening code
   - Setting up CI/CD pipelines
   - Implementing observability

---

## 💡 What This Skill Does

### 🎯 Three Core Capabilities

#### 1️⃣ Multi-Agent Automation Systems
Create self-orchestrating agent systems that coordinate via MCP servers.

**Example:**
> "Build a voice-driven ops assistant that can deploy to staging, check service status, and rollback if needed. Use MCP for GitHub and GCP access."

**You get:**
- Complete agent architecture (planner, executor, safety checker, router)
- Message schemas and communication protocols
- MCP integration code
- Safety mechanisms (approval gates, rate limits, kill switches)
- Observability (logging, metrics, dashboards)

#### 2️⃣ Full-Stack Applications
Build complete applications from frontend to database.

**Example:**
> "Create a task automation dashboard with Electron frontend, FastAPI backend, and Postgres database. Include real-time WebSocket updates."

**You get:**
- Complete folder structure (~75 files)
- Frontend (React + TypeScript + Electron)
- Backend (FastAPI + SQLAlchemy + Alembic)
- Database schema with migrations
- MCP and LLM integration
- Docker deployment configs

#### 3️⃣ Production Hardening
Refactor messy code into reliable, observable systems.

**Example:**
> "This agent system has no error handling, crashes often, and has hard-coded credentials. Make it production-ready."

**You get:**
- Refactored code with comprehensive error handling
- Retry logic with exponential backoff
- Circuit breakers for resilience
- Structured logging throughout
- Secrets management (AWS/Vault)
- CI/CD pipeline with canary deployments
- Migration plan with rollback procedures

---

## 📖 How to Use

### Basic Pattern

1. **Describe what you need** (be specific about your stack)
2. **Claude plans the approach** (shows architecture, confirms direction)
3. **Claude builds it** (creates files, writes code, adds tests)
4. **You iterate** (refine, add features, adjust)

### Example Conversations

#### Building a Multi-Agent System

```
You: I need a continuous deployment system with multiple agents:
     - Code checker validates commits
     - Test runner executes test suites
     - Deployer pushes to staging
     - Monitor watches for errors
     My stack: Python, GitHub Actions, GCP, MCP for tool access

Claude: I'll build a 4-agent orchestration system. Here's the architecture:
        [shows agent topology, message flow, MCP integration]
        
        Starting with:
        1. Agent role definitions
        2. Message schemas  
        3. Implementation code
        4. Safety mechanisms
        
        Sound good?

You: Yes, but add approval gates for production deploys

Claude: [creates complete system with approval gates]
```

#### Creating a Full-Stack App

```
You: Build a workflow automation dashboard:
     - Frontend: Electron app with React
     - Backend: Python FastAPI
     - Database: Postgres on Neon
     - Features: Create workflows, view execution history, real-time logs

Claude: I'll create a complete 3-tier app. Structure:
        - Frontend: Electron + React + TypeScript
        - Backend: FastAPI with 11 REST endpoints + WebSocket
        - Database: 9 tables with proper indexes
        
        I'll include:
        - JWT authentication
        - Real-time updates via WebSocket
        - Database migrations
        - Docker deployment
        
        Proceed?

You: Yes, and add support for multiple LLM providers

Claude: [creates complete app with Claude + OpenAI integration]
```

#### Refactoring Messy Code

```
You: This planner.py has no error handling and the executor.py crashes 
     constantly. Also need CI/CD and proper secrets management.

Claude: I'll refactor both files and add operational infrastructure:
        
        Improvements:
        1. planner.py - retry logic, circuit breaker, validation
        2. executor.py - timeout management, metrics, resource cleanup
        3. CI/CD pipeline - testing, security scanning, canary deployment
        4. Secrets - AWS Secrets Manager + Vault support
        5. Migration plan - 7 weeks with rollback procedures
        
        Each phase has rollback capability. Start?

You: Yes, show me the refactored planner first

Claude: [shows improved planner with comprehensive error handling]
```

---

## 🎨 Advanced Features

### Dry-Run Mode
Preview plans before execution:
```
You: Show me what deploying to production would do, but don't execute yet

Claude: [generates complete plan with steps, timing, costs, risks]
```

### Cost Awareness
Track and estimate cloud costs:
```
You: What will this system cost to run?

Claude: Estimated monthly costs:
        - Cloud Run: $2.50/day ($75/month)
        - Database: $15/month
        - LLM calls: ~$50/month (1000 requests)
        Total: ~$140/month
```

### Multi-Turn Context
Build iteratively without repeating yourself:
```
You: Add error handling to the planner

Claude: [adds error handling]

You: Now add metrics collection

Claude: [adds metrics, remembers context from earlier changes]

You: Update the README to document these features

Claude: [updates README based on all previous changes]
```

---

## 🛠 Technology Support

### What This Skill Works Best With

**Languages:**
- Python (FastAPI, asyncio, Pydantic)
- TypeScript/JavaScript (React, Node.js, Electron)
- SQL (Postgres, MySQL)

**Frontends:**
- React + TypeScript
- Electron for desktop apps
- Tailwind CSS, shadcn/ui

**Backends:**
- FastAPI (primary)
- Express, Next.js
- WebSocket support

**Databases:**
- Postgres (Neon, Supabase, self-hosted)
- JSONB, indexes, migrations
- Alembic, TypeORM

**Cloud/Platforms:**
- GCP (Cloud Run, Vertex AI)
- AWS (Lambda, Secrets Manager)
- Docker, Kubernetes

**AI/LLM:**
- Anthropic Claude
- OpenAI GPT
- Google Vertex AI/Gemini
- MCP server integration

---

## 📋 Best Practices

### Get Better Results

**Do:**
- ✅ Specify your exact stack upfront
- ✅ Share constraints (timeline, budget, must-haves)
- ✅ Build incrementally (MVP → features → polish)
- ✅ Ask for explanations of architecture decisions
- ✅ Request dry-run for high-stakes operations

**Don't:**
- ❌ Use vague descriptions ("build an app")
- ❌ Mix unrelated requests in one message
- ❌ Expect perfection on first attempt (iterate!)
- ❌ Skip validation of critical operations

### Example: Specific vs Vague

**Vague:**
> "Build me a chatbot"

**Specific:**
> "Build a customer support chatbot using FastAPI backend with Claude API, React frontend, Postgres for conversation history. Needs JWT auth and should handle 100 concurrent users."

The specific version will get you production-ready code immediately.

---

## 🔍 What Gets Created

### For Multi-Agent Systems

- Agent implementation files (Python classes)
- Message schema definitions (Pydantic)
- Orchestrator/router code
- MCP integration clients
- Safety mechanisms (approval gates, kill switch)
- Observability (logging, metrics)
- Database schemas for state
- Deployment configs

### For Full-Stack Apps

- Complete folder structure
- Frontend components (React + TS)
- Backend API routes (FastAPI)
- Database models and migrations
- Authentication/authorization
- Real-time features (WebSocket)
- Docker/K8s configs
- README with setup instructions

### For Refactoring/Hardening

- Refactored code files
- Error handling (exceptions, retries, circuit breakers)
- Logging infrastructure (structured, correlation IDs)
- CI/CD pipeline (GitHub Actions, GitLab CI)
- Secrets management (AWS, Vault)
- Migration plan with rollback
- Monitoring/alerting setup

---

## 🎯 When to Use This Skill

### Perfect For

- Building multi-agent automation systems
- Creating full-stack web/desktop applications
- Refactoring legacy code for production
- Adding CI/CD and observability
- Implementing secrets management
- Setting up monitoring and alerting
- Designing resilient distributed systems
- MCP server integration

### Not Ideal For

- Pure data science/ML training
- Mobile app development (iOS/Android)
- Game development
- Hardware/embedded systems
- Low-level systems programming

---

## 💬 Tips & Tricks

### Iterative Development

Start small, build up:
```
1. "Create the core agent architecture"
2. "Add error handling and retries"
3. "Now add observability"
4. "Create the CI/CD pipeline"
5. "Add secrets management"
```

### Ask for Options

When unsure:
```
You: Should I use Postgres or MongoDB for this?

Claude: For your use case (structured workflow data with relationships), 
        I recommend Postgres because:
        - Strong ACID guarantees
        - JSON support (JSONB) for flexible fields
        - Better tooling for migrations
        - Your team already knows SQL
        
        MongoDB would be better if you had truly schema-less data.
        Want to proceed with Postgres?
```

### Request Explanations

For learning:
```
You: Why did you use a circuit breaker here?

Claude: Circuit breaker prevents cascading failures:
        - After 5 failed calls to external service
        - Opens circuit (rejects new requests)
        - Waits 60s for service to recover
        - Tests with limited requests (HALF_OPEN)
        - Closes if recovery confirmed
        
        Without it, failed service could overwhelm your system.
```

---

## 📦 What's Included

```
omni-stack-agentic-architect/
├── SKILL.md              # Complete skill definition
├── README.md             # This file (user guide)
├── skill.json            # Metadata for Claude
├── examples/             # Example use cases
│   ├── multi-agent.md
│   ├── full-stack.md
│   └── refactoring.md
└── reference/            # Reference documentation
    ├── architecture-patterns.md
    ├── tech-stack-guide.md
    └── best-practices.md
```

---

## 🚦 Quick Reference

| Task | Command Example |
|------|----------------|
| **Multi-agent system** | "Build a deployment automation system with MCP integration" |
| **Full-stack app** | "Create a workflow dashboard with React, FastAPI, Postgres" |
| **Refactor code** | "Add error handling, logging, and CI/CD to this system" |
| **Check plan** | "Show me the deployment plan without executing it" |
| **Estimate costs** | "What will this cost to run on GCP?" |
| **Continue work** | "Now add metrics collection to the executor" |

---

## 📚 Learn More

- **Detailed examples**: See `examples/` directory
- **Architecture patterns**: See `reference/architecture-patterns.md`
- **Best practices**: See `reference/best-practices.md`
- **Tech stack guide**: See `reference/tech-stack-guide.md`

---

## 🆘 Troubleshooting

**Issue:** "Claude isn't using the skill"
- **Solution:** Be more specific about your task (mention "multi-agent", "full-stack", or "refactor")

**Issue:** "Code doesn't match my stack"
- **Solution:** State your exact stack upfront: "Using FastAPI 0.109, Python 3.11, Postgres 15"

**Issue:** "Too much code at once"
- **Solution:** Request incremental: "Start with just the database schema" then build up

**Issue:** "Want to see plan first"
- **Solution:** Request dry-run: "Show me what you'll build before implementing"

---

## 🎉 You're Ready!

Start with something simple:

```
You: Create a simple agent that checks GitHub PR status every 5 minutes 
     and sends Slack notifications. Use MCP for GitHub access.

Claude: [builds complete system with detailed implementation]
```

Then scale up to complex systems:

```
You: Build a multi-agent deployment pipeline with:
     - Code validation agent
     - Test execution agent  
     - Deployment agent (staging → canary → production)
     - Monitoring agent with rollback capability
     Stack: Python, GCP, GitHub, MCP servers for all tool access
     Include: Error handling, observability, CI/CD, secrets management

Claude: [builds production-grade system with all components]
```

**The skill adapts to your needs. Happy building! 🚀**
