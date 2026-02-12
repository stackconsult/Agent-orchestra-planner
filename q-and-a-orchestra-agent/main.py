"""
Main entry point for the Q&A Orchestra Agent system.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from uuid import UUID

from anthropic import AsyncAnthropic

from orchestrator.orchestrator import OrchestraOrchestrator
from integrations.repo_reader import UnifiedRepositoryReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global orchestrator instance
orchestrator: Optional[OrchestraOrchestrator] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global orchestrator
    
    # Startup
    try:
        # Initialize Anthropic client
        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        if not anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is required")
        
        anthropic_client = AsyncAnthropic(api_key=anthropic_api_key)
        
        # Initialize repository reader
        repo_reader = UnifiedRepositoryReader(
            local_repo_path=os.getenv("LOCAL_REPO_PATH"),
            github_token=os.getenv("GITHUB_TOKEN"),
            repo_owner=os.getenv("GITHUB_REPO_OWNER"),
            repo_name=os.getenv("GITHUB_REPO_NAME")
        )
        
        # Initialize orchestrator
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        orchestrator = OrchestraOrchestrator(anthropic_client, redis_url)
        
        # Connect to services
        await repo_reader.connect()
        await orchestrator.start()
        
        logger.info("Q&A Orchestra Agent started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        raise
    
    # Shutdown
    try:
        if orchestrator:
            await orchestrator.stop()
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")


# Create FastAPI app
app = FastAPI(
    title="Q&A Orchestra Agent",
    description="Production-grade meta-agent system for designing agent orchestras",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models for API
class SessionCreateRequest(BaseModel):
    user_id: Optional[str] = None
    metadata: Optional[dict] = None


class UserInputRequest(BaseModel):
    session_id: UUID
    user_input: str
    input_type: str = "text"


class RefinementRequest(BaseModel):
    session_id: UUID
    refinement_type: str
    description: str


class SessionResponse(BaseModel):
    session_id: UUID
    status: str
    message: str


class UserInputResponse(BaseModel):
    status: str
    message: str
    agent_response: Optional[str] = None


# API endpoints
@app.post("/api/v1/sessions", response_model=SessionResponse)
async def create_session(request: SessionCreateRequest):
    """Create a new design session."""
    try:
        session_id = await orchestrator.start_design_session(
            user_id=request.user_id,
            metadata=request.metadata or {}
        )
        
        return SessionResponse(
            session_id=session_id,
            status="created",
            message="Design session created successfully"
        )
    except Exception as e:
        logger.error(f"Failed to create session: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/sessions/{session_id}/input", response_model=UserInputResponse)
async def process_user_input(session_id: UUID, request: UserInputRequest):
    """Process user input in a session."""
    try:
        response = await orchestrator.process_user_input(
            session_id=session_id,
            user_input=request.user_input,
            input_type=request.input_type
        )
        
        return UserInputResponse(
            status=response.get("status", "processed"),
            message=response.get("message", "Input processed"),
            agent_response=response.get("agent_response")
        )
    except Exception as e:
        logger.error(f"Failed to process user input: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/sessions/{session_id}/refine", response_model=dict)
async def request_refinement(session_id: UUID, request: RefinementRequest):
    """Request a refinement to the current design."""
    try:
        response = await orchestrator.request_refinement(
            session_id=session_id,
            refinement_type=request.refinement_type,
            description=request.description
        )
        
        return response
    except Exception as e:
        logger.error(f"Failed to request refinement: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/sessions/{session_id}", response_model=dict)
async def get_session_status(session_id: UUID):
    """Get session status and information."""
    try:
        status = await orchestrator.get_session_status(session_id)
        return status
    except Exception as e:
        logger.error(f"Failed to get session status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/sessions/{session_id}/history", response_model=list)
async def get_conversation_history(session_id: UUID, limit: Optional[int] = None):
    """Get conversation history for a session."""
    try:
        history = await orchestrator.get_conversation_history(session_id, limit)
        return history
    except Exception as e:
        logger.error(f"Failed to get conversation history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/sessions/{session_id}", response_model=dict)
async def end_session(session_id: UUID):
    """End a session and get summary."""
    try:
        summary = await orchestrator.end_session(session_id)
        return summary
    except Exception as e:
        logger.error(f"Failed to end session: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/system/status", response_model=dict)
async def get_system_status():
    """Get overall system status."""
    try:
        status = await orchestrator.get_system_status()
        return status
    except Exception as e:
        logger.error(f"Failed to get system status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "q-and-a-orchestra-agent"}


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Q&A Orchestra Agent",
        "version": "1.0.0",
        "description": "Production-grade meta-agent system for designing agent orchestras",
        "endpoints": {
            "create_session": "POST /api/v1/sessions",
            "process_input": "POST /api/v1/sessions/{session_id}/input",
            "get_status": "GET /api/v1/sessions/{session_id}",
            "get_history": "GET /api/v1/sessions/{session_id}/history",
            "request_refinement": "POST /api/v1/sessions/{session_id}/refine",
            "end_session": "DELETE /api/v1/sessions/{session_id}",
            "system_status": "GET /api/v1/system/status",
            "health": "GET /api/v1/health"
        }
    }


def main():
    """Main entry point for running the application."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Q&A Orchestra Agent")
    parser.add_argument("--host", default=os.getenv("API_HOST", "0.0.0.0"), help="Host to bind to")
    parser.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "8000")), help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"), help="Log level")
    
    args = parser.parse_args()
    
    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    
    logger.info(f"Starting Q&A Orchestra Agent on {args.host}:{args.port}")
    
    uvicorn.run(
        "main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level.lower()
    )


if __name__ == "__main__":
    main()
