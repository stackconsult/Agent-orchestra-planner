"""
Distributed tracing using OpenTelemetry for request tracking across agents.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from opentelemetry import trace, baggage, context
from opentelemetry.trace import Span, SpanKind, Tracer
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.propagate import inject, extract
from opentelemetry.propagators.b3 import B3MultiFormat

logger = logging.getLogger(__name__)


class OpenTelemetryTracer:
    """OpenTelemetry tracer for distributed tracing."""
    
    def __init__(self, service_name: str = "q-and-a-orchestra-agent", 
                 jaeger_endpoint: Optional[str] = None,
                 sample_rate: float = 0.1):
        self.service_name = service_name
        self.jaeger_endpoint = jaeger_endpoint
        self.sample_rate = sample_rate
        self.tracer: Optional[Tracer] = None
        self._initialized = False
    
    def initialize(self) -> None:
        """Initialize OpenTelemetry tracing."""
        try:
            # Create resource
            resource = Resource.create({
                "service.name": self.service_name,
                "service.version": "1.0.0"
            })
            
            # Create tracer provider
            tracer_provider = TracerProvider(resource=resource)
            trace.set_tracer_provider(tracer_provider)
            
            # Set up Jaeger exporter if endpoint provided
            if self.jaeger_endpoint:
                jaeger_exporter = JaegerExporter(
                    endpoint=self.jaeger_endpoint,
                    collector_endpoint=self.jaeger_endpoint,
                    insecure=True
                )
                
                span_processor = BatchSpanProcessor(jaeger_exporter)
                tracer_provider.add_span_processor(span_processor)
            
            # Get tracer
            self.tracer = trace.get_tracer(__name__)
            
            # Set propagator
            from opentelemetry import propagate
            propagate.set_global_textmap(B3MultiFormat())
            
            self._initialized = True
            logger.info(f"OpenTelemetry tracing initialized for {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize OpenTelemetry tracing: {str(e)}")
    
    def create_span(self, name: str, kind: SpanKind = SpanKind.INTERNAL) -> Span:
        """Create a new span."""
        if not self._initialized or not self.tracer:
            # Return a no-op span if not initialized
            return trace.NoOpTracer().start_span(name)
        
        return self.tracer.start_span(name, kind=kind)
    
    def start_span(self, name: str, **kwargs) -> Span:
        """Start a new span."""
        return self.create_span(name, **kwargs)
    
    def finish_span(self, span: Span) -> None:
        """Finish a span."""
        if span and span.is_recording():
            span.end()
    
    def get_current_span(self) -> Span:
        """Get the current active span."""
        return trace.get_current_span()
    
    def set_baggage(self, key: str, value: str) -> None:
        """Set baggage item."""
        baggage.set_baggage(key, value)
    
    def get_baggage(self, key: str) -> Optional[str]:
        """Get baggage item."""
        return baggage.get_baggage(key)
    
    def inject_context(self, headers: Dict[str, str]) -> None:
        """Inject tracing context into headers."""
        inject(headers)
    
    def extract_context(self, headers: Dict[str, str]) -> context.Context:
        """Extract tracing context from headers."""
        return extract(headers)


class TracingContext:
    """Context manager for tracing operations."""
    
    def __init__(self, tracer: OpenTelemetryTracer, span_name: str, 
                 span_kind: SpanKind = SpanKind.INTERNAL, **attributes):
        self.tracer = tracer
        self.span_name = span_name
        self.span_kind = span_kind
        self.attributes = attributes
        self.span: Optional[Span] = None
    
    async def __aenter__(self) -> Span:
        """Enter tracing context."""
        self.span = self.tracer.start_span(self.span_name, kind=self.span_kind)
        
        # Set attributes
        for key, value in self.attributes.items():
            self.span.set_attribute(key, value)
        
        return self.span
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit tracing context."""
        if self.span:
            if exc_type:
                self.span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc_val)))
                self.span.set_attribute("error", True)
                self.span.set_attribute("error.type", exc_type.__name__)
                self.span.set_attribute("error.message", str(exc_val))
            else:
                self.span.set_status(trace.Status(trace.StatusCode.OK))
            
            self.tracer.finish_span(self.span)


class AgentTracer:
    """Tracer specifically for agent operations."""
    
    def __init__(self, tracer: OpenTelemetryTracer, agent_id: str):
        self.tracer = tracer
        self.agent_id = agent_id
    
    async def trace_message_processing(self, message_id: UUID, message_type: str, 
                                     operation: str) -> Span:
        """Trace message processing."""
        span_name = f"{self.agent_id}.{operation}"
        
        span = await TracingContext(
            self.tracer,
            span_name,
            SpanKind.SERVER,
            agent_id=self.agent_id,
            message_id=str(message_id),
            message_type=message_type,
            operation=operation
        ).__aenter__()
        
        return span
    
    async def trace_agent_invocation(self, operation: str, **kwargs) -> Span:
        """Trace agent invocation."""
        span_name = f"{self.agent_id}.{operation}"
        
        return await TracingContext(
            self.tracer,
            span_name,
            SpanKind.INTERNAL,
            agent_id=self.agent_id,
            operation=operation,
            **kwargs
        ).__aenter__()
    
    def trace_session_activity(self, session_id: UUID, activity: str) -> TracingContext:
        """Trace session activity."""
        span_name = f"session.{activity}"
        
        return TracingContext(
            self.tracer,
            span_name,
            SpanKind.INTERNAL,
            session_id=str(session_id),
            activity=activity
        )
    
    def trace_workflow_step(self, workflow_id: UUID, step: str, step_number: int) -> TracingContext:
        """Trace workflow step."""
        span_name = f"workflow.step_{step_number}"
        
        return TracingContext(
            self.tracer,
            span_name,
            SpanKind.INTERNAL,
            workflow_id=str(workflow_id),
            step=step,
            step_number=step_number
        )


class WorkflowTracer:
    """Tracer for complete workflow orchestration."""
    
    def __init__(self, tracer: OpenTelemetryTracer):
        self.tracer = tracer
        self.active_workflows: Dict[UUID, Span] = {}
    
    async def start_workflow(self, workflow_id: UUID, workflow_type: str, 
                           user_id: Optional[str] = None) -> Span:
        """Start tracing a workflow."""
        span_name = f"workflow.{workflow_type}"
        
        span = await TracingContext(
            self.tracer,
            span_name,
            SpanKind.INTERNAL,
            workflow_id=str(workflow_id),
            workflow_type=workflow_type,
            user_id=user_id
        ).__aenter__()
        
        self.active_workflows[workflow_id] = span
        return span
    
    async def end_workflow(self, workflow_id: UUID, status: str = "completed") -> None:
        """End workflow tracing."""
        if workflow_id in self.active_workflows:
            span = self.active_workflows[workflow_id]
            span.set_attribute("workflow.status", status)
            self.tracer.finish_span(span)
            del self.active_workflows[workflow_id]
    
    def trace_workflow_phase(self, workflow_id: UUID, phase: str) -> TracingContext:
        """Trace a workflow phase."""
        span_name = f"workflow.{workflow_id}.{phase}"
        
        return TracingContext(
            self.tracer,
            span_name,
            SpanKind.INTERNAL,
            workflow_id=str(workflow_id),
            phase=phase
        )


# Decorators for automatic tracing
def trace_operation(operation_name: str, tracer: Optional[OpenTelemetryTracer] = None):
    """Decorator to automatically trace function operations."""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            # Get tracer
            current_tracer = tracer or get_global_tracer()
            if not current_tracer:
                return await func(*args, **kwargs)
            
            # Extract agent ID if available
            agent_id = None
            for arg in args:
                if hasattr(arg, 'agent_id'):
                    agent_id = arg.agent_id
                    break
            
            span_name = operation_name
            if agent_id:
                span_name = f"{agent_id}.{operation_name}"
            
            async with TracingContext(current_tracer, span_name) as span:
                # Add function-specific attributes
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)
                
                if agent_id:
                    span.set_attribute("agent.id", agent_id)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_attribute("operation.success", True)
                    return result
                except Exception as e:
                    span.set_attribute("operation.success", False)
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    raise
        
        def sync_wrapper(*args, **kwargs):
            # For synchronous functions
            current_tracer = tracer or get_global_tracer()
            if not current_tracer:
                return func(*args, **kwargs)
            
            span = current_tracer.start_span(operation_name)
            try:
                result = func(*args, **kwargs)
                span.set_attribute("operation.success", True)
                return result
            except Exception as e:
                span.set_attribute("operation.success", False)
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                raise
            finally:
                current_tracer.finish_span(span)
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def trace_agent_message(tracer: Optional[OpenTelemetryTracer] = None):
    """Decorator to trace agent message processing."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Get tracer
            current_tracer = tracer or get_global_tracer()
            if not current_tracer:
                return await func(*args, **kwargs)
            
            # Extract message information
            message = None
            agent_id = None
            
            for arg in args:
                if hasattr(arg, 'message_id') and hasattr(arg, 'agent_id'):
                    message = arg
                    agent_id = arg.agent_id
                    break
            
            if not message:
                return await func(*args, **kwargs)
            
            span_name = f"{agent_id}.process_message"
            
            async with TracingContext(
                current_tracer,
                span_name,
                SpanKind.SERVER,
                agent_id=agent_id,
                message_id=str(message.message_id),
                message_type=message.message_type.value,
                correlation_id=str(message.correlation_id)
            ) as span:
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_attribute("message.processed", True)
                    return result
                except Exception as e:
                    span.set_attribute("message.processed", False)
                    span.set_attribute("error.type", type(e).__name__)
                    raise
        
        return wrapper
    return decorator


# Global tracer instance
_global_tracer: Optional[OpenTelemetryTracer] = None


def initialize_tracing(service_name: str = "q-and-a-orchestra-agent",
                     jaeger_endpoint: Optional[str] = None,
                     sample_rate: float = 0.1) -> OpenTelemetryTracer:
    """Initialize global tracing instance."""
    global _global_tracer
    _global_tracer = OpenTelemetryTracer(service_name, jaeger_endpoint, sample_rate)
    _global_tracer.initialize()
    return _global_tracer


def get_global_tracer() -> Optional[OpenTelemetryTracer]:
    """Get the global tracer instance."""
    return _global_tracer


def get_agent_tracer(agent_id: str) -> Optional[AgentTracer]:
    """Get an agent-specific tracer."""
    global_tracer = get_global_tracer()
    if global_tracer:
        return AgentTracer(global_tracer, agent_id)
    return None


def get_workflow_tracer() -> Optional[WorkflowTracer]:
    """Get a workflow tracer."""
    global_tracer = get_global_tracer()
    if global_tracer:
        return WorkflowTracer(global_tracer)
    return None


# Utility functions
def create_trace_context(span_name: str, **attributes) -> TracingContext:
    """Create a tracing context."""
    tracer = get_global_tracer()
    if tracer:
        return TracingContext(tracer, span_name, **attributes)
    else:
        # Return a no-op context
        class NoOpContext:
            async def __aenter__(self):
                return None
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        return NoOpContext()


def correlate_spans(correlation_id: UUID) -> None:
    """Add correlation ID to current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.set_attribute("correlation_id", str(correlation_id))


def add_span_tags(**tags) -> None:
    """Add tags to current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        for key, value in tags.items():
            span.set_attribute(key, value)


def add_span_event(event_name: str, **attributes) -> None:
    """Add event to current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.add_event(event_name, attributes)


class TraceContextExtractor:
    """Extracts and manages trace context from messages."""
    
    @staticmethod
    def extract_from_headers(headers: Dict[str, str]) -> context.Context:
        """Extract trace context from HTTP headers."""
        tracer = get_global_tracer()
        if tracer:
            return tracer.extract_context(headers)
        return context.Context()
    
    @staticmethod
    def inject_to_headers(headers: Dict[str, str]) -> None:
        """Inject trace context into HTTP headers."""
        tracer = get_global_tracer()
        if tracer:
            tracer.inject_context(headers)
    
    @staticmethod
    def extract_from_message(message: Any) -> context.Context:
        """Extract trace context from agent message."""
        headers = {}
        
        # Try to get trace info from message
        if hasattr(message, 'correlation_id'):
            headers["x-correlation-id"] = str(message.correlation_id)
        
        return TraceContextExtractor.extract_from_headers(headers)
    
    @staticmethod
    def inject_to_message(message: Any) -> None:
        """Inject trace context into agent message."""
        headers = {}
        TraceContextExtractor.inject_to_headers(headers)
        
        # Store trace info in message if possible
        if hasattr(message, 'trace_context'):
            message.trace_context = headers
