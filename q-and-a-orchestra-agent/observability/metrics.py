"""
Metrics collection using Prometheus for monitoring system performance.
"""

import asyncio
import time
from typing import Dict, List, Optional
from uuid import UUID

from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response

logger = __import__('logging').getLogger(__name__)


class PrometheusMetrics:
    """Prometheus metrics collector for the Q&A Orchestra Agent."""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        
        # Counters
        self.messages_total = Counter(
            'orchestra_messages_total',
            'Total number of messages processed',
            ['agent_id', 'message_type', 'status'],
            registry=self.registry
        )
        
        self.sessions_total = Counter(
            'orchestra_sessions_total',
            'Total number of sessions created',
            ['status'],
            registry=self.registry
        )
        
        self.agent_invocations_total = Counter(
            'orchestra_agent_invocations_total',
            'Total number of agent invocations',
            ['agent_id', 'operation'],
            registry=self.registry
        )
        
        self.errors_total = Counter(
            'orchestra_errors_total',
            'Total number of errors',
            ['agent_id', 'error_type', 'operation'],
            registry=self.registry
        )
        
        # Histograms
        self.message_processing_duration = Histogram(
            'orchestra_message_processing_duration_seconds',
            'Time spent processing messages',
            ['agent_id', 'message_type'],
            registry=self.registry
        )
        
        self.session_duration = Histogram(
            'orchestra_session_duration_seconds',
            'Duration of sessions',
            ['phase'],
            registry=self.registry
        )
        
        self.agent_response_time = Histogram(
            'orchestra_agent_response_time_seconds',
            'Agent response time',
            ['agent_id', 'operation'],
            registry=self.registry
        )
        
        # Gauges
        self.active_sessions = Gauge(
            'orchestra_active_sessions',
            'Number of active sessions',
            registry=self.registry
        )
        
        self.active_agents = Gauge(
            'orchestra_active_agents',
            'Number of active agents',
            registry=self.registry
        )
        
        self.message_queue_depth = Gauge(
            'orchestra_message_queue_depth',
            'Number of messages in queue',
            ['queue_name'],
            registry=self.registry
        )
        
        self.agent_load = Gauge(
            'orchestra_agent_load',
            'Current load of agents (concurrent tasks)',
            ['agent_id'],
            registry=self.registry
        )
        
        self.memory_usage = Gauge(
            'orchestra_memory_usage_bytes',
            'Memory usage in bytes',
            ['component'],
            registry=self.registry
        )
        
        self.cpu_usage = Gauge(
            'orchestra_cpu_usage_percent',
            'CPU usage percentage',
            ['component'],
            registry=self.registry
        )
    
    def record_message_processed(self, agent_id: str, message_type: str, status: str = "success") -> None:
        """Record a processed message."""
        self.messages_total.labels(
            agent_id=agent_id,
            message_type=message_type,
            status=status
        ).inc()
    
    def record_session_created(self, status: str = "created") -> None:
        """Record a session creation."""
        self.sessions_total.labels(status=status).inc()
        self.active_sessions.inc()
    
    def record_session_ended(self) -> None:
        """Record a session end."""
        self.active_sessions.dec()
    
    def record_agent_invocation(self, agent_id: str, operation: str) -> None:
        """Record an agent invocation."""
        self.agent_invocations_total.labels(
            agent_id=agent_id,
            operation=operation
        ).inc()
    
    def record_error(self, agent_id: str, error_type: str, operation: str) -> None:
        """Record an error."""
        self.errors_total.labels(
            agent_id=agent_id,
            error_type=error_type,
            operation=operation
        ).inc()
    
    def record_message_processing_time(self, agent_id: str, message_type: str, duration: float) -> None:
        """Record message processing time."""
        self.message_processing_duration.labels(
            agent_id=agent_id,
            message_type=message_type
        ).observe(duration)
    
    def record_session_phase_duration(self, phase: str, duration: float) -> None:
        """Record session phase duration."""
        self.session_duration.labels(phase=phase).observe(duration)
    
    def record_agent_response_time(self, agent_id: str, operation: str, response_time: float) -> None:
        """Record agent response time."""
        self.agent_response_time.labels(
            agent_id=agent_id,
            operation=operation
        ).observe(response_time)
    
    def update_active_agents(self, count: int) -> None:
        """Update active agents count."""
        self.active_agents.set(count)
    
    def update_message_queue_depth(self, queue_name: str, depth: int) -> None:
        """Update message queue depth."""
        self.message_queue_depth.labels(queue_name=queue_name).set(depth)
    
    def update_agent_load(self, agent_id: str, load: int) -> None:
        """Update agent load."""
        self.agent_load.labels(agent_id=agent_id).set(load)
    
    def update_memory_usage(self, component: str, usage_bytes: int) -> None:
        """Update memory usage."""
        self.memory_usage.labels(component=component).set(usage_bytes)
    
    def update_cpu_usage(self, component: str, usage_percent: float) -> None:
        """Update CPU usage."""
        self.cpu_usage.labels(component=component).set(usage_percent)
    
    def get_metrics(self) -> str:
        """Get Prometheus metrics output."""
        return generate_latest(self.registry).decode('utf-8')


class MetricsMiddleware:
    """FastAPI middleware for collecting HTTP metrics."""
    
    def __init__(self, metrics: PrometheusMetrics):
        self.metrics = metrics
        
        # HTTP metrics
        self.http_requests_total = Counter(
            'orchestra_http_requests_total',
            'Total HTTP requests',
            ['method', 'endpoint', 'status_code'],
            registry=metrics.registry
        )
        
        self.http_request_duration = Histogram(
            'orchestra_http_request_duration_seconds',
            'HTTP request duration',
            ['method', 'endpoint'],
            registry=metrics.registry
        )
    
    async def __call__(self, request, call_next):
        """Middleware implementation."""
        start_time = time.time()
        
        response = await call_next(request)
        
        # Record metrics
        duration = time.time() - start_time
        
        self.http_requests_total.labels(
            method=request.method,
            endpoint=request.url.path,
            status_code=response.status_code
        ).inc()
        
        self.http_request_duration.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(duration)
        
        return response


class SystemMetricsCollector:
    """Collects system-level metrics."""
    
    def __init__(self, metrics: PrometheusMetrics):
        self.metrics = metrics
        self._running = False
    
    async def start_collection(self, interval: int = 30) -> None:
        """Start collecting system metrics."""
        self._running = True
        
        while self._running:
            try:
                await self._collect_system_metrics()
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error collecting system metrics: {str(e)}")
                await asyncio.sleep(interval)
    
    def stop_collection(self) -> None:
        """Stop collecting system metrics."""
        self._running = False
    
    async def _collect_system_metrics(self) -> None:
        """Collect system metrics."""
        try:
            import psutil
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.metrics.update_memory_usage("system", memory.used)
            
            # CPU usage
            cpu_percent = psutil.cpu_percent()
            self.metrics.update_cpu_usage("system", cpu_percent)
            
            # Process memory
            process = psutil.Process()
            process_memory = process.memory_info().rss
            self.metrics.update_memory_usage("process", process_memory)
            
            # Process CPU
            process_cpu = process.cpu_percent()
            self.metrics.update_cpu_usage("process", process_cpu)
            
        except ImportError:
            logger.warning("psutil not available, system metrics disabled")
        except Exception as e:
            logger.error(f"Error collecting system metrics: {str(e)}")


class MetricsEndpoint:
    """FastAPI endpoint for serving metrics."""
    
    def __init__(self, metrics: PrometheusMetrics):
        self.metrics = metrics
    
    async def get_metrics(self) -> Response:
        """Return Prometheus metrics."""
        metrics_data = self.metrics.get_metrics()
        return Response(
            content=metrics_data,
            media_type=CONTENT_TYPE_LATEST
        )


# Decorators for automatic metrics collection
def track_agent_metrics(agent_id: str, operation: str):
    """Decorator to automatically track agent metrics."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                # Record invocation
                metrics = get_global_metrics()
                if metrics:
                    metrics.record_agent_invocation(agent_id, operation)
                
                # Execute function
                result = await func(*args, **kwargs)
                
                # Record success and response time
                duration = time.time() - start_time
                if metrics:
                    metrics.record_agent_response_time(agent_id, operation, duration)
                
                return result
                
            except Exception as e:
                # Record error
                duration = time.time() - start_time
                metrics = get_global_metrics()
                if metrics:
                    metrics.record_error(agent_id, type(e).__name__, operation)
                    metrics.record_agent_response_time(agent_id, operation, duration)
                
                raise
        
        return wrapper
    return decorator


def track_message_processing(agent_id: str, message_type: str):
    """Decorator to track message processing metrics."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                
                # Record successful processing
                duration = time.time() - start_time
                metrics = get_global_metrics()
                if metrics:
                    metrics.record_message_processed(agent_id, message_type, "success")
                    metrics.record_message_processing_time(agent_id, message_type, duration)
                
                return result
                
            except Exception as e:
                # Record failed processing
                duration = time.time() - start_time
                metrics = get_global_metrics()
                if metrics:
                    metrics.record_message_processed(agent_id, message_type, "error")
                    metrics.record_message_processing_time(agent_id, message_type, duration)
                
                raise
        
        return wrapper
    return decorator


# Global metrics instance
_global_metrics: Optional[PrometheusMetrics] = None


def initialize_metrics(registry: Optional[CollectorRegistry] = None) -> PrometheusMetrics:
    """Initialize global metrics instance."""
    global _global_metrics
    _global_metrics = PrometheusMetrics(registry)
    return _global_metrics


def get_global_metrics() -> Optional[PrometheusMetrics]:
    """Get the global metrics instance."""
    return _global_metrics


def setup_metrics_endpoint(app, metrics: PrometheusMetrics, path: str = "/metrics"):
    """Set up metrics endpoint on FastAPI app."""
    metrics_endpoint = MetricsEndpoint(metrics)
    
    @app.get(path)
    async def metrics_handler():
        return await metrics_endpoint.get_metrics()
    
    # Add middleware
    metrics_middleware = MetricsMiddleware(metrics)
    app.middleware("http")(metrics_middleware)
    
    logger.info(f"Metrics endpoint set up at {path}")


# Utility functions
def create_session_metrics_tracker(session_id: UUID):
    """Create a metrics tracker for a session."""
    return SessionMetricsTracker(session_id)


class SessionMetricsTracker:
    """Tracks metrics for a specific session."""
    
    def __init__(self, session_id: UUID):
        self.session_id = session_id
        self.start_time = time.time()
        self.phase_start_times: Dict[str, float] = {}
        self.metrics = get_global_metrics()
    
    def start_phase(self, phase: str) -> None:
        """Start tracking a session phase."""
        self.phase_start_times[phase] = time.time()
    
    def end_phase(self, phase: str) -> None:
        """End tracking a session phase."""
        if phase in self.phase_start_times:
            duration = time.time() - self.phase_start_times[phase]
            if self.metrics:
                self.metrics.record_session_phase_duration(phase, duration)
            del self.phase_start_times[phase]
    
    def end_session(self) -> None:
        """End the session tracking."""
        duration = time.time() - self.start_time
        if self.metrics:
            self.metrics.record_session_phase_duration("total", duration)
        
        # End any remaining phases
        for phase in list(self.phase_start_times.keys()):
            self.end_phase(phase)
