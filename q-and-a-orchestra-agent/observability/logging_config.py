"""
Logging configuration for structured logging with correlation IDs.
"""

import asyncio
import logging
import logging.config
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

import structlog
from pythonjsonlogger import jsonlogger


class CorrelationFilter(logging.Filter):
    """Filter to add correlation ID to log records."""
    
    def filter(self, record):
        # Try to get correlation ID from context or record
        correlation_id = getattr(record, 'correlation_id', None)
        if not correlation_id:
            # Try to get from thread local or context
            correlation_id = self._get_correlation_from_context()
        
        if correlation_id:
            record.correlation_id = str(correlation_id)
        else:
            record.correlation_id = "none"
        
        return True
    
    def _get_correlation_from_context(self) -> str:
        """Get correlation ID from current context."""
        # In a real implementation, this would use contextvars or similar
        # For now, return None
        return None


class JSONFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with additional fields."""
    
    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]):
        """Add custom fields to log record."""
        super().add_fields(log_record, record, message_dict)
        
        # Add timestamp if not present
        if not log_record.get('timestamp'):
            log_record['timestamp'] = datetime.utcnow().isoformat()
        
        # Add log level
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname
        
        # Add service name
        log_record['service'] = 'q-and-a-orchestra-agent'
        
        # Add agent ID if available
        if hasattr(record, 'agent_id'):
            log_record['agent_id'] = record.agent_id
        
        # Add session ID if available
        if hasattr(record, 'session_id'):
            log_record['session_id'] = str(record.session_id)


def setup_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    """
    Set up structured logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Log format (json or console)
    """
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if log_format == "json" else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": JSONFormatter,
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s"
            },
            "console": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": log_format,
                "stream": sys.stdout,
                "filters": ["correlation_filter"]
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": log_level,
                "formatter": "json",
                "filename": "logs/orchestra.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "filters": ["correlation_filter"]
            }
        },
        "filters": {
            "correlation_filter": {
                "()": CorrelationFilter
            }
        },
        "loggers": {
            "": {  # Root logger
                "level": log_level,
                "handlers": ["console", "file"],
                "propagate": False
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False
            },
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False
            },
            "sqlalchemy.engine": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            },
            "httpx": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            },
            "anthropic": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            }
        }
    }
    
    # Create logs directory if it doesn't exist
    import os
    os.makedirs("logs", exist_ok=True)
    
    # Apply configuration
    logging.config.dictConfig(config)


class ContextLogger:
    """Logger with automatic context binding."""
    
    def __init__(self, name: str):
        self.logger = structlog.get_logger(name)
        self._context = {}
    
    def bind(self, **kwargs) -> 'ContextLogger':
        """Bind context to the logger."""
        new_logger = ContextLogger(self.logger.name)
        new_logger.logger = self.logger.bind(**kwargs)
        new_logger._context = {**self._context, **kwargs}
        return new_logger
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(message, **kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self.logger.debug(message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self.logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self.logger.critical(message, **kwargs)
    
    def exception(self, message: str, **kwargs) -> None:
        """Log exception with traceback."""
        self.logger.exception(message, **kwargs)


def get_logger(name: str) -> ContextLogger:
    """
    Get a context-aware logger.
    
    Args:
        name: Logger name
        
    Returns:
        ContextLogger instance
    """
    return ContextLogger(name)


# Logging decorators
def log_execution_time(logger: ContextLogger):
    """Decorator to log function execution time."""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            try:
                result = await func(*args, **kwargs)
                duration = (datetime.utcnow() - start_time).total_seconds()
                logger.info(
                    f"Function {func.__name__} completed",
                    function=func.__name__,
                    duration_seconds=duration,
                    success=True
                )
                return result
            except Exception as e:
                duration = (datetime.utcnow() - start_time).total_seconds()
                logger.error(
                    f"Function {func.__name__} failed",
                    function=func.__name__,
                    duration_seconds=duration,
                    success=False,
                    error=str(e)
                )
                raise
        
        def sync_wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            try:
                result = func(*args, **kwargs)
                duration = (datetime.utcnow() - start_time).total_seconds()
                logger.info(
                    f"Function {func.__name__} completed",
                    function=func.__name__,
                    duration_seconds=duration,
                    success=True
                )
                return result
            except Exception as e:
                duration = (datetime.utcnow() - start_time).total_seconds()
                logger.error(
                    f"Function {func.__name__} failed",
                    function=func.__name__,
                    duration_seconds=duration,
                    success=False,
                    error=str(e)
                )
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def log_agent_message(logger: ContextLogger):
    """Decorator to log agent messages."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Try to extract message info from arguments
            message = None
            for arg in args:
                if hasattr(arg, 'message_id') and hasattr(arg, 'agent_id'):
                    message = arg
                    break
            
            bound_logger = logger
            if message:
                bound_logger = logger.bind(
                    message_id=str(message.message_id),
                    agent_id=message.agent_id,
                    message_type=message.message_type.value,
                    correlation_id=str(message.correlation_id)
                )
            
            bound_logger.info(
                f"Processing message in {func.__name__}",
                function=func.__name__
            )
            
            try:
                result = await func(*args, **kwargs)
                bound_logger.info(
                    f"Message processed successfully in {func.__name__}",
                    function=func.__name__,
                    success=True
                )
                return result
            except Exception as e:
                bound_logger.error(
                    f"Message processing failed in {func.__name__}",
                    function=func.__name__,
                    success=False,
                    error=str(e)
                )
                raise
        
        return wrapper
    return decorator


# Context management for correlation IDs
class CorrelationContext:
    """Context manager for correlation IDs."""
    
    def __init__(self, correlation_id: UUID):
        self.correlation_id = correlation_id
        self._old_context = None
    
    async def __aenter__(self):
        # Set correlation ID in context
        # In a real implementation, this would use contextvars
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Clear correlation ID from context
        pass


# Utility functions
def set_correlation_id(correlation_id: UUID) -> None:
    """Set correlation ID in current context."""
    # In a real implementation, this would use contextvars
    pass


def get_correlation_id() -> Optional[UUID]:
    """Get correlation ID from current context."""
    # In a real implementation, this would use contextvars
    return None


def bind_agent_context(agent_id: str, session_id: Optional[UUID] = None) -> ContextLogger:
    """
    Get logger with agent context bound.
    
    Args:
        agent_id: Agent ID
        session_id: Optional session ID
        
    Returns:
        Logger with agent context
    """
    logger = get_logger(f"agent.{agent_id}")
    
    context = {"agent_id": agent_id}
    if session_id:
        context["session_id"] = str(session_id)
    
    return logger.bind(**context)
