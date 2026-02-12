"""
Message Bus - Simplified Redis-based event-driven communication for agents.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import UUID

import redis
from schemas.messages import AgentMessage, MessageType, Priority

logger = logging.getLogger(__name__)


class MessageBus:
    """Simplified Redis-based message bus for agent communication."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.subscribers: Dict[str, Set[Callable]] = {}
        self.message_history: List[AgentMessage] = []
        self.dead_letter_queue: List[Dict[str, Any]] = []
        self.max_history = 1000
        self.max_dead_letter_queue = 100
    
    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            logger.info("Connected to Redis message bus")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.redis_client:
            self.redis_client.close()
            self.redis_client = None
            logger.info("Disconnected from Redis")
    
    async def publish_message(self, message: AgentMessage) -> None:
        """Publish a message to Redis."""
        if not self.redis_client:
            raise RuntimeError("Not connected to Redis")
        
        try:
            # Serialize message
            message_data = message.model_dump_json()
            
            # Publish to appropriate channel
            channel = f"agent:{message.agent_id}"
            self.redis_client.publish(channel, message_data)
            
            # Add to history
            self.message_history.append(message)
            if len(self.message_history) > self.max_history:
                self.message_history.pop(0)
            
            logger.debug(f"Published message to {channel}")
            
        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            # Add to dead letter queue
            self.dead_letter_queue.append({
                "message": message.model_dump(),
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
            raise
    
    async def subscribe_to_agent(self, agent_id: str, callback: Callable) -> None:
        """Subscribe to messages from a specific agent."""
        if agent_id not in self.subscribers:
            self.subscribers[agent_id] = set()
        self.subscribers[agent_id].add(callback)
        logger.info(f"Subscribed to agent: {agent_id}")
    
    async def subscribe_to_message_type(self, message_type: MessageType, callback: Callable) -> None:
        """Subscribe to messages of a specific type."""
        channel = f"type:{message_type.value}"
        if channel not in self.subscribers:
            self.subscribers[channel] = set()
        self.subscribers[channel].add(callback)
        logger.info(f"Subscribed to message type: {message_type.value}")
    
    async def get_message_history(self, limit: Optional[int] = None) -> List[AgentMessage]:
        """Get message history."""
        if limit:
            return self.message_history[-limit:]
        return self.message_history.copy()
    
    async def get_dead_letter_queue(self) -> List[Dict[str, Any]]:
        """Get dead letter queue."""
        return self.dead_letter_queue.copy()
    
    async def clear_dead_letter_queue(self) -> None:
        """Clear dead letter queue."""
        self.dead_letter_queue.clear()
        logger.info("Cleared dead letter queue")
    
    async def get_bus_stats(self) -> Dict[str, Any]:
        """Get bus statistics."""
        return {
            "total_messages": len(self.message_history),
            "dead_letter_count": len(self.dead_letter_queue),
            "active_subscribers": len(self.subscribers),
            "connected": self.redis_client is not None
        }


class DeadLetterQueue:
    """Dead letter queue for failed messages."""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.messages: List[Dict[str, Any]] = []
    
    async def add_message(self, message: AgentMessage, error: str) -> None:
        """Add a failed message to the dead letter queue."""
        dead_letter = {
            "message": message.model_dump(),
            "error": error,
            "timestamp": datetime.utcnow().isoformat(),
            "retry_count": 0
        }
        
        self.messages.append(dead_letter)
        
        # Remove oldest messages if queue is full
        if len(self.messages) > self.max_size:
            self.messages.pop(0)
        
        logger.warning(f"Added message to dead letter queue: {error}")
    
    async def get_messages(self) -> List[Dict[str, Any]]:
        """Get all messages in the dead letter queue."""
        return self.messages.copy()
    
    async def retry_message(self, index: int) -> Dict[str, Any]:
        """Get a message for retry."""
        if 0 <= index < len(self.messages):
            message = self.messages[index]
            message["retry_count"] += 1
            return message
        raise IndexError("Invalid message index")
    
    async def remove_message(self, index: int) -> None:
        """Remove a message from the dead letter queue."""
        if 0 <= index < len(self.messages):
            self.messages.pop(index)
        else:
            raise IndexError("Invalid message index")
    
    async def clear(self) -> None:
        """Clear all messages from the dead letter queue."""
        self.messages.clear()
        logger.info("Cleared dead letter queue")
