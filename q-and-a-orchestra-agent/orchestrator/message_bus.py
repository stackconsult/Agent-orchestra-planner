"""
Message Bus - Redis-based event-driven communication for agents.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import UUID

import aioredis
from schemas.messages import AgentMessage, MessageType, Priority

logger = logging.getLogger(__name__)


class MessageBus:
    """Redis-based message bus for agent communication."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis_client: Optional[aioredis.Redis] = None
        self.subscribers: Dict[str, Set[Callable]] = {}
        self.message_handlers: Dict[str, Callable] = {}
        self.is_connected = False
        
        # Channel patterns
        self.agent_channel_pattern = "agent:{agent_id}:*"
        self.message_type_channel_pattern = "message_type:{message_type}:*"
        self.session_channel_pattern = "session:{session_id}:*"
        self.priority_channel_pattern = "priority:{priority}:*"
    
    async def connect(self) -> None:
        """Connect to Redis and set up subscriptions."""
        try:
            self.redis_client = aioredis.from_url(self.redis_url)
            
            # Test connection
            await self.redis_client.ping()
            
            self.is_connected = True
            logger.info("Connected to Redis message bus")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.redis_client:
            await self.redis_client.close()
            self.is_connected = False
            logger.info("Disconnected from Redis message bus")
    
    async def publish_message(self, message: AgentMessage) -> None:
        """
        Publish a message to the message bus.
        
        Args:
            message: The message to publish
        """
        if not self.is_connected:
            raise RuntimeError("Message bus not connected")
        
        try:
            # Serialize message
            message_data = message.json()
            
            # Publish to multiple channels based on message properties
            channels = self._get_publish_channels(message)
            
            # Publish to all relevant channels
            tasks = []
            for channel in channels:
                task = self.redis_client.publish(channel, message_data)
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            
            logger.debug(f"Published message {message.message_id} to {len(channels)} channels")
            
        except Exception as e:
            logger.error(f"Failed to publish message {message.message_id}: {str(e)}")
            raise
    
    async def subscribe_to_agent(self, agent_id: str, handler: Callable) -> None:
        """
        Subscribe to messages for a specific agent.
        
        Args:
            agent_id: The agent ID to subscribe to
            handler: The handler function for incoming messages
        """
        if not self.is_connected:
            raise RuntimeError("Message bus not connected")
        
        channel_pattern = f"agent:{agent_id}:*"
        
        if channel_pattern not in self.subscribers:
            self.subscribers[channel_pattern] = set()
        
        self.subscribers[channel_pattern].add(handler)
        self.message_handlers[agent_id] = handler
        
        # Start listening if this is the first subscriber
        if len(self.subscribers[channel_pattern]) == 1:
            await self._start_subscription(channel_pattern)
        
        logger.info(f"Subscribed to messages for agent: {agent_id}")
    
    async def subscribe_to_message_type(self, message_type: MessageType, handler: Callable) -> None:
        """
        Subscribe to messages of a specific type.
        
        Args:
            message_type: The message type to subscribe to
            handler: The handler function for incoming messages
        """
        if not self.is_connected:
            raise RuntimeError("Message bus not connected")
        
        channel_pattern = f"message_type:{message_type.value}:*"
        
        if channel_pattern not in self.subscribers:
            self.subscribers[channel_pattern] = set()
        
        self.subscribers[channel_pattern].add(handler)
        
        # Start listening if this is the first subscriber
        if len(self.subscribers[channel_pattern]) == 1:
            await self._start_subscription(channel_pattern)
        
        logger.info(f"Subscribed to message type: {message_type.value}")
    
    async def subscribe_to_session(self, session_id: UUID, handler: Callable) -> None:
        """
        Subscribe to messages for a specific session.
        
        Args:
            session_id: The session ID to subscribe to
            handler: The handler function for incoming messages
        """
        if not self.is_connected:
            raise RuntimeError("Message bus not connected")
        
        channel_pattern = f"session:{session_id}:*"
        
        if channel_pattern not in self.subscribers:
            self.subscribers[channel_pattern] = set()
        
        self.subscribers[channel_pattern].add(handler)
        
        # Start listening if this is the first subscriber
        if len(self.subscribers[channel_pattern]) == 1:
            await self._start_subscription(channel_pattern)
        
        logger.info(f"Subscribed to session: {session_id}")
    
    async def unsubscribe_from_agent(self, agent_id: str, handler: Callable) -> None:
        """
        Unsubscribe from messages for a specific agent.
        
        Args:
            agent_id: The agent ID to unsubscribe from
            handler: The handler function to remove
        """
        channel_pattern = f"agent:{agent_id}:*"
        
        if channel_pattern in self.subscribers:
            self.subscribers[channel_pattern].discard(handler)
            
            if len(self.subscribers[channel_pattern]) == 0:
                del self.subscribers[channel_pattern]
                # Note: In production, you'd want to stop the pubsub subscription
        
        if agent_id in self.message_handlers:
            del self.message_handlers[agent_id]
        
        logger.info(f"Unsubscribed from messages for agent: {agent_id}")
    
    async def get_message_history(self, session_id: Optional[UUID] = None, 
                                 agent_id: Optional[str] = None,
                                 limit: int = 100) -> List[AgentMessage]:
        """
        Get message history from Redis.
        
        Args:
            session_id: Filter by session ID
            agent_id: Filter by agent ID
            limit: Maximum number of messages to retrieve
            
        Returns:
            List of messages
        """
        if not self.is_connected:
            raise RuntimeError("Message bus not connected")
        
        try:
            # Build Redis key pattern
            if session_id:
                pattern = f"history:session:{session_id}:*"
            elif agent_id:
                pattern = f"history:agent:{agent_id}:*"
            else:
                pattern = "history:*"
            
            # Get keys
            keys = await self.redis_client.keys(pattern)
            keys.sort()  # Sort by timestamp (should be in key)
            
            # Get messages
            messages = []
            for key in keys[:limit]:
                message_data = await self.redis_client.get(key)
                if message_data:
                    message = AgentMessage.parse_raw(message_data)
                    messages.append(message)
            
            return messages
            
        except Exception as e:
            logger.error(f"Failed to get message history: {str(e)}")
            return []
    
    async def clear_message_history(self, session_id: Optional[UUID] = None) -> None:
        """
        Clear message history.
        
        Args:
            session_id: Clear history for specific session, or all if None
        """
        if not self.is_connected:
            raise RuntimeError("Message bus not connected")
        
        try:
            if session_id:
                pattern = f"history:session:{session_id}:*"
            else:
                pattern = "history:*"
            
            keys = await self.redis_client.keys(pattern)
            if keys:
                await self.redis_client.delete(*keys)
                logger.info(f"Cleared {len(keys)} message history entries")
            
        except Exception as e:
            logger.error(f"Failed to clear message history: {str(e)}")
    
    async def get_bus_stats(self) -> Dict[str, Any]:
        """Get message bus statistics."""
        if not self.is_connected:
            return {"connected": False}
        
        try:
            # Get Redis info
            info = await self.redis_client.info()
            
            # Count active subscriptions
            active_subscriptions = sum(len(handlers) for handlers in self.subscribers.values())
            
            # Count message history
            history_keys = await self.redis_client.keys("history:*")
            
            return {
                "connected": True,
                "redis_info": {
                    "used_memory": info.get("used_memory_human"),
                    "connected_clients": info.get("connected_clients"),
                    "total_commands_processed": info.get("total_commands_processed")
                },
                "active_subscriptions": active_subscriptions,
                "message_history_count": len(history_keys),
                "subscriber_count": len(self.subscribers)
            }
            
        except Exception as e:
            logger.error(f"Failed to get bus stats: {str(e)}")
            return {"connected": True, "error": str(e)}
    
    def _get_publish_channels(self, message: AgentMessage) -> List[str]:
        """Get all channels to publish the message to."""
        channels = []
        
        # Agent-specific channel
        channels.append(f"agent:{message.agent_id}")
        
        # Message type channel
        channels.append(f"message_type:{message.message_type.value}")
        
        # Session channel (if present)
        if message.session_id:
            channels.append(f"session:{message.session_id}")
        
        # Priority channel
        channels.append(f"priority:{message.priority.value}")
        
        # Global channel (for all messages)
        channels.append("global")
        
        return channels
    
    async def _start_subscription(self, channel_pattern: str) -> None:
        """Start listening to a channel pattern."""
        try:
            pubsub = self.redis_client.pubsub()
            await pubsub.psubscribe(channel_pattern)
            
            # Start listening task
            asyncio.create_task(self._listen_to_channel(pubsub, channel_pattern))
            
        except Exception as e:
            logger.error(f"Failed to start subscription to {channel_pattern}: {str(e)}")
    
    async def _listen_to_channel(self, pubsub: aioredis.client.PubSub, channel_pattern: str) -> None:
        """Listen to messages on a subscribed channel."""
        try:
            async for message in pubsub.listen():
                if message["type"] == "pmessage":
                    await self._handle_received_message(message, channel_pattern)
                    
        except Exception as e:
            logger.error(f"Error listening to channel {channel_pattern}: {str(e)}")
    
    async def _handle_received_message(self, redis_message: Dict[str, Any], channel_pattern: str) -> None:
        """Handle a received message."""
        try:
            # Parse message
            message_data = redis_message["data"]
            if isinstance(message_data, bytes):
                message_data = message_data.decode('utf-8')
            
            message = AgentMessage.parse_raw(message_data)
            
            # Store in history
            await self._store_message_in_history(message)
            
            # Find handlers for this channel pattern
            handlers = self.subscribers.get(channel_pattern, set())
            
            # Call all handlers
            for handler in handlers:
                try:
                    await handler(message)
                except Exception as e:
                    logger.error(f"Handler error for message {message.message_id}: {str(e)}")
            
            logger.debug(f"Processed message {message.message_id} for {len(handlers)} handlers")
            
        except Exception as e:
            logger.error(f"Failed to handle received message: {str(e)}")
    
    async def _store_message_in_history(self, message: AgentMessage) -> None:
        """Store message in Redis history."""
        try:
            # Create history key with timestamp
            timestamp = message.timestamp.isoformat()
            history_key = f"history:session:{message.session_id}:{timestamp}"
            
            # Store message with TTL (24 hours)
            await self.redis_client.setex(
                history_key, 
                86400,  # 24 hours TTL
                message.json()
            )
            
            # Also store by agent
            agent_history_key = f"history:agent:{message.agent_id}:{timestamp}"
            await self.redis_client.setex(
                agent_history_key,
                86400,
                message.json()
            )
            
        except Exception as e:
            logger.error(f"Failed to store message in history: {str(e)}")


class DeadLetterQueue:
    """Dead letter queue for failed messages."""
    
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        self.dlq_key = "dlq:messages"
    
    async def add_message(self, message: AgentMessage, error: str, retry_count: int = 0) -> None:
        """
        Add a failed message to the dead letter queue.
        
        Args:
            message: The failed message
            error: The error that caused the failure
            retry_count: Number of times the message has been retried
        """
        dlq_entry = {
            "message": message.dict(),
            "error": error,
            "retry_count": retry_count,
            "failed_at": datetime.utcnow().isoformat(),
            "next_retry_at": self._calculate_next_retry(retry_count).isoformat()
        }
        
        await self.redis.lpush(self.dlq_key, json.dumps(dlq_entry))
        
        logger.warning(f"Added message {message.message_id} to DLQ: {error}")
    
    async def get_retryable_messages(self) -> List[Dict[str, Any]]:
        """Get messages that are ready for retry."""
        now = datetime.utcnow()
        retryable = []
        
        # Get all DLQ messages
        dlq_messages = await self.redis.lrange(self.dlq_key, 0, -1)
        
        for msg_data in dlq_messages:
            try:
                entry = json.loads(msg_data)
                next_retry = datetime.fromisoformat(entry["next_retry_at"])
                
                if next_retry <= now and entry["retry_count"] < 5:  # Max 5 retries
                    retryable.append(entry)
                    
            except Exception as e:
                logger.error(f"Error parsing DLQ message: {str(e)}")
        
        return retryable
    
    async def remove_message(self, message_id: str) -> None:
        """Remove a message from the DLQ."""
        # This is a simplified implementation
        # In production, you'd want to remove the specific message
        pass
    
    def _calculate_next_retry(self, retry_count: int) -> datetime:
        """Calculate when to retry the message using exponential backoff."""
        base_delay = 60  # 1 minute
        max_delay = 3600  # 1 hour
        
        delay = min(base_delay * (2 ** retry_count), max_delay)
        return datetime.utcnow() + timedelta(seconds=delay)
