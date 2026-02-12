"""
Router - Routes messages to appropriate agents and manages workflow orchestration.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from uuid import UUID

from schemas.messages import AgentMessage, MessageType, Priority
from orchestrator.message_bus import MessageBus

logger = logging.getLogger(__name__)


class MessageRouter:
    """Routes messages to appropriate agents based on content and workflow state."""
    
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.agent_registry: Dict[str, Dict[str, Any]] = {}
        self.workflow_states: Dict[UUID, Dict[str, Any]] = {}
        self.routing_rules: List[Dict[str, Any]] = []
        self.message_handlers: Dict[str, callable] = {}
        
        # Default routing rules
        self._setup_default_rules()
    
    async def register_agent(self, agent_id: str, agent_config: Dict[str, Any]) -> None:
        """
        Register an agent with the router.
        
        Args:
            agent_id: Unique identifier for the agent
            agent_config: Agent configuration including capabilities and message types
        """
        self.agent_registry[agent_id] = {
            "id": agent_id,
            "config": agent_config,
            "registered_at": datetime.utcnow(),
            "last_heartbeat": datetime.utcnow(),
            "status": "active",
            "message_types": agent_config.get("message_types", []),
            "capabilities": agent_config.get("capabilities", []),
            "max_concurrent_tasks": agent_config.get("max_concurrent_tasks", 1),
            "current_tasks": 0
        }
        
        logger.info(f"Registered agent: {agent_id}")
    
    async def unregister_agent(self, agent_id: str) -> None:
        """
        Unregister an agent from the router.
        
        Args:
            agent_id: Agent ID to unregister
        """
        if agent_id in self.agent_registry:
            del self.agent_registry[agent_id]
            logger.info(f"Unregistered agent: {agent_id}")
    
    async def route_message(self, message: AgentMessage) -> List[str]:
        """
        Route a message to appropriate agents.
        
        Args:
            message: The message to route
            
        Returns:
            List of agent IDs the message was routed to
        """
        try:
            routed_agents = []
            
            # Update workflow state
            await self._update_workflow_state(message)
            
            # Apply routing rules
            target_agents = await self._apply_routing_rules(message)
            
            # Filter to active agents
            available_agents = [
                agent_id for agent_id in target_agents
                if self._is_agent_available(agent_id)
            ]
            
            # Route to available agents
            for agent_id in available_agents:
                await self.message_bus.publish_message(message)
                routed_agents.append(agent_id)
                
                # Update agent task count
                if agent_id in self.agent_registry:
                    self.agent_registry[agent_id]["current_tasks"] += 1
            
            # Log routing
            logger.info(f"Routed message {message.message_id} to agents: {routed_agents}")
            
            return routed_agents
            
        except Exception as e:
            logger.error(f"Failed to route message {message.message_id}: {str(e)}")
            return []
    
    async def add_routing_rule(self, rule: Dict[str, Any]) -> None:
        """
        Add a routing rule.
        
        Args:
            rule: Routing rule configuration
        """
        required_fields = ["name", "condition", "target_agents"]
        for field in required_fields:
            if field not in rule:
                raise ValueError(f"Missing required field in routing rule: {field}")
        
        self.routing_rules.append(rule)
        logger.info(f"Added routing rule: {rule['name']}")
    
    async def remove_routing_rule(self, rule_name: str) -> None:
        """
        Remove a routing rule.
        
        Args:
            rule_name: Name of the rule to remove
        """
        self.routing_rules = [
            rule for rule in self.routing_rules
            if rule.get("name") != rule_name
        ]
        logger.info(f"Removed routing rule: {rule_name}")
    
    async def get_workflow_state(self, correlation_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Get the current state of a workflow.
        
        Args:
            correlation_id: Workflow correlation ID
            
        Returns:
            Workflow state or None if not found
        """
        return self.workflow_states.get(correlation_id)
    
    async def get_agent_status(self, agent_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get status of agents.
        
        Args:
            agent_id: Specific agent ID, or None for all agents
            
        Returns:
            Agent status information
        """
        if agent_id:
            if agent_id in self.agent_registry:
                return self.agent_registry[agent_id]
            else:
                return {"error": f"Agent {agent_id} not found"}
        
        return {
            "total_agents": len(self.agent_registry),
            "active_agents": len([
                a for a in self.agent_registry.values()
                if a["status"] == "active"
            ]),
            "agents": {
                agent_id: {
                    "status": agent["status"],
                    "current_tasks": agent["current_tasks"],
                    "max_concurrent_tasks": agent["max_concurrent_tasks"],
                    "last_heartbeat": agent["last_heartbeat"].isoformat()
                }
                for agent_id, agent in self.agent_registry.items()
            }
        }
    
    async def handle_agent_heartbeat(self, agent_id: str) -> None:
        """
        Handle heartbeat from an agent.
        
        Args:
            agent_id: Agent ID sending heartbeat
        """
        if agent_id in self.agent_registry:
            self.agent_registry[agent_id]["last_heartbeat"] = datetime.utcnow()
            self.agent_registry[agent_id]["status"] = "active"
        else:
            logger.warning(f"Heartbeat from unregistered agent: {agent_id}")
    
    async def handle_task_completion(self, agent_id: str, message_id: UUID) -> None:
        """
        Handle task completion notification from an agent.
        
        Args:
            agent_id: Agent that completed the task
            message_id: ID of the completed task message
        """
        if agent_id in self.agent_registry:
            self.agent_registry[agent_id]["current_tasks"] = max(
                0, self.agent_registry[agent_id]["current_tasks"] - 1
            )
        
        logger.info(f"Agent {agent_id} completed task {message_id}")
    
    async def cleanup_inactive_agents(self) -> None:
        """Clean up agents that haven't sent heartbeats recently."""
        now = datetime.utcnow()
        inactive_threshold = timedelta(minutes=5)
        
        inactive_agents = []
        for agent_id, agent_info in self.agent_registry.items():
            if now - agent_info["last_heartbeat"] > inactive_threshold:
                inactive_agents.append(agent_id)
        
        for agent_id in inactive_agents:
            self.agent_registry[agent_id]["status"] = "inactive"
            logger.warning(f"Marked agent as inactive: {agent_id}")
    
    async def _apply_routing_rules(self, message: AgentMessage) -> List[str]:
        """Apply routing rules to determine target agents."""
        target_agents = []
        
        for rule in self.routing_rules:
            try:
                if await self._evaluate_rule_condition(rule["condition"], message):
                    rule_targets = rule["target_agents"]
                    
                    # Handle different target types
                    if isinstance(rule_targets, str):
                        if rule_targets == "all":
                            target_agents.extend(self.agent_registry.keys())
                        elif rule_targets == "message_type_handlers":
                            target_agents.extend(
                                self._get_agents_for_message_type(message.message_type)
                            )
                        else:
                            target_agents.append(rule_targets)
                    elif isinstance(rule_targets, list):
                        target_agents.extend(rule_targets)
                    
                    # Apply priority and limits if specified
                    if "priority" in rule:
                        target_agents = self._apply_priority_filter(
                            target_agents, rule["priority"]
                        )
                    
                    if "max_targets" in rule:
                        target_agents = target_agents[:rule["max_targets"]]
                    
            except Exception as e:
                logger.error(f"Error evaluating routing rule {rule.get('name', 'unknown')}: {str(e)}")
        
        # Remove duplicates and return
        return list(set(target_agents))
    
    async def _evaluate_rule_condition(self, condition: Dict[str, Any], message: AgentMessage) -> bool:
        """Evaluate a routing rule condition."""
        
        condition_type = condition.get("type", "always")
        
        if condition_type == "always":
            return True
        
        elif condition_type == "message_type":
            return message.message_type.value in condition.get("values", [])
        
        elif condition_type == "agent_id":
            return message.agent_id in condition.get("values", [])
        
        elif condition_type == "priority":
            return message.priority.value in condition.get("values", [])
        
        elif condition_type == "session_exists":
            return message.session_id is not None
        
        elif condition_type == "intent_contains":
            intent = message.intent.lower()
            return any(
                keyword.lower() in intent 
                for keyword in condition.get("keywords", [])
            )
        
        elif condition_type == "custom":
            # Custom evaluation function
            # In production, this would be more sophisticated
            return True
        
        else:
            logger.warning(f"Unknown condition type: {condition_type}")
            return False
    
    def _get_agents_for_message_type(self, message_type: MessageType) -> List[str]:
        """Get agents that can handle a specific message type."""
        capable_agents = []
        
        for agent_id, agent_info in self.agent_registry.items():
            if message_type.value in agent_info["message_types"]:
                capable_agents.append(agent_id)
        
        return capable_agents
    
    def _apply_priority_filter(self, agents: List[str], priority_config: Dict[str, Any]) -> List[str]:
        """Apply priority-based filtering to agent selection."""
        
        priority_type = priority_config.get("type", "none")
        
        if priority_type == "load_balanced":
            # Sort by current task count (least loaded first)
            return sorted(
                agents,
                key=lambda aid: self.agent_registry.get(aid, {}).get("current_tasks", 0)
            )
        
        elif priority_type == "round_robin":
            # Simple round-robin (in production, maintain state)
            return agents
        
        elif priority_type == "capability_based":
            # Sort by capability score
            return sorted(
                agents,
                key=lambda aid: len(self.agent_registry.get(aid, {}).get("capabilities", [])),
                reverse=True
            )
        
        else:
            return agents
    
    def _is_agent_available(self, agent_id: str) -> bool:
        """Check if an agent is available to handle messages."""
        
        if agent_id not in self.agent_registry:
            return False
        
        agent_info = self.agent_registry[agent_id]
        
        # Check status
        if agent_info["status"] != "active":
            return False
        
        # Check concurrent task limit
        if agent_info["current_tasks"] >= agent_info["max_concurrent_tasks"]:
            return False
        
        return True
    
    async def _update_workflow_state(self, message: AgentMessage) -> None:
        """Update workflow state based on the message."""
        
        correlation_id = message.correlation_id
        
        if correlation_id not in self.workflow_states:
            # Initialize workflow state
            self.workflow_states[correlation_id] = {
                "correlation_id": correlation_id,
                "started_at": datetime.utcnow(),
                "last_updated": datetime.utcnow(),
                "messages": [],
                "current_phase": "initial",
                "agents_involved": set(),
                "status": "active"
            }
        
        # Update workflow state
        state = self.workflow_states[correlation_id]
        state["last_updated"] = datetime.utcnow()
        state["messages"].append({
            "message_id": message.message_id,
            "agent_id": message.agent_id,
            "message_type": message.message_type.value,
            "timestamp": message.timestamp
        })
        state["agents_involved"].add(message.agent_id)
        
        # Update phase based on message types
        if message.message_type == MessageType.REQUIREMENTS_EXTRACTED:
            state["current_phase"] = "requirements_complete"
        elif message.message_type == MessageType.DESIGN_COMPLETED:
            state["current_phase"] = "design_complete"
        elif message.message_type == MessageType.PLAN_COMPLETED:
            state["current_phase"] = "planning_complete"
        elif message.message_type == MessageType.VALIDATION_COMPLETED:
            state["current_phase"] = "validation_complete"
        elif message.message_type == MessageType.ERROR_OCCURRED:
            state["status"] = "error"
    
    def _setup_default_rules(self) -> None:
        """Set up default routing rules."""
        
        # Rule 1: Route requirements messages to requirements extractor
        self.routing_rules.append({
            "name": "requirements_routing",
            "condition": {
                "type": "message_type",
                "values": ["question_asked", "question_answered", "requirements_extracted"]
            },
            "target_agents": ["requirements_extractor"],
            "priority": {"type": "none"}
        })
        
        # Rule 2: Route repository analysis messages
        self.routing_rules.append({
            "name": "repository_analysis_routing",
            "condition": {
                "type": "message_type",
                "values": ["repo_analysis_requested", "repo_analysis_completed"]
            },
            "target_agents": ["repository_analyzer"],
            "priority": {"type": "none"}
        })
        
        # Rule 3: Route design messages
        self.routing_rules.append({
            "name": "design_routing",
            "condition": {
                "type": "message_type",
                "values": ["design_requested", "design_completed"]
            },
            "target_agents": ["architecture_designer"],
            "priority": {"type": "none"}
        })
        
        # Rule 4: Route planning messages
        self.routing_rules.append({
            "name": "planning_routing",
            "condition": {
                "type": "message_type",
                "values": ["plan_requested", "plan_completed"]
            },
            "target_agents": ["implementation_planner"],
            "priority": {"type": "none"}
        })
        
        # Rule 5: Route validation messages
        self.routing_rules.append({
            "name": "validation_routing",
            "condition": {
                "type": "message_type",
                "values": ["validation_requested", "validation_completed"]
            },
            "target_agents": ["validator"],
            "priority": {"type": "none"}
        })
        
        # Rule 6: Route error messages to all agents
        self.routing_rules.append({
            "name": "error_routing",
            "condition": {
                "type": "message_type",
                "values": ["error_occurred"]
            },
            "target_agents": "all",
            "priority": {"type": "high"}
        })
        
        # Rule 7: Route session messages to all agents in session
        self.routing_rules.append({
            "name": "session_routing",
            "condition": {
                "type": "session_exists"
            },
            "target_agents": "message_type_handlers",
            "priority": {"type": "none"}
        })


class LoadBalancer:
    """Load balancer for distributing messages across agents."""
    
    def __init__(self, router: MessageRouter):
        self.router = router
        self.load_balancing_strategy = "round_robin"
        self.agent_load_index: Dict[str, int] = {}
    
    async def select_agent(self, capable_agents: List[str], strategy: str = None) -> Optional[str]:
        """
        Select the best agent from a list of capable agents.
        
        Args:
            capable_agents: List of agents that can handle the message
            strategy: Load balancing strategy to use
            
        Returns:
            Selected agent ID or None if no agents available
        """
        if not capable_agents:
            return None
        
        strategy = strategy or self.load_balancing_strategy
        
        if strategy == "round_robin":
            return self._round_robin_selection(capable_agents)
        elif strategy == "least_loaded":
            return self._least_loaded_selection(capable_agents)
        elif strategy == "random":
            return self._random_selection(capable_agents)
        elif strategy == "capability_based":
            return self._capability_based_selection(capable_agents)
        else:
            return capable_agents[0]
    
    def _round_robin_selection(self, agents: List[str]) -> str:
        """Select agent using round-robin algorithm."""
        if not agents:
            return ""
        
        # Get the first agent and rotate the list
        selected = agents[0]
        
        # Update round-robin index (simplified)
        return selected
    
    def _least_loaded_selection(self, agents: List[str]) -> str:
        """Select the agent with the least current load."""
        if not agents:
            return ""
        
        def get_load(agent_id: str) -> int:
            agent_info = self.router.agent_registry.get(agent_id, {})
            return agent_info.get("current_tasks", 0)
        
        return min(agents, key=get_load)
    
    def _random_selection(self, agents: List[str]) -> str:
        """Select a random agent."""
        import random
        return random.choice(agents) if agents else ""
    
    def _capability_based_selection(self, agents: List[str]) -> str:
        """Select agent based on capability score."""
        if not agents:
            return ""
        
        def get_capability_score(agent_id: str) -> int:
            agent_info = self.router.agent_registry.get(agent_id, {})
            return len(agent_info.get("capabilities", []))
        
        return max(agents, key=get_capability_score)
