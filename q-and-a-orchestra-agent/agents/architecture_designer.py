"""
Architecture Designer Agent - Applies patterns to design custom agent orchestras.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from anthropic import AsyncAnthropic
from schemas.messages import AgentMessage, MessageType, DesignPayload
from schemas.design import (
    OrchestraDesign, AgentDefinition, MessageFlow, AgentType, 
    CommunicationPattern, SafetyMechanism
)

logger = logging.getLogger(__name__)


class ArchitectureDesignerAgent:
    """Designs agent orchestras based on requirements and repository patterns."""
    
    def __init__(self, anthropic_client: AsyncAnthropic):
        self.anthropic = anthropic_client
        self.agent_id = "architecture_designer"
        
        # Design templates for common patterns
        self._design_templates = {
            "document_processing": self._get_document_processing_template(),
            "data_pipeline": self._get_data_pipeline_template(),
            "customer_support": self._get_customer_support_template(),
            "monitoring_alerting": self._get_monitoring_template(),
            "deployment_automation": self._get_deployment_template()
        }
    
    async def design_orchestra(self, message: AgentMessage) -> AgentMessage:
        """
        Design an agent orchestra based on requirements and patterns.
        
        Args:
            message: Message containing requirements and patterns
            
        Returns:
            Message containing orchestra design
        """
        try:
            requirements = message.payload.get("requirements", {})
            patterns = message.payload.get("patterns", {})
            
            logger.info(f"Starting orchestra design for correlation_id: {message.correlation_id}")
            
            # Determine design approach
            design_approach = await self._determine_design_approach(requirements)
            
            # Create agent definitions
            agents = await self._create_agents(requirements, patterns, design_approach)
            
            # Design message flows
            message_flows = await self._design_message_flows(agents, requirements)
            
            # Define coordination protocol
            coordination_protocol = await self._design_coordination_protocol(agents, requirements)
            
            # Add safety mechanisms
            safety_mechanisms = await self._design_safety_mechanisms(agents, requirements)
            
            # Design observability setup
            observability_setup = await self._design_observability(agents, requirements)
            
            # Create orchestra design
            design = OrchestraDesign(
                design_id=UUID(),
                session_id=message.session_id or UUID(),
                orchestra_name=requirements.get("project_description", "Agent Orchestra"),
                orchestra_description=requirements.get("primary_goal", "Automated agent system"),
                primary_goal=requirements.get("primary_goal", ""),
                agents={agent_id: agent for agent_id, agent in agents.items()},
                message_flows=message_flows,
                coordination_protocol=coordination_protocol,
                safety_mechanisms=safety_mechanisms,
                monitoring_setup=observability_setup,
                expected_load=self._estimate_load(requirements),
                scalability_plan=self._design_scalability_plan(agents, requirements),
                external_integrations=self._identify_integrations(requirements),
                design_patterns_used=list(patterns.get("agent_patterns", {}).keys()),
                best_practices_applied=patterns.get("best_practices", []),
                risks_identified=await self._identify_risks(agents, requirements)
            )
            
            # Create response payload
            payload = DesignPayload(
                agent_count=len(agents),
                agent_roles={agent_id: agent.description for agent_id, agent in agents.items()},
                message_flow=[
                    {
                        "from": flow.from_agent,
                        "to": flow.to_agent,
                        "type": flow.message_type,
                        "pattern": flow.communication_pattern
                    }
                    for flow in message_flows
                ],
                coordination_protocol=coordination_protocol,
                mcp_integrations=self._extract_mcp_integrations(agents),
                safety_mechanisms=[mechanism.value for mechanism in safety_mechanisms],
                observability_setup=observability_setup
            )
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="design_completed",
                message_type=MessageType.DESIGN_COMPLETED,
                payload={
                    "design": design.dict(),
                    "design_summary": payload.dict()
                },
                session_id=message.session_id
            )
            
            logger.info(f"Orchestra design completed for correlation_id: {message.correlation_id}")
            return response_message
            
        except Exception as e:
            logger.error(f"Orchestra design failed: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def refine_design(self, message: AgentMessage) -> AgentMessage:
        """
        Refine an existing design based on feedback.
        
        Args:
            message: Message containing refinement requests
            
        Returns:
            Message with refined design
        """
        try:
            existing_design = message.payload.get("design", {})
            refinement_requests = message.payload.get("refinements", [])
            
            refined_design = await self._apply_refinements(existing_design, refinement_requests)
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="design_refined",
                message_type=MessageType.DESIGN_COMPLETED,
                payload={"design": refined_design},
                session_id=message.session_id
            )
            
            return response_message
            
        except Exception as e:
            logger.error(f"Design refinement failed: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def _determine_design_approach(self, requirements: Dict[str, Any]) -> str:
        """Determine the best design approach based on requirements."""
        
        # Use Claude to analyze requirements and suggest approach
        prompt = f"""
        Analyze these requirements and suggest the best design approach:
        
        {requirements}
        
        Consider:
        1. Project complexity
        2. Scale requirements
        3. Real-time needs
        4. Integration complexity
        5. Safety requirements
        
        Choose one of these approaches:
        - "document_processing" - for document-heavy workflows
        - "data_pipeline" - for data processing pipelines
        - "customer_support" - for customer-facing systems
        - "monitoring_alerting" - for monitoring systems
        - "deployment_automation" - for DevOps workflows
        - "custom" - for unique requirements
        
        Return just the approach name.
        """
        
        try:
            response = await self.anthropic.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=100,
                messages=[{"role": "user", "content": prompt}]
            )
            
            approach = response.content[0].text.strip().lower()
            return approach if approach in self._design_templates else "custom"
            
        except Exception as e:
            logger.error(f"Failed to determine design approach: {str(e)}")
            return "custom"
    
    async def _create_agents(self, requirements: Dict[str, Any], patterns: Dict[str, Any], approach: str) -> Dict[str, AgentDefinition]:
        """Create agent definitions based on requirements and patterns."""
        
        if approach in self._design_templates:
            # Use template as starting point
            template_agents = self._design_templates[approach]["agents"]
            agents = {}
            
            for agent_data in template_agents:
                agent = AgentDefinition(**agent_data)
                
                # Customize based on requirements
                await self._customize_agent_for_requirements(agent, requirements)
                
                agents[agent.agent_id] = agent
            
            return agents
        else:
            # Custom design - use Claude to generate agents
            return await self._generate_custom_agents(requirements, patterns)
    
    async def _customize_agent_for_requirements(self, agent: AgentDefinition, requirements: Dict[str, Any]):
        """Customize an agent based on specific requirements."""
        
        # Add required integrations
        integrations = requirements.get("integrations_required", [])
        if integrations:
            agent.mcp_integrations.extend(integrations)
        
        # Adjust performance requirements
        performance_reqs = requirements.get("performance_requirements", {})
        if "max_response_time_ms" in performance_reqs:
            agent.timeout_seconds = performance_reqs["max_response_time_ms"] // 1000
        
        # Add safety mechanisms based on requirements
        if requirements.get("security_requirements"):
            agent.safety_mechanisms.append(SafetyMechanism.VALIDATION_CHECK)
        
        if requirements.get("reliability_requirements"):
            agent.safety_mechanisms.append(SafetyMechanism.RETRY_LOGIC)
            agent.retry_attempts = 3
    
    async def _generate_custom_agents(self, requirements: Dict[str, Any], patterns: Dict[str, Any]) -> Dict[str, AgentDefinition]:
        """Generate custom agents using Claude."""
        
        prompt = f"""
        Design a set of agents for this system:
        
        Requirements: {requirements}
        Available Patterns: {patterns}
        
        Design 3-7 agents with:
        1. Clear roles and responsibilities
        2. Appropriate communication patterns
        3. Required integrations
        4. Safety mechanisms
        
        Return a JSON array of agent definitions.
        """
        
        try:
            response = await self.anthropic.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse and create agents
            # In production, implement proper JSON parsing
            agents = {}
            
            # Create a basic set of agents for now
            agents["coordinator"] = AgentDefinition(
                agent_id="coordinator",
                agent_name="Orchestra Coordinator",
                agent_type=AgentType.ROUTER,
                description="Coordinates overall workflow and routes messages",
                responsibilities=["workflow_coordination", "message_routing", "error_handling"],
                communication_patterns=[CommunicationPattern.EVENT_DRIVEN],
                safety_mechanisms=[SafetyMechanism.TIMEOUT, SafetyMechanism.CIRCUIT_BREAKER]
            )
            
            agents["executor"] = AgentDefinition(
                agent_id="executor",
                agent_name="Task Executor",
                agent_type=AgentType.EXECUTOR,
                description="Executes specific tasks and operations",
                responsibilities=["task_execution", "api_calls", "data_processing"],
                communication_patterns=[CommunicationPattern.REQUEST_RESPONSE],
                safety_mechanisms=[SafetyMechanism.RETRY_LOGIC, SafetyMechanism.TIMEOUT]
            )
            
            agents["monitor"] = AgentDefinition(
                agent_id="monitor",
                agent_name="System Monitor",
                agent_type=AgentType.MONITOR,
                description="Monitors system health and performance",
                responsibilities=["health_checks", "performance_monitoring", "alerting"],
                communication_patterns=[CommunicationPattern.PUBLISH_SUBSCRIBE],
                safety_mechanisms=[SafetyMechanism.KILL_SWITCH]
            )
            
            return agents
            
        except Exception as e:
            logger.error(f"Failed to generate custom agents: {str(e)}")
            # Return basic agents as fallback
            return {}
    
    async def _design_message_flows(self, agents: Dict[str, AgentDefinition], requirements: Dict[str, Any]) -> List[MessageFlow]:
        """Design message flows between agents."""
        
        flows = []
        agent_ids = list(agents.keys())
        
        # Create basic flows
        if len(agent_ids) >= 2:
            # Coordinator to executor flow
            flows.append(MessageFlow(
                flow_id="coordinator_to_executor",
                from_agent="coordinator",
                to_agent="executor",
                message_type="task_assignment",
                communication_pattern=CommunicationPattern.REQUEST_RESPONSE,
                expected_frequency="high",
                latency_requirement_ms=1000
            ))
            
            # Executor to coordinator flow
            flows.append(MessageFlow(
                flow_id="executor_to_coordinator",
                from_agent="executor",
                to_agent="coordinator",
                message_type="task_completion",
                communication_pattern=CommunicationPattern.EVENT_DRIVEN,
                expected_frequency="high",
                latency_requirement_ms=500
            ))
            
            # Monitor to all agents (health checks)
            for agent_id in agent_ids:
                if agent_id != "monitor":
                    flows.append(MessageFlow(
                        flow_id=f"monitor_to_{agent_id}",
                        from_agent="monitor",
                        to_agent=agent_id,
                        message_type="health_check",
                        communication_pattern=CommunicationPattern.REQUEST_RESPONSE,
                        expected_frequency="medium",
                        latency_requirement_ms=2000
                    ))
        
        return flows
    
    async def _design_coordination_protocol(self, agents: Dict[str, AgentDefinition], requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Design the coordination protocol for the orchestra."""
        
        return {
            "protocol_type": "event_driven",
            "message_bus": "redis",
            "message_format": "json",
            "correlation_ids": True,
            "message_routing": "topic_based",
            "error_handling": "dead_letter_queue",
            "ordering_guarantees": "at_least_once",
            "delivery_guarantees": "persistent"
        }
    
    async def _design_safety_mechanisms(self, agents: Dict[str, AgentDefinition], requirements: Dict[str, Any]) -> List[SafetyMechanism]:
        """Design safety mechanisms for the orchestra."""
        
        mechanisms = [
            SafetyMechanism.TIMEOUT,
            SafetyMechanism.RETRY_LOGIC,
            SafetyMechanism.CIRCUIT_BREAKER
        ]
        
        # Add approval gates for production or critical systems
        if requirements.get("deployment_environment") == "production":
            mechanisms.append(SafetyMechanism.APPROVAL_GATE)
        
        # Add rate limiting for high-traffic systems
        if requirements.get("user_count_estimate") in ["100-1000 users", "1000+ users"]:
            mechanisms.append(SafetyMechanism.RATE_LIMIT)
        
        # Add kill switch for critical systems
        if requirements.get("reliability_requirements"):
            mechanisms.append(SafetyMechanism.KILL_SWITCH)
        
        return mechanisms
    
    async def _design_observability(self, agents: Dict[str, AgentDefinition], requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Design observability setup for the orchestra."""
        
        return {
            "logging": {
                "level": "INFO",
                "format": "structured_json",
                "correlation_ids": True,
                "aggregation": "centralized"
            },
            "metrics": {
                "system": "prometheus",
                "custom_metrics": ["agent_performance", "message_latency", "error_rates"],
                "collection_interval": "30s"
            },
            "tracing": {
                "system": "opentelemetry",
                "sampling_rate": "0.1",
                "span_exporters": ["jaeger", "prometheus"]
            },
            "dashboards": {
                "operations": True,
                "performance": True,
                "business": True,
                "alerts": True
            }
        }
    
    def _estimate_load(self, requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate expected load based on requirements."""
        
        user_scale = requirements.get("user_count_estimate", "1-10 users")
        
        load_estimates = {
            "1-10 users": {"requests_per_second": 1, "concurrent_users": 5},
            "10-100 users": {"requests_per_second": 10, "concurrent_users": 25},
            "100-1000 users": {"requests_per_second": 100, "concurrent_users": 200},
            "1000+ users": {"requests_per_second": 1000, "concurrent_users": 2000}
        }
        
        return load_estimates.get(user_scale, load_estimates["1-10 users"])
    
    def _design_scalability_plan(self, agents: Dict[str, AgentDefinition], requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Design scalability plan for the orchestra."""
        
        return {
            "horizontal_scaling": True,
            "auto_scaling": True,
            "load_balancing": "round_robin",
            "scaling_triggers": ["cpu_usage", "memory_usage", "queue_depth"],
            "max_replicas": 10,
            "min_replicas": 1,
            "target_utilization": 0.7
        }
    
    def _identify_integrations(self, requirements: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify required external integrations."""
        
        integrations = []
        
        # Add integrations from requirements
        for integration in requirements.get("integrations_required", []):
            integrations.append({
                "name": integration,
                "type": "mcp",
                "required": True,
                "purpose": f"Integration with {integration}"
            })
        
        return integrations
    
    def _extract_mcp_integrations(self, agents: Dict[str, AgentDefinition]) -> List[str]:
        """Extract all MCP integrations from agents."""
        
        integrations = set()
        for agent in agents.values():
            integrations.update(agent.mcp_integrations)
        
        return list(integrations)
    
    async def _identify_risks(self, agents: Dict[str, AgentDefinition], requirements: Dict[str, Any]) -> List[str]:
        """Identify potential risks in the design."""
        
        risks = []
        
        # Complexity risks
        if len(agents) > 7:
            risks.append("High complexity with many agents may impact maintainability")
        
        # Integration risks
        if len(requirements.get("integrations_required", [])) > 3:
            risks.append("Many external integrations increase failure surface")
        
        # Performance risks
        if requirements.get("performance_requirements", {}).get("max_response_time_ms", 1000) < 100:
            risks.append("Very low latency requirements may be challenging")
        
        # Scale risks
        if requirements.get("user_count_estimate") == "1000+ users":
            risks.append("High scale requires careful capacity planning")
        
        return risks
    
    async def _apply_refinements(self, existing_design: Dict[str, Any], refinements: List[str]) -> Dict[str, Any]:
        """Apply refinements to an existing design."""
        
        refined_design = existing_design.copy()
        
        for refinement in refinements:
            if "add error handling" in refinement.lower():
                # Add error handling to all agents
                for agent in refined_design.get("agents", {}).values():
                    if "error_handling" not in agent:
                        agent["error_handling"] = "comprehensive"
            
            elif "add monitoring" in refinement.lower():
                # Add monitoring setup
                if "monitoring_setup" not in refined_design:
                    refined_design["monitoring_setup"] = self._design_observability({}, {})
            
            elif "add security" in refinement.lower():
                # Add security mechanisms
                for agent in refined_design.get("agents", {}).values():
                    if "safety_mechanisms" not in agent:
                        agent["safety_mechanisms"] = ["validation_check"]
        
        return refined_design
    
    def _create_error_message(self, original_message: AgentMessage, error: str) -> AgentMessage:
        """Create an error message."""
        return AgentMessage(
            correlation_id=original_message.correlation_id,
            agent_id=self.agent_id,
            intent="error_occurred",
            message_type=MessageType.ERROR_OCCURRED,
            payload={
                "error_type": "ArchitectureDesignError",
                "error_message": error,
                "context": {"agent": self.agent_id}
            },
            session_id=original_message.session_id
        )
    
    def _get_document_processing_template(self) -> Dict[str, Any]:
        """Get template for document processing systems."""
        return {
            "agents": [
                {
                    "agent_id": "document_ingester",
                    "agent_name": "Document Ingester",
                    "agent_type": "data_processor",
                    "description": "Ingests and validates incoming documents",
                    "responsibilities": ["file_validation", "format_detection", "metadata_extraction"]
                },
                {
                    "agent_id": "content_extractor",
                    "agent_name": "Content Extractor",
                    "agent_type": "data_processor",
                    "description": "Extracts structured content from documents",
                    "responsibilities": ["text_extraction", "entity_recognition", "data_structuring"]
                },
                {
                    "agent_id": "validation_agent",
                    "agent_name": "Validation Agent",
                    "agent_type": "validator",
                    "description": "Validates extracted content against business rules",
                    "responsibilities": ["business_rule_validation", "compliance_check", "quality_assurance"]
                }
            ]
        }
    
    def _get_data_pipeline_template(self) -> Dict[str, Any]:
        """Get template for data pipeline systems."""
        return {
            "agents": [
                {
                    "agent_id": "data_collector",
                    "agent_name": "Data Collector",
                    "agent_type": "data_processor",
                    "description": "Collects data from various sources",
                    "responsibilities": ["source_connection", "data_fetching", "initial_validation"]
                },
                {
                    "agent_id": "data_transformer",
                    "agent_name": "Data Transformer",
                    "agent_type": "data_processor",
                    "description": "Transforms and processes data",
                    "responsibilities": ["data_cleaning", "transformation", "enrichment"]
                },
                {
                    "agent_id": "data_loader",
                    "agent_name": "Data Loader",
                    "agent_type": "storage",
                    "description": "Loads processed data to destination",
                    "responsibilities": ["batch_loading", "streaming", "error_handling"]
                }
            ]
        }
    
    def _get_customer_support_template(self) -> Dict[str, Any]:
        """Get template for customer support systems."""
        return {
            "agents": [
                {
                    "agent_id": "ticket_router",
                    "agent_name": "Ticket Router",
                    "agent_type": "router",
                    "description": "Routes customer tickets to appropriate handlers",
                    "responsibilities": ["ticket_classification", "priority_assignment", "agent_routing"]
                },
                {
                    "agent_id": "response_generator",
                    "agent_name": "Response Generator",
                    "agent_type": "executor",
                    "description": "Generates automated responses",
                    "responsibilities": ["response_generation", "template_selection", "personalization"]
                },
                {
                    "agent_id": "escalation_manager",
                    "agent_name": "Escalation Manager",
                    "agent_type": "monitor",
                    "description": "Manages ticket escalations",
                    "responsibilities": ["escalation_detection", "human_handoff", "sl_monitoring"]
                }
            ]
        }
    
    def _get_monitoring_template(self) -> Dict[str, Any]:
        """Get template for monitoring and alerting systems."""
        return {
            "agents": [
                {
                    "agent_id": "metrics_collector",
                    "agent_name": "Metrics Collector",
                    "agent_type": "monitor",
                    "description": "Collects system metrics",
                    "responsibilities": ["metric_collection", "aggregation", "storage"]
                },
                {
                    "agent_id": "alert_evaluator",
                    "agent_name": "Alert Evaluator",
                    "agent_type": "monitor",
                    "description": "Evaluates alerts and conditions",
                    "responsibilities": ["threshold_evaluation", "alert_generation", "suppression"]
                },
                {
                    "agent_id": "notification_sender",
                    "agent_name": "Notification Sender",
                    "agent_type": "notification",
                    "description": "Sends alerts and notifications",
                    "responsibilities": ["notification_routing", "delivery", "acknowledgment"]
                }
            ]
        }
    
    def _get_deployment_template(self) -> Dict[str, Any]:
        """Get template for deployment automation systems."""
        return {
            "agents": [
                {
                    "agent_id": "build_agent",
                    "agent_name": "Build Agent",
                    "agent_type": "executor",
                    "description": "Manages build processes",
                    "responsibilities": ["code_compilation", "artifact_creation", "quality_checks"]
                },
                {
                    "agent_id": "deployment_agent",
                    "agent_name": "Deployment Agent",
                    "agent_type": "executor",
                    "description": "Manages deployment processes",
                    "responsibilities": ["environment_provisioning", "service_deployment", "health_checks"]
                },
                {
                    "agent_id": "rollback_agent",
                    "agent_name": "Rollback Agent",
                    "agent_type": "monitor",
                    "description": "Manages rollback processes",
                    "responsibilities": ["deployment_monitoring", "rollback_triggering", "recovery"]
                }
            ]
        }
