"""
Implementation Planner Agent - Generates phased implementation plans with cost estimates.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from anthropic import AsyncAnthropic
from schemas.messages import AgentMessage, MessageType, ImplementationPlanPayload
from schemas.design import OrchestraDesign

logger = logging.getLogger(__name__)


class ImplementationPlannerAgent:
    """Creates detailed implementation plans for agent orchestras."""
    
    def __init__(self, anthropic_client: AsyncAnthropic):
        self.anthropic = anthropic_client
        self.agent_id = "implementation_planner"
        
        # Cost estimation constants
        self._cost_factors = {
            "agent_development": {"hours_per_agent": 40, "hourly_rate": 150},
            "integration_development": {"hours_per_integration": 20, "hourly_rate": 150},
            "testing": {"percentage_of_dev": 0.3},
            "deployment": {"hours_per_project": 40, "hourly_rate": 150},
            "documentation": {"percentage_of_dev": 0.2}
        }
        
        # Cloud cost estimates (monthly)
        self._cloud_costs = {
            "cloud_run_per_agent": 15,
            "redis_managed": 15,
            "postgres_neon": 25,
            "claude_api_per_1k_calls": 0.05,
            "monitoring": 10
        }
    
    async def create_implementation_plan(self, message: AgentMessage) -> AgentMessage:
        """
        Create a detailed implementation plan for the orchestra design.
        
        Args:
            message: Message containing orchestra design
            
        Returns:
            Message containing implementation plan
        """
        try:
            design_data = message.payload.get("design", {})
            design = OrchestraDesign(**design_data)
            
            logger.info(f"Creating implementation plan for correlation_id: {message.correlation_id}")
            
            # Generate implementation phases
            phases = await self._generate_implementation_phases(design)
            
            # Create file structure
            file_structure = await self._create_file_structure(design)
            
            # Identify dependencies
            dependencies = await self._identify_dependencies(design)
            
            # Estimate timeline
            timeline_estimate = await self._estimate_timeline(design, phases)
            
            # Calculate costs
            cost_estimate = await self._calculate_costs(design, phases)
            
            # Identify resource requirements
            resource_requirements = await self._identify_resource_requirements(design)
            
            # Identify risks
            risks = await self._identify_implementation_risks(design, phases)
            
            # Create payload
            payload = ImplementationPlanPayload(
                phases=phases,
                file_structure=file_structure,
                dependencies=dependencies,
                timeline_estimate=timeline_estimate,
                cost_estimate=cost_estimate,
                resource_requirements=resource_requirements,
                risks=risks
            )
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="plan_completed",
                message_type=MessageType.PLAN_COMPLETED,
                payload=payload.dict(),
                session_id=message.session_id
            )
            
            logger.info(f"Implementation plan created for correlation_id: {message.correlation_id}")
            return response_message
            
        except Exception as e:
            logger.error(f"Implementation planning failed: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def create_dry_run_plan(self, message: AgentMessage) -> AgentMessage:
        """
        Create a dry-run execution plan for validation.
        
        Args:
            message: Message requesting dry-run plan
            
        Returns:
            Message containing dry-run plan
        """
        try:
            design_data = message.payload.get("design", {})
            design = OrchestraDesign(**design_data)
            
            # Generate execution steps with estimates
            execution_steps = await self._generate_execution_steps(design)
            
            # Calculate resource usage
            resource_usage = await self._estimate_resource_usage(design)
            
            # Identify approval requirements
            approval_requirements = await self._identify_approval_requirements(design)
            
            dry_run_plan = {
                "execution_steps": execution_steps,
                "estimated_duration": sum(step.get("duration_minutes", 0) for step in execution_steps),
                "resource_usage": resource_usage,
                "approval_requirements": approval_requirements,
                "risk_level": self._assess_risk_level(design),
                "can_rollback": True
            }
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="dry_run_plan_created",
                message_type=MessageType.PLAN_COMPLETED,
                payload={"dry_run_plan": dry_run_plan},
                session_id=message.session_id
            )
            
            return response_message
            
        except Exception as e:
            logger.error(f"Dry-run planning failed: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def _generate_implementation_phases(self, design: OrchestraDesign) -> List[Dict[str, Any]]:
        """Generate phased implementation plan."""
        
        phases = []
        
        # Phase 1: Foundation
        phases.append({
            "phase_id": "foundation",
            "phase_name": "Foundation Setup",
            "description": "Set up core infrastructure and basic agent framework",
            "duration_weeks": 2,
            "tasks": [
                "Set up project structure and dependencies",
                "Implement message bus and communication protocols",
                "Create base agent classes and interfaces",
                "Set up database schema and migrations",
                "Implement basic logging and error handling"
            ],
            "deliverables": [
                "Project skeleton with all directories",
                "Message bus implementation",
                "Base agent framework",
                "Database schema",
                "Basic observability setup"
            ],
            "dependencies": [],
            "risks": ["Technology setup complexity", "Integration issues"],
            "success_criteria": ["Message bus working", "Agents can communicate", "Database operational"]
        })
        
        # Phase 2: Core Agents
        phases.append({
            "phase_id": "core_agents",
            "phase_name": "Core Agent Implementation",
            "description": "Implement the main agents for the orchestra",
            "duration_weeks": max(3, len(design.agents) // 2),
            "tasks": [
                f"Implement {len(design.agents)} core agents",
                "Define agent communication flows",
                "Implement agent coordination logic",
                "Add agent-specific business logic",
                "Create agent configuration management"
            ],
            "deliverables": [
                "All core agents implemented",
                "Message flows working",
                "Agent coordination functional",
                "Business logic implemented"
            ],
            "dependencies": ["foundation"],
            "risks": ["Complex agent interactions", "Performance bottlenecks"],
            "success_criteria": ["All agents functional", "Message flows working", "Basic orchestration working"]
        })
        
        # Phase 3: Integrations
        phases.append({
            "phase_id": "integrations",
            "phase_name": "External Integrations",
            "description": "Implement external system integrations",
            "duration_weeks": max(2, len(design.external_integrations)),
            "tasks": [
                f"Implement {len(design.external_integrations)} external integrations",
                "Set up MCP server connections",
                "Implement authentication and security",
                "Add integration error handling",
                "Create integration tests"
            ],
            "deliverables": [
                "All integrations implemented",
                "MCP connections working",
                "Security measures in place",
                "Integration test suite"
            ],
            "dependencies": ["core_agents"],
            "risks": ["External API changes", "Authentication issues", "Rate limiting"],
            "success_criteria": ["All integrations working", "Security validated", "Tests passing"]
        })
        
        # Phase 4: Safety & Reliability
        phases.append({
            "phase_id": "safety_reliability",
            "phase_name": "Safety and Reliability",
            "description": "Implement safety mechanisms and reliability features",
            "duration_weeks": 2,
            "tasks": [
                "Implement safety mechanisms",
                "Add comprehensive error handling",
                "Implement retry logic and circuit breakers",
                "Set up monitoring and alerting",
                "Create health checks and diagnostics"
            ],
            "deliverables": [
                "Safety mechanisms implemented",
                "Error handling comprehensive",
                "Monitoring and alerting setup",
                "Health checks functional"
            ],
            "dependencies": ["integrations"],
            "risks": ["Complex failure scenarios", "Performance impact"],
            "success_criteria": ["Safety features working", "Error handling robust", "Monitoring operational"]
        })
        
        # Phase 5: Testing & Validation
        phases.append({
            "phase_id": "testing_validation",
            "phase_name": "Testing and Validation",
            "description": "Comprehensive testing and validation",
            "duration_weeks": 2,
            "tasks": [
                "Write comprehensive unit tests",
                "Implement integration tests",
                "Create end-to-end test scenarios",
                "Performance testing and optimization",
                "Security testing and validation"
            ],
            "deliverables": [
                "Complete test suite",
                "Performance benchmarks",
                "Security validation report",
                "Test documentation"
            ],
            "dependencies": ["safety_reliability"],
            "risks": ["Test coverage gaps", "Performance issues discovered"],
            "success_criteria": ["Test coverage > 90%", "Performance meets requirements", "Security validated"]
        })
        
        # Phase 6: Deployment
        phases.append({
            "phase_id": "deployment",
            "phase_name": "Production Deployment",
            "description": "Deploy to production environment",
            "duration_weeks": 1,
            "tasks": [
                "Set up production infrastructure",
                "Configure deployment pipelines",
                "Implement monitoring and logging",
                "Create deployment documentation",
                "Train operations team"
            ],
            "deliverables": [
                "Production deployment",
                "CI/CD pipeline",
                "Monitoring dashboards",
                "Deployment documentation",
                "Operations runbook"
            ],
            "dependencies": ["testing_validation"],
            "risks": ["Deployment failures", "Configuration issues"],
            "success_criteria": ["System deployed", "Monitoring working", "Team trained"]
        })
        
        return phases
    
    async def _create_file_structure(self, design: OrchestraDesign) -> Dict[str, Any]:
        """Create the file structure for the project."""
        
        structure = {
            "project_root": {
                "agents": {},
                "schemas": {},
                "orchestrator": {},
                "integrations": {},
                "observability": {},
                "tests": {},
                "deployment": {},
                "docs": {},
                "scripts": {}
            }
        }
        
        # Add agent files
        for agent_id, agent in design.agents.items():
            structure["project_root"]["agents"][f"{agent_id}.py"] = f"Implementation of {agent.agent_name}"
        
        # Add schema files
        structure["project_root"]["schemas"]["messages.py"] = "Message schemas"
        structure["project_root"]["schemas"]["agents.py"] = "Agent configuration schemas"
        structure["project_root"]["schemas"]["orchestra.py"] = "Orchestra configuration schemas"
        
        # Add orchestrator files
        structure["project_root"]["orchestrator"]["message_bus.py"] = "Message bus implementation"
        structure["project_root"]["orchestrator"]["router.py"] = "Message routing logic"
        structure["project_root"]["orchestrator"]["coordinator.py"] = "Orchestration coordination"
        structure["project_root"]["orchestrator"]["context_manager.py"] = "Session and context management"
        
        # Add integration files
        for integration in design.external_integrations:
            integration_name = integration.get("name", "unknown")
            structure["project_root"]["integrations"][f"{integration_name.lower()}_client.py"] = f"{integration_name} integration client"
        
        # Add observability files
        structure["project_root"]["observability"]["logging.py"] = "Logging configuration"
        structure["project_root"]["observability"]["metrics.py"] = "Metrics collection"
        structure["project_root"]["observability"]["tracing.py"] = "Distributed tracing"
        structure["project_root"]["observability"]["health_checks.py"] = "Health check endpoints"
        
        # Add test files
        structure["project_root"]["tests"]["test_agents.py"] = "Agent unit tests"
        structure["project_root"]["tests"]["test_integration.py"] = "Integration tests"
        structure["project_root"]["tests"]["test_e2e.py"] = "End-to-end tests"
        structure["project_root"]["tests"]["mock_mcp_servers.py"] = "Mock MCP servers for testing"
        
        # Add deployment files
        structure["project_root"]["deployment"]["docker-compose.yml"] = "Local development setup"
        structure["project_root"]["deployment"]["Dockerfile"] = "Container configuration"
        structure["project_root"]["deployment"]["kubernetes/"] = "Kubernetes manifests"
        structure["project_root"]["deployment"]["ci-cd.yml"] = "CI/CD pipeline configuration"
        
        # Add documentation files
        structure["project_root"]["docs"]["architecture.md"] = "System architecture documentation"
        structure["project_root"]["docs"]["deployment-guide.md"] = "Deployment guide"
        structure["project_root"]["docs"]["user-guide.md"] = "User guide"
        structure["project_root"]["docs"]["api-reference.md"] = "API reference"
        
        return structure
    
    async def _identify_dependencies(self, design: OrchestraDesign) -> List[str]:
        """Identify project dependencies."""
        
        dependencies = [
            "fastapi>=0.104.0",
            "uvicorn[standard]>=0.24.0",
            "pydantic>=2.5.0",
            "sqlalchemy>=2.0.0",
            "alembic>=1.13.0",
            "asyncpg>=0.29.0",
            "redis>=5.0.0",
            "aioredis>=2.0.0",
            "httpx>=0.25.0",
            "anthropic>=0.7.0",
            "prometheus-client>=0.19.0",
            "structlog>=23.2.0",
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0"
        ]
        
        # Add integration-specific dependencies
        for integration in design.external_integrations:
            integration_name = integration.get("name", "").lower()
            if "github" in integration_name:
                dependencies.append("PyGithub>=2.1.0")
            elif "slack" in integration_name:
                dependencies.append("slack-sdk>=3.26.0")
            elif "stripe" in integration_name:
                dependencies.append("stripe>=7.0.0")
        
        return sorted(list(set(dependencies)))
    
    async def _estimate_timeline(self, design: OrchestraDesign, phases: List[Dict[str, Any]]) -> str:
        """Estimate overall timeline."""
        
        total_weeks = sum(phase.get("duration_weeks", 0) for phase in phases)
        
        # Add buffer for complexity
        if len(design.agents) > 5:
            total_weeks *= 1.2
        if len(design.external_integrations) > 3:
            total_weeks *= 1.1
        
        total_weeks = int(total_weeks)
        
        if total_weeks < 4:
            return f"{total_weeks} weeks"
        elif total_weeks < 12:
            months = total_weeks // 4
            remaining_weeks = total_weeks % 4
            return f"{months} months{', ' + str(remaining_weeks) + ' weeks' if remaining_weeks else ''}"
        else:
            return f"{total_weeks // 4} months"
    
    async def _calculate_costs(self, design: OrchestraDesign, phases: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate implementation and operational costs."""
        
        # Development costs
        agent_count = len(design.agents)
        integration_count = len(design.external_integrations)
        
        dev_costs = {
            "agent_development": agent_count * self._cost_factors["agent_development"]["hours_per_agent"] * self._cost_factors["agent_development"]["hourly_rate"],
            "integration_development": integration_count * self._cost_factors["integration_development"]["hours_per_integration"] * self._cost_factors["integration_development"]["hourly_rate"],
            "testing": sum(phase.get("duration_weeks", 0) * 40 * self._cost_factors["agent_development"]["hourly_rate"] * self._cost_factors["testing"]["percentage_of_dev"] for phase in phases),
            "deployment": self._cost_factors["deployment"]["hours_per_project"] * self._cost_factors["deployment"]["hourly_rate"],
            "documentation": sum(phase.get("duration_weeks", 0) * 40 * self._cost_factors["agent_development"]["hourly_rate"] * self._cost_factors["documentation"]["percentage_of_dev"] for phase in phases)
        }
        
        total_development_cost = sum(dev_costs.values())
        
        # Monthly operational costs
        monthly_costs = {
            "cloud_run_agents": agent_count * self._cloud_costs["cloud_run_per_agent"],
            "redis": self._cloud_costs["redis_managed"],
            "postgres": self._cloud_costs["postgres_neon"],
            "claude_api": self._cloud_costs["claude_api_per_1k_calls"] * 1000,  # Assume 1000 calls/month
            "monitoring": self._cloud_costs["monitoring"]
        }
        
        total_monthly_cost = sum(monthly_costs.values())
        
        return {
            "development": {
                "one_time_total": total_development_cost,
                "breakdown": dev_costs
            },
            "monthly_operational": {
                "total": total_monthly_cost,
                "breakdown": monthly_costs
            },
            "annual_operational": total_monthly_cost * 12,
            "total_first_year": total_development_cost + (total_monthly_cost * 12)
        }
    
    async def _identify_resource_requirements(self, design: OrchestraDesign) -> Dict[str, Any]:
        """Identify resource requirements for implementation."""
        
        return {
            "team_composition": {
                "backend_developers": max(2, len(design.agents) // 3),
                "devops_engineer": 1,
                "qa_engineer": 1,
                "technical_writer": 0.5
            },
            "infrastructure": {
                "development_environment": {
                    "cpu_cores": 8,
                    "memory_gb": 16,
                    "storage_gb": 100
                },
                "staging_environment": {
                    "cpu_cores": 4,
                    "memory_gb": 8,
                    "storage_gb": 50
                },
                "production_environment": {
                    "cpu_cores": len(design.agents) * 2,
                    "memory_gb": len(design.agents) * 4,
                    "storage_gb": 200
                }
            },
            "external_services": {
                "database": "PostgreSQL (Neon)",
                "message_bus": "Redis (managed)",
                "monitoring": "Prometheus + Grafana",
                "llm_provider": "Anthropic Claude"
            },
            "tools_and_licenses": {
                "development_tools": "Open source",
                "monitoring_tools": "Open source",
                "cloud_platform": "GCP/AWS/Azure",
                "llm_costs": "Pay-per-use"
            }
        }
    
    async def _identify_implementation_risks(self, design: OrchestraDesign, phases: List[Dict[str, Any]]) -> List[str]:
        """Identify implementation risks."""
        
        risks = []
        
        # Complexity risks
        if len(design.agents) > 7:
            risks.append("High agent complexity may impact development velocity")
        
        if len(design.message_flows) > 15:
            risks.append("Complex message flows may lead to integration challenges")
        
        # Integration risks
        if len(design.external_integrations) > 3:
            risks.append("Multiple external integrations increase dependency risk")
        
        # Technology risks
        if design.technology_stack.get("novel_technologies", []):
            risks.append("Novel technologies may have learning curves and unknown issues")
        
        # Performance risks
        expected_load = design.expected_load.get("requests_per_second", 0)
        if expected_load > 100:
            risks.append("High expected load requires careful performance optimization")
        
        # Timeline risks
        total_weeks = sum(phase.get("duration_weeks", 0) for phase in phases)
        if total_weeks > 16:
            risks.append("Long timeline increases risk of requirement changes and team turnover")
        
        return risks
    
    async def _generate_execution_steps(self, design: OrchestraDesign) -> List[Dict[str, Any]]:
        """Generate detailed execution steps for dry-run."""
        
        steps = []
        
        # Setup steps
        steps.append({
            "step_id": "setup_project",
            "name": "Set up project structure",
            "description": "Create project directories and initialize git repository",
            "duration_minutes": 15,
            "requires_approval": False,
            "risk_level": "low"
        })
        
        steps.append({
            "step_id": "install_dependencies",
            "name": "Install dependencies",
            "description": "Install Python packages and set up virtual environment",
            "duration_minutes": 10,
            "requires_approval": False,
            "risk_level": "low"
        })
        
        # Agent implementation steps
        for i, (agent_id, agent) in enumerate(design.agents.items()):
            steps.append({
                "step_id": f"implement_agent_{agent_id}",
                "name": f"Implement {agent.agent_name}",
                "description": f"Create the {agent.agent_name} agent with its responsibilities",
                "duration_minutes": 60,
                "requires_approval": False,
                "risk_level": "medium"
            })
        
        # Integration steps
        for integration in design.external_integrations:
            steps.append({
                "step_id": f"setup_integration_{integration.get('name', 'unknown')}",
                "name": f"Set up {integration.get('name', 'unknown')} integration",
                "description": f"Configure and test {integration.get('name', 'unknown')} integration",
                "duration_minutes": 30,
                "requires_approval": True if integration.get("required", False) else False,
                "risk_level": "medium"
            })
        
        # Testing steps
        steps.append({
            "step_id": "run_tests",
            "name": "Run test suite",
            "description": "Execute unit, integration, and end-to-end tests",
            "duration_minutes": 20,
            "requires_approval": False,
            "risk_level": "low"
        })
        
        # Deployment step
        steps.append({
            "step_id": "deploy_system",
            "name": "Deploy system",
            "description": "Deploy the complete system to production",
            "duration_minutes": 45,
            "requires_approval": True,
            "risk_level": "high"
        })
        
        return steps
    
    async def _estimate_resource_usage(self, design: OrchestraDesign) -> Dict[str, Any]:
        """Estimate resource usage for execution."""
        
        return {
            "compute": {
                "cpu_cores_required": len(design.agents) * 0.5,
                "memory_gb_required": len(design.agents) * 1,
                "disk_gb_required": 50
            },
            "network": {
                "bandwidth_mbps": 100,
                "api_calls_estimated": 1000,
                "data_transfer_gb": 10
            },
            "storage": {
                "database_gb": 20,
                "logs_gb": 5,
                "artifacts_gb": 2
            },
            "external_apis": {
                "claude_tokens_estimated": 50000,
                "github_api_calls": 100,
                "other_api_calls": len(design.external_integrations) * 50
            }
        }
    
    async def _identify_approval_requirements(self, design: OrchestraDesign) -> List[Dict[str, Any]]:
        """Identify steps that require approval."""
        
        approval_requirements = []
        
        # Production deployment always needs approval
        approval_requirements.append({
            "step": "deploy_system",
            "reason": "Production deployment requires approval",
            "approver": "operations_team",
            "criteria": ["All tests passing", "Security review complete", "Performance benchmarks met"]
        })
        
        # Required integrations need approval
        for integration in design.external_integrations:
            if integration.get("required", False):
                approval_requirements.append({
                    "step": f"setup_integration_{integration.get('name', 'unknown')}",
                    "reason": f"Required integration {integration.get('name')} needs approval",
                    "approver": "technical_lead",
                    "criteria": ["API access granted", "Security review complete", "Testing successful"]
                })
        
        return approval_requirements
    
    def _assess_risk_level(self, design: OrchestraDesign) -> str:
        """Assess overall risk level of the implementation."""
        
        risk_score = 0
        
        # Agent count risk
        if len(design.agents) > 7:
            risk_score += 2
        elif len(design.agents) > 5:
            risk_score += 1
        
        # Integration risk
        if len(design.external_integrations) > 5:
            risk_score += 2
        elif len(design.external_integrations) > 3:
            risk_score += 1
        
        # Complexity risk
        if len(design.message_flows) > 15:
            risk_score += 2
        elif len(design.message_flows) > 10:
            risk_score += 1
        
        # Scale risk
        expected_load = design.expected_load.get("requests_per_second", 0)
        if expected_load > 1000:
            risk_score += 2
        elif expected_load > 100:
            risk_score += 1
        
        if risk_score >= 5:
            return "high"
        elif risk_score >= 3:
            return "medium"
        else:
            return "low"
    
    def _create_error_message(self, original_message: AgentMessage, error: str) -> AgentMessage:
        """Create an error message."""
        return AgentMessage(
            correlation_id=original_message.correlation_id,
            agent_id=self.agent_id,
            intent="error_occurred",
            message_type=MessageType.ERROR_OCCURRED,
            payload={
                "error_type": "ImplementationPlanningError",
                "error_message": error,
                "context": {"agent": self.agent_id}
            },
            session_id=original_message.session_id
        )
