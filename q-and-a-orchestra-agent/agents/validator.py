"""
Validator Agent - Reviews designs against best practices and safety mechanisms.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from anthropic import AsyncAnthropic
from schemas.messages import AgentMessage, MessageType, ValidationPayload
from schemas.design import OrchestraDesign, ORCHESTRA_VALIDATION_RULES, ValidationResult

logger = logging.getLogger(__name__)


class ValidatorAgent:
    """Validates orchestra designs against best practices and safety requirements."""
    
    def __init__(self, anthropic_client: AsyncAnthropic):
        self.anthropic = anthropic_client
        self.agent_id = "validator"
        
        # Validation rules
        self.validation_rules = ORCHESTRA_VALIDATION_RULES
        
        # Best practices checklist
        self.best_practices_checklist = {
            "architecture": [
                "clear_agent_responsibilities",
                "well_defined_message_flows",
                "appropriate_communication_patterns",
                "scalable_design"
            ],
            "safety": [
                "timeout_configuration",
                "retry_logic",
                "error_handling",
                "approval_gates_for_critical_operations",
                "rate_limiting"
            ],
            "observability": [
                "structured_logging",
                "metrics_collection",
                "health_checks",
                "distributed_tracing",
                "alerting"
            ],
            "security": [
                "input_validation",
                "secrets_management",
                "authentication",
                "authorization",
                "audit_logging"
            ],
            "performance": [
                "connection_pooling",
                "caching_strategy",
                "load_balancing",
                "resource_limits",
                "monitoring"
            ]
        }
    
    async def validate_design(self, message: AgentMessage) -> AgentMessage:
        """
        Validate an orchestra design against best practices and safety requirements.
        
        Args:
            message: Message containing orchestra design
            
        Returns:
            Message containing validation results
        """
        try:
            design_data = message.payload.get("design", {})
            design = OrchestraDesign(**design_data)
            
            logger.info(f"Starting design validation for correlation_id: {message.correlation_id}")
            
            # Run validation rules
            rule_results = await self._run_validation_rules(design)
            
            # Check best practices
            best_practices_results = await self._check_best_practices(design)
            
            # Safety check
            safety_check_results = await self._perform_safety_check(design)
            
            # Generate recommendations
            recommendations = await self._generate_recommendations(design, rule_results, best_practices_results)
            
            # Identify warnings and errors
            warnings = self._extract_warnings(rule_results, best_practices_results)
            errors = self._extract_errors(rule_results, best_practices_results)
            
            # Calculate overall score
            overall_score = self._calculate_overall_score(rule_results, best_practices_results, safety_check_results)
            
            # Determine production readiness
            is_production_ready = self._assess_production_readiness(design, rule_results, safety_check_results)
            
            # Create validation result
            validation_result = ValidationResult(
                validation_id=UUID(),
                design_id=design.design_id,
                passed_rules=[rule_id for rule_id, result in rule_results.items() if result.get("passed", False)],
                failed_rules=[rule_id for rule_id, result in rule_results.items() if not result.get("passed", False)],
                warnings=warnings,
                rule_results=rule_results,
                recommendations=recommendations,
                blocking_issues=errors,
                overall_score=overall_score,
                is_production_ready=is_production_ready,
                requires_changes=len(errors) > 0
            )
            
            # Create response payload
            payload = ValidationPayload(
                validation_results={
                    "rules_passed": len(validation_result.passed_rules),
                    "rules_failed": len(validation_result.failed_rules),
                    "total_rules": len(self.validation_rules)
                },
                safety_check_results=safety_check_results,
                best_practices_check=best_practices_results,
                recommendations=recommendations,
                warnings=warnings,
                errors=errors
            )
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="validation_completed",
                message_type=MessageType.VALIDATION_COMPLETED,
                payload={
                    "validation_result": validation_result.dict(),
                    "validation_summary": payload.dict()
                },
                session_id=message.session_id
            )
            
            logger.info(f"Design validation completed for correlation_id: {message.correlation_id}")
            return response_message
            
        except Exception as e:
            logger.error(f"Design validation failed: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def validate_implementation_plan(self, message: AgentMessage) -> AgentMessage:
        """
        Validate an implementation plan for feasibility and completeness.
        
        Args:
            message: Message containing implementation plan
            
        Returns:
            Message containing implementation plan validation
        """
        try:
            implementation_plan = message.payload.get("implementation_plan", {})
            
            # Validate phases
            phase_validation = await self._validate_implementation_phases(implementation_plan.get("phases", []))
            
            # Validate dependencies
            dependency_validation = await self._validate_dependencies(implementation_plan.get("dependencies", []))
            
            # Validate timeline
            timeline_validation = await self._validate_timeline(implementation_plan.get("timeline_estimate", ""))
            
            # Validate costs
            cost_validation = await self._validate_costs(implementation_plan.get("cost_estimate", {}))
            
            # Validate resources
            resource_validation = await self._validate_resources(implementation_plan.get("resource_requirements", {}))
            
            validation_results = {
                "phases": phase_validation,
                "dependencies": dependency_validation,
                "timeline": timeline_validation,
                "costs": cost_validation,
                "resources": resource_validation
            }
            
            # Overall assessment
            overall_valid = all(result.get("valid", False) for result in validation_results.values())
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="implementation_validation_completed",
                message_type=MessageType.VALIDATION_COMPLETED,
                payload={
                    "implementation_validation": validation_results,
                    "overall_valid": overall_valid,
                    "recommendations": self._generate_implementation_recommendations(validation_results)
                },
                session_id=message.session_id
            )
            
            return response_message
            
        except Exception as e:
            logger.error(f"Implementation plan validation failed: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def _run_validation_rules(self, design: OrchestraDesign) -> Dict[str, Dict[str, Any]]:
        """Run all validation rules against the design."""
        
        results = {}
        
        for rule in self.validation_rules:
            try:
                result = await self._evaluate_rule(rule, design)
                results[rule.rule_id] = result
            except Exception as e:
                logger.error(f"Failed to evaluate rule {rule.rule_id}: {str(e)}")
                results[rule.rule_id] = {
                    "passed": False,
                    "error": str(e),
                    "rule_name": rule.rule_name,
                    "severity": rule.severity
                }
        
        return results
    
    async def _evaluate_rule(self, rule: Any, design: OrchestraDesign) -> Dict[str, Any]:
        """Evaluate a single validation rule."""
        
        # Check if rule applies
        if rule.applies_when:
            for condition in rule.applies_when:
                if not self._evaluate_condition(condition, design):
                    return {
                        "passed": True,
                        "skipped": True,
                        "reason": "Rule does not apply",
                        "rule_name": rule.rule_name,
                        "severity": rule.severity
                    }
        
        # Check if rule should not apply
        if rule.does_not_apply_when:
            for condition in rule.does_not_apply_when:
                if self._evaluate_condition(condition, design):
                    return {
                        "passed": True,
                        "skipped": True,
                        "reason": "Rule explicitly does not apply",
                        "rule_name": rule.rule_name,
                        "severity": rule.severity
                    }
        
        # Evaluate the rule logic
        try:
            # In production, implement proper rule evaluation
            # For now, implement some basic rules
            
            if rule.rule_id == "safety_approval_gates":
                passed = SafetyMechanism.APPROVAL_GATE in design.safety_mechanisms
            elif rule.rule_id == "agent_count_limit":
                passed = len(design.agents) <= 10
            elif rule.rule_id == "observability_coverage":
                passed = all(
                    hasattr(agent, 'monitoring_setup') and agent.monitoring_setup
                    for agent in design.agents.values()
                )
            elif rule.rule_id == "timeout_configuration":
                passed = all(
                    hasattr(agent, 'timeout_seconds') and agent.timeout_seconds > 0
                    for agent in design.agents.values()
                )
            elif rule.rule_id == "retry_logic":
                passed = all(
                    hasattr(agent, 'retry_attempts') and agent.retry_attempts >= 1
                    for agent in design.agents.values()
                    if hasattr(agent, 'mcp_integrations') and agent.mcp_integrations
                )
            else:
                passed = True  # Default to passed for unknown rules
            
            return {
                "passed": passed,
                "rule_name": rule.rule_name,
                "severity": rule.severity,
                "description": rule.description
            }
            
        except Exception as e:
            return {
                "passed": False,
                "error": str(e),
                "rule_name": rule.rule_name,
                "severity": rule.severity
            }
    
    def _evaluate_condition(self, condition: str, design: OrchestraDesign) -> bool:
        """Evaluate a condition string against the design."""
        
        # Simple condition evaluation - in production, use more sophisticated parsing
        if "deployment_target == 'production'" in condition:
            return design.deployment_pattern == "production"
        elif "design.complexity in ['simple', 'moderate']" in condition:
            return len(design.agents) <= 5
        else:
            return True  # Default to True for unknown conditions
    
    async def _check_best_practices(self, design: OrchestraDesign) -> Dict[str, bool]:
        """Check design against best practices checklist."""
        
        results = {}
        
        for category, practices in self.best_practices_checklist.items():
            category_results = {}
            
            for practice in practices:
                try:
                    practice_result = await self._check_single_practice(practice, design)
                    category_results[practice] = practice_result
                except Exception as e:
                    logger.error(f"Failed to check practice {practice}: {str(e)}")
                    category_results[practice] = False
            
            results[category] = category_results
        
        return results
    
    async def _check_single_practice(self, practice: str, design: OrchestraDesign) -> bool:
        """Check a single best practice."""
        
        # Implement practice checks
        if practice == "clear_agent_responsibilities":
            return all(
                hasattr(agent, 'responsibilities') and agent.responsibilities
                for agent in design.agents.values()
            )
        elif practice == "well_defined_message_flows":
            return len(design.message_flows) > 0 and all(
                hasattr(flow, 'from_agent') and hasattr(flow, 'to_agent')
                for flow in design.message_flows
            )
        elif practice == "timeout_configuration":
            return all(
                hasattr(agent, 'timeout_seconds') and agent.timeout_seconds > 0
                for agent in design.agents.values()
            )
        elif practice == "retry_logic":
            return all(
                hasattr(agent, 'retry_attempts') and agent.retry_attempts >= 1
                for agent in design.agents.values()
            )
        elif practice == "structured_logging":
            return hasattr(design, 'monitoring_setup') and design.monitoring_setup.get("logging", {}).get("format") == "structured_json"
        elif practice == "metrics_collection":
            return hasattr(design, 'monitoring_setup') and design.monitoring_setup.get("metrics", {}).get("system") == "prometheus"
        elif practice == "health_checks":
            return any(
                "health" in agent.agent_type.value.lower() or "monitor" in agent.agent_type.value.lower()
                for agent in design.agents.values()
            )
        else:
            return True  # Default to True for unknown practices
    
    async def _perform_safety_check(self, design: OrchestraDesign) -> Dict[str, bool]:
        """Perform comprehensive safety check."""
        
        safety_results = {}
        
        # Check for essential safety mechanisms
        safety_results["has_timeouts"] = all(
            hasattr(agent, 'timeout_seconds') and agent.timeout_seconds > 0
            for agent in design.agents.values()
        )
        
        safety_results["has_retry_logic"] = all(
            hasattr(agent, 'retry_attempts') and agent.retry_attempts >= 1
            for agent in design.agents.values()
        )
        
        safety_results["has_error_handling"] = len(design.error_handling_strategy) > 0
        
        safety_results["has_approval_gates"] = SafetyMechanism.APPROVAL_GATE in design.safety_mechanisms
        
        safety_results["has_rate_limiting"] = SafetyMechanism.RATE_LIMIT in design.safety_mechanisms
        
        safety_results["has_circuit_breaker"] = SafetyMechanism.CIRCUIT_BREAKER in design.safety_mechanisms
        
        safety_results["has_kill_switch"] = SafetyMechanism.KILL_SWITCH in design.safety_mechanisms
        
        # Check for production safety
        safety_results["production_ready"] = all([
            safety_results["has_timeouts"],
            safety_results["has_retry_logic"],
            safety_results["has_error_handling"]
        ])
        
        return safety_results
    
    async def _generate_recommendations(self, design: OrchestraDesign, rule_results: Dict[str, Any], best_practices_results: Dict[str, bool]) -> List[str]:
        """Generate recommendations based on validation results."""
        
        recommendations = []
        
        # Rule-based recommendations
        for rule_id, result in rule_results.items():
            if not result.get("passed", False) and not result.get("skipped", False):
                if rule_id == "safety_approval_gates":
                    recommendations.append("Add approval gates for critical operations, especially for production deployments")
                elif rule_id == "agent_count_limit":
                    recommendations.append("Consider simplifying the design by reducing the number of agents or combining responsibilities")
                elif rule_id == "observability_coverage":
                    recommendations.append("Ensure all agents have proper monitoring and observability setup")
                elif rule_id == "timeout_configuration":
                    recommendations.append("Configure appropriate timeouts for all agents to prevent hanging operations")
                elif rule_id == "retry_logic":
                    recommendations.append("Add retry logic for agents that interact with external services")
        
        # Best practice recommendations
        for category, practices in best_practices_results.items():
            failed_practices = [practice for practice, passed in practices.items() if not passed]
            
            if failed_practices:
                if category == "architecture":
                    recommendations.append("Improve architectural design: " + ", ".join(failed_practices))
                elif category == "safety":
                    recommendations.append("Enhance safety mechanisms: " + ", ".join(failed_practices))
                elif category == "observability":
                    recommendations.append("Strengthen observability: " + ", ".join(failed_practices))
                elif category == "security":
                    recommendations.append("Improve security measures: " + ", ".join(failed_practices))
                elif category == "performance":
                    recommendations.append("Optimize performance aspects: " + ", ".join(failed_practices))
        
        return recommendations
    
    def _extract_warnings(self, rule_results: Dict[str, Any], best_practices_results: Dict[str, bool]) -> List[str]:
        """Extract warnings from validation results."""
        
        warnings = []
        
        # Rule warnings
        for rule_id, result in rule_results.items():
            if not result.get("passed", False) and result.get("severity") == "warning":
                rule_name = result.get("rule_name", rule_id)
                warnings.append(f"Warning: {rule_name}")
        
        # Best practice warnings
        for category, practices in best_practices_results.items():
            failed_count = sum(1 for passed in practices.values() if not passed)
            if failed_count > 0:
                warnings.append(f"{category.title()}: {failed_count} best practices not followed")
        
        return warnings
    
    def _extract_errors(self, rule_results: Dict[str, Any], best_practices_results: Dict[str, bool]) -> List[str]:
        """Extract errors from validation results."""
        
        errors = []
        
        # Rule errors
        for rule_id, result in rule_results.items():
            if not result.get("passed", False) and result.get("severity") == "error":
                rule_name = result.get("rule_name", rule_id)
                errors.append(f"Error: {rule_name}")
        
        return errors
    
    def _calculate_overall_score(self, rule_results: Dict[str, Any], best_practices_results: Dict[str, bool], safety_check_results: Dict[str, bool]) -> float:
        """Calculate overall validation score."""
        
        # Rule score (40% weight)
        passed_rules = sum(1 for result in rule_results.values() if result.get("passed", False))
        total_rules = len(rule_results)
        rule_score = passed_rules / total_rules if total_rules > 0 else 0
        
        # Best practices score (40% weight)
        all_practices = []
        passed_practices = []
        
        for practices in best_practices_results.values():
            all_practices.extend(practices.keys())
            passed_practices.extend([practice for practice, passed in practices.items() if passed])
        
        practices_score = len(passed_practices) / len(all_practices) if all_practices else 0
        
        # Safety score (20% weight)
        safety_checks = list(safety_check_results.values())
        safety_score = sum(safety_checks) / len(safety_checks) if safety_checks else 0
        
        # Weighted average
        overall_score = (rule_score * 0.4) + (practices_score * 0.4) + (safety_score * 0.2)
        
        return round(overall_score, 2)
    
    def _assess_production_readiness(self, design: OrchestraDesign, rule_results: Dict[str, Any], safety_check_results: Dict[str, bool]) -> bool:
        """Assess if the design is ready for production."""
        
        # Must pass all error-level rules
        error_rules = [rule_id for rule_id, result in rule_results.items() 
                      if not result.get("passed", False) and result.get("severity") == "error"]
        
        if error_rules:
            return False
        
        # Must have essential safety mechanisms
        essential_safety = [
            "has_timeouts",
            "has_retry_logic", 
            "has_error_handling"
        ]
        
        if not all(safety_check_results.get(check, False) for check in essential_safety):
            return False
        
        # Must have minimum observability
        if not (design.monitoring_setup.get("logging") and design.monitoring_setup.get("metrics")):
            return False
        
        return True
    
    async def _validate_implementation_phases(self, phases: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate implementation phases."""
        
        if not phases:
            return {"valid": False, "errors": ["No implementation phases defined"]}
        
        errors = []
        warnings = []
        
        # Check for required fields
        for i, phase in enumerate(phases):
            if not phase.get("phase_name"):
                errors.append(f"Phase {i+1}: Missing phase name")
            if not phase.get("duration_weeks"):
                errors.append(f"Phase {i+1}: Missing duration")
            if not phase.get("tasks"):
                warnings.append(f"Phase {i+1}: No tasks defined")
        
        # Check for logical flow
        phase_names = [phase.get("phase_name", "") for phase in phases]
        if len(phase_names) != len(set(phase_names)):
            errors.append("Duplicate phase names found")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    async def _validate_dependencies(self, dependencies: List[str]) -> Dict[str, Any]:
        """Validate project dependencies."""
        
        if not dependencies:
            return {"valid": False, "errors": ["No dependencies specified"]}
        
        # Check for essential dependencies
        essential_deps = ["fastapi", "pydantic", "sqlalchemy"]
        missing_essential = [dep for dep in essential_deps if not any(dep in d.lower() for d in dependencies)]
        
        errors = []
        if missing_essential:
            errors.append(f"Missing essential dependencies: {', '.join(missing_essential)}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    async def _validate_timeline(self, timeline: str) -> Dict[str, Any]:
        """Validate timeline estimate."""
        
        if not timeline:
            return {"valid": False, "errors": ["No timeline estimate provided"]}
        
        # Simple validation - check if timeline is reasonable
        errors = []
        
        if "week" in timeline.lower():
            try:
                weeks = int(timeline.lower().split("week")[0].strip())
                if weeks < 1:
                    errors.append("Timeline too short (minimum 1 week)")
                elif weeks > 52:
                    errors.append("Timeline very long (consider breaking into smaller projects")
            except ValueError:
                errors.append("Invalid timeline format")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    async def _validate_costs(self, cost_estimate: Dict[str, Any]) -> Dict[str, Any]:
        """Validate cost estimates."""
        
        if not cost_estimate:
            return {"valid": False, "errors": ["No cost estimate provided"]}
        
        errors = []
        warnings = []
        
        # Check for required cost categories
        required_categories = ["development", "monthly_operational"]
        for category in required_categories:
            if category not in cost_estimate:
                errors.append(f"Missing cost category: {category}")
        
        # Check for reasonable costs
        if "monthly_operational" in cost_estimate:
            monthly_total = cost_estimate["monthly_operational"].get("total", 0)
            if monthly_total > 10000:
                warnings.append("High monthly operational costs - consider optimization")
            elif monthly_total < 50:
                warnings.append("Very low monthly costs - may be underestimated")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    async def _validate_resources(self, resource_requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Validate resource requirements."""
        
        if not resource_requirements:
            return {"valid": False, "errors": ["No resource requirements specified"]}
        
        errors = []
        
        # Check for team composition
        if "team_composition" not in resource_requirements:
            errors.append("Missing team composition")
        
        # Check for infrastructure requirements
        if "infrastructure" not in resource_requirements:
            errors.append("Missing infrastructure requirements")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    def _generate_implementation_recommendations(self, validation_results: Dict[str, Any]) -> List[str]:
        """Generate recommendations for implementation plan improvements."""
        
        recommendations = []
        
        for category, result in validation_results.items():
            if not result.get("valid", False):
                errors = result.get("errors", [])
                if errors:
                    recommendations.extend([f"{category.title()}: {error}" for error in errors])
            
            warnings = result.get("warnings", [])
            if warnings:
                recommendations.extend([f"{category.title()} Warning: {warning}" for warning in warnings])
        
        return recommendations
    
    def _create_error_message(self, original_message: AgentMessage, error: str) -> AgentMessage:
        """Create an error message."""
        return AgentMessage(
            correlation_id=original_message.correlation_id,
            agent_id=self.agent_id,
            intent="error_occurred",
            message_type=MessageType.ERROR_OCCURRED,
            payload={
                "error_type": "ValidationError",
                "error_message": error,
                "context": {"agent": self.agent_id}
            },
            session_id=original_message.session_id
        )
