"""
Repository Analysis Agent - Reads and understands architecture patterns from the repo.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from anthropic import AsyncAnthropic
from schemas.messages import AgentMessage, MessageType, RepositoryAnalysisPayload
from schemas.design import AgentType, CommunicationPattern, SafetyMechanism

logger = logging.getLogger(__name__)


class RepositoryAnalyzerAgent:
    """Analyzes repository structure and extracts architecture patterns."""
    
    def __init__(self, anthropic_client: AsyncAnthropic, repo_reader: Any):
        self.anthropic = anthropic_client
        self.repo_reader = repo_reader
        self.agent_id = "repository_analyzer"
        
        # Cache for analysis results
        self._analysis_cache: Dict[str, Any] = {}
    
    async def analyze_repository(self, message: AgentMessage) -> AgentMessage:
        """
        Analyze the repository to extract patterns and best practices.
        
        Args:
            message: Message containing analysis request
            
        Returns:
            Message containing analysis results
        """
        try:
            logger.info(f"Starting repository analysis for correlation_id: {message.correlation_id}")
            
            # Get repository content
            repo_content = await self._get_repository_content()
            
            # Extract patterns using Claude
            patterns = await self._extract_patterns_with_claude(repo_content)
            
            # Analyze architecture principles
            architecture_principles = await self._analyze_architecture_principles(repo_content)
            
            # Extract best practices
            best_practices = await self._extract_best_practices(repo_content)
            
            # Identify technology stack
            tech_stack = await self._identify_technology_stack(repo_content)
            
            # Create response payload
            payload = RepositoryAnalysisPayload(
                repo_files=list(repo_content.keys()),
                patterns_identified=patterns,
                architecture_principles=architecture_principles,
                best_practices=best_practices,
                technology_stack=tech_stack
            )
            
            response_message = AgentMessage(
                correlation_id=message.correlation_id,
                agent_id=self.agent_id,
                intent="repository_analysis_completed",
                message_type=MessageType.REPO_ANALYSIS_COMPLETED,
                payload=payload.dict(),
                session_id=message.session_id
            )
            
            logger.info(f"Repository analysis completed for correlation_id: {message.correlation_id}")
            return response_message
            
        except Exception as e:
            logger.error(f"Repository analysis failed: {str(e)}", exc_info=True)
            return self._create_error_message(message, str(e))
    
    async def _get_repository_content(self) -> Dict[str, str]:
        """Get content from key repository files."""
        key_files = [
            "SKILL.md",
            "architecture-patterns.md", 
            "best-practices.md",
            "multi-agent.md",
            "full-stack.md",
            "tech-stack-guide.md",
            "refactoring.md"
        ]
        
        content = {}
        for file_path in key_files:
            try:
                file_content = await self.repo_reader.read_file(file_path)
                content[file_path] = file_content
            except Exception as e:
                logger.warning(f"Could not read file {file_path}: {str(e)}")
                content[file_path] = ""
        
        return content
    
    async def _extract_patterns_with_claude(self, repo_content: Dict[str, str]) -> Dict[str, Any]:
        """Use Claude to extract patterns from repository content."""
        
        prompt = f"""
        Analyze this repository content and extract multi-agent architecture patterns:
        
        {self._format_repo_content(repo_content)}
        
        Focus on:
        1. Multi-agent coordination patterns
        2. Communication protocols
        3. Agent role definitions
        4. Message flow patterns
        5. Event-driven architectures
        
        Return a JSON object with:
        {{
            "agent_patterns": [
                {{
                    "name": "pattern_name",
                    "description": "description",
                    "use_case": "when to use",
                    "example": "brief example"
                }}
            ],
            "communication_patterns": [
                {{
                    "pattern": "pattern_name",
                    "description": "description",
                    "agents_involved": ["agent1", "agent2"],
                    "message_types": ["type1", "type2"]
                }}
            ],
            "coordination_strategies": [
                {{
                    "strategy": "strategy_name",
                    "description": "description",
                    "benefits": ["benefit1", "benefit2"],
                    "trade_offs": ["trade_off1", "trade_off2"]
                }}
            ]
        }}
        """
        
        try:
            response = await self.anthropic.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse Claude's response
            content = response.content[0].text
            # In production, you'd want proper JSON parsing with error handling
            return {"extracted_patterns": content}
            
        except Exception as e:
            logger.error(f"Pattern extraction failed: {str(e)}")
            return {"extracted_patterns": "Failed to extract patterns"}
    
    async def _analyze_architecture_principles(self, repo_content: Dict[str, str]) -> List[str]:
        """Extract architecture principles from repository."""
        
        principles = []
        
        # Look for principle indicators in content
        for file_path, content in repo_content.items():
            if "principle" in content.lower() or "guideline" in content.lower():
                # Extract principle statements
                lines = content.split('\n')
                for line in lines:
                    if any(keyword in line.lower() for keyword in ['always', 'never', 'must', 'should']):
                        principles.append(f"{file_path}: {line.strip()}")
        
        return principles[:20]  # Limit to top 20 principles
    
    async def _extract_best_practices(self, repo_content: Dict[str, str]) -> List[str]:
        """Extract best practices from repository."""
        
        practices = []
        
        # Look for best practice indicators
        for file_path, content in repo_content.items():
            if "practice" in content.lower() or "pattern" in content.lower():
                lines = content.split('\n')
                for line in lines:
                    if line.strip().startswith(('✅', '❌', '→', '-')):
                        practices.append(f"{file_path}: {line.strip()}")
        
        return practices[:30]  # Limit to top 30 practices
    
    async def _identify_technology_stack(self, repo_content: Dict[str, str]) -> Dict[str, Any]:
        """Identify technology stack from repository."""
        
        tech_indicators = {
            'python': ['python', 'fastapi', 'pydantic', 'asyncio', 'sqlalchemy'],
            'javascript': ['javascript', 'node', 'react', 'typescript', 'express'],
            'databases': ['postgres', 'mysql', 'mongodb', 'redis', 'neon'],
            'cloud': ['aws', 'gcp', 'azure', 'cloud run', 'lambda'],
            'llm': ['claude', 'openai', 'anthropic', 'gpt'],
            'mcp': ['mcp', 'model context protocol']
        }
        
        stack = {}
        all_content = ' '.join(repo_content.values()).lower()
        
        for category, indicators in tech_indicators.items():
            found = [tech for tech in indicators if tech in all_content]
            if found:
                stack[category] = found
        
        return stack
    
    def _format_repo_content(self, repo_content: Dict[str, str]) -> str:
        """Format repository content for Claude prompt."""
        formatted = []
        for file_path, content in repo_content.items():
            formatted.append(f"=== {file_path} ===")
            # Limit content length to avoid token limits
            formatted.append(content[:2000] + "..." if len(content) > 2000 else content)
            formatted.append("")
        
        return '\n'.join(formatted)
    
    def _create_error_message(self, original_message: AgentMessage, error: str) -> AgentMessage:
        """Create an error message."""
        return AgentMessage(
            correlation_id=original_message.correlation_id,
            agent_id=self.agent_id,
            intent="error_occurred",
            message_type=MessageType.ERROR_OCCURRED,
            payload={
                "error_type": "RepositoryAnalysisError",
                "error_message": error,
                "context": {"agent": self.agent_id}
            },
            session_id=original_message.session_id
        )
    
    async def get_pattern_recommendations(self, requirements: Dict[str, Any]) -> List[str]:
        """
        Get pattern recommendations based on requirements.
        
        Args:
            requirements: User requirements
            
        Returns:
            List of recommended patterns
        """
        recommendations = []
        
        # Analyze requirements to suggest patterns
        if requirements.get('complexity') == 'complex':
            recommendations.append("event_driven_architecture")
            recommendations.append("circuit_breaker_pattern")
        
        if requirements.get('scalability_requirements'):
            recommendations.append("horizontal_scaling_pattern")
            recommendations.append("load_balancing_pattern")
        
        if requirements.get('reliability_requirements'):
            recommendations.append("retry_with_backoff_pattern")
            recommendations.append("graceful_degradation_pattern")
        
        return recommendations
