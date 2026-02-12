"""
GitHub MCP Integration - Connects to GitHub repositories for reading architecture patterns.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path

import aiohttp
import aiofiles

logger = logging.getLogger(__name__)


class GitHubMCPClient:
    """Client for interacting with GitHub repositories via MCP."""
    
    def __init__(self, github_token: str, repo_owner: str, repo_name: str):
        self.github_token = github_token
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
        self.headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def connect(self) -> None:
        """Initialize HTTP session."""
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=30)
        )
        logger.info(f"Connected to GitHub repo: {self.repo_owner}/{self.repo_name}")
    
    async def disconnect(self) -> None:
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            logger.info("Disconnected from GitHub")
    
    async def get_repository_info(self) -> Dict[str, Any]:
        """
        Get basic repository information.
        
        Returns:
            Repository metadata
        """
        if not self.session:
            raise RuntimeError("Not connected to GitHub")
        
        try:
            async with self.session.get(self.base_url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Failed to get repository info: {str(e)}")
            raise
    
    async def list_files(self, path: str = "", ref: str = "main") -> List[Dict[str, Any]]:
        """
        List files in a directory.
        
        Args:
            path: Directory path (empty for root)
            ref: Git reference (branch, tag, or commit)
            
        Returns:
            List of file information
        """
        if not self.session:
            raise RuntimeError("Not connected to GitHub")
        
        url = f"{self.base_url}/contents/{path}"
        params = {"ref": ref} if ref else {}
        
        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Failed to list files in {path}: {str(e)}")
            raise
    
    async def get_file_content(self, file_path: str, ref: str = "main") -> str:
        """
        Get content of a specific file.
        
        Args:
            file_path: Path to the file
            ref: Git reference (branch, tag, or commit)
            
        Returns:
            File content as string
        """
        if not self.session:
            raise RuntimeError("Not connected to GitHub")
        
        url = f"{self.base_url}/contents/{file_path}"
        params = {"ref": ref} if ref else {}
        
        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                
                # GitHub returns base64 encoded content
                import base64
                content = base64.b64decode(data["content"]).decode('utf-8')
                return content
        except Exception as e:
            logger.error(f"Failed to get file content for {file_path}: {str(e)}")
            raise
    
    async def get_architecture_files(self) -> Dict[str, str]:
        """
        Get all architecture-related files from the repository.
        
        Returns:
            Dictionary mapping file paths to content
        """
        architecture_files = {}
        
        # Known architecture files to look for
        target_files = [
            "SKILL.md",
            "architecture-patterns.md",
            "best-practices.md",
            "multi-agent.md",
            "full-stack.md",
            "tech-stack-guide.md",
            "refactoring.md",
            "README.md"
        ]
        
        for file_path in target_files:
            try:
                content = await self.get_file_content(file_path)
                architecture_files[file_path] = content
                logger.info(f"Retrieved architecture file: {file_path}")
            except Exception as e:
                logger.warning(f"Could not retrieve {file_path}: {str(e)}")
                architecture_files[file_path] = ""
        
        return architecture_files
    
    async def search_files(self, query: str, file_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for files matching a query.
        
        Args:
            query: Search query
            file_pattern: Optional file pattern filter
            
        Returns:
            List of search results
        """
        if not self.session:
            raise RuntimeError("Not connected to GitHub")
        
        url = f"{self.base_url}/search/code"
        params = {
            "q": f"{query} repo:{self.repo_owner}/{self.repo_name}"
        }
        
        if file_pattern:
            params["q"] += f" filename:{file_pattern}"
        
        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("items", [])
        except Exception as e:
            logger.error(f"Failed to search files: {str(e)}")
            raise
    
    async def get_file_history(self, file_path: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get commit history for a specific file.
        
        Args:
            file_path: Path to the file
            limit: Maximum number of commits to return
            
        Returns:
            List of commit information
        """
        if not self.session:
            raise RuntimeError("Not connected to GitHub")
        
        url = f"{self.base_url}/commits"
        params = {
            "path": file_path,
            "per_page": limit
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Failed to get file history for {file_path}: {str(e)}")
            raise
    
    async def get_branches(self) -> List[Dict[str, Any]]:
        """
        Get list of branches in the repository.
        
        Returns:
            List of branch information
        """
        if not self.session:
            raise RuntimeError("Not connected to GitHub")
        
        url = f"{self.base_url}/branches"
        
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Failed to get branches: {str(e)}")
            raise
    
    async def get_tags(self) -> List[Dict[str, Any]]:
        """
        Get list of tags in the repository.
        
        Returns:
            List of tag information
        """
        if not self.session:
            raise RuntimeError("Not connected to GitHub")
        
        url = f"{self.base_url}/tags"
        
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Failed to get tags: {str(e)}")
            raise


class RepositoryReader:
    """High-level interface for reading repository content."""
    
    def __init__(self, github_client: GitHubMCPClient):
        self.github_client = github_client
        self._file_cache: Dict[str, str] = {}
        self._cache_ttl = 3600  # 1 hour
        self._last_cache_update = 0
    
    async def read_file(self, file_path: str, use_cache: bool = True) -> str:
        """
        Read a file from the repository.
        
        Args:
            file_path: Path to the file
            use_cache: Whether to use cached content
            
        Returns:
            File content
        """
        import time
        
        # Check cache
        if use_cache and file_path in self._file_cache:
            cache_age = time.time() - self._last_cache_update
            if cache_age < self._cache_ttl:
                return self._file_cache[file_path]
        
        try:
            content = await self.github_client.get_file_content(file_path)
            
            # Update cache
            if use_cache:
                self._file_cache[file_path] = content
                self._last_cache_update = time.time()
            
            return content
            
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {str(e)}")
            raise
    
    async def read_multiple_files(self, file_paths: List[str]) -> Dict[str, str]:
        """
        Read multiple files concurrently.
        
        Args:
            file_paths: List of file paths to read
            
        Returns:
            Dictionary mapping file paths to content
        """
        tasks = []
        for file_path in file_paths:
            task = self.read_file(file_path)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        file_contents = {}
        for file_path, result in zip(file_paths, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to read {file_path}: {str(result)}")
                file_contents[file_path] = ""
            else:
                file_contents[file_path] = result
        
        return file_contents
    
    async def get_architecture_patterns(self) -> Dict[str, Any]:
        """
        Extract architecture patterns from repository files.
        
        Returns:
            Dictionary containing architecture patterns
        """
        try:
            # Get architecture files
            architecture_files = await self.github_client.get_architecture_files()
            
            patterns = {
                "multi_agent_patterns": self._extract_multi_agent_patterns(architecture_files),
                "communication_patterns": self._extract_communication_patterns(architecture_files),
                "safety_patterns": self._extract_safety_patterns(architecture_files),
                "best_practices": self._extract_best_practices(architecture_files),
                "technology_stack": self._extract_technology_stack(architecture_files)
            }
            
            return patterns
            
        except Exception as e:
            logger.error(f"Failed to extract architecture patterns: {str(e)}")
            return {}
    
    def _extract_multi_agent_patterns(self, files: Dict[str, str]) -> List[Dict[str, Any]]:
        """Extract multi-agent patterns from files."""
        patterns = []
        
        # Look in multi-agent.md
        multi_agent_content = files.get("multi-agent.md", "")
        if multi_agent_content:
            # Simple pattern extraction - in production, use more sophisticated parsing
            if "Voice-Driven DevOps Assistant" in multi_agent_content:
                patterns.append({
                    "name": "voice_driven_ops",
                    "description": "Voice-driven DevOps automation system",
                    "agents": ["intent_parser", "planner", "safety_checker", "router", "executors", "reporter"],
                    "communication": "event_driven"
                })
        
        return patterns
    
    def _extract_communication_patterns(self, files: Dict[str, str]) -> List[Dict[str, Any]]:
        """Extract communication patterns from files."""
        patterns = []
        
        # Look in architecture-patterns.md
        arch_content = files.get("architecture-patterns.md", "")
        if arch_content:
            if "Event-Driven Architecture" in arch_content:
                patterns.append({
                    "name": "event_driven",
                    "description": "Loose coupling via message bus",
                    "benefits": ["async_processing", "parallelism", "scalability"]
                })
        
        return patterns
    
    def _extract_safety_patterns(self, files: Dict[str, str]) -> List[Dict[str, Any]]:
        """Extract safety patterns from files."""
        patterns = []
        
        # Look in best-practices.md
        best_practices_content = files.get("best-practices.md", "")
        if best_practices_content:
            if "Retry Logic" in best_practices_content:
                patterns.append({
                    "name": "retry_with_backoff",
                    "description": "Retry transient failures with exponential backoff"
                })
        
        return patterns
    
    def _extract_best_practices(self, files: Dict[str, str]) -> List[str]:
        """Extract best practices from files."""
        practices = []
        
        # Look for checkmarks and best practices indicators
        for file_path, content in files.items():
            lines = content.split('\n')
            for line in lines:
                if line.strip().startswith('✅'):
                    practices.append(line.strip())
        
        return practices[:20]  # Limit to top 20
    
    def _extract_technology_stack(self, files: Dict[str, str]) -> Dict[str, List[str]]:
        """Extract technology stack information from files."""
        stack = {
            "languages": [],
            "frameworks": [],
            "databases": [],
            "cloud": [],
            "tools": []
        }
        
        all_content = ' '.join(files.values()).lower()
        
        # Language detection
        if "python" in all_content:
            stack["languages"].append("Python")
        if "javascript" in all_content or "typescript" in all_content:
            stack["languages"].append("JavaScript/TypeScript")
        
        # Framework detection
        if "fastapi" in all_content:
            stack["frameworks"].append("FastAPI")
        if "react" in all_content:
            stack["frameworks"].append("React")
        
        # Database detection
        if "postgres" in all_content:
            stack["databases"].append("PostgreSQL")
        if "redis" in all_content:
            stack["databases"].append("Redis")
        
        # Cloud detection
        if "gcp" in all_content or "google cloud" in all_content:
            stack["cloud"].append("GCP")
        if "aws" in all_content:
            stack["cloud"].append("AWS")
        
        return stack
    
    async def clear_cache(self) -> None:
        """Clear the file cache."""
        self._file_cache.clear()
        self._last_cache_update = 0
        logger.info("Cleared file cache")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        import time
        
        return {
            "cached_files": len(self._file_cache),
            "cache_age_seconds": time.time() - self._last_cache_update,
            "cache_ttl_seconds": self._cache_ttl
        }
