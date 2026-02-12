"""
Repository Reader - High-level interface for reading repository content and patterns.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path

from integrations.mcp_github import GitHubMCPClient, RepositoryReader

logger = logging.getLogger(__name__)


class LocalRepositoryReader:
    """Reader for local repository files."""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        if not self.repo_path.exists():
            raise ValueError(f"Repository path does not exist: {repo_path}")
    
    async def read_file(self, file_path: str) -> str:
        """
        Read a file from the local repository.
        
        Args:
            file_path: Relative path to the file
            
        Returns:
            File content
        """
        try:
            full_path = self.repo_path / file_path
            if not full_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            return content
            
        except Exception as e:
            logger.error(f"Failed to read local file {file_path}: {str(e)}")
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
    
    async def list_files(self, directory: str = "", pattern: str = "*") -> List[str]:
        """
        List files in a directory.
        
        Args:
            directory: Directory path (relative to repo root)
            pattern: File pattern to match
            
        Returns:
            List of file paths
        """
        try:
            dir_path = self.repo_path / directory if directory else self.repo_path
            files = []
            
            for file_path in dir_path.rglob(pattern):
                if file_path.is_file():
                    # Get relative path from repo root
                    rel_path = file_path.relative_to(self.repo_path)
                    files.append(str(rel_path))
            
            return sorted(files)
            
        except Exception as e:
            logger.error(f"Failed to list files in {directory}: {str(e)}")
            raise
    
    async def get_architecture_files(self) -> Dict[str, str]:
        """
        Get all architecture-related files from the local repository.
        
        Returns:
            Dictionary mapping file paths to content
        """
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
        
        return await self.read_multiple_files(target_files)


class UnifiedRepositoryReader:
    """Unified reader that can read from both local and remote repositories."""
    
    def __init__(self, local_repo_path: Optional[str] = None,
                 github_token: Optional[str] = None,
                 repo_owner: Optional[str] = None,
                 repo_name: Optional[str] = None):
        
        self.local_reader = None
        self.github_reader = None
        
        # Initialize local reader if path provided
        if local_repo_path:
            try:
                self.local_reader = LocalRepositoryReader(local_repo_path)
                logger.info(f"Initialized local repository reader: {local_repo_path}")
            except Exception as e:
                logger.warning(f"Failed to initialize local reader: {str(e)}")
        
        # Initialize GitHub reader if credentials provided
        if github_token and repo_owner and repo_name:
            try:
                github_client = GitHubMCPClient(github_token, repo_owner, repo_name)
                self.github_reader = RepositoryReader(github_client)
                logger.info(f"Initialized GitHub repository reader: {repo_owner}/{repo_name}")
            except Exception as e:
                logger.warning(f"Failed to initialize GitHub reader: {str(e)}")
    
    async def connect(self) -> None:
        """Connect to remote repositories if needed."""
        if self.github_reader:
            await self.github_reader.github_client.connect()
    
    async def disconnect(self) -> None:
        """Disconnect from remote repositories."""
        if self.github_reader:
            await self.github_reader.github_client.disconnect()
    
    async def read_file(self, file_path: str, prefer_local: bool = True) -> str:
        """
        Read a file, trying local first then remote.
        
        Args:
            file_path: Path to the file
            prefer_local: Whether to try local reader first
            
        Returns:
            File content
        """
        readers = []
        if prefer_local and self.local_reader:
            readers.append(self.local_reader)
        elif not prefer_local and self.github_reader:
            readers.append(self.github_reader)
        
        # Add the other reader as fallback
        if prefer_local and self.github_reader:
            readers.append(self.github_reader)
        elif not prefer_local and self.local_reader:
            readers.append(self.local_reader)
        
        last_error = None
        
        for reader in readers:
            try:
                content = await reader.read_file(file_path)
                if content:  # Non-empty content
                    return content
            except Exception as e:
                last_error = e
                continue
        
        # If we get here, all readers failed
        if last_error:
            raise last_error
        else:
            raise FileNotFoundError(f"File not found: {file_path}")
    
    async def read_multiple_files(self, file_paths: List[str], prefer_local: bool = True) -> Dict[str, str]:
        """
        Read multiple files concurrently.
        
        Args:
            file_paths: List of file paths to read
            prefer_local: Whether to try local reader first
            
        Returns:
            Dictionary mapping file paths to content
        """
        tasks = []
        for file_path in file_paths:
            task = self.read_file(file_path, prefer_local)
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
        Get architecture patterns from repository.
        
        Returns:
            Dictionary containing architecture patterns
        """
        if self.github_reader:
            try:
                return await self.github_reader.get_architecture_patterns()
            except Exception as e:
                logger.warning(f"Failed to get patterns from GitHub: {str(e)}")
        
        if self.local_reader:
            try:
                architecture_files = await self.local_reader.get_architecture_files()
                return self._extract_patterns_from_files(architecture_files)
            except Exception as e:
                logger.warning(f"Failed to get patterns from local: {str(e)}")
        
        return {}
    
    def _extract_patterns_from_files(self, files: Dict[str, str]) -> Dict[str, Any]:
        """Extract patterns from local files (similar to GitHub reader)."""
        patterns = {
            "multi_agent_patterns": [],
            "communication_patterns": [],
            "safety_patterns": [],
            "best_practices": [],
            "technology_stack": {
                "languages": [],
                "frameworks": [],
                "databases": [],
                "cloud": [],
                "tools": []
            }
        }
        
        # Extract multi-agent patterns
        multi_agent_content = files.get("multi-agent.md", "")
        if "Voice-Driven DevOps Assistant" in multi_agent_content:
            patterns["multi_agent_patterns"].append({
                "name": "voice_driven_ops",
                "description": "Voice-driven DevOps automation system",
                "agents": ["intent_parser", "planner", "safety_checker", "router", "executors", "reporter"],
                "communication": "event_driven"
            })
        
        # Extract communication patterns
        arch_content = files.get("architecture-patterns.md", "")
        if "Event-Driven Architecture" in arch_content:
            patterns["communication_patterns"].append({
                "name": "event_driven",
                "description": "Loose coupling via message bus",
                "benefits": ["async_processing", "parallelism", "scalability"]
            })
        
        # Extract best practices
        for file_path, content in files.items():
            lines = content.split('\n')
            for line in lines:
                if line.strip().startswith('✅'):
                    patterns["best_practices"].append(line.strip())
        
        patterns["best_practices"] = patterns["best_practices"][:20]  # Limit to top 20
        
        return patterns
    
    async def get_repository_info(self) -> Dict[str, Any]:
        """
        Get repository information.
        
        Returns:
            Repository metadata
        """
        info = {
            "readers_available": {
                "local": self.local_reader is not None,
                "github": self.github_reader is not None
            }
        }
        
        if self.github_reader:
            try:
                github_info = await self.github_reader.github_client.get_repository_info()
                info["github"] = {
                    "name": github_info.get("name"),
                    "description": github_info.get("description"),
                    "stars": github_info.get("stargazers_count"),
                    "language": github_info.get("language"),
                    "updated_at": github_info.get("updated_at")
                }
            except Exception as e:
                logger.warning(f"Failed to get GitHub info: {str(e)}")
        
        if self.local_reader:
            try:
                # Get local repository info
                info["local"] = {
                    "path": str(self.local_reader.repo_path),
                    "files": len(await self.local_reader.list_files())
                }
            except Exception as e:
                logger.warning(f"Failed to get local info: {str(e)}")
        
        return info
    
    async def search_files(self, query: str, file_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for files matching a query.
        
        Args:
            query: Search query
            file_pattern: Optional file pattern filter
            
        Returns:
            List of search results
        """
        if self.github_reader:
            try:
                return await self.github_reader.github_client.search_files(query, file_pattern)
            except Exception as e:
                logger.warning(f"Failed to search GitHub: {str(e)}")
        
        # For local search, implement simple text search
        if self.local_reader:
            try:
                files = await self.local_reader.list_files(pattern=file_pattern or "*")
                results = []
                
                for file_path in files:
                    try:
                        content = await self.local_reader.read_file(file_path)
                        if query.lower() in content.lower():
                            results.append({
                                "name": file_path,
                                "path": file_path,
                                "type": "file"
                            })
                    except Exception:
                        continue
                
                return results
                
            except Exception as e:
                logger.warning(f"Failed to search local files: {str(e)}")
        
        return []
    
    async def get_file_history(self, file_path: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get commit history for a file (GitHub only).
        
        Args:
            file_path: Path to the file
            limit: Maximum number of commits
            
        Returns:
            List of commit information
        """
        if self.github_reader:
            try:
                return await self.github_reader.github_client.get_file_history(file_path, limit)
            except Exception as e:
                logger.warning(f"Failed to get file history: {str(e)}")
        
        return []
    
    def get_reader_status(self) -> Dict[str, Any]:
        """Get status of all readers."""
        status = {
            "local_reader": {
                "available": self.local_reader is not None,
                "path": str(self.local_reader.repo_path) if self.local_reader else None
            },
            "github_reader": {
                "available": self.github_reader is not None,
                "repo": f"{self.github_reader.github_client.repo_owner}/{self.github_reader.github_client.repo_name}" if self.github_reader else None
            }
        }
        
        if self.github_reader:
            status["github_reader"]["cache_stats"] = self.github_reader.get_cache_stats()
        
        return status
