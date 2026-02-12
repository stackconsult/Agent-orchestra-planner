"""
Context Manager - Maintains conversation context and session state across multi-turn interactions.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from schemas.messages import AgentMessage, MessageType
from schemas.requirements import UserRequirements, RequirementsExtractionSession
from schemas.design import OrchestraDesign, ValidationResult

logger = logging.getLogger(__name__)


class ContextManager:
    """Manages conversation context and session state for multi-turn interactions."""
    
    def __init__(self):
        self.sessions: Dict[UUID, Dict[str, Any]] = {}
        self.conversation_history: Dict[UUID, List[Dict[str, Any]]] = {}
        self.user_preferences: Dict[str, Any] = {}
        self.session_timeout = timedelta(hours=24)
        
    async def create_session(self, user_id: Optional[str] = None, metadata: Dict[str, Any] = None) -> UUID:
        """
        Create a new conversation session.
        
        Args:
            user_id: Optional user identifier
            metadata: Optional session metadata
            
        Returns:
            Session ID
        """
        session_id = UUID()
        
        session = {
            "session_id": session_id,
            "user_id": user_id,
            "created_at": datetime.utcnow(),
            "last_activity": datetime.utcnow(),
            "status": "active",
            "current_phase": "requirements_extraction",
            "metadata": metadata or {},
            "context": {
                "requirements": None,
                "repository_analysis": None,
                "orchestra_design": None,
                "implementation_plan": None,
                "validation_results": None
            },
            "conversation_state": {
                "questions_asked": [],
                "answers_received": {},
                "current_question_index": 0,
                "is_complete": False,
                "confidence_score": 0.0
            },
            "user_inputs": [],
            "agent_responses": [],
            "refinements_requested": [],
            "decisions_made": []
        }
        
        self.sessions[session_id] = session
        self.conversation_history[session_id] = []
        
        logger.info(f"Created session {session_id} for user {user_id}")
        return session_id
    
    async def get_session(self, session_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Get session information.
        
        Args:
            session_id: Session ID
            
        Returns:
            Session data or None if not found
        """
        return self.sessions.get(session_id)
    
    async def update_session_activity(self, session_id: UUID) -> None:
        """
        Update session last activity timestamp.
        
        Args:
            session_id: Session ID
        """
        if session_id in self.sessions:
            self.sessions[session_id]["last_activity"] = datetime.utcnow()
    
    async def update_session_phase(self, session_id: UUID, new_phase: str) -> None:
        """
        Update the current phase of a session.
        
        Args:
            session_id: Session ID
            new_phase: New phase name
        """
        if session_id in self.sessions:
            old_phase = self.sessions[session_id]["current_phase"]
            self.sessions[session_id]["current_phase"] = new_phase
            
            logger.info(f"Session {session_id} phase changed: {old_phase} -> {new_phase}")
    
    async def store_requirements(self, session_id: UUID, requirements: UserRequirements) -> None:
        """
        Store requirements in session context.
        
        Args:
            session_id: Session ID
            requirements: Requirements data
        """
        if session_id in self.sessions:
            self.sessions[session_id]["context"]["requirements"] = requirements.dict()
            await self.update_session_activity(session_id)
            
            logger.info(f"Stored requirements for session {session_id}")
    
    async def store_repository_analysis(self, session_id: UUID, analysis: Dict[str, Any]) -> None:
        """
        Store repository analysis in session context.
        
        Args:
            session_id: Session ID
            analysis: Repository analysis data
        """
        if session_id in self.sessions:
            self.sessions[session_id]["context"]["repository_analysis"] = analysis
            await self.update_session_activity(session_id)
            
            logger.info(f"Stored repository analysis for session {session_id}")
    
    async def store_orchestra_design(self, session_id: UUID, design: OrchestraDesign) -> None:
        """
        Store orchestra design in session context.
        
        Args:
            session_id: Session ID
            design: Orchestra design data
        """
        if session_id in self.sessions:
            self.sessions[session_id]["context"]["orchestra_design"] = design.dict()
            await self.update_session_activity(session_id)
            
            logger.info(f"Stored orchestra design for session {session_id}")
    
    async def store_implementation_plan(self, session_id: UUID, plan: Dict[str, Any]) -> None:
        """
        Store implementation plan in session context.
        
        Args:
            session_id: Session ID
            plan: Implementation plan data
        """
        if session_id in self.sessions:
            self.sessions[session_id]["context"]["implementation_plan"] = plan
            await self.update_session_activity(session_id)
            
            logger.info(f"Stored implementation plan for session {session_id}")
    
    async def store_validation_results(self, session_id: UUID, validation: ValidationResult) -> None:
        """
        Store validation results in session context.
        
        Args:
            session_id: Session ID
            validation: Validation results
        """
        if session_id in self.sessions:
            self.sessions[session_id]["context"]["validation_results"] = validation.dict()
            await self.update_session_activity(session_id)
            
            logger.info(f"Stored validation results for session {session_id}")
    
    async def add_user_input(self, session_id: UUID, user_input: str, input_type: str = "text") -> None:
        """
        Add user input to conversation history.
        
        Args:
            session_id: Session ID
            user_input: User's input
            input_type: Type of input (text, selection, etc.)
        """
        if session_id in self.sessions:
            input_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "user_input",
                "input_type": input_type,
                "content": user_input
            }
            
            self.sessions[session_id]["user_inputs"].append(input_entry)
            self.conversation_history[session_id].append(input_entry)
            
            await self.update_session_activity(session_id)
    
    async def add_agent_response(self, session_id: UUID, agent_id: str, response: str, 
                               response_type: str = "text", metadata: Dict[str, Any] = None) -> None:
        """
        Add agent response to conversation history.
        
        Args:
            session_id: Session ID
            agent_id: ID of the agent that responded
            response: Agent's response
            response_type: Type of response
            metadata: Additional response metadata
        """
        if session_id in self.sessions:
            response_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "agent_response",
                "agent_id": agent_id,
                "response_type": response_type,
                "content": response,
                "metadata": metadata or {}
            }
            
            self.sessions[session_id]["agent_responses"].append(response_entry)
            self.conversation_history[session_id].append(response_entry)
            
            await self.update_session_activity(session_id)
    
    async def add_refinement_request(self, session_id: UUID, refinement_type: str, 
                                   description: str) -> None:
        """
        Add a refinement request to session history.
        
        Args:
            session_id: Session ID
            refinement_type: Type of refinement (e.g., "add_error_handling")
            description: Description of the refinement
        """
        if session_id in self.sessions:
            refinement_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "refinement_request",
                "refinement_type": refinement_type,
                "description": description
            }
            
            self.sessions[session_id]["refinements_requested"].append(refinement_entry)
            self.conversation_history[session_id].append(refinement_entry)
            
            await self.update_session_activity(session_id)
    
    async def add_decision(self, session_id: UUID, decision_type: str, 
                         decision: str, rationale: str = "") -> None:
        """
        Add a decision made during the session.
        
        Args:
            session_id: Session ID
            decision_type: Type of decision
            decision: The decision made
            rationale: Rationale for the decision
        """
        if session_id in self.sessions:
            decision_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "decision",
                "decision_type": decision_type,
                "decision": decision,
                "rationale": rationale
            }
            
            self.sessions[session_id]["decisions_made"].append(decision_entry)
            self.conversation_history[session_id].append(decision_entry)
            
            await self.update_session_activity(session_id)
    
    async def get_conversation_summary(self, session_id: UUID) -> Dict[str, Any]:
        """
        Get a summary of the conversation.
        
        Args:
            session_id: Session ID
            
        Returns:
            Conversation summary
        """
        if session_id not in self.sessions:
            return {"error": "Session not found"}
        
        session = self.sessions[session_id]
        
        return {
            "session_id": session_id,
            "user_id": session["user_id"],
            "created_at": session["created_at"].isoformat(),
            "last_activity": session["last_activity"].isoformat(),
            "current_phase": session["current_phase"],
            "status": session["status"],
            "total_user_inputs": len(session["user_inputs"]),
            "total_agent_responses": len(session["agent_responses"]),
            "total_refinements": len(session["refinements_requested"]),
            "total_decisions": len(session["decisions_made"]),
            "context_available": {
                "requirements": session["context"]["requirements"] is not None,
                "repository_analysis": session["context"]["repository_analysis"] is not None,
                "orchestra_design": session["context"]["orchestra_design"] is not None,
                "implementation_plan": session["context"]["implementation_plan"] is not None,
                "validation_results": session["context"]["validation_results"] is not None
            }
        }
    
    async def get_conversation_history(self, session_id: UUID, 
                                     limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get conversation history for a session.
        
        Args:
            session_id: Session ID
            limit: Maximum number of entries to return
            
        Returns:
            Conversation history
        """
        if session_id not in self.conversation_history:
            return []
        
        history = self.conversation_history[session_id]
        
        if limit:
            history = history[-limit:]
        
        return history
    
    async def search_conversation_history(self, session_id: UUID, 
                                        query: str, entry_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search conversation history for specific content.
        
        Args:
            session_id: Session ID
            query: Search query
            entry_type: Filter by entry type
            
        Returns:
            Matching conversation entries
        """
        if session_id not in self.conversation_history:
            return []
        
        history = self.conversation_history[session_id]
        query_lower = query.lower()
        
        matches = []
        for entry in history:
            # Filter by type if specified
            if entry_type and entry.get("type") != entry_type:
                continue
            
            # Search content
            content = entry.get("content", "").lower()
            if query_lower in content:
                matches.append(entry)
        
        return matches
    
    async def get_session_context(self, session_id: UUID) -> Dict[str, Any]:
        """
        Get the complete context for a session.
        
        Args:
            session_id: Session ID
            
        Returns:
            Complete session context
        """
        if session_id not in self.sessions:
            return {"error": "Session not found"}
        
        session = self.sessions[session_id]
        
        return {
            "session_info": {
                "session_id": session_id,
                "user_id": session["user_id"],
                "created_at": session["created_at"].isoformat(),
                "last_activity": session["last_activity"].isoformat(),
                "current_phase": session["current_phase"],
                "status": session["status"]
            },
            "context": session["context"],
            "conversation_state": session["conversation_state"],
            "metadata": session["metadata"]
        }
    
    async def update_user_preferences(self, user_id: str, preferences: Dict[str, Any]) -> None:
        """
        Update user preferences.
        
        Args:
            user_id: User identifier
            preferences: User preferences
        """
        if user_id not in self.user_preferences:
            self.user_preferences[user_id] = {}
        
        self.user_preferences[user_id].update(preferences)
        logger.info(f"Updated preferences for user {user_id}")
    
    async def get_user_preferences(self, user_id: str) -> Dict[str, Any]:
        """
        Get user preferences.
        
        Args:
            user_id: User identifier
            
        Returns:
            User preferences
        """
        return self.user_preferences.get(user_id, {})
    
    async def cleanup_expired_sessions(self) -> int:
        """
        Clean up sessions that have expired.
        
        Returns:
            Number of sessions cleaned up
        """
        now = datetime.utcnow()
        expired_sessions = []
        
        for session_id, session in self.sessions.items():
            if now - session["last_activity"] > self.session_timeout:
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            del self.sessions[session_id]
            if session_id in self.conversation_history:
                del self.conversation_history[session_id]
        
        if expired_sessions:
            logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
        
        return len(expired_sessions)
    
    async def export_session_data(self, session_id: UUID) -> Dict[str, Any]:
        """
        Export all session data for backup or analysis.
        
        Args:
            session_id: Session ID
            
        Returns:
            Complete session data
        """
        if session_id not in self.sessions:
            return {"error": "Session not found"}
        
        session = self.sessions[session_id]
        
        return {
            "session": session,
            "conversation_history": self.conversation_history.get(session_id, []),
            "exported_at": datetime.utcnow().isoformat()
        }
    
    async def import_session_data(self, session_data: Dict[str, Any]) -> UUID:
        """
        Import session data from backup.
        
        Args:
            session_data: Session data to import
            
        Returns:
            Session ID
        """
        session = session_data.get("session")
        if not session:
            raise ValueError("Invalid session data")
        
        session_id = UUID(session["session_id"])
        
        # Convert datetime strings back to datetime objects
        session["created_at"] = datetime.fromisoformat(session["created_at"])
        session["last_activity"] = datetime.fromisoformat(session["last_activity"])
        
        self.sessions[session_id] = session
        
        # Import conversation history
        history = session_data.get("conversation_history", [])
        self.conversation_history[session_id] = history
        
        logger.info(f"Imported session {session_id}")
        return session_id
    
    async def get_session_statistics(self) -> Dict[str, Any]:
        """
        Get overall session statistics.
        
        Returns:
            Session statistics
        """
        total_sessions = len(self.sessions)
        active_sessions = len([
            s for s in self.sessions.values() 
            if s["status"] == "active"
        ])
        
        phase_counts = {}
        for session in self.sessions.values():
            phase = session["current_phase"]
            phase_counts[phase] = phase_counts.get(phase, 0) + 1
        
        return {
            "total_sessions": total_sessions,
            "active_sessions": active_sessions,
            "total_users": len(set(s["user_id"] for s in self.sessions.values() if s["user_id"])),
            "phase_distribution": phase_counts,
            "average_session_duration": self._calculate_average_session_duration(),
            "most_common_phase": max(phase_counts.items(), key=lambda x: x[1])[0] if phase_counts else None
        }
    
    def _calculate_average_session_duration(self) -> float:
        """Calculate average session duration in minutes."""
        if not self.sessions:
            return 0.0
        
        now = datetime.utcnow()
        total_duration = sum(
            (now - s["created_at"]).total_seconds() / 60
            for s in self.sessions.values()
        )
        
        return total_duration / len(self.sessions)


class ConversationMemory:
    """Enhanced memory for conversation context and learning."""
    
    def __init__(self, context_manager: ContextManager):
        self.context_manager = context_manager
        self.patterns_learned: Dict[str, List[Dict[str, Any]]] = {}
        self.user_interaction_patterns: Dict[str, Dict[str, Any]] = {}
    
    async def learn_from_session(self, session_id: UUID) -> None:
        """
        Learn patterns from a completed session.
        
        Args:
            session_id: Session ID to learn from
        """
        session = await self.context_manager.get_session(session_id)
        if not session:
            return
        
        # Extract patterns from conversation
        await self._extract_question_patterns(session_id)
        await self._extract_refinement_patterns(session_id)
        await self._extract_decision_patterns(session_id)
        
        logger.info(f"Learned patterns from session {session_id}")
    
    async def _extract_question_patterns(self, session_id: UUID) -> None:
        """Extract question asking patterns."""
        history = await self.context_manager.get_conversation_history(session_id)
        
        for entry in history:
            if entry.get("type") == "user_input":
                # Analyze user input patterns
                pass
    
    async def _extract_refinement_patterns(self, session_id: UUID) -> None:
        """Extract refinement request patterns."""
        session = await self.context_manager.get_session(session_id)
        if not session:
            return
        
        refinements = session.get("refinements_requested", [])
        
        for refinement in refinements:
            refinement_type = refinement.get("refinement_type")
            
            if refinement_type not in self.patterns_learned:
                self.patterns_learned[refinement_type] = []
            
            self.patterns_learned[refinement_type].append({
                "session_id": session_id,
                "timestamp": refinement.get("timestamp"),
                "description": refinement.get("description")
            })
    
    async def _extract_decision_patterns(self, session_id: UUID) -> None:
        """Extract decision-making patterns."""
        session = await self.context_manager.get_session(session_id)
        if not session:
            return
        
        decisions = session.get("decisions_made", [])
        
        for decision in decisions:
            decision_type = decision.get("decision_type")
            
            if decision_type not in self.patterns_learned:
                self.patterns_learned[decision_type] = []
            
            self.patterns_learned[decision_type].append({
                "session_id": session_id,
                "timestamp": decision.get("timestamp"),
                "decision": decision.get("decision"),
                "rationale": decision.get("rationale")
            })
    
    async def get_similar_sessions(self, session_id: UUID, similarity_threshold: float = 0.7) -> List[UUID]:
        """
        Find sessions similar to the given session.
        
        Args:
            session_id: Reference session ID
            similarity_threshold: Minimum similarity score
            
        Returns:
            List of similar session IDs
        """
        # This is a simplified implementation
        # In production, use more sophisticated similarity algorithms
        return []
    
    async def suggest_improvements(self, session_id: UUID) -> List[str]:
        """
        Suggest improvements based on learned patterns.
        
        Args:
            session_id: Session ID to analyze
            
        Returns:
            List of improvement suggestions
        """
        suggestions = []
        
        # Analyze session for improvement opportunities
        session = await self.context_manager.get_session(session_id)
        if not session:
            return suggestions
        
        # Check for common refinement patterns
        refinements = session.get("refinements_requested", [])
        refinement_types = [r.get("refinement_type") for r in refinements]
        
        # Suggest proactive improvements
        if "add_error_handling" in refinement_types:
            suggestions.append("Consider adding comprehensive error handling in initial designs")
        
        if "add_monitoring" in refinement_types:
            suggestions.append("Include monitoring and observability from the start")
        
        return suggestions
