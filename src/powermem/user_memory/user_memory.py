"""
User Memory module for managing user profiles and events

This module provides high-level interface for creating and maintaining user profiles
and events extracted from conversations.
"""

import logging
from typing import Any, Dict, Optional

from .storage.factory import UserProfileStoreFactory
from ..core.memory import Memory
from ..prompts.user_profile_prompts import get_user_profile_extraction_prompt
from ..utils.utils import remove_code_blocks

logger = logging.getLogger(__name__)


class UserMemory:
    """
    High-level manager for creating and maintaining user profiles and events.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any] | Any] = None,
        storage_type: Optional[str] = None,
        llm_provider: Optional[str] = None,
        embedding_provider: Optional[str] = None,
        agent_id: Optional[str] = None,
    ):
        """
        Initializes the UserMemory layer.

        Args:
            ... see Memory.__init__() for more details
        """
        # Initialize Memory instance internally
        self.memory = Memory(
            config=config,
            storage_type=storage_type,
            llm_provider=llm_provider,
            embedding_provider=embedding_provider,
            agent_id=agent_id,
        )
        
        # Initialize UserProfileStore using factory based on storage_type
        # Extract connection config from memory's vector store
        vector_store = self.memory.storage.vector_store
        if hasattr(vector_store, 'connection_args'):
            connection_args = vector_store.connection_args
        else:
            # Fallback to default connection args
            connection_args = {}
        
        # Build config for UserProfileStore
        profile_store_config = {
            "table_name": "user_profiles",
            "connection_args": connection_args,
            "host": connection_args.get("host"),
            "port": connection_args.get("port"),
            "user": connection_args.get("user"),
            "password": connection_args.get("password"),
            "db_name": connection_args.get("db_name"),
        }
        
        # Use factory to create UserProfileStore based on storage_type
        provider = self.memory.storage_type.lower()
        self.profile_store = UserProfileStoreFactory.create(provider, profile_store_config)
        
        logger.info("UserMemory initialized")

    def add(
        self,
        messages,
        user_id: str,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        filters: Optional[Dict[str, Any]] = None,
        scope: Optional[str] = None,
        memory_type: Optional[str] = None,
        prompt: Optional[str] = None,
        infer: bool = True,
    ) -> Dict[str, Any]:
        """
        Add messages and extract user profile information.

        This method executes two steps:
        1. Store messages event (calls memory.add())
        2. Extract profile information (uses LLM to extract user profile from messages)

        Args:
            ... see memory.add() for more details

        Returns:
            Dict[str, Any]: A dictionary containing the add operation results with the following structure:
                ... see memory.add() for more details
                - "profile_extracted" (bool): Whether profile information was extracted
                - "profile_content" (str, optional): Profile content text
        """

        # Step 1: Store messages event
        logger.info(f"Step 1: Storing messages event for user_id: {user_id}")
        memory_result = self.memory.add(
            messages=messages,
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
            metadata=metadata,
            filters=filters,
            scope=scope,
            memory_type=memory_type,
            prompt=prompt,
            infer=infer,
        )
        
        # Step 2: Extract profile information
        logger.info(f"Step 2: Extracting profile information for user_id: {user_id}")
        profile_content = self._extract_profile(
            messages=messages,
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
        )
        
        if profile_content:
            # Save profile to UserProfileStore
            profile_id = self.profile_store.save_profile(
                user_id=user_id,
                profile_content=profile_content,
                agent_id=agent_id,
                run_id=run_id,
            )
            logger.info(f"Profile saved for user_id: {user_id}, profile_id: {profile_id}")
        else:
            logger.debug(f"No profile information extracted for user_id: {user_id}")
        
        # Return combined result
        result = memory_result.copy()
        result["profile_extracted"] = bool(profile_content)
        if profile_content:
            result["profile_content"] = profile_content
        
        return result

    def _extract_profile(
        self,
        messages: Any,
        user_id: str,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> str:
        """
        Extract user profile information from conversation using LLM.
        First retrieves existing profile if available, then asks LLM to update it based on new conversation.

        Args:
            messages: Conversation messages (str, dict, or list[dict])
            user_id: User identifier
            agent_id: Optional agent identifier
            run_id: Optional run identifier

        Returns:
            Extracted profile content as text string, or empty string if no profile found
        """
        # Parse conversation into text format
        if isinstance(messages, str):
            conversation_text = messages
        elif isinstance(messages, dict):
            conversation_text = messages.get("content", "")
        elif isinstance(messages, list):
            # Parse messages similar to memory.py
            conversation_text = ""
            for msg in messages:
                if isinstance(msg, dict) and 'role' in msg and 'content' in msg:
                    role = msg['role']
                    content = msg.get('content', '')
                    if role != "system":  # Skip system messages
                        conversation_text += f"{role}: {content}\n"
        else:
            conversation_text = str(messages)
        
        if not conversation_text or not conversation_text.strip():
            logger.debug("Empty conversation, skipping profile extraction")
            return ""
        
        # Get existing profile if available
        existing_profile = None
        try:
            profile = self.profile_store.get_profile(
                user_id=user_id,
                agent_id=agent_id,
                run_id=run_id,
            )
            if profile and profile.get("profile_content"):
                existing_profile = profile["profile_content"]
                logger.debug(f"Found existing profile for user_id: {user_id}, will update based on new conversation")
        except Exception as e:
            logger.warning(f"Error retrieving existing profile: {e}, will extract new profile")
        
        # Generate system prompt and user message
        system_prompt, user_message = get_user_profile_extraction_prompt(conversation_text, existing_profile=existing_profile)
        
        # Call LLM to extract profile
        try:
            response = self.memory.llm.generate_response(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message},
                ],
            )
            
            # Remove code blocks if present
            profile_content = remove_code_blocks(response).strip()
            
            # Return empty string if response is empty or indicates no profile
            if not profile_content or profile_content.lower() in ["", "none", "no profile information", "no relevant information"]:
                return ""
            
            return profile_content
            
        except Exception as e:
            logger.error(f"Error extracting profile: {e}")
            raise

    def search(
        self,
        query: str,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 30,
        threshold: Optional[float] = None,
        add_profile: bool = False,
    ) -> Dict[str, Any]:
        """
        Search for memories, optionally including user profile information.

        Args:
            ... see memory.search() for more details
            - add_profile: If True, include user profile content in results

        Returns:
            ... see memory.search() for more details
            - "profile_content" (str, optional): Profile content text if add_profile is True and user_id is provided
        """

        # Call memory.search()
        search_result = self.memory.search(
            query=query,
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
            filters=filters,
            limit=limit,
            threshold=threshold,
        )
        
        # Add profile if requested and user_id is provided
        if add_profile and user_id:
            profile = self.profile_store.get_profile(
                user_id=user_id,
                agent_id=agent_id,
                run_id=run_id,
            )
            if profile:
                search_result["profile_content"] = profile["profile_content"]
        
        return search_result

    def profile(
        self,
        user_id: str,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get user profile information.

        Args:
            user_id: User identifier
            agent_id: Optional agent identifier for filtering
            run_id: Optional run identifier for filtering

        Returns:
            Profile dictionary with the following keys:
            - "id" (int): Profile ID
            - "user_id" (str): User identifier
            - "agent_id" (str): Agent identifier
            - "run_id" (str): Run identifier
            - "profile_content" (str): Profile content text
            - "created_at" (str): Creation timestamp in ISO format
            - "updated_at" (str): Last update timestamp in ISO format
            or empty dict if not found
        """

        profile = self.profile_store.get_profile(
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
        )
        if profile:
            return {
                "id": profile["id"],
                "user_id": profile["user_id"],
                "agent_id": profile["agent_id"],
                "run_id": profile["run_id"],
                "profile_content": profile["profile_content"],
                "created_at": profile["created_at"],
                "updated_at": profile["updated_at"],
            }
        return {}

