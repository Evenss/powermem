"""
User Profile storage implementation for OceanBase

This module provides storage for user profile information extracted from conversations.
"""

import logging
from typing import Optional, Dict, Any, List

from sqlalchemy import and_, or_, func, literal, null

from ...storage.oceanbase import constants
from ...utils.utils import serialize_datetime, generate_snowflake_id, get_current_datetime

try:
    from pyobvector import ObVecClient
    from sqlalchemy import Column, String, Table, BigInteger, desc, JSON
    from sqlalchemy.dialects.mysql import LONGTEXT
except ImportError as e:
    raise ImportError(
        f"Required dependencies not found: {e}. Please install pyobvector and sqlalchemy."
    )

from .base import UserProfileStoreBase

logger = logging.getLogger(__name__)


class OceanBaseUserProfileStore(UserProfileStoreBase):
    """OceanBase-based user profile storage implementation"""

    def __init__(
            self,
            table_name: str = "user_profiles",
            connection_args: Optional[Dict[str, Any]] = None,
            host: Optional[str] = None,
            port: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            db_name: Optional[str] = None,
            **kwargs,
    ):
        """
        Initialize the UserProfileStore.

        Args:
            table_name (str): Name of the table to store user profiles.
            connection_args (Optional[Dict[str, Any]]): Connection parameters for OceanBase.
            host (Optional[str]): OceanBase server host.
            port (Optional[str]): OceanBase server port.
            user (Optional[str]): OceanBase username.
            password (Optional[str]): OceanBase password.
            db_name (Optional[str]): OceanBase database name.
        """
        self.table_name = table_name
        self.primary_field = "id"

        # Handle connection arguments - prioritize individual parameters over connection_args
        if connection_args is None:
            connection_args = {}

        # Merge individual connection parameters with connection_args
        final_connection_args = {
            "host": host or connection_args.get("host", constants.DEFAULT_OCEANBASE_CONNECTION["host"]),
            "port": port or connection_args.get("port", constants.DEFAULT_OCEANBASE_CONNECTION["port"]),
            "user": user or connection_args.get("user", constants.DEFAULT_OCEANBASE_CONNECTION["user"]),
            "password": password or connection_args.get("password", constants.DEFAULT_OCEANBASE_CONNECTION["password"]),
            "db_name": db_name or connection_args.get("db_name", constants.DEFAULT_OCEANBASE_CONNECTION["db_name"]),
        }

        self.connection_args = final_connection_args

        # Initialize client
        self._create_client(**kwargs)
        assert self.obvector is not None

        # Create table if it doesn't exist
        self._create_table()

    def _create_client(self, **kwargs):
        """Create and initialize the OceanBase client."""
        host = self.connection_args.get("host")
        port = self.connection_args.get("port")
        user = self.connection_args.get("user")
        password = self.connection_args.get("password")
        db_name = self.connection_args.get("db_name")

        self.obvector = ObVecClient(
            uri=f"{host}:{port}",
            user=user,
            password=password,
            db_name=db_name,
            **kwargs,
        )

    def _create_table(self) -> None:
        """Create user profiles table if it doesn't exist."""
        if not self.obvector.check_table_exists(self.table_name):
            # Define columns for user profiles table
            cols = [
                # Primary key - Snowflake ID (BIGINT without AUTO_INCREMENT)
                Column(self.primary_field, BigInteger, primary_key=True, autoincrement=False),
                Column("user_id", String(128)),  # User identifier
                Column("agent_id", String(128)),  # Agent identifier
                Column("run_id", String(128)),  # Run identifier
                Column("profile_content", LONGTEXT),
                Column("topics", JSON),  # Structured topics (main topics and sub-topics)
                Column("created_at", String(128)),
                Column("updated_at", String(128)),
            ]

            # Create table without vector index (simple table)
            self.obvector.create_table_with_index_params(
                table_name=self.table_name,
                columns=cols,
                indexes=None,
                vidxs=None,
                partitions=None,
            )

            logger.info(f"Created user profiles table: {self.table_name}")
        else:
            logger.info(f"User profiles table '{self.table_name}' already exists")

        # Load table metadata
        self.table = Table(self.table_name, self.obvector.metadata_obj, autoload_with=self.obvector.engine)

    def save_profile(
            self,
            user_id: str,
            profile_content: Optional[str] = None,
            topics: Optional[Dict[str, Any]] = None,
            agent_id: Optional[str] = None,
            run_id: Optional[str] = None,
    ) -> int:
        """
        Save or update user profile based on unique combination of user_id, agent_id, run_id.
        If a record exists with the same combination, update it; otherwise, insert a new record.

        Args:
            user_id: User identifier
            profile_content: Profile content text (for non-structured profile)
            topics: Structured topics dictionary (for structured profile)
            agent_id: Optional agent identifier
            run_id: Optional run identifier

        Returns:
            Profile ID (existing or newly generated Snowflake ID)
        """
        now = serialize_datetime(get_current_datetime())

        # Normalize empty strings to None for comparison
        agent_id_normalized = agent_id or ""
        run_id_normalized = run_id or ""

        # Check if profile exists with the same combination
        with self.obvector.engine.connect() as conn:
            conditions = [
                self.table.c.user_id == user_id,
                self.table.c.agent_id == agent_id_normalized,
                self.table.c.run_id == run_id_normalized,
            ]

            stmt = self.table.select().where(and_(*conditions)).limit(1)
            result = conn.execute(stmt)
            existing_row = result.fetchone()

            # Prepare update/insert values
            values = {
                "updated_at": now,
            }
            if profile_content is not None:
                values["profile_content"] = profile_content
            if topics is not None:
                values["topics"] = topics

            if existing_row:
                # Update existing record
                profile_id = existing_row.id
                update_stmt = (
                    self.table.update()
                    .where(and_(self.table.c.id == profile_id))
                    .values(**values)
                )
                conn.execute(update_stmt)
                conn.commit()
                logger.debug(f"Updated profile for user_id: {user_id}, profile_id: {profile_id}")
            else:
                # Insert new record
                profile_id = generate_snowflake_id()
                insert_values = {
                    "id": profile_id,
                    "user_id": user_id,
                    "agent_id": agent_id_normalized,
                    "run_id": run_id_normalized,
                    "created_at": now,
                    **values,
                }
                insert_stmt = self.table.insert().values(**insert_values)
                conn.execute(insert_stmt)
                conn.commit()
                logger.debug(f"Created profile for user_id: {user_id}, profile_id: {profile_id}")

        return profile_id

    def get_profile(
            self,
            user_id: Optional[str] = None,
            agent_id: Optional[str] = None,
            run_id: Optional[str] = None,
            main_topic: Optional[List[str]] = None,
            sub_topic: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Get user profile by user_id and optional filters.

        Args:
            user_id: User identifier
            agent_id: Optional agent identifier for filtering
            run_id: Optional run identifier for filtering
            main_topic: Optional list of main topic names to filter
            sub_topic: Optional list of sub topic names to filter by

        Returns:
            Profile dictionary with the following keys:
            - "id" (int): Profile ID
            - "user_id" (str): User identifier
            - "agent_id" (str): Agent identifier
            - "run_id" (str): Run identifier
            - "profile_content" (str): Profile content text
            - "topics" (dict): Structured topics dictionary (filtered if main_topic or sub_topic provided)
            - "created_at" (str): Creation timestamp in ISO format
            - "updated_at" (str): Last update timestamp in ISO format
            or None if not found
        """
        with self.obvector.engine.connect() as conn:
            # Build where conditions
            conditions = [self.table.c.user_id == user_id,
                          self.table.c.agent_id == (agent_id or ""),
                          self.table.c.run_id == (run_id or "")]

            # Add JSON filtering conditions for main_topic and sub_topic
            if main_topic and len(main_topic) > 0:
                # Check if any of the main topics exist in the JSON
                # Use JSON_CONTAINS_PATH to check if main topic exists
                main_topic_conditions = []
                for mt in main_topic:
                    # Format: JSON_CONTAINS_PATH(topics, 'one', '$.main_topic') = 1
                    main_topic_conditions.append(
                        func.json_contains_path(
                            self.table.c.topics,
                            literal('one'),
                            literal(f"$.{mt}")
                        ) == 1
                    )
                if main_topic_conditions:
                    conditions.append(or_(*main_topic_conditions))

            if sub_topic and len(sub_topic) > 0:
                # Check if any of the sub topics exist in any main topic
                # Use JSON_SEARCH to find sub topic in any main topic
                sub_topic_conditions = []
                for st in sub_topic:
                    # Format: JSON_SEARCH(topics, 'one', 'sub_topic', NULL, '$.*') IS NOT NULL
                    sub_topic_conditions.append(
                        func.json_search(
                            self.table.c.topics,
                            literal('one'),
                            literal(st),
                            null(),
                            literal('$.*')
                        ).isnot(None)
                    )
                if sub_topic_conditions:
                    conditions.append(or_(*sub_topic_conditions))

            stmt = self.table.select().where(and_(*conditions))

            # Order by id desc to get the latest profile
            stmt = stmt.order_by(desc(self.table.c.id))
            stmt = stmt.limit(1)

            result = conn.execute(stmt)
            row = result.fetchone()

            if row:
                topics = getattr(row, "topics", None)

                # Filter topics in memory after SQL filtering (to return only matching parts)
                if topics and isinstance(topics, dict) and (main_topic or sub_topic):
                    topics = self._filter_topics_in_memory(topics, main_topic, sub_topic)

                return {
                    "id": row.id,
                    "user_id": row.user_id,
                    "agent_id": row.agent_id,
                    "run_id": row.run_id,
                    "profile_content": getattr(row, "profile_content", None),
                    "topics": topics,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at,
                }
            return None

    def _filter_topics_in_memory(
            self,
            topics: Dict[str, Any],
            main_topic: Optional[List[str]],
            sub_topic: Optional[List[str]],
    ) -> Dict[str, Any]:
        """
        Filter topics dictionary in memory after SQL filtering.
        This ensures only matching main topics and sub topics are returned.
        
        Args:
            topics: Full topics dictionary
            main_topic: Optional list of main topic names to include
            sub_topic: Optional list of sub topic names to include
        
        Returns:
            Filtered topics dictionary
        """
        if not topics or not isinstance(topics, dict):
            return {}

        filtered_result = {}

        for mt, st_dict in topics.items():
            # Check if main topic should be included
            include_main = True
            if main_topic and len(main_topic) > 0:
                include_main = any(mt.lower() == m.lower() for m in main_topic)

            if not include_main:
                continue

            # Filter sub topics
            if isinstance(st_dict, dict):
                filtered_sub = {}
                for st_key, st_value in st_dict.items():
                    # Check if sub topic should be included
                    include_sub = True
                    if sub_topic and len(sub_topic) > 0:
                        include_sub = any(st_key.lower() == s.lower() for s in sub_topic)

                    if include_sub:
                        filtered_sub[st_key] = st_value

                # Only add main topic if it has matching sub topics (or no sub_topic filter)
                if filtered_sub or not sub_topic or len(sub_topic) == 0:
                    filtered_result[mt] = filtered_sub
            else:
                # If sub_topic is not a dict, include it if main topic matches
                if include_main:
                    filtered_result[mt] = st_dict

        return filtered_result

    def delete_profile(self, profile_id: int) -> bool:
        """
        Delete user profile by profile_id.

        Args:
            profile_id: Profile ID (Snowflake ID)

        Returns:
            True if deleted, False if not found
        """
        with self.obvector.engine.connect() as conn:
            condition = self.table.c.id == profile_id
            stmt = self.table.delete().where(and_(condition))
            result = conn.execute(stmt)
            conn.commit()

            deleted = result.rowcount > 0
            if deleted:
                logger.debug(f"Deleted profile with id: {profile_id}")
            return deleted
