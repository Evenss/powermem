"""
Token tracking for LLM and Embedding calls.

Provides per-request token statistics, distinguishing between
LLM calls (input/output) and Embedding calls (dense/sparse).
Statistics are persisted to a JSONL file.
"""

import json
import logging
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class LLMCallRecord:
    """Record for a single LLM generate_response call."""

    def __init__(
        self,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        purpose: str = "unknown",
    ):
        self.provider = provider
        self.model = model
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.purpose = purpose
        self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.input_tokens + self.output_tokens,
            "purpose": self.purpose,
            "timestamp": self.timestamp,
        }


class EmbeddingCallRecord:
    """Record for a single embedding call."""

    def __init__(
        self,
        provider: str,
        model: str,
        input_tokens: int,
        embedding_type: str = "dense",
        dimensions: int = 0,
        memory_action: Optional[str] = None,
    ):
        self.provider = provider
        self.model = model
        self.input_tokens = input_tokens
        self.embedding_type = embedding_type  # 'dense' or 'sparse'
        self.dimensions = dimensions
        self.memory_action = memory_action
        self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "embedding_type": self.embedding_type,
            "dimensions": self.dimensions,
            "memory_action": self.memory_action,
            "timestamp": self.timestamp,
        }


class RequestTokenStats:
    """Aggregated token statistics for one high-level operation (add/search/update/...)."""

    def __init__(self, request_id: str, operation: str, user_id: Optional[str] = None):
        self.request_id = request_id
        self.operation = operation
        self.user_id = user_id
        self.start_time = datetime.utcnow().isoformat()
        self.end_time: Optional[str] = None

        self.llm_calls: List[LLMCallRecord] = []
        self.embedding_calls: List[EmbeddingCallRecord] = []

    # ---- LLM helpers ----

    def add_llm_call(self, record: LLMCallRecord) -> None:
        self.llm_calls.append(record)

    @property
    def llm_input_tokens(self) -> int:
        return sum(r.input_tokens for r in self.llm_calls)

    @property
    def llm_output_tokens(self) -> int:
        return sum(r.output_tokens for r in self.llm_calls)

    @property
    def llm_total_tokens(self) -> int:
        return self.llm_input_tokens + self.llm_output_tokens

    # ---- Embedding helpers ----

    def add_embedding_call(self, record: EmbeddingCallRecord) -> None:
        self.embedding_calls.append(record)

    @property
    def dense_embedding_tokens(self) -> int:
        return sum(r.input_tokens for r in self.embedding_calls if r.embedding_type == "dense")

    @property
    def sparse_embedding_tokens(self) -> int:
        return sum(r.input_tokens for r in self.embedding_calls if r.embedding_type == "sparse")

    @property
    def total_embedding_tokens(self) -> int:
        return self.dense_embedding_tokens + self.sparse_embedding_tokens

    # ---- Totals ----

    @property
    def grand_total_tokens(self) -> int:
        return self.llm_total_tokens + self.total_embedding_tokens

    def finish(self) -> None:
        self.end_time = datetime.utcnow().isoformat()

    def to_dict(self, include_details: bool = True) -> Dict[str, Any]:
        llm_providers = {}
        for call in self.llm_calls:
            key = f"{call.provider}/{call.model}"
            if key not in llm_providers:
                llm_providers[key] = {
                    "provider": call.provider,
                    "model": call.model,
                    "total_input_tokens": 0,
                    "total_output_tokens": 0,
                    "call_count": 0,
                }
            llm_providers[key]["total_input_tokens"] += call.input_tokens
            llm_providers[key]["total_output_tokens"] += call.output_tokens
            llm_providers[key]["call_count"] += 1

        embed_providers = {}
        for call in self.embedding_calls:
            key = f"{call.provider}/{call.model}/{call.embedding_type}"
            if key not in embed_providers:
                embed_providers[key] = {
                    "provider": call.provider,
                    "model": call.model,
                    "embedding_type": call.embedding_type,
                    "total_input_tokens": 0,
                    "call_count": 0,
                }
            embed_providers[key]["total_input_tokens"] += call.input_tokens
            embed_providers[key]["call_count"] += 1

        result: Dict[str, Any] = {
            "request_id": self.request_id,
            "operation": self.operation,
            "user_id": self.user_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "llm_tokens": {
                "total_input_tokens": self.llm_input_tokens,
                "total_output_tokens": self.llm_output_tokens,
                "total_tokens": self.llm_total_tokens,
                "call_count": len(self.llm_calls),
                "providers": list(llm_providers.values()),
            },
            "embedding_tokens": {
                "dense_tokens": self.dense_embedding_tokens,
                "sparse_tokens": self.sparse_embedding_tokens,
                "total_tokens": self.total_embedding_tokens,
                "call_count": len(self.embedding_calls),
                "providers": list(embed_providers.values()),
            },
            "total_tokens": self.grand_total_tokens,
        }

        if include_details:
            result["llm_tokens"]["calls"] = [c.to_dict() for c in self.llm_calls]
            result["embedding_tokens"]["calls"] = [c.to_dict() for c in self.embedding_calls]

        return result


class TokenTracker:
    """
    Tracks token usage across LLM and Embedding calls for each high-level operation.

    Usage:
        tracker = TokenTracker(config)
        with tracker.track("add", user_id="u1") as ctx:
            # ctx is the RequestTokenStats; inject tracker into LLM/Embedding instances
            result = memory_operation(...)
        # stats are automatically flushed to disk after the with-block exits
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}
        self.enabled: bool = self._get(cfg, ["enabled"], True)
        self.log_file: str = self._get(cfg, ["log_file"], "./logs/token_usage.jsonl")
        self.include_details: bool = self._get(cfg, ["include_details"], True)

        # thread-local storage for current request stats
        self._local = threading.local()
        self._lock = threading.Lock()

        if self.enabled:
            log_dir = os.path.dirname(self.log_file)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            logger.info(f"TokenTracker initialized - log_file: {self.log_file}")

    @staticmethod
    def _get(cfg: Dict, keys: List[str], default: Any) -> Any:
        for k in keys:
            if k in cfg:
                return cfg[k]
        return default

    # ---- Context management ----

    @contextmanager
    def track(self, operation: str, user_id: Optional[str] = None):
        """Context manager that creates a request tracking scope.

        Yields the RequestTokenStats so callers can inspect mid-flight if needed.
        On exit (even on exception), stats are finalised and flushed to disk.
        """
        if not self.enabled:
            yield None
            return

        request_id = str(uuid.uuid4())
        stats = RequestTokenStats(request_id, operation, user_id)
        self._local.current_stats = stats
        try:
            yield stats
        finally:
            stats.finish()
            self._flush(stats)
            self._local.current_stats = None

    def start_tracking(self, operation: str, user_id: Optional[str] = None) -> Optional[str]:
        """Start tracking without a context manager.

        Returns request_id; call finish_tracking(request_id) when done.
        """
        if not self.enabled:
            return None
        request_id = str(uuid.uuid4())
        stats = RequestTokenStats(request_id, operation, user_id)
        self._local.current_stats = stats
        return request_id

    def finish_tracking(self) -> Optional[RequestTokenStats]:
        """Finalize and flush the current request stats."""
        if not self.enabled:
            return None
        stats: Optional[RequestTokenStats] = getattr(self._local, "current_stats", None)
        if stats is not None:
            stats.finish()
            self._flush(stats)
            self._local.current_stats = None
        return stats

    @property
    def current_stats(self) -> Optional[RequestTokenStats]:
        return getattr(self._local, "current_stats", None)

    # ---- Recording ----

    def record_llm_call(
        self,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        purpose: str = "unknown",
    ) -> None:
        """Record a single LLM call. No-op if tracking is disabled or no active context."""
        if not self.enabled:
            return
        stats = self.current_stats
        if stats is None:
            return
        record = LLMCallRecord(provider, model, input_tokens, output_tokens, purpose)
        stats.add_llm_call(record)

    def record_embedding_call(
        self,
        provider: str,
        model: str,
        input_tokens: int,
        embedding_type: str = "dense",
        dimensions: int = 0,
        memory_action: Optional[str] = None,
    ) -> None:
        """Record a single Embedding call. No-op if tracking is disabled or no active context."""
        if not self.enabled:
            return
        stats = self.current_stats
        if stats is None:
            return
        record = EmbeddingCallRecord(
            provider, model, input_tokens, embedding_type, dimensions, memory_action
        )
        stats.add_embedding_call(record)

    # ---- Persistence ----

    def _flush(self, stats: RequestTokenStats) -> None:
        """Write one stats record as a JSONL line."""
        try:
            line = json.dumps(stats.to_dict(include_details=self.include_details), ensure_ascii=False)
            with self._lock:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception as e:
            logger.error(f"TokenTracker: failed to flush stats to {self.log_file}: {e}")
