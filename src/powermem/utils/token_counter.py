"""
Token counter utilities for LLM and Embedding providers.

Supports:
- OpenAI: tiktoken local calculation (fast, accurate)
- Qwen: DashScope Tokenization API
- Others: character-based estimation fallback
"""

import logging
from typing import Dict, List, Union

logger = logging.getLogger(__name__)


def _estimate_tokens(text: str) -> int:
    """Estimate token count using character-based heuristics.

    Chinese characters count as 1 token each.
    English/other characters follow the ~4 chars per token rule.
    """
    if not text:
        return 0
    chinese_chars = sum(1 for c in text if "\u4e00" <= c <= "\u9fff")
    other_chars = len(text) - chinese_chars
    return chinese_chars + max(1, other_chars // 4)


def _messages_to_text(messages: List[Dict]) -> str:
    """Concatenate message content into a single string for token counting."""
    parts = []
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(item.get("text", ""))
    return "\n".join(parts)


def count_tokens_for_text(text: str, provider: str, model: str) -> int:
    """Count tokens for a plain text string.

    Args:
        text: The text to count tokens for.
        provider: LLM/Embedding provider name (e.g., 'openai', 'qwen').
        model: Model name (e.g., 'gpt-4o', 'qwen-turbo').

    Returns:
        Token count, or -1 on failure.
    """
    if not text:
        return 0

    provider_lower = provider.lower()

    if provider_lower in ("openai", "azure", "azure_openai", "siliconflow", "deepseek",
                          "openai_structured", "zai", "lmstudio"):
        try:
            import tiktoken
            try:
                encoding = tiktoken.encoding_for_model(model)
            except KeyError:
                encoding = tiktoken.get_encoding("cl100k_base")
            return len(encoding.encode(text))
        except Exception as e:
            logger.debug(f"tiktoken counting failed for provider={provider}, model={model}: {e}")
            return _estimate_tokens(text)

    elif provider_lower == "qwen":
        try:
            import dashscope
            from dashscope.api_entities.dashscope_response import Message
            response = dashscope.Tokenization.call(
                model=model,
                messages=[Message(role="user", content=text)],
            )
            if response.status_code == 200:
                return response.usage.get("input_tokens", 0)
            else:
                logger.debug(
                    f"Qwen Tokenization API failed: status={response.status_code}, "
                    f"message={response.message}"
                )
                return _estimate_tokens(text)
        except Exception as e:
            logger.debug(f"Qwen token counting failed: {e}")
            return _estimate_tokens(text)

    else:
        return _estimate_tokens(text)


def count_tokens_for_messages(
    messages: List[Dict],
    provider: str,
    model: str,
) -> int:
    """Count tokens for a list of chat messages.

    Args:
        messages: List of message dicts with 'role' and 'content'.
        provider: LLM provider name.
        model: Model name.

    Returns:
        Estimated total input token count.
    """
    if not messages:
        return 0

    provider_lower = provider.lower()

    if provider_lower in ("openai", "azure", "azure_openai", "siliconflow", "deepseek",
                          "openai_structured", "zai", "lmstudio"):
        try:
            import tiktoken
            try:
                encoding = tiktoken.encoding_for_model(model)
            except KeyError:
                encoding = tiktoken.get_encoding("cl100k_base")

            total = 0
            for msg in messages:
                total += 4  # per-message overhead (role + framing tokens)
                content = msg.get("content", "")
                if isinstance(content, str):
                    total += len(encoding.encode(content))
                elif isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            total += len(encoding.encode(item.get("text", "")))
            total += 2  # reply priming
            return total
        except Exception as e:
            logger.debug(f"tiktoken message counting failed: {e}")
            return _estimate_tokens(_messages_to_text(messages))

    elif provider_lower == "qwen":
        try:
            import dashscope
            from dashscope.api_entities.dashscope_response import Message as DashMessage
            dash_messages = []
            for msg in messages:
                content = msg.get("content", "")
                if isinstance(content, list):
                    content = _messages_to_text([msg])
                dash_messages.append(DashMessage(role=msg.get("role", "user"), content=content))

            response = dashscope.Tokenization.call(model=model, messages=dash_messages)
            if response.status_code == 200:
                return response.usage.get("input_tokens", 0)
            else:
                logger.debug(
                    f"Qwen Tokenization API failed: status={response.status_code}"
                )
                return _estimate_tokens(_messages_to_text(messages))
        except Exception as e:
            logger.debug(f"Qwen message token counting failed: {e}")
            return _estimate_tokens(_messages_to_text(messages))

    else:
        return _estimate_tokens(_messages_to_text(messages))
