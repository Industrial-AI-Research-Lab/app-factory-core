from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from llm.agent_model_params import DEFAULT_AGENT_MODEL


class AsyncBuffer:
    def __init__(
        self,
        max_chars: int = 120_000,
        compress_threshold: float = 0.92,
        keep_last_messages: int = 20,
    ):
        self.max_chars = int(max_chars)
        self.compress_threshold = float(compress_threshold)
        self.keep_last_messages = int(keep_last_messages)

    def approx_size(self, messages: List[Dict[str, Any]]) -> int:
        total = 0
        for m in messages:
            total += len(str(m.get("role", "")))
            total += len(str(m.get("content", "") or ""))
            tc = m.get("tool_calls")
            if tc is not None:
                total += len(str(tc))
        return total

    async def maybe_compress(
        self,
        llm_client,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        write_markdown: Optional[Callable[[str, str], Any]] = None,
        markdown_path: str = ".AppFactory/memory.md",
    ) -> List[Dict[str, Any]]:
        if not messages:
            return messages

        size = self.approx_size(messages)
        if size <= int(self.max_chars * self.compress_threshold):
            return messages

        if len(messages) <= self.keep_last_messages + 2:
            return messages

        head = messages[:-self.keep_last_messages]
        tail = messages[-self.keep_last_messages :]

        sys = {
            "role": "system",
            "content": "Summarize the earlier conversation/context into a concise, factual memory for continuing the task. Keep names, decisions, and constraints. No fluff.",
        }
        user = {
            "role": "user",
            "content": "Summarize these messages:\n\n" + _render_messages(head),
        }

        try:
            resp = await llm_client.chat_completion(
                messages=[sys, user],
                # Summarize with the caller's own tenant-resolved model; the client
                # default is tenant-agnostic and may be blocked on the caller's key.
                model=model or getattr(llm_client, "default_model", None) or DEFAULT_AGENT_MODEL,
                temperature=0.2,
            )
            summary = (resp or {}).get("content") or ""
        except Exception:
            return messages

        summary_msg = {"role": "system", "content": "Context summary:\n" + summary}

        if write_markdown and summary:
            try:
                await write_markdown(markdown_path, summary)
            except Exception:
                pass

        return [summary_msg] + tail


def _render_messages(msgs: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for m in msgs:
        role = str(m.get("role", ""))
        content = str(m.get("content", "") or "")
        parts.append(f"{role.upper()}: {content}")
    return "\n\n".join(parts)
