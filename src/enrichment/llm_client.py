"""OpenAI-compatible LLM client with retries, rate limiting and cost tracking.

Provider-agnostic by design: any gateway exposing an OpenAI-style
/v1/chat/completions endpoint works (config-driven via LLM_BASE_URL /
LLM_MODEL / LLM_API_KEY env vars). No vendor endpoints or keys in code.
"""
from __future__ import annotations

import json
import re
import threading
import time

import requests


class LLMError(RuntimeError):
    pass


class CostTracker:
    """Thread-safe token/cost accounting across the enrichment run."""

    def __init__(self, cost_per_1k_input: float = 0.0, cost_per_1k_output: float = 0.0):
        self._lock = threading.Lock()
        self.calls = 0
        self.failures = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.cost_per_1k_input = cost_per_1k_input
        self.cost_per_1k_output = cost_per_1k_output

    def record(self, input_tokens: int, output_tokens: int) -> None:
        with self._lock:
            self.calls += 1
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens

    def record_failure(self) -> None:
        with self._lock:
            self.failures += 1

    @property
    def cost_usd(self) -> float:
        return (
            self.input_tokens / 1000 * self.cost_per_1k_input
            + self.output_tokens / 1000 * self.cost_per_1k_output
        )

    def summary(self) -> dict:
        with self._lock:
            return {
                "calls": self.calls,
                "failures": self.failures,
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "cost_usd": round(self.cost_usd, 6),
            }


_JSON_BLOCK = re.compile(r"\{.*\}", re.DOTALL)


def extract_json(text: str) -> dict:
    """Pull the first JSON object out of an LLM response (handles code fences)."""
    match = _JSON_BLOCK.search(text)
    if not match:
        raise LLMError(f"no JSON object in response: {text[:200]!r}")
    return json.loads(match.group(0))


class LLMClient:
    def __init__(self, cfg: dict, tracker: CostTracker | None = None):
        llm = cfg["llm"]
        self.base_url = llm["base_url"].rstrip("/")
        self.model = llm["model"]
        self.api_key = llm["api_key"]
        self.temperature = llm.get("temperature", 0.0)
        self.max_output_tokens = llm.get("max_output_tokens", 120)
        self.timeout = llm.get("timeout_seconds", 12)
        self.max_retries = llm.get("max_retries", 1)
        self.rate_limit = llm.get("rate_limit_seconds", 0.0)
        self.tracker = tracker or CostTracker()
        self._last_call = 0.0
        self._rl_lock = threading.Lock()
        self._session = requests.Session()
        if self.api_key:
            self._session.headers["Authorization"] = f"Bearer {self.api_key}"

    @property
    def enabled(self) -> bool:
        return bool(self.base_url and self.model and self.api_key)

    def _respect_rate_limit(self) -> None:
        if not self.rate_limit:
            return
        with self._rl_lock:
            wait = self.rate_limit - (time.time() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()

    def chat(self, system: str, user: str) -> str:
        """One chat completion; raises LLMError after retries are exhausted."""
        if not self.enabled:
            raise LLMError("LLM client not configured (LLM_BASE_URL/LLM_MODEL/LLM_API_KEY)")
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": self.temperature,
            "max_tokens": self.max_output_tokens,
        }
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._respect_rate_limit()
            try:
                resp = self._session.post(
                    f"{self.base_url}/chat/completions", json=payload, timeout=self.timeout
                )
                if resp.status_code != 200:
                    raise LLMError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                data = resp.json()
                usage = data.get("usage") or {}
                self.tracker.record(usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0))
                return data["choices"][0]["message"]["content"]
            except (requests.RequestException, LLMError, KeyError, ValueError) as exc:
                last_exc = exc
        self.tracker.record_failure()
        raise LLMError(f"LLM call failed after {self.max_retries + 1} attempts: {last_exc}")
