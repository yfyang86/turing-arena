"""Multi-provider LLM client. Retry with backoff. Per-laureate provider override."""

from __future__ import annotations
import asyncio
import json
import logging
import httpx
from typing import AsyncIterator

log = logging.getLogger(__name__)


class LLMError(Exception):
    """Raised when LLM API returns a non-200 status."""


class LLMRouter:
    def __init__(self, config: dict):
        self.providers = config.get("providers", {})
        self.default = config.get("default", {}).get("provider", "openai")
        self.laureate_providers: dict[str, str] = config.get("laureate_providers", {})
        self._client: httpx.AsyncClient | None = None
        self._wiki_client: httpx.AsyncClient | None = None
        self.max_retries = 3
        self.base_delay = 1.0
        # Wiki-specific timeouts from config
        wiki_cfg = config.get("wiki", {})
        self.wiki_heartbeat_s = wiki_cfg.get("heartbeat_timeout_s", 5)
        self.wiki_generation_s = wiki_cfg.get("generation_timeout_s", 600)
        self.wiki_max_context = wiki_cfg.get("max_context_length", 20000)

    def reload_config(self, config: dict):
        """L32: hot-reload provider config."""
        self.providers = config.get("providers", {})
        self.default = config.get("default", {}).get("provider", "openai")
        self.laureate_providers = config.get("laureate_providers", {})
        wiki_cfg = config.get("wiki", {})
        self.wiki_heartbeat_s = wiki_cfg.get("heartbeat_timeout_s", 5)
        self.wiki_generation_s = wiki_cfg.get("generation_timeout_s", 600)
        self.wiki_max_context = wiki_cfg.get("max_context_length", 20000)
        log.info("LLM config reloaded: %d providers, wiki timeout=%ds", len(self.providers), self.wiki_generation_s)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=120.0)
        return self._client

    async def _get_wiki_client(self) -> httpx.AsyncClient:
        """Separate client for wiki operations with longer total timeout + heartbeat read timeout."""
        if self._wiki_client is None or self._wiki_client.is_closed:
            self._wiki_client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=float(self.wiki_heartbeat_s),   # per-chunk read timeout (heartbeat)
                    write=10.0,
                    pool=10.0,
                ),
            )
        return self._wiki_client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        if self._wiki_client and not self._wiki_client.is_closed:
            await self._wiki_client.aclose()

    def get_provider(self, name: str | None = None) -> dict:
        name = name or self.default
        if name not in self.providers:
            raise ValueError(f"Provider '{name}' not found. Available: {list(self.providers.keys())}")
        return self.providers[name]

    def provider_for_laureate(self, slug: str) -> str | None:
        """L30: get provider override for a specific laureate, or None for default."""
        return self.laureate_providers.get(slug)

    async def chat_complete(
        self, messages: list[dict], system: str = "",
        provider_name: str | None = None,
    ) -> str:
        """Non-streaming completion. Collects streamed tokens into a string."""
        result = []
        async for chunk in self.chat_stream(messages, system, provider_name):
            result.append(chunk)
        return "".join(result)

    async def chat_complete_wiki(
        self, messages: list[dict], system: str = "",
        provider_name: str | None = None,
    ) -> str:
        """Wiki-specific completion with configurable timeouts.
        Uses wiki_heartbeat_s for per-chunk read timeout, wiki_generation_s for total."""
        result = []

        async def _collect():
            async for chunk in self.chat_stream(messages, system, provider_name,
                                                 use_wiki_client=True):
                result.append(chunk)

        try:
            await asyncio.wait_for(_collect(), timeout=float(self.wiki_generation_s))
        except asyncio.TimeoutError:
            if result:
                # Partial result — return what we got
                log.warning("Wiki generation timed out after %ds, returning partial (%d chars)",
                            self.wiki_generation_s, sum(len(c) for c in result))
            else:
                raise LLMError(f"Wiki generation timed out after {self.wiki_generation_s}s with no output")
        return "".join(result)

    async def chat_stream(
        self, messages: list[dict], system: str = "",
        provider_name: str | None = None, use_wiki_client: bool = False,
    ) -> AsyncIterator[str]:
        """Stream with retry/backoff. use_wiki_client=True for wiki-specific timeouts."""
        last_err = None
        for attempt in range(self.max_retries):
            try:
                prov = self.get_provider(provider_name)
                ptype = prov.get("type", "openai")
                client = await (self._get_wiki_client() if use_wiki_client else self._get_client())
                gen = self._stream_claude(prov, messages, system, client) if ptype == "claude" \
                    else self._stream_openai(prov, messages, system, client)
                async for chunk in gen:
                    yield chunk
                return
            except LLMError as e:
                last_err = e
                if "401" in str(e) or "403" in str(e):
                    raise
                delay = self.base_delay * (2 ** attempt)
                log.warning("LLM attempt %d failed: %s. Retry in %.1fs", attempt + 1, e, delay)
                await asyncio.sleep(delay)
            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                last_err = LLMError(str(e))
                delay = self.base_delay * (2 ** attempt)
                log.warning("LLM connection error attempt %d: %s. Retry in %.1fs", attempt + 1, e, delay)
                await asyncio.sleep(delay)
            except Exception as e:
                err_name = type(e).__name__
                if "Protocol" in err_name or "Remote" in err_name or "incomplete" in str(e).lower():
                    last_err = LLMError(f"{err_name}: {e}")
                    delay = self.base_delay * (2 ** attempt)
                    log.warning("LLM transport error attempt %d: %s. Retry in %.1fs", attempt + 1, e, delay)
                    await asyncio.sleep(delay)
                else:
                    raise
        if last_err:
            raise last_err

    async def _stream_openai(self, prov, messages, system, client=None) -> AsyncIterator[str]:
        url = f"{prov['api_base'].rstrip('/')}/chat/completions"
        msgs = ([{"role": "system", "content": system}] if system else []) + messages
        body = {"model": prov["model"], "messages": msgs, "stream": True, "max_tokens": 4096}
        headers = {"Authorization": f"Bearer {prov['api_key']}", "Content-Type": "application/json"}
        if client is None:
            client = await self._get_client()
        async with client.stream("POST", url, json=body, headers=headers) as resp:
            if resp.status_code != 200:
                t = ""; 
                async for c in resp.aiter_bytes():
                    t += c.decode(errors="replace")
                    if len(t) > 500: break
                raise LLMError(f"OpenAI API {resp.status_code}: {t[:300]}")
            async for line in resp.aiter_lines():
                if not line.startswith("data: "): continue
                d = line[6:]
                if d.strip() == "[DONE]": break
                try:
                    text = json.loads(d)["choices"][0].get("delta", {}).get("content", "")
                    if text: yield text
                except (json.JSONDecodeError, KeyError, IndexError): continue

    async def _stream_claude(self, prov, messages, system, client=None) -> AsyncIterator[str]:
        url = f"{prov['api_base'].rstrip('/')}/v1/messages"
        body = {"model": prov["model"], "max_tokens": 4096, "stream": True, "messages": messages}
        if system: body["system"] = system
        headers = {"x-api-key": prov["api_key"], "anthropic-version": "2023-06-01", "Content-Type": "application/json"}
        if client is None:
            client = await self._get_client()
        async with client.stream("POST", url, json=body, headers=headers) as resp:
            if resp.status_code != 200:
                t = ""
                async for c in resp.aiter_bytes():
                    t += c.decode(errors="replace")
                    if len(t) > 500: break
                raise LLMError(f"Claude API {resp.status_code}: {t[:300]}")
            async for line in resp.aiter_lines():
                if not line.startswith("data: "): continue
                try:
                    obj = json.loads(line[6:])
                    if obj.get("type") == "content_block_delta":
                        text = obj.get("delta", {}).get("text", "")
                        if text: yield text
                except (json.JSONDecodeError, KeyError): continue
