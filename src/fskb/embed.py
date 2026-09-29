"""Embeddings client with two selectable providers.

  EMBED_PROVIDER=azure   -> Azure OpenAI native shape:
                            POST {aoai_endpoint}/openai/deployments/{dep}/embeddings?api-version=...
                            header: api-key
  EMBED_PROVIDER=openai  -> OpenAI-compatible shape (LiteLLM proxy, OpenAI, ...):
                            POST {embed_base_url}/v1/embeddings
                            header: Authorization: Bearer
                            model name travels in the request body.

One model, one dimension count, fixed per index. Change the model => build a new
index (see README's blue/green note); never mix vectors in one index.
"""

from __future__ import annotations

import time
from typing import List, Optional, Sequence

import requests

from .config import Settings

_RETRYABLE = {429, 500, 502, 503, 504}


class EmbeddingError(RuntimeError):
    pass


class EmbeddingClient:
    """Unified embedding client. ``provider`` defaults to ``settings.embed_provider``."""

    def __init__(
        self,
        settings: Settings,
        session: Optional[requests.Session] = None,
        provider: Optional[str] = None,
    ):
        settings.require_embedding()
        self.settings = settings
        self.provider = (provider or settings.embed_provider or "azure").lower()
        self.session = session or requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

        if self.provider == "openai":
            self.session.headers.update(
                {"Authorization": f"Bearer {settings.embed_api_key or ''}"}
            )
            self._url = f"{settings.embed_base_url}/v1/embeddings"
            self._model = settings.embed_model
        else:  # azure
            self.session.headers.update({"api-key": settings.aoai_api_key or ""})
            self._url = (
                f"{settings.aoai_endpoint}/openai/deployments/{settings.aoai_embed_deployment}"
                f"/embeddings?api-version={settings.aoai_api_version}"
            )
            self._model = settings.aoai_embed_deployment  # unused in the body for Azure

    @property
    def model(self) -> str:
        return self._model

    def _payload(self, texts: Sequence[str]) -> dict:
        body = {"input": list(texts)}
        if self.provider == "openai":
            body["model"] = self._model
        return body

    def embed_batch(self, texts: Sequence[str], max_retries: int = 5) -> List[List[float]]:
        if not texts:
            return []
        payload = self._payload(texts)
        backoff = 1.0
        for attempt in range(max_retries + 1):
            resp = self.session.post(self._url, json=payload, timeout=60)
            if resp.status_code in _RETRYABLE:
                if attempt == max_retries:
                    raise EmbeddingError(f"embeddings failed: HTTP {resp.status_code}")
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                time.sleep(min(delay, 30.0))
                backoff = min(backoff * 2, 30.0)
                continue
            if resp.status_code >= 400:
                raise EmbeddingError(f"embeddings failed: HTTP {resp.status_code} {resp.text[:300]}")
            data = resp.json()
            items = sorted(data.get("data", []), key=lambda d: d.get("index", 0))
            return [item["embedding"] for item in items]
        raise EmbeddingError("embeddings: retries exhausted")

    def embed_one(self, text: str) -> List[float]:
        vectors = self.embed_batch([text])
        return vectors[0] if vectors else []

    def embed_all(self, texts: Sequence[str]) -> List[List[float]]:
        """Embed many texts in batches of ``embed_batch_size``."""

        out: List[List[float]] = []
        size = max(1, self.settings.embed_batch_size)
        for start in range(0, len(texts), size):
            chunk = list(texts[start : start + size])
            out.extend(self.embed_batch(chunk))
        return out
