"""freshservice-kb: build an Azure AI Search knowledge base from FreshService tickets.

Symptom-as-query / resolution-as-answer design:

    extract -> qualify -> sanitize -> enrich -> embed -> push

The module is import-safe: no network calls or required environment variables at
import time. Configuration is read lazily by ``Settings.from_env()``.
"""

__version__ = "0.1.0"
