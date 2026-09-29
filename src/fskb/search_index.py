"""Azure AI Search index definition + lifecycle (create / delete).

Field attributes follow the design:
  - filterable/facetable metadata for pre-filtering before vector search,
  - searchable text for BM25,
  - vector fields for k-NN,
  - a semantic configuration prioritizing subject/symptom/resolution.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from .config import Settings


def index_schema(name: str, dimensions: int) -> Dict[str, Any]:
    """Return the Search index definition body."""

    return {
        "name": name,
        "fields": [
            {"name": "id", "type": "Edm.String", "key": True, "filterable": True},
            {"name": "ticket_id", "type": "Edm.Int32", "filterable": True, "sortable": True, "facetable": True},
            {"name": "display_id", "type": "Edm.String", "filterable": True, "retrievable": True},
            {"name": "subject", "type": "Edm.String", "searchable": True, "retrievable": True},
            {"name": "symptom_text", "type": "Edm.String", "searchable": True, "retrievable": True},
            {"name": "resolution_text", "type": "Edm.String", "searchable": True, "retrievable": True},
            {"name": "resolution_source", "type": "Edm.String", "filterable": True, "facetable": True, "retrievable": True},
            {"name": "has_resolution", "type": "Edm.Boolean", "filterable": True, "retrievable": True},
            {"name": "category", "type": "Edm.String", "filterable": True, "facetable": True, "retrievable": True},
            {"name": "sub_category", "type": "Edm.String", "filterable": True, "facetable": True, "retrievable": True},
            {"name": "item_category", "type": "Edm.String", "filterable": True, "facetable": True, "retrievable": True},
            {"name": "department", "type": "Edm.String", "filterable": True, "facetable": True, "retrievable": True},
            {
                "name": "store",
                "type": "Collection(Edm.String)",
                "filterable": True,
                "facetable": True,
                "retrievable": True,
            },
            {"name": "group", "type": "Edm.String", "filterable": True, "facetable": True, "retrievable": True},
            {"name": "ticket_type", "type": "Edm.String", "filterable": True, "facetable": True, "retrievable": True},
            {
                "name": "tags",
                "type": "Collection(Edm.String)",
                "filterable": True,
                "facetable": True,
                "retrievable": True,
            },
            {
                "name": "error_codes",
                "type": "Collection(Edm.String)",
                "filterable": True,
                "facetable": True,
                "retrievable": True,
            },
            {
                "name": "hostnames",
                "type": "Collection(Edm.String)",
                "filterable": True,
                "facetable": True,
                "retrievable": True,
            },
            {
                "name": "apps",
                "type": "Collection(Edm.String)",
                "filterable": True,
                "facetable": True,
                "retrievable": True,
            },
            {"name": "created_at", "type": "Edm.DateTimeOffset", "filterable": True, "sortable": True, "retrievable": True},
            {"name": "closed_at", "type": "Edm.DateTimeOffset", "filterable": True, "sortable": True, "retrievable": True},
            {"name": "updated_at", "type": "Edm.DateTimeOffset", "filterable": True, "sortable": True, "retrievable": True},
            {"name": "age_days", "type": "Edm.Int32", "filterable": True, "sortable": True, "retrievable": True},
            {
                "name": "linked_itglue_doc_ids",
                "type": "Collection(Edm.String)",
                "filterable": True,
                "retrievable": True,
            },
            {
                "name": "linked_itglue_urls",
                "type": "Collection(Edm.String)",
                "retrievable": True,
            },
            {
                "name": "symptom_vector",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "retrievable": False,
                "dimensions": dimensions,
                "vectorSearchProfile": "hnsw-profile",
            },
            {
                "name": "resolution_vector",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "retrievable": False,
                "dimensions": dimensions,
                "vectorSearchProfile": "hnsw-profile",
            },
        ],
        "vectorSearch": {
            "algorithms": [
                {
                    "name": "hnsw-config",
                    "kind": "hnsw",
                    "hnswParameters": {"m": 4, "efConstruction": 400, "efSearch": 500, "metric": "cosine"},
                }
            ],
            "profiles": [
                {"name": "hnsw-profile", "algorithm": "hnsw-config"}
            ],
        },
        "semantic": {
            "configurations": [
                {
                    "name": "default-semantic",
                    "prioritizedFields": {
                        "titleField": {"fieldName": "subject"},
                        "prioritizedContentFields": [
                            {"fieldName": "symptom_text"},
                            {"fieldName": "resolution_text"},
                        ],
                        "prioritizedKeywordsFields": [
                            {"fieldName": "error_codes"},
                            {"fieldName": "hostnames"},
                            {"fieldName": "apps"},
                        ],
                    },
                }
            ]
        },
    }


class IndexManager:
    def __init__(self, settings: Settings, session: Optional[requests.Session] = None):
        settings.require_search()
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.update(
            {"api-key": settings.search_api_key or "", "Content-Type": "application/json"}
        )
        self.api = settings.search_api_version
        self.base = settings.search_endpoint

    @property
    def index_name(self) -> str:
        return self.settings.search_index_name

    def exists(self) -> bool:
        resp = self.session.get(
            f"{self.base}/indexes/{self.index_name}?api-version={self.api}", timeout=30
        )
        if resp.status_code == 404:
            return False
        if resp.status_code >= 400:
            raise RuntimeError(f"index check failed: HTTP {resp.status_code} {resp.text[:200]}")
        return True

    def create(self, dimensions: Optional[int] = None, recreate: bool = False) -> Dict[str, Any]:
        dims = dimensions or self.settings.embed_dimensions
        if recreate and self.exists():
            self.delete()
        body = index_schema(self.index_name, dims)
        resp = self.session.put(
            f"{self.base}/indexes/{self.index_name}?api-version={self.api}", json=body, timeout=30
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"index create failed: HTTP {resp.status_code} {resp.text[:400]}")
        return resp.json()

    def delete(self) -> None:
        resp = self.session.delete(
            f"{self.base}/indexes/{self.index_name}?api-version={self.api}", timeout=30
        )
        if resp.status_code >= 400 and resp.status_code != 404:
            raise RuntimeError(f"index delete failed: HTTP {resp.status_code} {resp.text[:200]}")

    def count(self) -> int:
        resp = self.session.get(
            f"{self.base}/indexes/{self.index_name}/docs/$count?api-version={self.api}", timeout=30
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"count failed: HTTP {resp.status_code} {resp.text[:200]}")
        return int(resp.text.strip())
