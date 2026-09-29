"""Domain models.

Two layers:

* ``KbDocument`` - one record per ticket, the exact shape pushed to Azure AI
  Search. ``to_search_doc`` renders the JSON body the Search REST API expects.
* Helper types for the raw FreshService payloads we consume (kept loose on
  purpose: the FreshService API is sparsely documented and we only read fields).
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

from pydantic import BaseModel, Field

_STATUS_RESOLVED = 4
_STATUS_CLOSED = 5
_TERMINAL_STATUSES = {_STATUS_RESOLVED, _STATUS_CLOSED}


def is_terminal_status(status: Any) -> bool:
    """True when a FreshService ticket status is Resolved(4) or Closed(5)."""

    try:
        return int(status) in _TERMINAL_STATUSES
    except (TypeError, ValueError):
        return False


class KbDocument(BaseModel):
    """One ticket -> one knowledge-base record."""

    id: str
    ticket_id: int
    display_id: Optional[str] = None
    subject: str = ""

    # The query side (embedded) and the answer side (stored).
    symptom_text: str = ""
    resolution_text: str = ""
    resolution_source: str = "unknown"  # private_note | resolution_field | reply
    has_resolution: bool = False

    # Classification / metadata (filterable).
    category: Optional[str] = None
    sub_category: Optional[str] = None
    item_category: Optional[str] = None
    department: Optional[str] = None
    store: List[str] = Field(default_factory=list)
    group: Optional[str] = None
    ticket_type: Optional[str] = None
    tags: List[str] = Field(default_factory=list)

    # Extracted entities that make hybrid search beat pure vectors.
    error_codes: List[str] = Field(default_factory=list)
    hostnames: List[str] = Field(default_factory=list)
    apps: List[str] = Field(default_factory=list)

    # Timestamps / derived.
    created_at: Optional[str] = None
    closed_at: Optional[str] = None
    updated_at: Optional[str] = None
    age_days: Optional[int] = None

    # Provenance to the authoritative documentation layer.
    linked_itglue_doc_ids: List[str] = Field(default_factory=list)
    linked_itglue_urls: List[str] = Field(default_factory=list)

    # Vectors (filled by the embedding step).
    symptom_vector: List[float] = Field(default_factory=list)
    resolution_vector: List[float] = Field(default_factory=list)

    def to_search_doc(self) -> dict:
        """Render the document for the Search ``mergeOrUpload`` action.

        Empty vectors are omitted so a document can be upserted before embedding
        (e.g. during verification) without tripping dimension validation.
        """

        doc: dict[str, Any] = {
            "id": self.id,
            "ticket_id": self.ticket_id,
            "display_id": self.display_id,
            "subject": self.subject,
            "symptom_text": self.symptom_text,
            "resolution_text": self.resolution_text,
            "resolution_source": self.resolution_source,
            "has_resolution": self.has_resolution,
            "category": self.category,
            "sub_category": self.sub_category,
            "item_category": self.item_category,
            "department": self.department,
            "store": self.store,
            "group": self.group,
            "ticket_type": self.ticket_type,
            "tags": self.tags,
            "error_codes": self.error_codes,
            "hostnames": self.hostnames,
            "apps": self.apps,
            "created_at": self.created_at,
            "closed_at": self.closed_at,
            "updated_at": self.updated_at,
            "age_days": self.age_days,
            "linked_itglue_doc_ids": self.linked_itglue_doc_ids,
            "linked_itglue_urls": self.linked_itglue_urls,
        }
        if self.symptom_vector:
            doc["symptom_vector"] = self.symptom_vector
        if self.resolution_vector:
            doc["resolution_vector"] = self.resolution_vector
        return doc

    @property
    def age_days_estimate(self) -> Optional[int]:
        return self.age_days


def slugify_id(ticket_id: Any) -> str:
    """Stable string key for a ticket id."""

    return str(int(ticket_id))


# Secret-material detectors used to keep recovery keys / passwords out of the KB.
_BITLOCKER_GUID = re.compile(
    r"\b[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\b"
)
_PASSWORDISH = re.compile(
    r"(?i)\b(?:password|passwd|pwd|recovery\s*key|api\s*key|secret|token|license\s*key)\b\s*[:=]\s*(?!<redacted)\S+"
)
