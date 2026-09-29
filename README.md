# freshservice-kb

Build an **Azure AI Search** knowledge base from resolved **FreshService** tickets,
so a helpdesk agent can pull "how did we fix this before?" on a new ticket.

The design principle is **symptom-as-query / resolution-as-answer**:

```
FreshService API ──▶ qualify ──▶ sanitize ──▶ enrich ──▶ embed ──▶ Azure AI Search
   (tickets +          (resolved   (redact    (error      (Azure      (hybrid
    conversations)      + real      PII/       codes,      OpenAI      BM25+vector
                        fix only)   secrets)   hosts,      embed)      + semantic)
                                               apps)
```

- We embed the **symptom** (`description` + first requester replies) and store the
  **resolution** (resolution field → last private note → last public reply)
  alongside it. A new ticket's symptom is matched to historical symptoms; the
  record hands back the paired fix.
- Exact tokens (error codes, hostnames, apps) are extracted into filterable fields
  so hybrid search beats pure vectors.
- Secrets (BitLocker keys, passwords) and PII (emails, phones) never reach the index.

## Layout

```
src/fskb/
  config.py         env-driven settings (redacted view, capability checks)
  models.py         KbDocument (the Search record) + status/secret helpers
  freshservice.py   FreshService v2 client (paged list, one ticket + conversations)
  state.py          watermark file (incremental contract)
  transform.py      ticket + conversations -> KbDocument
  sanitize.py       HTML strip, quote/signature trim, secret + PII redaction
  enrich.py         error codes / hostnames / apps extraction
  embed.py          embeddings client (Azure OpenAI OR OpenAI-compatible/LiteLLM)
  search_index.py   index schema + create/delete/count
  search_client.py  document push, hybrid query, OData filter builder
  pipeline.py       extraction/transform/embed/push orchestration + reconcile
  evaluate.py       gold set (build/load/save) + hit@k / recall@k / MRR scoring
  feedback.py       accept/reject event log + boost table
  cli.py            fskb command
scripts/run_pipeline.py   scheduler entrypoint (incremental / reconcile / both)
```

## Setup

```bash
cd freshservice-kb
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env   # fill in real values
```

## CLI

```bash
fskb status                          # redacted config + index doc count
fskb init-index                      # create the index (--recreate to rebuild)
fskb extract --limit 200 --out out/records.jsonl   # prove the data, no embed/push
fskb index                           # incremental: extract -> embed -> push
fskb index --dry-run                 # everything except embed/push
fskb backfill --limit 5000           # index the most recent 5000 (ignores/keeps watermark)
fskb query "outlook account locked after password change"
fskb query "0x80070005" --app excel  # hybrid + OData pre-filter
fskb reconcile                       # soft-delete tickets no longer resolved/closed
fskb gold --size 50                  # build a review-ready gold eval set
fskb eval                            # leave-one-out retrieval score (quality gate)
fskb eval --integrity                # self-match smoke test (proves plumbing only)
fskb feedback --ticket 48001 --suggested-ticket 47211 --accepted --resolved true
```

## Evaluation (do this before you trust it)

Retrieval quality is measured, not assumed. Two modes, on purpose:

- **`fskb eval --integrity`** - query each ticket with its own symptom and check
  the index returns it. This only proves the plumbing (embeddings land, filters
  work). It is a smoke test, **not** a quality number.
- **`fskb eval`** (leave-one-out, the default) - withhold the ticket from its own
  results and check the top-k still surfaces an *equivalent fix* (another ticket
  in the same fix cluster). This is the number that predicts whether a brand-new
  ticket finds the right prior fix.

Workflow:

```bash
fskb gold --size 50 --out gold/goldset.jsonl   # samples from the INDEX
fskb gold --from-jsonl out/records.jsonl       # or from a local extract
$EDITOR gold/goldset.jsonl                # confirm expected_ticket_ids, set "labeled": true
fskb eval --k 3 --min-hit 0.8             # exits non-zero below the gate
```

`fskb gold` reads the **index** by default (the source of truth once `backfill`
has run), so it samples from all indexed tickets. Pass `--from-jsonl` to use a
local `extract` file instead. If the sample size is unexpectedly small, the
likely cause is reading a stale JSONL that predates the backfill.

`fskb gold` pre-fills `expected_ticket_ids` with the ticket itself plus any
cluster mates (tickets sharing a resolution signature), so review is
confirm-or-edit rather than blank-page. Metrics reported: `hit@1`, `hit@k`,
`recall@k`, `MRR`, plus a sample of misses. Use `--json out.json` to save it.

**Gate before you trust it:** an LOO `hit@3` of 0.8+ means 4 in 5 unseen tickets
would be shown the correct prior fix. Below that, fix the corpus (duplicates,
chunking, filters) before wiring it into the helpdesk flow.

## Feedback loop (how it improves in production)

Evaluation is the offline check; feedback is the live one. When an agent is shown
top-k suggestions and acts, log the judgement:

```bash
fskb feedback --ticket 48001 --suggested-ticket 47211 --rank 1 --accepted --resolved true \
  --agent "Axel W" --note "same fix worked"
fskb feedback --ticket 48002 --suggested-ticket 47211 --rank 1 --accepted --resolved false
fskb feedback --ticket 48003 --suggested-ticket 47211 --accepted   # accepted, outcome unknown
fskb feedback --ticket 48001 --suggested-ticket 47211 --accepted --boost-out feedback/boost.json
```

Events append to `feedback/events.jsonl`; `(ticket, suggestion)` pairs are
deduplicated keeping the last event, so re-judging does not double-count.
`--boost-out` writes a bounded `{suggested_ticket_id: 1.0-1.5}` table (driven by
success rate with a small volume term) that the caller multiplies into the base
retrieval score. Blend it with the recency term (`created_at`/`age_days` are
returned by every query).

## Backfill vs incremental (important)

`fskb index` is **incremental**: it asks FreshService only for tickets changed
since the stored watermark (`.state/state.json`, `last_updated_at`), then
advances that watermark. A capped run (`--limit N`) does **not** advance it.

To load history the first time, use **`fskb backfill`**, which **ignores the
stored watermark entirely** (it neither reads nor writes it), pulls the most
recent N tickets `created_at desc`, and does **not** advance the watermark - so
it cannot poison the scheduled incremental runs:

```bash
fskb backfill --dry-run --limit 5000   # proof: pull + build, no embed/push
fskb backfill --limit 5000             # embed + push the last 5000
```

If `backfill` ever returns only a handful of tickets, the bug to look for is the
watermark being *read*: an incremental run that advanced `last_updated_at`
(e.g. an earlier `index`) will make a naive backfill pull only what changed
since. `backfill` must pass `no_watermark=True` so `resolve_watermark` returns
`None` and the pull is unbounded.

FreshService caps offset paging at **9000 records** per filter combination, so a
backfill larger than that needs date-window slicing on `created_at`. For this
corpus (a few thousand tickets) one `backfill --limit 5000` is enough.

## Embeddings provider

Two providers, selected by `EMBED_PROVIDER`:

- **`azure`** (default) — Azure OpenAI native endpoint. Uses `AOAI_ENDPOINT`,
  `AOAI_API_KEY`, `AOAI_EMBED_DEPLOYMENT`. Posts to
  `/openai/deployments/{dep}/embeddings` with the `api-key` header.
- **`openai`** — any OpenAI-compatible `/v1/embeddings` endpoint, including a
  **LiteLLM proxy** fronting `text-embedding-3-large`. Uses `EMBED_BASE_URL`
  (no `/v1` suffix), `EMBED_API_KEY`, `EMBED_MODEL` (sent in the body), with the
  `Authorization: Bearer` header.

```bash
# LiteLLM example
EMBED_PROVIDER=openai
EMBED_BASE_URL=http://litellm.internal:4000
EMBED_API_KEY=sk-...
EMBED_MODEL=text-embedding-3-large
EMBED_DIMENSIONS=3072
```

Either way, **`EMBED_DIMENSIONS` must equal the model's output size** (3072 =
`text-embedding-3-large`, 1536 = `text-embedding-3-small`). Changing the model
means building a new index (blue/green), never re-embedding into a mixed one.
The Azure AI Search side is unchanged — LiteLLM only replaces the embedding call.

## Index schema

One document per ticket, keyed by `ticket_id`. Fields:

| Group | Fields |
|---|---|
| identity | `id` (key), `ticket_id`, `display_id` |
| text | `subject`, `symptom_text`, `resolution_text` |
| resolution | `resolution_source`, `has_resolution` |
| classification | `category`, `sub_category`, `item_category`, `department`, `store`, `group`, `ticket_type`, `tags` |
| entities | `error_codes`, `hostnames`, `apps` |
| time | `created_at`, `closed_at`, `updated_at`, `age_days` |
| provenance | `linked_itglue_doc_ids`, `linked_itglue_urls` |
| vectors | `symptom_vector`, `resolution_vector` (dims = `EMBED_DIMENSIONS`) |

**Dimensions must match the embedding model.** `text-embedding-3-large` = 3072,
`text-embedding-3-small` = 1536. Changing the model means building a new index
(blue/green), never re-embedding into a mixed one.

## Automation

`scripts/run_pipeline.py` is the scheduler entrypoint (config from env):

```bash
python scripts/run_pipeline.py --mode incremental   # every 6h
python scripts/run_pipeline.py --mode reconcile     # nightly
python scripts/run_pipeline.py --mode both
```

Run it as an Azure Function (timer), a Container Apps job, or an OpenClaw
automation. `azure/function_timer/` is a ready timer wrapper.

Recommended cadence:

- **incremental every 6h** — `updated_since=<watermark>` upserts changed tickets.
- **nightly reconcile** — soft-delete ids whose ticket is no longer resolved/closed,
  so stale fixes stop surfacing.

## Consumption

Point the agent's retrieval tool at `SearchClient.hybrid_search`:

1. Build an OData **filter** first (`build_filter`): category/store/app narrows
   candidates before similarity.
2. Send the query text **and** its embedded vector; Azure fuses BM25 + k-NN with
   RRF and reranks with the `default-semantic` config.
3. Apply recency/frequency weighting in the caller (`created_at`/`age_days` are
   returned for this) — a fix used 40 times last quarter should outrank a
   one-off from 2023.
4. Return top-3 with ticket links, resolution text and IT Glue URLs. Keep a human
   in the loop; never auto-reply from retrieved content.

## Embeddings / secret handling

- PII and secrets are redacted in `sanitize.py` before embedding. Records whose
  "resolution" is nothing but a recovery key/password are dropped.
- Do **not** index attachments or requester identity. The ticket-id → person
  mapping stays in FreshService.

## Tests

```bash
pytest        # offline, no credentials or network required
```

## Build order

1. `fskb init-index`
2. `fskb extract` — review sanitized JSONL on a sample of ~100 tickets.
3. Tune qualification/redaction rules against that sample.
4. `fskb index` on a small batch; verify `fskb query` returns sensible pairs.
5. Build a gold set (~50 held-out tickets), measure "correct fix in top 3".
6. Wire the scheduler; add failure + zero-write alerting.
7. Ship the retrieval tool and log accept/reject feedback.

## Pitfalls

1. Embedding the whole ticket instead of symptom-as-vector + resolution alongside.
2. Ingesting unresolved/auto-closed noise.
3. Secrets or PII in the index.
4. Duplicates dominating top-K — cluster or weight by frequency.
5. Stale resolutions with no recency decay.
6. Re-embedding with a new model into the same index.
7. Watermark gaps (missed edits) or full re-pulls (cost).
8. No gold set — tuning by vibes.
