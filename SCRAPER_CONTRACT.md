# OVE Scraper Contract — Lease-Based Work Queue

This document is the source of truth for how scraper workers interact with
Virtual CarHub VPS when fetching OVE detail pages. It replaces the old
"poll `GET /detail/pending`" pattern, which is no longer safe with multiple
workers or multiple scraper machines.

**Audience:** the coding agent / engineer building and maintaining the scraper
fleet. Read this end-to-end before writing worker code or enabling
multi-profile parallelism.

---

## 1. Why the contract changed

Before: the scraper polled `GET /inventory/ove/detail/pending`, which
returned the same `request_id`s repeatedly until a successful detail POST
happened. Failed scrapes stayed visible and got re-served forever. Stale
requests retried for hours. With multiple workers, two workers could grab
the same `request_id` because there was no atomic claim.

After: pending detail work is now a **real lease-based queue keyed by
`request_id`**. One worker owns one request at a time. Ownership is
enforced server-side.

---

## 2. State machine

```
                +----------+                      +----------+
   enqueue ---> | PENDING  | ---- claim -------->  | CLAIMED  |
                +----------+                      +----+-----+
                     ^                                 |
                     |                                 | complete
                     |                                 v
                     |                            +----------+
                     |                            |COMPLETED |
                     |                            +----------+
                     |                                 ^
                     |                                 | (auto by detail POST)
                     |                                 |
                     |                                 |
                     |   +-------+                     |
                     +-- | FAILED| <-- fail -----------+
                         +---+---+
                             |
                             | next_retry_at <= now, re-claim
                             v
                         +----------+                      +----------+
                         | CLAIMED  | ---- terminal ---->  | TERMINAL |
                         +----------+                      +----------+
                                                             (never requeued)
```

- `PENDING` → eligible for first claim
- `CLAIMED` → leased to one worker, hidden from others until `lease_expires_at`
- `FAILED` → transient failure, eligible again when `next_retry_at <= now`
- `COMPLETED` → success, never requeued
- `TERMINAL` → permanently resolved, never requeued (use for "vehicle missing on VPS", "listing removed", etc.)

Expired leases (worker died mid-scrape) become eligible for re-claim
automatically. `attempts` is incremented on every successful claim so you
can see how many tries a request has taken.

---

## 3. API contract

All endpoints require the service-token auth header.

Base path: `/api/v1/inventory/ove`

### 3.1 Claim

`POST /detail/claim`

```json
{
  "worker_id": "scraper-node1-dealerA",
  "limit": 1,
  "lease_seconds": 300
}
```

Response:
```json
{
  "status": "ok",
  "data": {
    "worker_id": "scraper-node1-dealerA",
    "lease_seconds": 300,
    "count": 1,
    "items": [
      {
        "request_id": "uuid",
        "vin": "1HGCM82633A004352",
        "source_platform": "manheim",
        "priority": 100,
        "attempts": 1,
        "requested_at": "2026-04-07T10:00:00+00:00",
        "claimed_at": "2026-04-07T10:05:00+00:00",
        "lease_expires_at": "2026-04-07T10:10:00+00:00",
        "request_source": "scraper",
        "requested_by": "ove-sync",
        "reason": "need condition report",
        "metadata": {"job_id": "detail-job-1"}
      }
    ]
  }
}
```

Atomic. Eligible rows are selected with `SELECT ... FOR UPDATE SKIP LOCKED`
on Postgres, so two workers will never receive the same `request_id`.

### 3.2 Complete

`POST /detail/{request_id}/complete`

```json
{
  "worker_id": "scraper-node1-dealerA",
  "result": "success"
}
```

- 200 OK → request moved to `COMPLETED`, lease cleared
- 404 → request_id not found
- 409 → request is not in `CLAIMED` state, or is claimed by a different worker

### 3.3 Fail (retriable)

`POST /detail/{request_id}/fail`

```json
{
  "worker_id": "scraper-node1-dealerA",
  "error_category": "browser_error",
  "error_message": "Unable to open detail page",
  "retry_after_seconds": 600
}
```

Sets `status=FAILED`, `last_error`, `last_error_category`, and
`next_retry_at = now + retry_after_seconds`. The row will NOT be returned
by `/claim` until the backoff elapses.

### 3.4 Terminal (stop retrying forever)

`POST /detail/{request_id}/terminal`

```json
{
  "worker_id": "scraper-node1-dealerA",
  "reason": "vehicle_missing_on_vps",
  "message": "Detail POST returned 404 from VPS"
}
```

Sets `status=TERMINAL`. Never requeued. Use this for permanent conditions
(see §5 for the vocabulary).

### 3.5 Heartbeat (extend lease)

`POST /detail/{request_id}/heartbeat`

```json
{
  "worker_id": "scraper-node1-dealerA",
  "lease_seconds": 300
}
```

Extends `lease_expires_at` for long-running scrapes. Returns the new
`lease_expires_at`. If this returns 409, **abandon the job immediately** —
someone else now owns it.

### 3.6 Detail push (unchanged, but see §6)

`POST /detail/{vin}` — posts the scraped detail payload for a VIN. Does
**not** require the request_id; the VPS auto-completes any `CLAIMED` or
`PENDING` request for that VIN as a safety net. You should still call
`/complete` explicitly after a successful detail push so worker-side
accounting is clean.

### 3.7 Deprecated: pending poll

`GET /detail/pending` is now marked `deprecated=True` and is read-only.
Do not call it from worker code. It is for human diagnostics only.

---

## 4. Worker-side rules

### 4.1 Identity

- **Stable `worker_id` per profile**, not per process. Format:
  `scraper-<host>-<profile_slug>` (e.g. `scraper-node1-dealerA`).
  If the process restarts mid-lease it MUST use the same `worker_id` so it
  can heartbeat or finish its own in-flight claims.
- **One browser profile = one `worker_id`.** Never share a profile across
  two concurrent OS processes — OVE session cookies and CSRF tokens will
  collide and cause random mid-scrape logouts.

### 4.2 Claim sizing

- **`limit` should match your concurrency, not your appetite.** If a
  profile realistically works 1 detail page at a time, use `limit=1`.
  Claiming 10 and processing serially wastes 9 leases while a faster worker
  elsewhere could have taken them.
- **`lease_seconds` = p95 single-scrape time + buffer**, not worst case.
  Suggested starting values: `lease_seconds=300`, heartbeat every 120s.
- **Back off the claim poll** when `count: 0`. Suggested:
  5s → 15s → 30s → 60s cap. Do not hot-loop.

### 4.3 Heartbeats

- Any scrape crossing ~60% of its lease window MUST call `/heartbeat`
  with a fresh `lease_seconds`. Condition report pages in particular can
  render slowly.
- If `/heartbeat` returns 409, the lease has been reassigned — abandon
  the job immediately. Do NOT post `/complete`. Log and move on.

### 4.4 Worker state machine

Every claimed `request_id` flows through exactly this sequence:

```
CLAIMED -> SCRAPING -> POSTING_DETAIL -> COMPLETE
                           |
                           +--> FAIL (by category)
                           +--> TERMINAL (by reason)
```

Rules:
- Never let a job exit the worker without calling exactly one of
  `/complete`, `/fail`, or `/terminal`.
- If the process is killed mid-job, let the lease expire naturally. The
  VPS will reclaim it and `attempts` will increment correctly.
- Do NOT try to "resume" a claim across process restarts unless you call
  `/heartbeat` first and verify it returns 200.

### 4.5 Detail POST ordering

1. `POST /inventory/ove/detail/{vin}` with the scrape result
2. `POST /inventory/ove/detail/{request_id}/complete`

Step 2 is idempotent with the VPS auto-complete safety net, but do it
anyway so `attempts`, `completed_at`, and `result` are clean. If step 1
returns 404 "Vehicle not found", call `/terminal` with
`reason=vehicle_missing_on_vps` instead of `/complete`.

---

## 5. Error categorization

The `error_category` field on `/fail` drives retry decisions and metrics.
Use this exact vocabulary — do not invent new values without updating
this document first.

### 5.1 Retriable categories (use `/fail`)

| Category | When | Suggested `retry_after_seconds` |
|---|---|---|
| `browser_error` | Playwright crash, navigation timeout, selector race | 300 |
| `auth_expired` | Logged out mid-session | 60 (re-login profile first) |
| `rate_limited` | 429, CAPTCHA, "too many requests" | 1800 + jitter |
| `page_structure_changed` | Selector missing, schema mismatch | 3600 (page a human) |
| `transient_network` | DNS, connection reset, 5xx | 120 |
| `temporarily_unavailable` | Page loaded but CR not yet ready | 600 |

### 5.2 Terminal reasons (use `/terminal`)

| Reason | When |
|---|---|
| `vehicle_missing_on_vps` | Detail POST returned 404 from VPS |
| `vehicle_removed_from_auction` | OVE says the listing is gone |
| `permanently_unavailable` | Hard "listing deleted" page from OVE |
| `unsupported_listing_type` | Listing format the scraper cannot parse and never will |

Terminal = stop retrying forever. This is what prevents the hours-long
retry loops that prompted the queue redesign.

---

## 6. Rate limiting & fairness

**The lease queue protects you from duplicate work. It does NOT protect
OVE from being hammered.** With N parallel profiles you MUST add:

- **Global per-source-platform rate limit** on the scraper side, shared
  across profiles (Redis token bucket, file lock, or similar). Limit each
  platform separately: Manheim, ADESA, ACV, etc.
- **Jittered per-profile delays** (2–5s random) between requests on the
  same profile.
- **Per-profile circuit breaker**: 3 consecutive `rate_limited` or
  `auth_expired` → pause that single profile 10–15 min. Do NOT let the
  paused profile call `/claim` during the pause.

## 7. Profile isolation

- Each profile gets its own: cookies dir, user-data-dir, IP egress (if
  using proxies), and human-readable label in logs.
- **Do not share a proxy across profiles** if you want them to look like
  different dealers. OVE fingerprints IP + UA + cookie as a triad.
- `auth_expired` on profile X → re-login profile X only. Do not restart
  the fleet.

---

## 8. Observability

Emit one structured log line per state transition with at minimum:

```
worker_id, request_id, vin, profile, state, duration_ms, error_category
```

VPS-side audit events you can correlate against:

- `inventory_ove_detail_claim` — claim issued
- `inventory_ove_detail_complete` — completion recorded
- `inventory_ove_detail_fail` — retriable failure
- `inventory_ove_detail_terminal` — permanent resolution

These are written to the audit log on every state transition. Correlate
by `request_id`.

---

## 9. Pre-flight checklist before enabling multi-profile parallelism

Run through this list on a staging environment before turning on parallel
profiles in production:

- [ ] Kill a worker mid-scrape. Confirm the request returns to the queue
      after `lease_expires_at` and `attempts` is incremented.
- [ ] Two profiles running simultaneously against an empty queue do not
      both grab the same VIN. Test with `limit=1` each and seed 1 request.
- [ ] A 404 on detail POST results in `TERMINAL`, not `FAILED` with
      infinite retries.
- [ ] `/heartbeat` actually extends the lease — verify
      `lease_expires_at` in the response.
- [ ] A `/fail` with `retry_after_seconds=600` does NOT come back on a
      claim poll for 10 minutes.
- [ ] Enqueuing the same VIN twice while a claim is active returns
      `deduplicated: true`.
- [ ] Rate limiter is shared across profiles on the same machine (not
      per-process).
- [ ] Each profile has its own user-data-dir and cookie jar.
- [ ] `auth_expired` on one profile does not cascade to others.
- [ ] Worker logs include `worker_id`, `request_id`, and `vin` on every
      state transition.

---

## 10. Things the scraper no longer needs to do

These are handled server-side. Remove any local logic for them:

- Deduping pending requests by VIN. VPS dedupes against `PENDING`,
  `CLAIMED`, `IN_PROGRESS`, and `FAILED`-in-backoff rows automatically.
- Tracking which VINs are "already in progress" locally. The claim
  endpoint is the source of truth.
- Retry backoff math. VPS stores `next_retry_at` and filters server-side.
- Cleaning up zombie requests from dead workers. Lease expiry handles it.

---

## 11. Quick reference: full request lifecycle

```
1. Poll:    POST /detail/claim {worker_id, limit, lease_seconds}
               -> items: [{request_id, vin, lease_expires_at, ...}]

2. Scrape the VIN (with heartbeats if > 60% of lease used)

   Heartbeat: POST /detail/{request_id}/heartbeat {worker_id, lease_seconds}

3a. Success:
    POST /detail/{vin} (detail payload)
    POST /detail/{request_id}/complete {worker_id, result: "success"}

3b. Retriable failure:
    POST /detail/{request_id}/fail
         {worker_id, error_category, error_message, retry_after_seconds}

3c. Permanent failure:
    POST /detail/{request_id}/terminal
         {worker_id, reason, message}

4. Loop back to step 1 with backoff if count was 0.
```
