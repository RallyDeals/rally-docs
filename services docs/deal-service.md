# Deal Service — Design Spec
> GroupDeal platform · scoped strictly to Deal Service (the deal-state arbiter)

Deal Service is the **single source of truth for deal state**. No other service is allowed
to flip a deal's status. It owns: deal creation rules, two capacity counters
(`deal_stock` / `current_participants` / `authorized_count`), the state machine, and the
internal timer that fires resolution at `end_time`.

---

## 1. User Stories (Deal Service scope)

Some SRS user stories are split across services ("ownership boundaries"). Below, each story
is rewritten to cover **only** the slice Deal Service owns; the SRS ID it maps to is noted.

| ID | Story | Maps to | Notes |
|---|---|---|---|
| **DS-01** | As a seller, I want to create a deal on a product I own (deal price, deal stock cap, min participants, duration), so buyers can start joining it. | US-009 | Rejects if `min_participants > deal_stock`. Snapshots the product's current `base_price` from Catalog Service into `original_price` at creation. Deal starts as `pending`; reserves stock from Inventory Service synchronously before persisting. |
| **DS-02** | As a seller, I want my deal cancelled only if nobody has joined yet, so I can withdraw a listing safely. | US-010 | Only legal from `pending`. Rejected once `status != pending`. |
| **DS-03** | As a buyer, I want to see a deal's live status (participants vs. target, time remaining), so I can decide whether to join. | US-013 | Deal Service can answer this alone — `current_participants`/`deal_stock`/`end_time` all live on its own row. |
| **DS-04** | As a seller, I want to see the outcome of my past deals, so I can evaluate performance. | US-011 | Query by `sellerId` + `status IN (succeeded, failed)`. |
| **DS-05** | As an admin, I want to view all deals and their status, so I have platform-wide visibility. | US-034 | Same list endpoint, unfiltered by seller. |
| **DS-06** *(system-to-system)* | As Participation Service, I want to atomically reserve a slot on a deal when a buyer joins, so capacity is never oversold under concurrency. | US-012 (slot-reservation slice), NFR-005 | Sync call. First successful reservation flips `pending → active` and sets `start_time`/`end_time`. Increments `current_participants` only — **not** `authorized_count` (REVISED, see DS-11). |
| **DS-07** *(system-to-system)* | As Order Service, I want to atomically release a reserved slot when a buyer's payment is declined **before** authorization ever succeeded, so it becomes available again. | US-016, US-025 | Only legal while `status = active`. Decrements `current_participants` only. **REVISED**: previously bundled with the post-authorization leave case (DS-12) under one endpoint — split apart because they touch different counters (see §6.1 note on the apparent gap in Order Service's own `payment.failed` handling). |
| **DS-08** *(system)* | As the platform, I want a deal to resolve to `succeeded` the instant every stock slot has been claimed by an **authorized** payment, even mid-timer. | US-017 | **REVISED**: trigger is `authorized_count == deal_stock`, not `current_participants == deal_stock`. Same transaction as the `authorize-slot` call that fills the last authorized slot. |
| **DS-09** *(system)* | As the platform, I want a deal to auto-resolve to `succeeded` or `failed` when its timer expires, based on whether `authorized_count >= min_participants`. | US-018, US-019 | **REVISED**: was `current_participants >= min_participants`. Raw joins don't guarantee a completed payment; counting them toward the success threshold would let a deal "succeed" with buyers who never actually paid. Internal scheduled job; guarded update, only from `active`. |
| **DS-10** *(system)* | As the platform, I want deal resolution to publish an event so Order, Notification, and Inventory services can run their sagas, so outcomes propagate without polling. | US-020, US-021 | Deal Service's role ends at publishing `deal.succeeded` / `deal.failed`; it does not touch orders or payments. |
| **DS-11** *(system-to-system, new)* | As Order Service, I want to atomically mark a previously-reserved slot as payment-authorized, so Deal Service's success/failure decision reflects real ability to pay, not just intent to join. | *(new — driven by Order Service's actual `payment.authorized` handling, not in the original SRS)* | Sync call, fired when Order Service consumes `payment.authorized`. Increments `authorized_count`; may itself flip `active → succeeded` if this is the slot that fills `deal_stock`. |
| **DS-12** *(system-to-system, new)* | As Order Service, I want to atomically release an **already-authorized** slot when its participant leaves, so both counters stay consistent. | *(new — driven by Order Service's `participant.left` → void handling)* | Decrements **both** `current_participants` and `authorized_count` together — this is the case Order Service's doc explicitly describes doing both decrements in one call. Kept as a separate endpoint from DS-07 rather than one endpoint with a flag, so the two release paths can't be confused for each other by a caller. **Confirmed** (not just our own guess) by both the updated Order Service doc and the sequence-diagram flows: both independently name this exact endpoint `release-authorized-slot`. |
| **DS-13** *(system-to-system, new)* | As Participation Service, I want to check whether a buyer is still allowed to leave a deal before I commit to processing the leave, so I don't accept a leave request Deal Service would reject anyway. | *(new — from the "Deal Join 8" sequence flow, not in the original SRS/architecture doc)* | Sync, read-only permission check — distinct from DS-12, which does the actual counter release later, after the async void completes. Guard: deal must be `active`, **and** more than 10 minutes must remain before `end_time`. Notably asymmetric with joining: `reserve-slot` (DS-06) has no such time cutoff — buyers can join right up until `end_time`, but can only leave up to 10 minutes before it. |

**Explicitly out of scope for Deal Service:** participant identity/referrals (Participation
Service), pending-order creation and payment holds (Order/Payment Service), product-level
stock (Inventory Service), notifications (Notification Service).

---

## 2. State Machine

```
pending ──(first reserve-slot succeeds)──► active ──(authorized_count == deal_stock)──► succeeded
   │                                          │
   │                                          ├──(end_time reached, authorized_count >= min_participants)──► succeeded
   │                                          └──(end_time reached, authorized_count <  min_participants)──► failed
   └──(seller cancels, still pending)───────► cancelled
```

`succeeded`/`failed`/`cancelled` are terminal. Both `succeeded` and `failed` are only
reachable from `active`, guarded by the same atomic condition, so a deal can never land in
both (Postgres row-level locking arbitrates the stock-fill-vs-timer race — see §6).

**REVISED**: the success/failure trigger now reads `authorized_count`, not
`current_participants`. `current_participants` still exists and still gates `reserve-slot`
(no more than `deal_stock` people can even attempt to join) — it just no longer decides the
outcome. Think of `current_participants` as "intent to buy" and `authorized_count` as
"verified ability to pay"; only the second one should ever decide whether a deal succeeds.

---

## 3. Database Schema

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE deal_status AS ENUM ('pending', 'active', 'succeeded', 'failed', 'cancelled');
-- (Implementation note carried over from the scaffold: VARCHAR + CHECK is used instead of
-- this native type in the actual migration, for easier Hibernate mapping — see the
-- deal-service scaffold's V1 migration comment for the full reasoning.)

CREATE TABLE deals (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id            UUID NOT NULL,
    seller_id             UUID NOT NULL,

    original_price        NUMERIC(10,2) NOT NULL CHECK (original_price > 0), -- snapshot of product.base_price from Catalog Service at creation time; never updated afterward, even if the seller edits the product later
    deal_price            NUMERIC(10,2) NOT NULL CHECK (deal_price > 0),     -- the final absolute price a buyer pays on success — NOT a discount amount/delta
    deal_stock            INT NOT NULL CHECK (deal_stock > 0),               -- deal-level capacity cap; distinct from Inventory Service's product-level stock
    current_participants  INT NOT NULL DEFAULT 0 CHECK (current_participants >= 0), -- live count of joined buyers (intent to buy) — one unit per participant (FR-051)
    authorized_count      INT NOT NULL DEFAULT 0 CHECK (authorized_count >= 0),     -- NEW: count of joins whose payment has actually been authorized (verified ability to pay) — THIS is what deal success/failure is based on, not current_participants
    min_participants      INT NOT NULL CHECK (min_participants > 0),

    status                deal_status NOT NULL DEFAULT 'pending',
    start_time            TIMESTAMPTZ,            -- set on first successful reserve-slot
    duration_minutes      INT NOT NULL CHECK (duration_minutes > 0),
    end_time              TIMESTAMPTZ,            -- computed: start_time + duration_minutes

    version               INT NOT NULL DEFAULT 0, -- optimistic-lock guard, belt-and-suspenders alongside the WHERE-guarded updates
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_deal_price_lt_original_price CHECK (deal_price < original_price),
    CONSTRAINT chk_min_participants_le_deal_stock CHECK (min_participants <= deal_stock),
    CONSTRAINT chk_current_participants_le_deal_stock CHECK (current_participants <= deal_stock),
    CONSTRAINT chk_authorized_le_current_participants CHECK (authorized_count <= current_participants) -- can never have more verified payers than joiners
);

CREATE INDEX idx_deals_status            ON deals (status);
CREATE INDEX idx_deals_seller_id         ON deals (seller_id, status);
CREATE INDEX idx_deals_product_id        ON deals (product_id);
-- Powers the internal timer sweep (§6):
CREATE INDEX idx_deals_active_end_time   ON deals (end_time) WHERE status = 'active';
```

```sql
-- Transactional outbox: written in the SAME transaction as any deals row change,
-- so state transitions and their events can never diverge (Kafka publish is a separate,
-- at-least-once relay process reading this table).
CREATE TABLE deal_outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id         UUID NOT NULL REFERENCES deals(id),
    event_type      TEXT NOT NULL,     -- 'deal.created' | 'deal.cancelled' | 'deal.succeeded' | 'deal.failed'
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ         -- NULL until relayed to Kafka
);
CREATE INDEX idx_deal_outbox_unpublished ON deal_outbox (id) WHERE published_at IS NULL;
```

**Idempotency for sync calls:** `reserve-slot` / `release-slot` / `authorize-slot` /
`release-authorized-slot` are called synchronously by other services and may be retried on
timeout. Track a small dedup table keyed by the caller's request ID so a retried call is a
no-op rather than a double-decrement:

```sql
CREATE TABLE deal_slot_requests (
    request_id      UUID PRIMARY KEY,   -- idempotency key, generated by the caller (Participation/Order Service)
    deal_id         UUID NOT NULL REFERENCES deals(id),
    operation       TEXT NOT NULL,      -- 'RESERVE' | 'RELEASE' | 'AUTHORIZE' | 'RELEASE_AUTHORIZED'
    result          TEXT NOT NULL,      -- 'SUCCESS' | 'REJECTED'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 4. Endpoints

### 4.1 External (client-facing, via API Gateway, JWT-authenticated)

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| POST | `/deals` | Seller | Create a deal on a product the caller owns |
| GET | `/deals` | Any | Browse/filter deals — query params: `status`, `sellerId`, `productId`, `page`, `size` |
| GET | `/deals/{id}` | Any | Deal detail, including live progress (`currentParticipants`/`dealStock`, `timeRemainingSeconds`) |
| POST | `/deals/{id}/cancel` | Seller (owner) | Cancel — only legal while `pending` |

`GET /deals?sellerId={id}&status=succeeded,failed` covers DS-04 (seller outcome view).
`GET /deals?status=...` with no `sellerId` (admin role) covers DS-05. No separate endpoints
needed for either — one filterable list endpoint serves both.

### 4.2 Internal (service-to-service, sync, not gateway-routed)

| Method | Endpoint | Caller | Description |
|---|---|---|---|
| POST | `/internal/deals/{id}/reserve-slot` | Participation Service | Atomically reserve one slot on join; increments `current_participants` only; may flip `pending→active` |
| GET | `/internal/deals/{id}/check-leave-eligible` | Participation Service | **NEW.** Read-only permission check before a leave is processed — deal must be `active` and more than 10 minutes must remain before `end_time`. Does not modify any counter. |
| POST | `/internal/deals/{id}/release-slot` | Order Service | Release a slot whose payment was declined **before** authorization ever succeeded; decrements `current_participants` only |
| POST | `/internal/deals/{id}/authorize-slot` | Order Service | **NEW.** Marks a reserved slot as payment-authorized; increments `authorized_count`; may flip `active→succeeded` if this fills `deal_stock` |
| POST | `/internal/deals/{id}/release-authorized-slot` | Order Service | **NEW.** Releases an **already-authorized** slot whose participant subsequently left; decrements both `current_participants` and `authorized_count` |

---

## 5. Contracts (DTOs)

### 5.1 `POST /deals`

Request:
```json
{
  "productId": "8a2c1f0e-...",
  "dealPrice": 149.99,
  "dealStock": 100,
  "minParticipants": 40,
  "durationMinutes": 1440
}
```
`sellerId` is taken from the JWT (`X-User-Id` header set by the Gateway), never from the body.
`originalPrice` is **not** part of the request — Deal Service fetches it itself from Catalog
Service (`base_price` on the product) as part of the same ownership-check call already
required for creation, and snapshots it server-side.

Response `201 Created`:
```json
{
  "id": "9e1c4b7a-...",
  "productId": "8a2c1f0e-...",
  "sellerId": "331f2a90-...",
  "originalPrice": 199.99,
  "dealPrice": 149.99,
  "dealStock": 100,
  "currentParticipants": 0,
  "authorizedCount": 0,
  "minParticipants": 40,
  "status": "pending",
  "startTime": null,
  "durationMinutes": 1440,
  "endTime": null,
  "createdAt": "2026-07-12T10:00:00Z"
}
```

Errors: `400` `MIN_PARTICIPANTS_EXCEEDS_STOCK`, `400` `DEAL_PRICE_NOT_BELOW_ORIGINAL_PRICE`,
`403` `NOT_PRODUCT_OWNER`, `404` `PRODUCT_NOT_FOUND` (Catalog Service has no such product,
so `originalPrice` can't be snapshotted), `409` `INVENTORY_INSUFFICIENT` (Inventory Service
couldn't reserve the requested `dealStock`).

### 5.2 `GET /deals` / `GET /deals/{id}`

`DealResponse` (list and detail share the shape; detail adds `timeRemainingSeconds`):
```json
{
  "id": "9e1c4b7a-...",
  "productId": "8a2c1f0e-...",
  "sellerId": "331f2a90-...",
  "originalPrice": 199.99,
  "dealPrice": 149.99,
  "dealStock": 100,
  "currentParticipants": 68,
  "authorizedCount": 63,
  "minParticipants": 40,
  "status": "active",
  "startTime": "2026-07-12T10:15:00Z",
  "durationMinutes": 1440,
  "endTime": "2026-07-13T10:15:00Z",
  "timeRemainingSeconds": 41230,
  "createdAt": "2026-07-12T10:00:00Z"
}
```
`currentParticipants` and `authorizedCount` diverging (68 vs. 63) is normal and expected —
the gap is people who joined but whose payment authorization is still in flight or was
declined. `GET /deals` wraps this in a standard page envelope:
`{ content: DealResponse[], page, size, totalElements }`.

### 5.3 `POST /deals/{id}/cancel`

No body. Response `200 OK` → `DealResponse` with `status: "cancelled"`.
Errors: `409 DEAL_ALREADY_STARTED` (one or more participants already joined).

### 5.4 `POST /internal/deals/{id}/reserve-slot`

Request:
```json
{
  "requestId": "b3b6c9b0-6f2b-4e9a-9c0a-7a1e0e9c2f11"
}
```
`requestId` is the idempotency key (Participation Service should reuse it on retry).

Response `200 OK` (success):
```json
{
  "success": true,
  "dealId": "9e1c4b7a-...",
  "dealPrice": 149.99,
  "currentParticipants": 64,
  "authorizedCount": 59,
  "dealStock": 100,
  "status": "active",
  "startTime": "2026-07-12T10:15:00Z",
  "endTime": "2026-07-13T10:15:00Z"
}
```
`dealPrice` is returned here specifically so Participation Service can forward it in
`participant.joined` — Order Service needs it to build the order but never calls Deal
Service directly for this (see §6). `authorizedCount` is included for visibility only —
reserve-slot never modifies it.

Response `200 OK` (rejected — capacity or state, **not an HTTP error**, since "deal full" is
an expected business outcome, not a fault):
```json
{
  "success": false,
  "dealId": "9e1c4b7a-...",
  "reason": "DEAL_FULL"
}
```
`reason` ∈ `DEAL_FULL` | `DEAL_NOT_JOINABLE` (already resolved/cancelled). Capacity is
checked against `current_participants < dealStock` here — **not** `authorized_count` —
since joining is "intent," not payment.

### 5.5 `POST /internal/deals/{id}/release-slot`

For a slot whose payment was declined **before** it was ever authorized. Decrements
`current_participants` only — `authorized_count` was never incremented for this slot, so
there's nothing to release there.

> **Resolved** (previously flagged as a possible gap): Order Service's updated doc now
> explicitly documents this call — on `payment.failed` for a DEAL order still in
> `pending_authorization`, it calls `release-slot` and decrements `reserved_stock` only,
> leaving `authorized_count` untouched since the order never reached `authorized`. Matches
> this contract exactly; no change needed here.

Request/response:
```json
{ "requestId": "..." }
```
```json
{ "success": true, "dealId": "9e1c4b7a-...", "currentParticipants": 63, "authorizedCount": 59, "status": "active" }
```

### 5.6 `POST /internal/deals/{id}/authorize-slot` *(new)*

Called by Order Service when it consumes `payment.authorized`. Request/response mirror
reserve-slot:
```json
{ "requestId": "..." }
```
Response `200 OK` (success):
```json
{
  "success": true,
  "dealId": "9e1c4b7a-...",
  "currentParticipants": 64,
  "authorizedCount": 60,
  "dealStock": 100,
  "status": "active"
}
```
If this call is the one that brings `authorizedCount` up to `dealStock`, `status` in the
response reflects `succeeded` — the same transaction that increments the counter also
resolves the deal, mirroring how reserve-slot already handles the `pending→active`
transition atomically.

Rejected response (**ٍshould be rare** — a slot reaching authorization implies it was already
reserved, but the deal could have been resolved in between by the sweep):
```json
{ "success": false, "dealId": "9e1c4b7a-...", "reason": "DEAL_NOT_JOINABLE" }
```

### 5.7 `POST /internal/deals/{id}/release-authorized-slot`

For a slot that **was** authorized, then the participant left (`participant.left` →
Order Service voids the payment). Decrements both counters together. **Confirmed** by both
the updated Order Service doc and the Deal Join 8 sequence flow — both independently name
this exact endpoint and describe it as an atomic pair of decrements, resolving what was
previously an open question about a flag vs. a separate endpoint.
```json
{ "requestId": "..." }
```
```json
{ "success": true, "dealId": "9e1c4b7a-...", "currentParticipants": 63, "authorizedCount": 58, "status": "active" }
```

### 5.8 `GET /internal/deals/{id}/check-leave-eligible` *(new)*

Called by Participation Service **before** it commits to processing a leave request — a
read-only permission check, distinct from `release-authorized-slot` (§5.7), which happens
later, once the async void actually completes. Modifies nothing.

No request body (path param only).

Response `200 OK`:
```json
{
  "eligible": true,
  "dealId": "9e1c4b7a-..."
}
```
or, when not eligible:
```json
{
  "eligible": false,
  "dealId": "9e1c4b7a-...",
  "reason": "TOO_CLOSE_TO_END_TIME"
}
```
`reason` ∈ `TOO_CLOSE_TO_END_TIME` (fewer than 10 minutes remain before `end_time`) |
`DEAL_NOT_ACTIVE` (deal already resolved/cancelled — nothing to leave). Note the asymmetry
with `reserve-slot`: joining has no time cutoff and remains legal right up to `end_time`;
only leaving is cut off 10 minutes early, so a late-arriving swap can't itself be undone
last-minute in a way that would destabilize a deal that's about to resolve.

---

## 6. Communication With Other Services

Deal Service **subscribes to no events** — it is a pure publisher plus a synchronous callee.
This is intentional: it must never take orders from another service about its own state.
**Confirmed by Order Service's own doc**: it explicitly notes `authorized_count++` happens
via the sync `authorize-slot` RPC, "not by consuming this event" — validating this rule
rather than contradicting it.

### 6.1 Synchronous (Deal Service as callee)

| Caller | Endpoint | When |
|---|---|---|
| Participation Service | `reserve-slot` | Buyer requests to join (§5.4) |
| Participation Service | `check-leave-eligible` | Buyer requests to leave, checked before Participation Service commits to processing it (§5.8) |
| Order Service | `release-slot` | Payment declined **before** authorization (§5.5) — confirmed present in Order Service's updated doc |
| Order Service | `authorize-slot` | `payment.authorized` consumed (§5.6) |
| Order Service | `release-authorized-slot` | `payment.voided` consumed on the leave path, i.e. `participant.left` → void completes on an already-authorized order (§5.7) |


### 6.2 Synchronous (Deal Service as caller)

| Callee | When |
|---|---|
| Catalog Service `GET /products/{productId}` | On deal creation — validates seller ownership (existing requirement) and snapshots `base_price` into `original_price` (new) |
| Inventory Service `POST /inventory/{productId}/reserve` | On deal creation, to reserve product-level stock before persisting the deal |

### 6.3 Asynchronous — events published

All events are emitted via the outbox (§3) onto a Kafka topic, **keyed by `deal_id`** so
Kafka guarantees ordering per deal (critical: a `succeeded` must never be reordered behind a
stale `active` update for the same deal).

**REVISED — payload casing**: Order Service's and Payment Service's own Kafka payloads are
consistently snake_case (`deal_id`, `order_id`, `authorized_count`), while their synchronous
REST bodies are camelCase (`orderId`, `totalPrice`). The events below now follow that same
split — snake_case here, camelCase everywhere in §5's sync contracts — to match sibling
services exactly rather than introduce a third convention.

**`deal.created`**
```json
{
  "event_id": "e1a2...",
  "event_type": "deal.created",
  "occurred_at": "2026-07-12T10:00:00Z",
  "deal_id": "9e1c4b7a-...",
  "product_id": "8a2c1f0e-...",
  "seller_id": "331f2a90-...",
  "original_price": 199.99,
  "deal_price": 149.99,
  "deal_stock": 100,
  "min_participants": 40,
  "duration_minutes": 1440
}
```
Consumers: none currently wired (reserved for future analytics).

**`deal.cancelled`**
```json
{
  "event_id": "e1a3...",
  "event_type": "deal.cancelled",
  "occurred_at": "2026-07-12T11:00:00Z",
  "deal_id": "9e1c4b7a-...",
  "product_id": "8a2c1f0e-...",
  "deal_stock": 100
}
```
Consumers: **Inventory Service** — release the `deal_stock` units reserved at creation back to general product inventory.

**`deal.succeeded`**
```json
{
  "event_id": "e1a4...",
  "event_type": "deal.succeeded",
  "occurred_at": "2026-07-13T10:15:00Z",
  "deal_id": "9e1c4b7a-...",
  "product_id": "8a2c1f0e-...",
  "seller_id": "331f2a90-...",
  "original_price": 199.99,
  "deal_price": 149.99,
  "current_participants": 100,
  "authorized_count": 100,
  "deal_stock": 100,
  "min_participants": 40,
  "start_time": "2026-07-12T10:15:00Z",
  "end_time": "2026-07-13T10:15:00Z",
  "resolved_trigger": "STOCK_FILLED"
}
```
`resolved_trigger` ∈ `STOCK_FILLED` | `TIMER_EXPIRED`. **REVISED**: Order Service's own doc
shows it only reads `deal_id`, `deal_stock`, `authorized_count` from this event (its batch
`UPDATE ... WHERE deal_id = ? AND status = 'authorized'` doesn't need anything else) — the
remaining fields here are a superset kept for Notification Service's benefit (savings copy)
and are safe for Order Service to ignore. `original_price`/`deal_price` let downstream
consumers compute savings without a second lookup.
Consumers:
- **Order Service** — batch-transition every order `WHERE deal_id = ? AND status = 'authorized'` to `pending_capture`, firing a capture request per order (its own doc's `FOR UPDATE SKIP LOCKED` pattern, not Deal Service's concern).
- **Notification Service** — send the success outcome email to every participant.
- **Inventory Service** — finalize the reserved units as sold (convert deal-reserved stock into a permanent decrement).

**`deal.failed`**
```json
{
  "event_id": "e1a5...",
  "event_type": "deal.failed",
  "occurred_at": "2026-07-13T10:15:00Z",
  "deal_id": "9e1c4b7a-...",
  "product_id": "8a2c1f0e-...",
  "seller_id": "331f2a90-...",
  "current_participants": 31,
  "authorized_count": 27,
  "deal_stock": 100,
  "min_participants": 40,
  "start_time": "2026-07-12T10:15:00Z",
  "end_time": "2026-07-13T10:15:00Z",
  "resolved_trigger": "TIMER_EXPIRED_BELOW_MIN"
}
```
Consumers:
- **Order Service** — batch-transition every order `WHERE deal_id = ? AND status = 'authorized'` to `pending_void`, firing a void request per order.
- **Notification Service** — send the failure outcome email to every participant.
- **Inventory Service** — release the `deal_stock` units back to general product inventory.

### 6.4 Cross-service data contract summary

| Consumer | What it needs from Deal Service | How it gets it |
|---|---|---|
| Catalog Service | Nothing — Deal Service only reads from it | N/A (not a consumer of Deal Service data) |
| Participation Service | Whether a slot was reserved, plus `dealPrice` (to forward downstream); whether a leave is currently permitted | Sync response from `reserve-slot` (§5.4); sync response from `check-leave-eligible` (§5.8) |
| Order Service | `dealPrice` at join time; `authorized_count`/`deal_stock` at resolution time; slot-authorize/release acknowledgment | `dealPrice` arrives indirectly via Participation Service's `participant.joined` event; `authorize-slot`/`release-slot`/`release-authorized-slot` are synchronous calls Order Service makes into Deal Service (§5.6/§5.5/§5.7); `deal.succeeded`/`deal.failed` (§6.3) drive Order Service's batch settlement |
| Inventory Service | `product_id` + unit count to release or finalize | `deal.cancelled` (release), `deal.succeeded` (finalize as sold), `deal.failed` (release) |
| Notification Service | `deal_id`, `resolved_trigger`, `original_price`/`deal_price` (for savings copy) | `deal.succeeded` / `deal.failed` |

---

## 7. Internal Timer

Given the team's 5–6 week scope (per the architecture doc's open question #1), a periodic
sweep is the pragmatic default:

```sql
-- Runs every 5–10s via @Scheduled
UPDATE deals
SET status = CASE WHEN authorized_count >= min_participants THEN 'succeeded' ELSE 'failed' END,
    updated_at = now()
WHERE end_time <= now() AND status = 'active';
```

**REVISED**: was `current_participants >= min_participants` — see DS-09 and §2 for the
reasoning. This same guarded-update pattern (`WHERE status = 'active'`) is what lets the
sweep safely race against a concurrent `authorize-slot` transaction filling the last unit —
whichever commits first wins; the other affects 0 rows.
