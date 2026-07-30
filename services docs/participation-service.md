# Participation Service — Design Documentation

Rally / GroupDeal — high-write service owning join/leave state and referral links.
Does **not** own capacity — every capacity decision is deferred to Deal Service.

---

## 1. Responsibility Summary

- Records who has joined which deal (`participations`).
- Records referral links and resolves invite codes (`referral_links`).
- Gatekeeps joins/leaves through **synchronous** calls into Deal Service (`reserve-slot`,
  `check-leave-eligible`) — it never mutates a deal's capacity itself.
- Publishes `participant.joined` / `participant.left` for downstream consumers
  (Order Service, Notification Service).
- Never calls `release-slot` / `release-authorized-slot` on Deal Service — that's Order
  Service's job, since only Order Service knows whether a payment hold was ever placed.

---

## 2. User Stories

**Joining**
- As a buyer, I want to join a deal's pool so I can get the group discount once enough
  people join.
- As a buyer, I want to join via a friend's referral link so my join is credited to them.
- As a buyer, I want to be rejected clearly if the deal is full, expired, or already
  succeeded/failed, rather than silently failing.
- As a buyer, I want to be prevented from joining a deal I'm already an active
  participant in (no duplicate active participations).
- As a buyer who previously left a deal, I want to be able to rejoin it later.

**Leaving**
- As a buyer, I want to leave a deal I joined, as long as it's still safely before
  resolution, so I'm not stuck if I change my mind.
- As a buyer, I want to be blocked from leaving once the deal is too close to resolving
  (inside the cutoff window), since capacity can't be safely given back at that point.

**Visibility**
- As a buyer, I want to see how many people have joined a deal and how much time is
  left, so I can decide whether to join or invite others.
- As a seller, I want to see the participant list/count for my deal.

**Referrals**
- As a buyer, I want to generate a shareable invite link for a deal so friends who use
  it are recorded as referred by me.
- As anyone with a link, I want the invite code resolved to the right deal so I land on
  the correct join flow.

---

## 3. REST Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/deals/{dealId}/join` | Buyer joins a deal (optionally via referral code) |
| DELETE | `/deals/{dealId}/leave` | Buyer leaves a deal they're active in |
| GET | `/deals/{dealId}/participants` | List/count participants for a deal |
| GET | `/deals/{dealId}/progress` | Live progress: count vs. cap, time remaining |
| POST | `/deals/{dealId}/invite-link` | Generate a referral link for the caller |
| GET | `/invites/{code}` | Resolve an invite code → deal + referrer |

### 3.1 `POST /deals/{dealId}/join`

Request body:
```json
{
  "referralCode": "aB3xQ9"
}
```
`referralCode` is optional.

Flow:
1. If `referralCode` present, resolve it against `referral_links` → `referrerUserId`.
   Reject with `410 Gone` if expired, `404` if unknown.
2. Reject with `409 Conflict` if caller already has an `ACTIVE` participation for this deal.
3. Call Deal Service **`POST /deals/{dealId}/reserve-slot`** (sync). Propagate its
   failure semantics:
   - `409 Conflict` — deal full / not in a joinable state
   - `404 Not Found` — deal doesn't exist
4. Insert a `participations` row (`status = ACTIVE`).
5. Publish `participant.joined`.
6. Return `201 Created` with `ParticipationResponse`.

Responses:
- `201` → `ParticipationResponse`
- `404` → deal or referral code not found
- `409` → already an active participant, or deal not joinable
- `410` → referral code expired

### 3.2 `DELETE /deals/{dealId}/leave`

Flow:
1. Look up caller's `ACTIVE` participation for this deal. `404` if none.
2. Call Deal Service **`POST /deals/{dealId}/check-leave-eligible`** (sync, read-only).
   `409 Conflict` if ineligible (deal not `active`/`pending`, or inside the cutoff window).
3. **Synchronously**, guarded update:
   `UPDATE participations SET status='LEFT', left_at=now() WHERE id=? AND status='ACTIVE'`
4. Publish `participant.left` (reason: `SELF_INITIATED`).
5. Return `204 No Content`.

This is the *only* path that publishes `participant.left` — see §5.2.

### 3.3 `GET /deals/{dealId}/participants`

Query params: `status` (optional filter, default `ACTIVE`), `page`, `size`.
Returns a page of `ParticipantSummary` plus a total count.

### 3.4 `GET /deals/{dealId}/progress`

Returns `DealProgressResponse` — active participant count, (optionally cached) cap
from Deal Service, and time remaining. Designed to be cheap/hot-path; backed by
`idx_participations_deal_status`.

### 3.5 `POST /deals/{dealId}/invite-link`

Generates a `referral_links` row for the caller (must already be an `ACTIVE`
participant, otherwise `403`). Returns `ReferralLinkResponse`.

### 3.6 `GET /invites/{code}`

Resolves a code to `{ dealId, referrerUserId, expired }`. Used by the frontend to
route to the join screen with the referral pre-filled.

---

## 4. DTOs (Java records)

```java
public record JoinRequest(
    String referralCode // nullable
) {}

public record ParticipationResponse(
    UUID id,
    UUID dealId,
    UUID userId,
    UUID referredBy,      // nullable
    String status,        // ACTIVE | LEFT
    Instant joinedAt,
    Instant leftAt         // nullable
) {}

public record ParticipantSummary(
    UUID userId,
    UUID referredBy,
    Instant joinedAt
) {}

public record ParticipantsPageResponse(
    List<ParticipantSummary> participants,
    long activeCount,
    int page,
    int size
) {}

public record DealProgressResponse(
    UUID dealId,
    long activeParticipants,
    Integer minParticipants,   // from Deal Service, may be cached
    Integer stockCap,          // from Deal Service, may be cached
    Instant endTime,
    Duration timeRemaining
) {}

public record ReferralLinkResponse(
    String code,
    UUID dealId,
    UUID referrerUserId,
    Instant expiresAt // nullable
) {}

public record InviteResolutionResponse(
    UUID dealId,
    UUID referrerUserId,
    boolean expired
) {}
```

---

## 5. Kafka Events

### 5.1 `participant.joined` (published)

Fired once per successful join, after the local transaction commits.

```json
{
  "eventId": "uuid",
  "participationId": "uuid",
  "dealId": "uuid",
  "userId": "uuid",
  "referredBy": "uuid | null",
  "joinedAt": "2026-07-30T10:15:00Z"
}
```
Consumers: Order Service (creates the deal order + `pending_authorization`),
Notification Service (join confirmation / progress push — not a success message).

### 5.2 `participant.left` (published)

Fired **only** from the synchronous, buyer-initiated leave path (§3.2). It is *not*
fired when a row is flipped as a side effect of `order.deal_order_cancelled` (§5.3) —
in that case Order Service already caused and knows about the cancellation, so
re-notifying it would be redundant.

```json
{
  "eventId": "uuid",
  "participationId": "uuid",
  "dealId": "uuid",
  "userId": "uuid",
  "leftAt": "2026-07-30T10:20:00Z",
  "reason": "SELF_INITIATED"
}
```
Consumers: Order Service (parks the matching order into `pending_void` if still
`authorized`), Notification Service (optional).

### 5.3 `order.deal_order_cancelled` (subscribed)

Consumed only for termination paths Participation Service did **not** initiate itself
— payment declined at join, or payment/authorization timeout. On receipt, guarded
update:
```sql
UPDATE participations
SET status = 'LEFT', left_at = now()
WHERE deal_id = :dealId AND user_id = :userId AND status = 'ACTIVE';
```
No event is published in response — this consumer is a passive state sync, not a new
domain action. The update is idempotent (re-consuming the same event is a no-op once
status is already `LEFT`), which also covers Kafka at-least-once redelivery.

Expected payload (from Order Service, shape TBD/confirm against Order Service docs):
```json
{
  "eventId": "uuid",
  "orderId": "uuid",
  "dealId": "uuid",
  "userId": "uuid",
  "cancelReason": "payment_declined | payment_timeout",
  "cancelledAt": "2026-07-30T10:22:00Z"
}
```

---

## 6. Synchronous Contracts (outbound calls to Deal Service)

| Call | Direction | When | Failure handling |
|---|---|---|---|
| `POST /deals/{id}/reserve-slot` | → Deal Service | Before inserting a join row | `409`/`404` from Deal Service is propagated as-is; no local row is written on failure |
| `POST /deals/{id}/check-leave-eligible` | → Deal Service | Before flipping a row to `LEFT` | `409` from Deal Service is propagated as-is; row is left `ACTIVE` |

Both are read/gate calls from Participation Service's perspective — Participation
Service never calls `release-slot`, `release-authorized-slot`, or `authorize-slot`;
those are Order Service's exclusive responsibility.

**Recommendation:** wrap both calls with a short timeout + circuit breaker
(e.g. Resilience4j). Treat a Deal Service timeout on `reserve-slot` as a failed join
(`503`) rather than optimistically inserting — never assume success on ambiguity here,
since that would let a participation exist without a reserved slot.

---

## 7. Database Schema

```sql
CREATE TABLE participations (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id      UUID NOT NULL,              -- FK by reference only (different service/DB)
    user_id      UUID NOT NULL,              -- FK by reference only
    referred_by  UUID NULL,                  -- user_id of referrer, nullable
    status       VARCHAR(10) NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE | LEFT
    joined_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    left_at      TIMESTAMPTZ NULL,

    CONSTRAINT chk_participations_status CHECK (status IN ('ACTIVE', 'LEFT'))
);

-- Enforce FR-021: a user can only hold ONE active participation per deal at a time.
-- A partial unique index (rather than a plain composite unique) allows the same user
-- to rejoin after leaving (FR-021a) — each new join is a fresh ACTIVE row.
CREATE UNIQUE INDEX uq_participations_active_per_user_deal
    ON participations (deal_id, user_id)
    WHERE status = 'ACTIVE';

-- Hot-path lookups
CREATE INDEX idx_participations_deal_id ON participations (deal_id);
CREATE INDEX idx_participations_deal_status ON participations (deal_id, status);
CREATE INDEX idx_participations_user_id ON participations (user_id);

-- Referral link persistence
CREATE TABLE referral_links (
    code               VARCHAR(16) PRIMARY KEY,        -- short opaque base62 code
    deal_id            UUID NOT NULL,
    referrer_user_id   UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at         TIMESTAMPTZ NULL
);

CREATE INDEX idx_referral_links_deal_id ON referral_links (deal_id);

-- Recommended addition: transactional outbox for Kafka publication (see §8.1)
CREATE TABLE participation_outbox (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_id   UUID NOT NULL,          -- participations.id
    event_type     VARCHAR(50) NOT NULL,   -- participant.joined | participant.left
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ NULL
);

CREATE INDEX idx_participation_outbox_unpublished
    ON participation_outbox (created_at)
    WHERE published_at IS NULL;
```

---

## 8. Data Consistency Notes

### 8.1 Reserve-then-insert gap
`reserve-slot` (Deal Service) and the local `participations` insert are two separate
transactions across two databases. If the service crashes after `reserve-slot`
succeeds but before the local commit, Deal Service holds a reserved slot that no
participation, order, or event will ever reference — a **stuck slot** with no owning
record.

Mitigations to consider:
- Deal Service compensating sweep: release any `reserved_stock` slot that's never
  followed by a corresponding `participant.joined`-driven order within a short window.
- Or: use the transactional outbox above so the local insert + outbox row commit
  atomically, and a poller publishes to Kafka — this at least guarantees the event
  fires once the DB row exists, though it doesn't fully close the cross-service gap
  above (that needs a Deal-Service-side timeout/reconciliation, since Participation
  Service can't unilaterally undo `reserve-slot`).

### 8.2 Leave flip vs. async chain
The leave endpoint flips the row to `LEFT` synchronously, before the
publish/void/release chain runs. If the process crashes after the DB commit but
before `participant.left` is published, the participation is already `LEFT` locally,
but Order Service never learns to void the hold and Deal Service's capacity is never
released via `release-authorized-slot`. The outbox pattern (§7) closes this: publish
becomes a durable, retryable step decoupled from the request/response cycle, so a
crash after commit still results in eventual publication.

### 8.3 Idempotent consumption
`order.deal_order_cancelled` should be consumed with the guarded `WHERE status =
'ACTIVE'` update shown in §5.3 so redelivery (Kafka at-least-once) is a safe no-op.
