# GroupDeal — Architecture & Decisions Log

## 1. Concept Summary

GroupDeal is a group-buying platform. A seller lists a product with a **single discounted
group price**, a **deal-level stock cap**, and a **minimum participant count** required to
unlock the discount. Buyers join the deal's shared pool; the deal succeeds — held payments
are captured and orders confirmed — once either the stock cap fills or the timer expires
with the minimum met. If the timer expires without the minimum met, the deal fails and every
hold is released. Buyers can invite friends via referral links to help fill the pool faster.
Each buyer receives their own individual unit — nothing is split or shared. The platform
also supports **normal, non-group purchases** at base price, handled by the same Order/
Payment services through a separate, simpler checkout path (Section 5, decision #11).

This version simplifies one thing from the original concept, per the team's latest
decisions (Section 5):
- **Single-tier pricing only.** A seller who wants multiple discount tiers creates multiple
  separate deals (each with its own price/stock), rather than one deal with a tier ladder.

The original **time-boxed, all-or-nothing** mechanic is unchanged: a deal succeeds if enough
participants (≥ `min_participants`) have joined by the time it resolves — whether that
resolution is triggered by the stock cap filling or the timer expiring — and fails
otherwise, with all holds released. See Section 6 for the state machine and Section 7 for
how the race between the two resolution triggers is handled.

---

## 2. Deployment Scope

- Local development via **Docker Compose**.
- **Service Registry / Discovery** (e.g. Eureka) and **Kubernetes manifests** are included
  in the architecture diagram as forward-looking pieces, but are **not committed work** —
  built only if time remains after the MVP is functionally complete.
- No Config Server planned (each service manages its own local config).
- **Database:** PostgreSQL for every service that owns data.

---

## 3. Services Overview

| # | Service | Responsibility | DB |
|---|---|---|---|
| 1 | **API Gateway** | Single entry point, routing, JWT validation | — |
| 2 | **Auth Service** | Registration, login, JWT issuance, roles | Postgres |
| 3 | **Catalog Service** | Product listings, categories, search | Postgres |
| 4 | **Deal Service** ⭐ | Deal lifecycle, pricing, capacity, timer bookkeeping (merged former Scheduler Service), single source of truth for deal state | Postgres |
| 5 | **Participation Service** | Join/leave, referral links, high-write participant tracking | Postgres |
| 6 | **Inventory Service** | Product-level stock reservation for deals | Postgres |
| 7 | **Order Service** | Owns all orders (deal + normal); the only service whose payment needs Payment Service acts on — relationship is purely event-driven (Kafka), no synchronous calls either direction | Postgres |
| 8 | **Payment Service** | Stripe (test mode) integration: authorize / capture / void | Postgres |
| 9 | **Notification Service** | Kafka consumer → WebSocket/email push | Postgres (or stateless) |

---

## 4. Service Details

### 4.1 API Gateway
Reverse-proxies all client traffic, validates JWTs, routes to internal services.
No business logic, no own database.

### 4.2 Auth Service
| Method | Endpoint | Description |
|---|---|---|
| POST | `/auth/register` | Create account |
| POST | `/auth/login` | Authenticate, issue JWT |
| POST | `/auth/refresh` | Refresh token |
| GET | `/auth/me` | Current user profile |
| PATCH | `/users/{id}/role` | Admin: change role |

**DB:** `users(id, email, password_hash, role, created_at)`

### 4.3 Catalog Service
| Method | Endpoint | Description |
|---|---|---|
| POST | `/products` | Seller creates a product |
| GET | `/products` | Browse/search |
| GET | `/products/{id}` | Product detail |
| PATCH | `/products/{id}` | Update product |
| GET | `/sellers/{id}/products` | Seller's products |

**DB:** `products(id, seller_id, name, description, category, base_price, created_at)`

### 4.4 Deal Service ⭐ (core service)
Owns the deal's rules, its capacity counter, its state machine, and its timing metadata.
This is the **single arbiter** for deal state — no other service is allowed to flip a
deal's status directly.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/deals` | Seller creates a deal (product, discount price, stock cap, **min participants**, duration) |
| GET | `/deals` | Browse deals |
| GET | `/deals/{id}` | Deal detail + current status + live count |
| POST | `/deals/{id}/cancel` | Seller cancels — only allowed pre-first-join |
| POST | `/deals/{id}/reserve-slot` | **Internal, sync.** Called by Participation Service on join. Atomically reserves one slot if capacity allows: `reserved_stock++`. |
| POST | `/deals/{id}/check-leave-eligible` | **Internal, sync.** Called by Participation Service on leave. Read-only gate — `status IN (active, pending) AND now() < end_time - 10min` — does not touch capacity. |
| POST | `/deals/{id}/authorize-slot` | **Internal, sync.** Called by Order Service when a payment is authorized: `authorized_count++` — deal may flip to `succeeded` here if this fills `min_participants`/`stock`. |
| POST | `/deals/{id}/release-slot` | **Internal, sync.** Called by Order Service when an order is cancelled without ever reaching `authorized` (payment declined, timed out, or force-cancelled by the sweep). Undoes `reserve-slot`: `reserved_stock--`. |
| POST | `/deals/{id}/release-authorized-slot` | **Internal, sync.** Called by Order Service when an order had already reached `authorized` before being cancelled (participant left after a hold was placed). Undoes both `reserve-slot` and `authorize-slot`, atomically: `reserved_stock--` and `authorized_count--`. |

**DB:** `deals(id, product_id, seller_id, discount_price, stock, reserved_stock, min_participants,
  status, start_time, duration_minutes, end_time, created_at)`

**Resolution rule (unifies both end triggers into one condition):** a deal succeeds if
`reserved_stock >= min_participants` at the moment it resolves — whichever trigger caused
the resolution:
- **Stock fills up before the timer expires** → always `succeeded`. This is guaranteed by
  the creation-time constraint `min_participants <= stock`: if `reserved_stock` reaches
  `stock`, it has necessarily already reached (or passed) `min_participants`.
- **Timer expires before stock fills** → `succeeded` if `authorized_count >= min_participants`
  at that instant, otherwise `failed`.

So both triggers ultimately check the same thing; the stock-fill case just happens to always
evaluate true by construction.

**Internal timer (former Scheduler responsibility):** when a deal transitions to `active`
(first join), Deal Service schedules an internal one-shot job at `end_time` (e.g. Spring's
`TaskScheduler`, or a periodic sweep as a simpler fallback) that attempts the atomic
`active → failed` transition. See Section 7 for why this can safely race against a join
filling the stock cap without any cross-service coordination.

**Communication:**
- Sync: exposes `reserve-slot` / `check-leave-eligible` to Participation Service; exposes
  `authorize-slot` / `release-slot` / `release-authorized-slot` to Order Service
- Async (publishes): `deal.created`, `deal.cancelled`, `deal.succeeded`, `deal.failed`
- Async (subscribes): none.

### 4.5 Participation Service
High-write service. Tracks who joined which deal and referral relationships. Does **not**
own capacity — it defers every capacity decision to Deal Service: `reserve-slot` before
recording a join, `check-leave-eligible` (time-window only, no capacity change) before
recording a leave. It never calls `release-slot` / `release-authorized-slot` itself —
knowing whether a participant's order ever reached an authorized payment hold is state
Order Service owns, and capacity can only be safely released once Order Service has
resolved (voided) that hold. Its own row, though, is flipped to `removed` **synchronously**
on a leave request — right after `check-leave-eligible` succeeds, before the async
void/release chain even starts, since the buyer initiated this themselves and shouldn't
wait on it. For every other termination path it did **not** initiate (payment declined at
join, payment/authorization timeout), it depends on `order.deal_order_cancelled` to learn
the outcome and flips the row only then.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/deals/{id}/join` | Buyer joins |
| DELETE | `/deals/{id}/leave` | Buyer leaves |
| GET | `/deals/{id}/participants` | List/count participants |
| GET | `/deals/{id}/progress` | Live progress (count / cap, time remaining) |
| POST | `/deals/{id}/invite-link` | Generate referral link |
| GET | `/invites/{code}` | Resolve invite code |

**DB:** `participations(id, deal_id, user_id, referred_by, joined_at, status)` — `status`: `active` / `left`

**Communication:**
- Sync: calls Deal Service's `reserve-slot` on join, `check-leave-eligible` on leave (row
  flipped to `removed` synchronously in this same request).
- Async (publishes): `participant.joined`, `participant.left`
- Async (subscribes): `order.deal_order_cancelled` — only for the paths it didn't initiate
  itself (payment declined, payment/authorization timeout); flips participation row to
  `removed` there. The leave path already flipped synchronously and does not act on this
  event.

### 4.6 Inventory Service
Product-level stock, separate from a deal's own `stock`/`reserved_stock` cap. A deal's cap
is validated against — and reserves from — this at deal-creation time.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/inventory/{productId}/reserve-deal` | Reserve units for a new deal (called by Deal Service on creation) |
| POST | `/inventory/{productId}/release-deal` | Release units (deal cancelled, or deal failed by timer expiry) |
| POST | `/inventory/{productId}/reserve-order` | Reserve units for a normal order |
| POST | `/inventory/{productId}/release-order` | Release units for a normal order on payment failure |
| GET | `/inventory/{productId}` | Current stock / reserved stock |

**DB:** `inventory(id, product_id, stock, reserved_stock)`

### 4.7 Order Service

| Method | Endpoint | Description |
|---|---|---|
| POST | `/orders` | Normal checkout: buyer's cart → order + line items. Order Service fires `order.payment_charge_required` and waits briefly, in-request, for its own consumer to observe the resulting `payment.charged`/`payment.failed`; if that resolves in time the response is `201`/`402`, otherwise `202` with the order left `pending_charge` (resolved later via the async path) |
| GET | `/orders/{id}` | Order status (with line items) |
| GET | `/users/{id}/orders` | User's order history |

**DB:** `orders(id, user_id,order_type, deal_id, participant_id, status, total_price, payment_id, payment_intent_id, created_at)
order_products(id, order_id, product_id, quantity, unit_price, created_at)`

**Communication:**
- Async (subscribes): `participant.joined`, `participant.left`, `payment.failed`, `payment.authorized`, `payment.captured`, `payment.charged`,`payment.voided`, `deal.succeeded`, `deal.failed`
- Async (publishes): `order.created`, `order.normal_order_cancelled`, `order.deal_order_cancelled`, `order.payment_charge_required` (NORMAL charge, carries `user_id` + `order_id` + `amount` + `payment_intent_id`, `idempotencyKey = order_id`), `order.payment_authorize_required` (DEAL hold, same payload shape), `order.payment_capture_required` (`{payment_id, idempotencyKey = payment_id}`), `order.payment_void_required` (`{payment_id, idempotencyKey = payment_id}`), `Order.payment_timeout` (`{order_id, idempotencyKey = order_id}` — sweep-fired when an order sits in `pending_charge`/`pending_authorization` past its staleness threshold with no payment outcome; tells Payment Service to force-resolve/cancel the stuck intent), `order.authorized`

### 4.8 Payment Service

Maps onto Stripe's PaymentIntents API with manual capture:
- `authorize` → create a PaymentIntent with `capture_method: manual` and confirm it (places the hold)
- `capture` → call Stripe's capture endpoint on that PaymentIntent (can capture less than or equal to the authorized amount)
- `charge` → create a PaymentIntent with `capture_method: automatic`
- `void` → cancel the PaymentIntent
- Declines are real Stripe responses (triggered in test mode via Stripe's documented test card numbers, e.g. a card number that always declines), not something the team fakes — `payment.failed` is published when Stripe itself returns a decline.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/payments/authorize` | Hold `amount` against an `order_id` |
| POST | `/payments/capture` | Capture a previously authorized payment |
| POST | `/payments/charge` | Directly charge amount for normal orders |
| POST | `/payments/void` | Void a previously authorized payment |
| GET | `/payments/{id}` | Payment detail |

**DB:** `payments(id, order_id, amount, status, payment_intent_id, idempotency_key, created_at)` — `status`: `authorized` / `captured` / `charged` / `voided`; `payment_intent_id` (Stripe "pi_...") is echoed back in every `payment.*` event payload

**Communication:**
- Async (subscribes): `order.payment_charge_required` (create + confirm a PaymentIntent, `capture_method: automatic` — NORMAL), `order.payment_authorize_required` (create + confirm a PaymentIntent, `capture_method: manual` — DEAL hold), `order.payment_capture_required` (capture the existing PaymentIntent by `payment_id`), `order.payment_void_required` (cancel the existing PaymentIntent by `payment_id`), `Order.payment_timeout` (force-resolve/cancel a PaymentIntent stuck past its staleness threshold with no outcome yet — sweep-fired by Order Service, see Section 4.7). Order Service's `order.normal_order_cancelled`/`order.deal_order_cancelled` are **not** subscribed here — voiding a cancelled order's payment goes exclusively through `order.payment_void_required`, not a separate safety net off the cancellation events.
- Async (publishes): `payment.authorized`, `payment.charged`, `payment.captured`, `payment.voided`, `payment.failed` (real Stripe decline, e.g. via Stripe's test decline card numbers — consumed by Order Service to trigger its compensation, see Section 4.7) — every `payment.*` payload includes `payment_intent_id`

### 4.9 Notification Service
| Method | Endpoint | Description |
|---|---|---|
| GET | `/notifications` | User's notification history |
| PATCH | `/notifications/preferences` | Channel preferences |

**Communication:**
- Async (subscribes): `participant.joined` (send join confirmation + push progress update — **not** a success message), `deal.succeeded` / `deal.failed` (send outcome notification), `order.created` / `order.cancelled` (order confirmation/cancellation)

---

## 5. NORMAL ORDER flow

```
 (start)
    │  POST /orders — catalog lookup ok, order + order_products row inserted
    ▼
 reserving ──────────────────────────────────────────► cancelled [insufficient_stock]
    │                                                       (inventory-reserve returns 409
    │                                                        on any line item)
    │
    ├──────────────────────────────────────────────────► cancelled [inventory_unreachable]
    │                                                       (Inventory Service unreachable
    │                                                        mid-reservation loop)
    │
    ├──────────────────────────────────────────────────► cancelled [reservation_incomplete]
    │                                                       (sweep: stuck in `reserving` > 2 min
    │                                                        — process crashed mid-loop)
    │
    │  all items reserved
    ▼
 pending_charge ────────────────────────────────────────► confirmed
    │                                                       (payment.charged consumed)
    │
    ├──────────────────────────────────────────────────► cancelled [payment_declined]
    │                                                       (payment.failed consumed)
    │
    └──────────────────────────────────────────────────► cancelled [payment_timeout]
                                                            (sweep: stuck in `pending_charge`
                                                             > 5 min)
```

## 6. DEAL ORDER flow

```
 (start)
    │  participant.joined consumed — order + order_products row inserted
    ▼
 pending_authorization ─────────────────────────────────► authorized
    │                                                       (payment.authorized consumed)
    │
    ├──────────────────────────────────────────────────► cancelled [payment_declined]
    │                                                       (payment.failed consumed)
    │
    └──────────────────────────────────────────────────► cancelled [payment_timeout]
                                                            (sweep: stuck in
                                                             `pending_authorization` > 60s)

 authorized ─────────────────────────────────────────────► pending_capture
    │            (deal.succeeded batch: SELECT ... WHERE status='authorized' FOR UPDATE
    │             SKIP LOCKED → status='pending_capture')
    │
    ├──────────────────────────────────────────────────► pending_void
    │            (deal.failed batch: same pattern → status='pending_void')
    │
    └──────────────────────────────────────────────────► pending_void
                 (participant.left consumed → guarded UPDATE WHERE status='authorized'
                  parks the order → order.payment_settlement_requested VOID fired)

 pending_capture ────────────────────────────────────────► confirmed
                 (payment.captured consumed)

 pending_void ───────────────────────────────────────────► cancelled [deal_failed | participant_left]
                 (payment.voided consumed; cancel_reason depends on which path
                  parked the order in pending_void)
```
