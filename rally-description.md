# GroupDeal — Architecture & Decisions Log
> Internship graduation project — Spring Boot + Angular microservices platform
> Team size: 6 | Duration: 3–4 weeks
> Status: living document, updated after team meeting

---

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
| 7 | **Order Service** | Owns all orders (deal + normal); only service that calls Payment Service | Postgres |
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
| POST | `/deals/{id}/reserve-slot` | **Internal, sync.** Called by Participation Service on join. Atomically reserves one slot if capacity allows. |
| POST | `/deals/{id}/release-slot` | **Internal, sync.** Called by Participation Service on leave, and by Order Service if a payment authorization fails. |

**DB:**
```
deals(
  id, product_id, seller_id,
  discount_price,
  stock, reserved_stock,      -- deal-level cap, separate from Inventory Service's stock
  min_participants,           -- minimum headcount required for the deal to succeed; enforced at creation: min_participants <= stock
  status,                     -- pending | active | succeeded | failed | cancelled
  start_time,                 -- set on first join
  duration_minutes,
  end_time,                   -- computed: start_time + duration_minutes, once started
  created_at
)
```

**Resolution rule (unifies both end triggers into one condition):** a deal succeeds if
`reserved_stock >= min_participants` at the moment it resolves — whichever trigger caused
the resolution:
- **Stock fills up before the timer expires** → always `succeeded`. This is guaranteed by
  the creation-time constraint `min_participants <= stock`: if `reserved_stock` reaches
  `stock`, it has necessarily already reached (or passed) `min_participants`.
- **Timer expires before stock fills** → `succeeded` if `reserved_stock >= min_participants`
  at that instant, otherwise `failed`.

So both triggers ultimately check the same thing; the stock-fill case just happens to always
evaluate true by construction.

**Internal timer (former Scheduler responsibility):** when a deal transitions to `active`
(first join), Deal Service schedules an internal one-shot job at `end_time` (e.g. Spring's
`TaskScheduler`, or a periodic sweep as a simpler fallback) that attempts the atomic
`active → failed` transition. See Section 7 for why this can safely race against a join
filling the stock cap without any cross-service coordination.

**Communication:**
- Sync: exposes `reserve-slot` / `release-slot` to Participation Service (atomic guards, see Section 7).
- Async (publishes): `deal.created`, `deal.cancelled`, `deal.succeeded`, `deal.failed`
- Async (subscribes): none.

### 4.5 Participation Service
High-write service. Tracks who joined which deal and referral relationships. Does **not**
own capacity — always defers to Deal Service's `reserve-slot` / `release-slot` before
recording anything.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/deals/{id}/join` | Buyer joins — calls Deal Service's `reserve-slot` sync first; only records the participation and fires the event if the reservation succeeds |
| DELETE | `/deals/{id}/leave` | Buyer leaves — checks with Deal Service that >10 min remain and the deal is still `active`, then calls `release-slot` sync (frees the reserved slot immediately) before firing the event |
| GET | `/deals/{id}/participants` | List/count participants |
| GET | `/deals/{id}/progress` | Live progress (count / cap, time remaining) |
| POST | `/deals/{id}/invite-link` | Generate referral link |
| GET | `/invites/{code}` | Resolve invite code |

**DB:** `participations(id, deal_id, user_id, referred_by, joined_at, status)` — `status`: `active` / `left`

**Communication:**
- Sync: calls Deal Service's `reserve-slot`/`release-slot` before writing a participation row.
- Async (publishes): `participant.joined`, `participant.left`

### 4.6 Inventory Service
Product-level stock, separate from a deal's own `stock`/`reserved_stock` cap. A deal's cap
is validated against — and reserves from — this at deal-creation time.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/inventory/{productId}/reserve` | Reserve units for a new deal (called by Deal Service on creation) |
| POST | `/inventory/{productId}/release` | Release units (deal cancelled, or deal failed by timer expiry) |
| GET | `/inventory/{productId}` | Current stock / reserved stock |

**DB:** `inventory(id, product_id, stock, reserved_stock)`

### 4.7 Order Service
Owns **every** order in the system — both deal-sourced and normal purchases — and is the
only service that talks to Payment Service. This is what keeps Payment Service fully
generic (Section 4.8): Order Service is the sole thing that understands "this order came
from a deal."

| Method | Endpoint | Description |
|---|---|---|
| POST | `/orders` | Normal checkout: buyer's cart → order + line items, authorize + capture immediately |
| GET | `/orders/{id}` | Order status (with line items) |
| GET | `/users/{id}/orders` | User's order history |

**DB:**
```
orders(
  id, user_id,
  order_type,          -- NORMAL | DEAL
  deal_id,              -- nullable, only for DEAL
  participant_id,        -- nullable, only for DEAL
  status,               -- pending_payment | confirmed | cancelled
  total_price,
  created_at
)

order_products(
  id, order_id, product_id,
  quantity,             -- always 1 for DEAL orders; can be >1 for NORMAL cart items
  unit_price,           -- base_price (NORMAL) or the deal's discount_price (DEAL)
  created_at
)
```

**Deal flow (three sub-scenarios, all keyed by `deal_id`/`participant_id`):**
1. **Join** — on `participant.joined`: create `order` (`type=DEAL`, `status=pending_payment`, `deal_id`, `participant_id`, `total_price = deal.discount_price`) + one `order_products` row (`quantity=1`, `unit_price = deal.discount_price`). Then call Payment Service to authorize a hold for `total_price` against this `order_id`.
2. **Leave** — on `participant.left`: find the order by `(deal_id, participant_id)`, set `status=cancelled`, emit `order.cancelled` → Payment Service voids the matching payment.
3. **Payment authorization declined** — on `payment.failed`: set that order's `status=cancelled`, and call Deal Service's `release-slot` synchronously so the freed slot can be taken by someone else. Without this step a declined card would silently strand a reserved slot forever (see Section 7).
4. **Deal resolves:**
   - `deal.succeeded` → select all `pending_payment` orders for that `deal_id`, set `status=confirmed`, emit `order.created` per order → Payment Service captures each matching payment.
   - `deal.failed` → select all `pending_payment` orders for that `deal_id`, set `status=cancelled`, emit `order.cancelled` per order → Payment Service voids each matching payment.

**Normal purchase flow:** buyer checks out a cart → Order Service creates the `order`
(`type=NORMAL`, no `deal_id`/`participant_id`) and its `order_products` rows, computes
`total_price`, then synchronously authorizes and immediately captures via Payment Service
(no hold period — there's no group mechanic to wait on), and marks the order `confirmed`.

**Communication:**
- Async (subscribes): `participant.joined` (create pending order + authorize), `participant.left` (cancel order + void), `payment.failed` (cancel order + release deal slot), `deal.succeeded` (confirm + capture all), `deal.failed` (cancel + void all)
- Async (publishes): `order.created`, `order.cancelled`
- Sync: calls Payment Service (authorize/capture/void), calls Deal Service's `release-slot` (on payment failure)

### 4.8 Payment Service
A fully generic, **deal-blind** ledger, backed by **Stripe in test mode** — real Stripe API
calls (test keys, test card numbers), not an in-house mock. It never knows whether an
`order_id` came from a deal or a normal purchase — it only ever authorizes, captures, or
voids a given amount against a given order, on request from Order Service.

Maps onto Stripe's PaymentIntents API with manual capture:
- `authorize` → create a PaymentIntent with `capture_method: manual` and confirm it (places the hold)
- `capture` → call Stripe's capture endpoint on that PaymentIntent (can capture less than or equal to the authorized amount)
- `void` → cancel the PaymentIntent
- Declines are real Stripe responses (triggered in test mode via Stripe's documented test card numbers, e.g. a card number that always declines), not something the team fakes — `payment.failed` is published when Stripe itself returns a decline.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/payments/authorize` | Hold `amount` against an `order_id` |
| POST | `/payments/capture` | Capture a previously authorized payment |
| POST | `/payments/void` | Void a previously authorized payment |
| GET | `/payments/{id}` | Payment detail |

**DB:** `payments(id, order_id, amount, status, idempotency_key, created_at)` — `status`: `authorized` / `captured` / `voided`

**Communication:**
- Sync: called directly by Order Service for every authorize/capture/void — no event
  listening on deal or participant events at all. This is the actual fix for the original
  coupling problem: Payment Service has zero knowledge of `deal_id`/`participant_id`/tiers;
  all of that context lives only on Order Service's `orders` row.
- Async (publishes): `payment.authorized`, `payment.captured`, `payment.voided`, `payment.failed` (real Stripe decline, e.g. via Stripe's test decline card numbers — consumed by Order Service to trigger its compensation, see Section 4.7)

### 4.9 Notification Service
| Method | Endpoint | Description |
|---|---|---|
| GET | `/notifications` | User's notification history |
| PATCH | `/notifications/preferences` | Channel preferences |

**Communication:**
- Async (subscribes): `participant.joined` (send join confirmation + push progress update — **not** a success message), `deal.succeeded` / `deal.failed` (send outcome notification), `order.created` / `order.cancelled` (order confirmation/cancellation)

---

## 6. Deal Lifecycle (State Machine)

```
pending  --(first join succeeds)-->  active  --(reserved_stock == stock)-->  succeeded
   |                                    |
   |                                    --(end_time reached, reserved_stock >= min_participants)--> succeeded
   |                                    --(end_time reached, reserved_stock <  min_participants)--> failed
   --(seller cancels, no joins yet)--> cancelled
```

- **`pending`**: deal created, visible, joinable, but not yet started. No `start_time` set.
- **`active`**: set on first successful join. `start_time = now()`, `end_time = start_time + duration_minutes` computed and stored.
- **`succeeded`**: reached either by filling `stock` (always implies `min_participants` was already met, since `min_participants <= stock` is enforced at creation) or by the timer expiring with `reserved_stock >= min_participants`. Triggers the success saga (confirm + capture all pending orders for the deal).
- **`failed`**: reached only by the timer expiring with `reserved_stock < min_participants`. Triggers the compensation saga (cancel + void all pending orders, release inventory).
- **`cancelled`**: only reachable from `pending`.

Both `succeeded` and `failed` are only reachable from `active`, and both are guarded by the
same atomic condition, so a deal can never land in both (see Section 7).

---

## 7. Concurrency Pattern — Atomic Guarded Updates

Every capacity-sensitive operation in this system uses the same pattern: a conditional
`UPDATE ... WHERE <still-valid-state>`, so the database itself resolves races instead of
a distributed lock.

| Operation | Owner | Guard |
|---|---|---|
| Reserve a join slot | Deal Service | `UPDATE deals SET reserved_stock = reserved_stock + 1 WHERE id = ? AND reserved_stock < stock AND status = 'active'` (or `'pending'` if this is the first join) |
| Release a slot (leave, or a declined payment) | Deal Service | `UPDATE deals SET reserved_stock = reserved_stock - 1 WHERE id = ? AND status = 'active'` — called synchronously by Participation Service on leave, and by Order Service if `payment.failed` fires |
| Flip to `succeeded` | Deal Service | Same transaction as the reserve-slot update above — if it results in `reserved_stock = stock`, flip `status` to `succeeded` (always true here, since `min_participants <= stock` is enforced at creation) and emit `deal.succeeded`. |
| Flip to `succeeded` or `failed` on timer expiry | Deal Service | Fired by Deal Service's own internal timer job at `end_time`: `UPDATE deals SET status = CASE WHEN reserved_stock >= min_participants THEN 'succeeded' ELSE 'failed' END WHERE id = ? AND status = 'active'` |
| Reserve deal stock from product inventory | Inventory Service | `UPDATE inventory SET reserved_stock = reserved_stock + ? WHERE product_id = ? AND (stock - reserved_stock) >= ?` |

---

## 8. Event Catalog

| Event | Publisher | Consumers | Purpose |
|---|---|---|---|
| `deal.created` | Deal Service | (future: analytics) | Deal now live/pending |
| `deal.cancelled` | Deal Service | Inventory Service (release reserved stock) | Seller cancelled pre-join |
| `deal.succeeded` | Deal Service | Order Service, Notification Service, Inventory Service | Timer expired with `min_participants` met, or stock cap filled — start success saga |
| `deal.failed` | Deal Service | Order Service, Notification Service, Inventory Service (release reserved stock) | Timer expired with `min_participants` unmet — start compensation saga |
| `participant.joined` | Participation Service | Order Service (create pending order + authorize), Notification Service (join confirmation + progress) | Safe, reversible reactions only |
| `participant.left` | Participation Service | Order Service (cancel order + void) | Individual withdrawal before deal resolves — slot already released synchronously by Participation Service itself |
| `payment.failed` | Payment Service | Order Service (cancel that order + release the deal slot via Deal Service) | Real Stripe authorization decline (test mode) |
| `order.created` | Order Service | Notification Service | New order (normal purchase, or a deal order confirmed on success) |
| `order.cancelled` | Order Service | Notification Service | Order cancelled (leave, payment decline, or deal failure) |
| `payment.authorized` / `.captured` / `.voided` | Payment Service | Notification Service (optional) | Payment state changes |