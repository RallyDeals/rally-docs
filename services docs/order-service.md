# Order Service — Data Model, Events & Normal Purchase Flow

## 1. Order DB Schema

```sql
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =====================================================================
-- orders
-- =====================================================================

CREATE TABLE orders (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID NOT NULL,
    order_type        VARCHAR(10) NOT NULL CHECK (order_type IN ('NORMAL', 'DEAL')),

    deal_id           UUID NULL,               -- required if order_type = 'DEAL'
    participant_id    UUID NULL,               -- required if order_type = 'DEAL'

    status            VARCHAR(50) NOT NULL CHECK (status IN (
                          'RESERVING',
                          'PENDING_CHARGE',
                          'PENDING_AUTHORIZATION',
                          'AUTHORIZED',
                          'PENDING_CAPTURE',
                          'PENDING_VOID',
                          'CONFIRMED',
                          'CANCELLED'
                      )),

    total_price       NUMERIC(10,2) NOT NULL CHECK (total_price >= 0),
    payment_id        UUID NULL,               -- set once Payment Service returns a payment_id
    payment_intent_id VARCHAR(255) NULL,
    cancel_reason     VARCHAR(30) NULL CHECK (cancel_reason IN (
                          'INSUFFICIENT_STOCK', 'INVENTORY_UNREACHABLE', 'RESERVATION_INCOMPLETE',
                          'PAYMENT_DECLINED', 'PAYMENT_TIMEOUT', 'DEAL_FAILED', 'PARTICIPANT_LEFT'
                      )),

    version           INT NOT NULL DEFAULT 0,  -- optimistic locking / race auditing
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    status_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),   -- added V5; last time `status` changed

    CONSTRAINT deal_fields_consistency CHECK (
        (order_type = 'DEAL'   AND deal_id IS NOT NULL AND participant_id IS NOT NULL) OR
        (order_type = 'NORMAL' AND deal_id IS NULL AND participant_id IS NULL)
    )
);

-- Guards against a redelivered participant.joined creating two orders for the same slot.
CREATE UNIQUE INDEX uq_orders_deal_participant
    ON orders (deal_id, participant_id)
    WHERE order_type = 'DEAL';

CREATE INDEX idx_orders_user_id ON orders (user_id);
CREATE INDEX idx_orders_deal_id ON orders (deal_id);
CREATE INDEX idx_orders_status  ON orders (status);

-- Replaces idx_orders_status_updated_at (status, updated_at) from V1, dropped in V5.
-- Sweep jobs must key off status_updated_at, not updated_at, since updated_at
-- bumps on ANY column change (e.g. setting payment_id without a status transition).
CREATE INDEX idx_orders_status_status_updated_at ON orders (status, status_updated_at);

-- Keeps updated_at accurate automatically so sweep jobs never rely on
-- application code remembering to set it manually on every status change.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();


CREATE OR REPLACE FUNCTION set_status_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        NEW.status_updated_at = now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_status_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION set_status_updated_at();

-- =====================================================================
-- order_products
-- =====================================================================

CREATE TABLE order_products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL REFERENCES orders(id),
    product_id  UUID NOT NULL,
    quantity    INT NOT NULL CHECK (quantity > 0),   -- always 1 for DEAL, can be >1 for NORMAL
    unit_price  NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_products_order_id ON order_products (order_id);

-- =====================================================================
-- outbox_events
--
-- Transactional outbox for every event Order Service publishes. Makes the
-- publish part of the same commit as the business write; a separate relay
-- process polls WHERE status = 'PENDING' and delivers to Kafka at-least-once.
-- =====================================================================

CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id    UUID NOT NULL,             -- VARCHAR(100) in V3, restored to UUID in V7
    event_type      VARCHAR(100) NOT NULL,     -- widened from VARCHAR(50) in V3
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ NULL,
    aggregate_type  VARCHAR(50),               -- added V3
    topic           VARCHAR(100),              -- added V3
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- added V3
    attempts        INT NOT NULL DEFAULT 0,    -- added V3
    last_error      TEXT,                      -- added V3
    correlation_id  UUID NOT NULL              -- added V6
);


CREATE INDEX idx_outbox_events_pending ON outbox_events (created_at) WHERE status = 'PENDING';
CREATE INDEX idx_outbox_events_correlation_id ON outbox_events (correlation_id);

-- =====================================================================
-- processed_events
--
-- Inbound Kafka dedup: every consumer checks/inserts here in the same
-- transaction as its business write, so at-least-once redelivery is a no-op.
-- =====================================================================

CREATE TABLE processed_events (
    event_id      VARCHAR(100) NOT NULL,   -- was UUID, retyped in V3
    event_type    VARCHAR(100) NOT NULL,   -- widened from VARCHAR(50) in V3
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_topic  VARCHAR(100),            -- added V3
    PRIMARY KEY (event_id, source_topic)   -- old single-column PK (event_id) dropped in V3
);

```

---

## 2. Order Status & State Machine

Two independent flows share the `orders` table. `reserving`/`pending_charge` only exist on
`order_type = NORMAL`; `pending_authorization`/`authorized`/`pending_capture`/`pending_void`
only exist on `order_type = DEAL`. `confirmed` and `cancelled` are terminal for both, and
**every transition into a terminal or intermediate state is a conditional
`UPDATE ... WHERE status = <expected>`** — this guard is what makes a reconciliation-sweep
cancel and a late payment event mutually exclusive (whichever commits first wins; the other
is a no-op, never an error).

### 2.1 NORMAL flow

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

### 2.2 DEAL flow

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

**Sweep behavior differs by stage** (per the Deal Join 6/7 sweep notes): orders stuck in
`pending_authorization` > 60s are **force-cancelled** (silence = failure, slot released);
orders stuck in `pending_capture` or `pending_void` > 60s are instead **re-published** —
the same `order.payment_settlement_requested` (type=CAPTURE/VOID, idempotencyKey=payment_id) is fired
again, never a cancel, because capture/void against an existing `payment_id` is idempotent
and the deal outcome is already decided at that point.

Both void paths park the order in `pending_void` before firing the VOID (team decision):
the `deal.failed` batch does it via the `FOR UPDATE SKIP LOCKED` batch, and the
participant-leave path does it via a guarded `UPDATE ... WHERE status = 'authorized'`
immediately on consuming `participant.left`. This closes the race where a concurrent
`deal.succeeded`/`deal.failed` batch over the same `deal_id` could grab an order whose
leave-void was still in flight — the batch selects `WHERE status = 'authorized'` and
therefore skips anything already parked.

---

## 3. Events Appearing in the Sequence Diagrams (Payload Schemas)

| Event | Payload | Fired by |
|---|---|---|
| `participant.joined` | `{participant_id, deal_id, user_id, product_id, price, payment_intent_id}` | Participation Service |
| `participant.left` | `{participant_id, deal_id}` | Participation Service |
| `order.payment_charge_required` | `{user_id, order_id, amount, payment_intent_id, idempotencyKey = order_id}` | Order Service |
| `order.payment_authorize_required` | `{user_id, order_id, amount, payment_intent_id, idempotencyKey = order_id}` | Order Service |
| `order.payment_void_required` | `{payment_id, idempotencyKey = payment_id}` | Order Service |
| `order.payment_capture_required` | `{payment_id, idempotencyKey = payment_id}` | Order Service |
| `payment.authorized` | `{payment_id, payment_intent_id, order_id, amount}` | Payment Service |
| `payment.charged` | `{payment_id, payment_intent_id, order_id, amount}` | Payment Service |
| `payment.captured` | `{payment_id, payment_intent_id, order_id, amount}` | Payment Service |
| `payment.failed` | `{payment_id, payment_intent_id, order_id, amount, error}` (e.g. `card_declined`) | Payment Service |
| `payment.voided` | `{payment_id, payment_intent_id, order_id, amount}` | Payment Service |
| `order.authorized` | `{deal_id, participant_id}` | Order Service |
| `order.created` | `{order_id, user_id, items}` | Order Service |
| `order.normal_order_cancelled` | `{order_id, user_id, items: [{product_id, quantity}]}` | Order Service |
| `order.deal_order_cancelled` | `{order_id, deal_id, participant_id, user_id, items, reason}` — reason ∈ `card_declined` \| `payment_timeout` \| `deal_failed` \| `participant_left` | Order Service |
| `deal.succeeded` | `{deal_id, deal_stock, authorized_count}` | Deal Service |
| `deal.failed` | `{deal_id, deal_stock, authorized_count}` | Deal Service |

---

## 4. Events Fired by Order Service — Cause

| Event | Payload | Fired when... |
|---|---|---|
| `order.payment_charge_required` | `{user_id, order_id, amount, payment_intent_id, idempotencyKey = order_id}` | 1. A NORMAL order finishes inventory reservation → `reserving → pending_charge`|
| `order.payment_authorize_required` | `{user_id, order_id, amount, payment_intent_id, idempotencyKey = order_id}` | 1. A DEAL order is created from `participant.joined` → `pending_authorization` |
| `order.payment_capture_required` | `{payment_id, idempotencyKey = payment_id}` | 1. `deal.succeeded` batch converts `authorized → pending_capture` |
| `order.payment_void_required` | `{payment_id, idempotencyKey = payment_id}` | 1. `deal.failed` batch converts `authorized → pending_void` <br>2. `participant.left` is consumed → guarded UPDATE parks the order `authorized → pending_void` |
| `Order.created` | `(order_id, user_id, items)` | 1. Order Service consumes `payment.charged` (NORMAL, `pending_charge → confirmed`)<br>2. Order Service consumes `payment.captured` (DEAL, `pending_capture → confirmed`) |
| `Order.authorized` | `(deal_id, participant_id)` | 1. Order Service consumes `payment.authorized` (`pending_authorization → authorized`) |
| `Order.normal_order_cancelled` | `(order_id, user_id, [{product_id, quantity}])` | 1. Inventory Service unreachable mid-reservation<br>2. Insufficient stock on a line item<br>3. Order Service consumes `payment.failed` on a NORMAL order (`pending_charge`) |
| `Order.deal_order_cancelled` | `(order_id, deal_id, participant_id, user_id, items, reason)` | 1. Order Service consumes `payment.failed` on a DEAL order in `pending_authorization`<br>2. `deal.failed` batch: order voided (`pending_void → cancelled`, reason `deal_failed`)<br>3. `participant.left` consumed and the void completes (`authorized → cancelled`, reason `participant_left`) |
| `Order.payment_timeout` | `(order_id, idempotencyKey=order_id)` | 1. Sweep force-cancels a NORMAL order stuck in `pending_charge` (payment timeout)<br>2. Sweep force-cancels a NORMAL order stuck in `reserving` (crash mid-reservation)<br>3. Sweep force-cancels a DEAL order stuck in `pending_authorization` (no payment outcome, or a late `payment.authorized` arriving after the sweep already fired)|

---

## 5. Subscribers of Order Service's Events

| Event | Subscriber | Reaction |
|---|---|---|
| `Order.created` | Notification Service | Send confirmation email |
| `Order.authorized` | Notification Service | Push: "You are in — pending deal outcome" — Notification is this event's **only** subscriber; Deal Service's `authorized_count++` happens via the sync `authorize-slot` RPC, not by consuming this event (see correction below) |
| `Order.normal_order_cancelled` | Inventory Service | Release stock using the `items` list in the payload |
| `Order.normal_order_cancelled` | Notification Service | Send email |
| `Order.deal_order_cancelled` | Notification Service | Push notify buyer of the outcome |
| `Order.deal_order_cancelled` | Participation Service | Convert status to `removed` |
| `order.payment_charge_required` | Payment Service | Charge the amount using paymentIntentId |
| `order.payment_authorize_required` | Payment Service | Authorize the amount using paymentIntentId |
| `order.payment_capture_required` | Payment Service | Capture the held amount |
| `order.payment_void_required` | Payment Service | Release the held amount |
| `Order.payment_timeout` | Payment Service | To cancel any order stucked |

---

## 6. Events Order Service Subscribes To — Response

| Event | Payload | Order Service does |
|---|---|---|
| `Payment.failed` | `(payment_id, payment_intent_id, order_id, amount, error)` | 1. Get order by `order_id`<br>2. `UPDATE status = cancelled WHERE status IN (pending_charge, pending_authorization)` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. Fire `Order.normal_order_cancelled` (NORMAL) or `Order.deal_order_cancelled` (DEAL) |
| `Payment.charged` | `(payment_id, payment_intent_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = confirmed WHERE status = pending_charge` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. Fire `Order.created` |
| `Payment.authorized` | `(payment_id, payment_intent_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = authorized WHERE status = pending_authorization` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. Call `authorize-slot` on Deal Service (sync — `authorized_count++`, deal may flip `succeeded` here)<br>5. Fire `Order.authorized` |
| `Payment.captured` | `(payment_id, payment_intent_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = confirmed WHERE status = pending_capture` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. Fire `Order.created` |
| `Payment.voided` | `(order_id, payment_id, payment_intent_id, amount)` | *(not in your list, but present in every void path)* 1. Get order by `order_id`<br>2. `UPDATE status = cancelled WHERE status = pending_void` (guard) with `cancel_reason` = `deal_failed` or `participant_left` depending on which path parked the order in `pending_void`<br>3. On the leave path only: call `release-slot` on Deal Service (sync — `reserved_stock--`, `authorized_count--`)<br>4. Fire `Order.deal_order_cancelled` |
| `Participant.joined` | `(participant_id, deal_id, user_id, product_id, price, payment_intent_id)` | 1. Create order row, `status = pending_authorization`, `payment_intent_id` from the event (+ `order_products` row)<br>2. Fire `Order.payment_initiation_requested(user_id, order_id, amount, 'AUTHORIZE', payment_intent_id)` |
| `Participant.left` | `(participant_id, deal_id)` | 1. Find the **existing** order `WHERE deal_id = ? AND participant_id = ? AND status = authorized` — no new row is created<br>2. `UPDATE status = pending_void WHERE status = authorized` (guard — parks the order so deal-resolution batches skip it)<br>3. Fire `Order.payment_settlement_requested(payment_id, 'VOID')` |
| `Deal.succeeded` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: `SELECT ... WHERE deal_id = ? AND status = authorized FOR UPDATE SKIP LOCKED` → `status = pending_capture`<br>2. Fire `Order.payment_settlement_requested(payment_id, 'CAPTURE')` per order |
| `Deal.failed` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: same pattern → `status = pending_void`<br>2. Fire `Order.payment_settlement_requested(order_id, payment_id, 'VOID')` per order |

---

## 7. Order Service — Endpoints

### 7.1 `GET /users/{id}/orders`

Paginated list of a user's orders, filterable by `status` and `orderType`.

**Query params**
| Param | Type | Required | Notes |
|---|---|---|---|
| `status` | string | no | one of `pending_payment`, `confirmed`, `cancelled` |
| `orderType` | string | no | `NORMAL` \| `DEAL` |
| `page` | int | no | default `1` |
| `limit` | int | no | default `20`, max `100` |

**Response — 200**
```json
{
  "orders": [
    {
      "id": "ord_...",
      "userId": "b3f1...",
      "orderType": "NORMAL",
      "status": "confirmed",
      "totalPrice": 129.97,
      "items": [
        { "productId": "8a2c...", "quantity": 2, "unitPrice": 39.99 },
        { "productId": "c091...", "quantity": 1, "unitPrice": 49.99 }
      ],
      "createdAt": "2026-07-12T10:15:00Z"
    }
  ],
  "page": 1,
  "limit": 20,
  "total": 1
}
```

**Errors**
| Status | Cause |
|---|---|
| 400 | invalid `status`/`orderType` filter value, bad `page`/`limit` |
| 401/403 | requester is not `{id}` and does not hold an admin role (see order-service-spec.md §9.8 — authz on read endpoints was never fully resolved in the source doc) |

---

### 7.2 `GET /orders/{id}`

Returns a single order + its line items.

**Response — 200**
```json
{
  "id": "ord_...",
  "userId": "b3f1...",
  "orderType": "NORMAL",
  "status": "confirmed",
  "totalPrice": 129.97,
  "items": [
    { "productId": "8a2c...", "quantity": 2, "unitPrice": 39.99 },
    { "productId": "c091...", "quantity": 1, "unitPrice": 49.99 }
  ],
  "createdAt": "2026-07-12T10:15:00Z"
}
```

**Errors**
| Status | Cause |
|---|---|
| 404 | no order with this `id` |
| 401/403 | requester is neither the order's `userId` nor an admin/seller role that owns the underlying product |

---

### 7.3 `POST /orders` — Normal checkout

**Request**
```json
{
  "userId": "b3f1...",
  "paymentIntentId": "pi_...",
  "items": [
    { "productId": "8a2c...", "quantity": 2 },
    { "productId": "c091...", "quantity": 1 }
  ]
}
```

**Response — 201**
```json
{
  "id": "ord_...",
  "userId": "b3f1...",
  "orderType": "NORMAL",
  "status": "confirmed",
  "totalPrice": 129.97,
  "paymentIntentId": "pi_...",
  "items": [
    { "productId": "8a2c...", "quantity": 2, "unitPrice": 39.99 },
    { "productId": "c091...", "quantity": 1, "unitPrice": 49.99 }
  ],
  "createdAt": "2026-07-12T10:15:00Z"
}
```

**Errors**
| Status | Cause | Body |
|---|---|---|
| 400 | empty cart, bad quantity, unknown field | `{ "error": "..." }` |
| 404 | `productId` doesn't exist | `{ "error": "..." }` |
| 402 | Payment Service declined authorize/capture | `{ "error": "card_declined", "paymentIntentId": "pi_..." }` — `paymentIntentId` is still returned here since Stripe creates the intent before declining it |
| 503 | Payment Service or Catalog Service unreachable | `{ "error": "..." }` — no order persisted as `confirmed`; see order-service-spec.md §9.2 for the fail-fast-vs-reconcile decision |

---

## 8. NORMAL Flow — Request/Response Walkthrough

This section folds in the original `normal-order-in-detail.md` design doc, which works
through the NORMAL checkout flow at the request/response level (§2.1 above gives the terse
state-machine view). Status and event names have been updated to match §1–§7
(`pending_charge` instead of `pending_payment`, `order.normal_order_cancelled` instead of
`order.cancelled`, `payment_timeout` instead of `payment_stuck`).

### 8.1 Service Contracts

These are additions beyond the original architecture doc, agreed while designing this flow.
Payment is **not** among them — Order Service never calls Payment Service directly, in this
section or anywhere else in the doc. Charging goes exclusively through
`order.payment_charge_required` / `payment.charged` / `payment.failed` (§3, §5, §6); the
contracts below only cover the two services Order Service *does* call synchronously
(Catalog and Inventory).

#### Catalog Service — `POST /products/lookup`
```json
// Request
{ "productIds": ["8a2c...", "c091..."] }

// 200 Response
{
  "found": [
    { "id": "8a2c...", "basePrice": 39.99 }
  ],
  "notFound": ["c091..."]
}
```
Any non-empty `notFound` fails the order before any order row, reservation, or payment side
effect occurs — cheapest failure to check first.

#### Inventory Service — `POST /inventory/{productId}/order-reserve`
Hard-decrements real stock immediately

```json
// Request
{ "orderId": "ord_...", "quantity": 2 }

// 200 Response
{ "productId": "8a2c...", "available": 14, "reserved": true }

// 409 Response
{ "productId": "8a2c...", "available": 1, "reserved": false }
```
```sql
UPDATE inventory SET stock = stock - ? WHERE product_id = ? AND stock >= ?
```
`orderId` is included specifically so Inventory Service can dedupe `(orderId, productId)` —
if Order Service retries this call after a network timeout without knowing whether the
first attempt landed, dedup prevents a double-decrement. Inventory Service persists a
reservation record per successful `(orderId, productId)`

#### Inventory Service — `POST /inventory/{productId}/order-release`
```json
// Request
{ "orderId": "ord_...", "quantity": 2 }

// 200 Response
{ "productId": "8a2c...", "available": 16 }
```
**Trigger: event-driven, not a direct call from Order Service.** Inventory Service calls this
itself (internally) whenever it consumes an `order.normal_order_cancelled` event (§5), for
every `{product_id, quantity}` in that event's payload.

Two idempotency guarantees make this safe against at-least-once event delivery and against
`order.normal_order_cancelled` covering items that were *requested* but never actually reserved:
- **No matching reservation record** for `(orderId, productId)` → no-op, returns current
  `available` unchanged.
- **Reservation record already released** → no-op.
- Otherwise → increment `available` by the quantity (release).

### 8.2 `POST /orders` — Request/Response Shapes (walkthrough variant)

```json
// Request
{
  "userId": "usr_...",
  "items": [
    { "productId": "8a2c...", "quantity": 2 },
    { "productId": "c091...", "quantity": 1 }
  ]
}
```

**201 — confirmed within timeout**
```json
{
  "id": "ord_...",
  "userId": "usr_...",
  "orderType": "NORMAL",
  "status": "confirmed",
  "totalPrice": 129.97,
  "items": [
    { "productId": "8a2c...", "quantity": 2, "unitPrice": 39.99 },
    { "productId": "c091...", "quantity": 1, "unitPrice": 49.99 }
  ],
  "createdAt": "2026-07-13T10:15:00Z"
}
```

**202 — payment still in flight**
```json
{
  "id": "ord_...",
  "status": "pending_charge",
  "message": "Order created; payment is still processing."
}
```
Order Service publishes `order.payment_charge_required` and then waits briefly, in-request,
for its own consumer to observe the resulting `payment.charged`/`payment.failed` (§6) before
responding. If that resolves within the wait window, the `201`/`402` response reflects the
outcome directly; otherwise Order Service gives up waiting (the order is left in
`pending_charge`, unchanged) and responds `202`. Either way the charge itself was always
requested via the event, never a direct call — the wait is purely about what the HTTP
response can report, not about how the charge happens.

**Error responses**
| Status | Cause | Order row |
|---|---|---|
| 400 | empty cart, bad quantity, unknown field, or any `productId` in Catalog's `notFound` | never created |
| 409 | inventory reservation failed for one or more items (insufficient stock) | created, then `cancelled` (`cancel_reason='insufficient_stock'`) |
| 402 | `payment.failed` observed within the in-request wait window (§8.2 above) | created, then `cancelled` (`cancel_reason='payment_declined'`) |
| 503 | Catalog Service unreachable during lookup (step 2, before the order row exists) | never created |
| 503 | Inventory Service unreachable during reservation (step 4, after the order row exists) | created, then `cancelled` (`cancel_reason='inventory_unreachable'`) |

### 8.3 Flow — Step by Step

1. **Receive & validate.** `POST /orders` payload: non-empty `items`, all `quantity > 0`.
   Merge any duplicate `productId` entries by summing their quantities. Malformed request
   → `400`, nothing else touched.

2. **Catalog lookup.** `POST /products/lookup` with all (merged) `productIds` in one call.
   - Any `notFound` → `400`. Stop here — no order row, no reservation, no charge event yet.
   - `found` entries give the authoritative `unitPrice` for each line item (never trust a
     client-supplied price).
   - Catalog Service unreachable → `503`, order row never created.

3. **Create order.** In one DB transaction: insert `orders`
   (`status='reserving'`, `order_type='NORMAL'`, `total_price = Σ(unitPrice × qty)`)
   + one `order_products` row per (merged) item (prices from step 2).

4. **Inventory reservation.** For each item, `POST /inventory/{productId}/order-reserve`
   with the `orderId` from step 3.
   - If any call returns `409`, or Inventory Service is unreachable mid-loop:
     `UPDATE orders SET status='cancelled', cancel_reason=<'insufficient_stock'|'inventory_unreachable'> WHERE id=?`,
     write `order.normal_order_cancelled` to the outbox (payload: all originally-requested items —
     Inventory Service's dedup, §8.1, no-ops the ones that were never actually reserved),
     commit, return `409`/`503` immediately. No synchronous rollback call is made.

5. **Charge.** `UPDATE orders SET status='pending_charge' WHERE id=? AND status='reserving'`,
   then write `order.payment_charge_required` to the outbox (§3/§4: `{user_id, order_id,
   amount, payment_intent_id, idempotencyKey = order_id}`) and commit. Order Service never
   calls Payment Service directly here — Payment Service consumes this event, charges the
   card, and publishes `payment.charged` or `payment.failed` (§5) asynchronously.
   - If Order Service's own consumer resolves the order (Case1/Case2 in §8.4) within a short
     in-request wait → return `201` (confirmed) or `402` (declined) synchronously.
   - **Otherwise** → leave the order as `pending_charge`. Do **not** cancel/release yet — the
     charge may still succeed on Payment Service's side; releasing now risks confirming a
     payment against stock that's already been sold to someone else. Return `202`
     immediately. Resolution moves to the async path (§8.4).

### 8.4 Async Reconciliation

Order Service subscribes to `payment.charged` and `payment.failed`, and also runs a
periodic sweep — together these resolve orders that timed out at step 5, and orders
orphaned by a crash mid-flow. Order Service never polls or calls Payment Service directly
for any of this — resolution is either an inbound event or a sweep-fired outbound event.

**Event consumption**:
- **Case1** — consuming `payment.charged`: `UPDATE orders SET status='confirmed', payment_id=? WHERE id=? AND status='pending_charge'`; if the update affected a row, fire `order.created`.
- **Case2** — consuming `payment.failed`: `UPDATE orders SET status='cancelled', cancel_reason='payment_declined' WHERE id=? AND status='pending_charge'`; if the update affected a row, fire `order.normal_order_cancelled` (payload: `{order_id, user_id, [{product_id, quantity}]}`). Inventory Service reacts to this the same way it reacts to every other `order.normal_order_cancelled` (§8.1) — Order Service does not call `order-release` itself here either.

**Reconciliation sweep**, run every ~30s:
- For orders in `pending_charge` past the staleness threshold (5 min, per §2.1): the outbox
  guarantees `order.payment_charge_required` was delivered at least once, so there's nothing
  to retrigger — instead fire `Order.payment_timeout` (§4: `{order_id, idempotencyKey =
  order_id}`) to the outbox and run Case2 with `cancel_reason='payment_timeout'`. Payment
  Service subscribes to `Order.payment_timeout` (§5) and force-resolves/cancels the stuck
  charge on its own side; Order Service never queries Payment Service to check status.
- For orders in `reserving` past a short grace threshold (2 min, per §2.1 — this state is
  normally only held for the duration of step 4 within a single request; persisting past
  that threshold means the process crashed mid-reservation-loop): `UPDATE orders SET
  status='cancelled', cancel_reason='reservation_incomplete' WHERE id=? AND
  status='reserving'`, fire `order.normal_order_cancelled` with the order's full item list.
  Inventory Service's dedup (§8.1) safely no-ops any item that was never actually reserved
  before the crash.

### 8.5 Full Branch Summary

| Branch | Order status | Inventory | Payment | Client response | Async event |
|---|---|---|---|---|---|
| Happy path | `confirmed` | committed | charged | `201` | `order.created` |
| Unknown product | never created | untouched | not called | `400` | — |
| Catalog unreachable | never created | untouched | not called | `503` | — |
| Insufficient stock (any item) | created → `cancelled` (`insufficient_stock`) | released async (no-op for never-reserved items) | not called | `409` | `order.normal_order_cancelled` |
| Inventory Service unreachable mid-reservation | created → `cancelled` (`inventory_unreachable`) | released async | not called | `503` | `order.normal_order_cancelled` |
| Card declined (within timeout) | created → `cancelled` (`payment_declined`) | released async | declined | `402` | `order.normal_order_cancelled` |
| Payment timeout → later charged | `pending_charge` → `confirmed` | stays committed | charged (async) | `202` then resolved async | `order.created` |
| Payment timeout → later declined | `pending_charge` → `cancelled` (`payment_declined`) | released async | declined (async) | `202` then resolved async | `order.normal_order_cancelled` |
| No `payment.charged`/`payment.failed` before staleness threshold | `pending_charge` → `cancelled` (`payment_timeout`) | released async | force-cancelled via `Order.payment_timeout` (async) | `202` then resolved async | `order.normal_order_cancelled` |
| Crash mid-reservation loop | `reserving` → `cancelled` (`reservation_incomplete`), caught by sweep | released async | not called | original request already failed/disconnected — no live response | `order.normal_order_cancelled` |
</content>
