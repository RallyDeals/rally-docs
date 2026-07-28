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

-- Guards against a redelivered participant.Joined creating two orders for the same slot.
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
    causation_id UUID NOT NULL,                -- added V8
    trace_id UUID NOT NULL                     -- added V8
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
    │                                                       (Payment.Charged consumed)
    │
    ├──────────────────────────────────────────────────► cancelled [payment_declined]
    │                                                       (Payment.Failed consumed)
    │
    └──────────────────────────────────────────────────► cancelled [payment_timeout]
                                                            (sweep: stuck in `pending_charge`
                                                             > 5 min)
```

### 2.2 DEAL flow

```
 (start)
    │  participant.Joined consumed — order + order_products row inserted
    ▼
 pending_authorization ─────────────────────────────────► authorized
    │                                                       (Payment.Authorized consumed)
    │
    ├──────────────────────────────────────────────────► cancelled [payment_declined]
    │                                                       (Payment.Failed consumed)
    │
    └──────────────────────────────────────────────────► cancelled [payment_timeout]
                                                            (sweep: stuck in
                                                             `pending_authorization` > 60s)

 authorized ─────────────────────────────────────────────► pending_capture
    │            (deal.Succeeded batch: SELECT ... WHERE status='authorized' FOR UPDATE
    │             SKIP LOCKED → status='pending_capture')
    │
    ├──────────────────────────────────────────────────► pending_void
    │            (deal.Failed batch: same pattern → status='pending_void')
    │
    └──────────────────────────────────────────────────► pending_void
                 (participant.Left consumed → guarded UPDATE WHERE status='authorized'
                  parks the order → order.payment_settlement_requested VOID fired)

 pending_capture ────────────────────────────────────────► confirmed
                 (Payment.Captured consumed)

 pending_void ───────────────────────────────────────────► cancelled [deal_failed | participant_left]
                 (Payment.Voided consumed; cancel_reason depends on which path
                  parked the order in pending_void)
```

**Sweep behavior differs by stage** (per the Deal Join 6/7 sweep notes): orders stuck in
`pending_authorization` > 60s are **force-cancelled** (silence = failure; `release-slot`
called — `reserved_stock--` only, since the order never reached `authorized`);
orders stuck in `pending_capture` or `pending_void` > 60s are instead **re-published** —
the same `order.payment_settlement_requested` (type=CAPTURE/VOID, idempotencyKey=payment_id) is fired
again, never a cancel, because capture/void against an existing `payment_id` is idempotent
and the deal outcome is already decided at that point.

Both void paths park the order in `pending_void` before firing the VOID (team decision):
the `deal.Failed` batch does it via the `FOR UPDATE SKIP LOCKED` batch, and the
participant-leave path does it via a guarded `UPDATE ... WHERE status = 'authorized'`
immediately on consuming `participant.Left`. This closes the race where a concurrent
`deal.Succeeded`/`deal.Failed` batch over the same `deal_id` could grab an order whose
leave-void was still in flight — the batch selects `WHERE status = 'authorized'` and
therefore skips anything already parked.

---

## 3. Order Service — Endpoints

### 3.1 `GET /users/{id}/orders`

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

### 3.2 `GET /orders/{id}`

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

### 3.3 `POST /orders` — Normal checkout

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

## 4 Service Contracts
### 4.1 Catalog Service
 Request Enpoint | Request Body | Response Body |
|---|---|---|
| `POST /products/lookup` | `{  "productIds": ["8a2c...", "c091..."] }` | `{ "found": [{ "id": "8a2c...", "basePrice": 39.99 }], "notFound": ["c091..."]}`|

### 4.2 Inventory Service
- Published events are on `order.lifecycle` topic

| Request Enpoint | Request Body | Response Body |
|---|---|---|
| `POST /inventory/{productId}/order-reserve` | `{ "orderId": "ord_...", "quantity": 2 }` | `{ "productId": "8a2c...", "available": 14, "reserved": true }`|

| Published Event | Payload |
|---|---|
| `Order.NormalCancelled` | `(order_id, user_id, cancelReason, [{product_id, quantity}])` |

### 4.3 Deal Service
| Request Endpoint | Request Body | Expected Action
|---|---|---|
| `POST /deals/{deal_id}/authorize-slot` | None | increment `authorized_count` |
| `POST /deals/{deal_id}/release-slot` | None | decrement `reserved_stock` |
| `POST /deals/{deal_id}/release-authorized-slot` | None | increment `reserved_stock` & `authorized_count` |

| Received Event | Payload | Reaction |
|---|---|---|
| `Deal.Succeeded` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: `SELECT ... WHERE deal_id = ? AND status = authorized FOR UPDATE SKIP LOCKED` → `status = pending_capture`<br>2. Fire `Order.payment_settlement_requested(payment_id, 'CAPTURE')` per order |
| `Deal.Failed` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: same pattern → `status = pending_void`<br>2. Fire `Order.payment_settlement_requested(order_id, payment_id, 'VOID')` per order |

### 4.4 Payment Service
- Order Service publish payment related events on `order.payments_requested` topic
- Order Service recieve payment related events on `Payment.events` topic

| Published Event | Payload | Reaction |
|---|---|---|
| `Payment.InitRequired.Charge` | `{user_id, order_id, amount, payment_intent_id}` | Charge the amount using paymentIntentId |
| `Payment.InitRequired.Authorize` | `{user_id, order_id, amount, payment_intent_id}` | Authorize the amount using paymentIntentId |
| `Payment.SettlementRequired.Capture` | `{order_id, payment_id}` | Capture the held amount |
| `Payment.SettlementRequired.Void` | `{order_id, payment_id}` | Release the held amount |
| `Payment.Timeout` | `{order_id}`| To cancel any order stucked |

| Received Event | Payload | Reaction |
|---|---|---|
| `Payment.Failed` | `(payment_id, payment_intent_id, order_id, amount, errorMessage, errorCode)` | 1. Get order by `order_id`<br>2. `UPDATE status = cancelled WHERE status IN (pending_charge, pending_authorization)` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. On the DEAL path only: call `release-slot` on Deal Service (sync — `reserved_stock--`; the order never reached `authorized`, so `authorized_count` is untouched)<br>5. Fire `Order.NormalCancelled` (NORMAL) or `Order.DealCancelled` (DEAL) |
| `Payment.Charged` | `(payment_id, payment_intent_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = confirmed WHERE status = pending_charge` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. Fire `Order.Created` |
| `Payment.Authorized` | `(payment_id, payment_intent_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = authorized WHERE status = pending_authorization` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. Call `authorize-slot` on Deal Service (sync — `authorized_count++`, deal may flip `succeeded` here)<br>5. Fire `Order.Authorized` |
| `Payment.Captured` | `(payment_id, payment_intent_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = confirmed WHERE status = pending_capture` (guard)<br>3. Set `payment_id` + `payment_intent_id` on the order<br>4. Fire `Order.Created` |
| `Payment.Voided` | `(order_id, payment_id, payment_intent_id, amount)` | *(not in your list, but present in every void path)* 1. Get order by `order_id`<br>2. `UPDATE status = cancelled WHERE status = pending_void` (guard) with `cancel_reason` = `deal_failed` or `participant_left` depending on which path parked the order in `pending_void`<br>3. On the leave path only: call `release-authorized-slot` on Deal Service (sync — `reserved_stock--` and `authorized_count--`, atomically; the order had reached `authorized` before parking, unlike the plain `release-slot` case)<br>4. Fire `Order.DealCancelled` |

### 4.5 Participation Service
- Published events are on `order.lifecycle` topic

| Published Event | Payload |
|---|---|
| `Order.DealCancelled` | `(order_id, deal_id, participant_id, user_id, reason)` |

| Received Event | Payload | Reaction |
|---|---|---|
| `Participant.Joined` | `(participant_id, deal_id, user_id, product_id, price, payment_intent_id)` | 1. Create order row, `status = pending_authorization`, `payment_intent_id` from the event (+ `order_products` row)<br>2. Fire `Order.payment_initiation_requested(user_id, order_id, amount, 'AUTHORIZE', payment_intent_id)` |
| `Participant.Left` | `(participant_id, deal_id)` | 1. Find the **existing** order `WHERE deal_id = ? AND participant_id = ? AND status = authorized` — no new row is created<br>2. `UPDATE status = pending_void WHERE status = authorized` (guard — parks the order so deal-resolution batches skip it)<br>3. Fire `Order.payment_settlement_requested(payment_id, 'VOID')` |

### 4.6 Notification Service
- Published events are on `order.lifecycle` topic

| Published Event | Payload |
|---|---|
| `Order.Created` | `{order_id, user_id, items}` |
| `Order.Authorized` | `{deal_id, user_id}` |
| `Order.NormalCancelled` |  `(order_id, user_id, cancelReason, [{product_id, quantity}])` |
| `Order.DealCancelled` | `(order_id, deal_id, participant_id, user_id, reason)` |


## 5 Normal Order Flow

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
     write `order.NormalCancelled` to the outbox (payload: all originally-requested items —
     Inventory Service's dedup, §8.1, no-ops the ones that were never actually reserved),
     commit, return `409`/`503` immediately. No synchronous rollback call is made.

5. **Charge.** `UPDATE orders SET status='pending_charge' WHERE id=? AND status='reserving'`,
   then write `Payment.InitRequired.Charge` to the outbox (§3/§4: `{user_id, order_id,
   amount, payment_intent_id, idempotencyKey = order_id}`) and commit. Order Service never
   calls Payment Service directly here — Payment Service consumes this event, charges the
   card, and publishes `Payment.Charged` or `Payment.Failed` (§5) asynchronously.
   - If Order Service's own consumer resolves the order (Case1/Case2 in §8.4) within a short
     in-request wait → return `201` (confirmed) or `402` (declined) synchronously.
   - **Otherwise** → leave the order as `pending_charge`. Do **not** cancel/release yet — the
     charge may still succeed on Payment Service's side; releasing now risks confirming a
     payment against stock that's already been sold to someone else. Return `202`
     immediately. Resolution moves to the async path (§8.4).

Order Service subscribes to `Payment.Charged` and `Payment.Failed`, and also runs a
periodic sweep, resolution is either an inbound event or a sweep-fired outbound event.

**Event consumption**:
- **Case1** — consuming `Payment.Charged`: `UPDATE orders SET status='confirmed', payment_id=? WHERE id=? AND status='pending_charge'`; if the update affected a row, fire `Order.Created`.
- **Case2** — consuming `Payment.Failed`: `UPDATE orders SET status='cancelled', cancel_reason='payment_declined' WHERE id=? AND status='pending_charge'`; if the update affected a row, fire `order.NormalCancelled` (payload: `{order_id, user_id, cancelReason, [{product_id, quantity}]}`). Inventory Service reacts to this the same way it reacts to every other `order.NormalCancelled` (§8.1) — Order Service does not call `order-release` itself here either.

**Reconciliation sweep**, run every ~30s:
- For orders in `pending_charge` past the staleness threshold (5 min), fire `Payment.Timeout`
- For orders in `reserving` past a short grace threshold (2 sec), fire `order.NormalCancelled` 
  with the order's full item list.

## 6 Deal Order Flow
--To be written--

</content>
