# Order Service — Data Model & Events Spec

## 1. Order DB Schema

```sql
CREATE TABLE orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL,
    order_type      VARCHAR(10) NOT NULL CHECK (order_type IN ('NORMAL', 'DEAL')),

    deal_id         UUID NULL,               -- required if order_type = 'DEAL'
    participant_id  UUID NULL,               -- required if order_type = 'DEAL'

    status          VARCHAR(20) NOT NULL CHECK (status IN (
                        'reserving',             -- NORMAL: inventory reservation in flight
                        'pending_charge',        -- NORMAL: waiting on CHARGE outcome
                        'pending_authorization',  -- DEAL: waiting on AUTHORIZE outcome
                        'authorized',            -- DEAL: hold placed, waiting on deal resolution
                        'pending_capture',       -- DEAL: deal succeeded, waiting on CAPTURE outcome
                        'pending_void',          -- DEAL: deal failed, waiting on VOID outcome
                        'confirmed',             -- terminal
                        'cancelled'              -- terminal
                     )),

    total_price     NUMERIC(10,2) NOT NULL CHECK (total_price >= 0),
    payment_id      UUID NULL,               -- set once Payment Service returns a payment_id
    payment_intent_id VARCHAR(255) NULL,     
    cancel_reason   VARCHAR(30) NULL CHECK (cancel_reason IN (
                        'insufficient_stock', 'inventory_unreachable', 'reservation_incomplete',
                        'payment_declined', 'payment_timeout', 'deal_failed', 'participant_left'
                     )),

    version         INT NOT NULL DEFAULT 0,  -- optimistic locking / race auditing
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

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
-- Used by every reconciliation sweep (reserving / pending_charge / pending_authorization / pending_capture / pending_void)
CREATE INDEX idx_orders_status_updated_at ON orders (status, updated_at);

CREATE TABLE order_products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL REFERENCES orders(id),
    product_id  UUID NOT NULL,
    quantity    INT NOT NULL CHECK (quantity > 0),   -- always 1 for DEAL, can be >1 for NORMAL
    unit_price  NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_products_order_id ON order_products (order_id);

-- Inbound Kafka dedup: every consumer checks/inserts here in the same transaction
-- as its business write, so at-least-once redelivery is a no-op.
CREATE TABLE processed_events (
    event_id      UUID PRIMARY KEY,
    event_type    VARCHAR(50) NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox for every event Order Service publishes (§3). Makes the
-- publish part of the same commit as the status write; a separate relay process
-- polls WHERE published_at IS NULL and delivers to Kafka at-least-once.
CREATE TABLE outbox_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id  UUID NOT NULL,          -- order_id
    event_type    VARCHAR(50) NOT NULL,   -- one of the Order.* events in §3
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ NULL
);

CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at) WHERE published_at IS NULL;
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
| `participant.left` | `{participant_id, deal_id, user_id, order_id}` | Participation Service |
| `order.payment_initiation_requested` | `{user_id, order_id, amount, type: 'AUTHORIZE'\|'CHARGE', payment_intent_id, idempotencyKey = order_id}` — creates a **new** charge/hold; no `payment_id` exists yet | Order Service |
| `order.payment_settlement_requested` | `{payment_id, type: 'CAPTURE'\|'VOID', idempotencyKey = payment_id}` — acts on an **existing** payment; `payment_id` alone identifies it | Order Service |
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
| `Order.payment_initiation_requested` | `(user_id, order_id, amount, type: CHARGE \| AUTHORIZE, payment_intent_id)` | 1. A NORMAL order finishes inventory reservation → `reserving → pending_charge` (type=CHARGE)<br>2. A DEAL order is created from `participant.joined` → `pending_authorization` (type=AUTHORIZE) |
| `Order.payment_settlement_requested` | `(payment_id, type: CAPTURE \| VOID)` | 1. `deal.succeeded` batch converts `authorized → pending_capture` (type=CAPTURE)<br>2. `deal.failed` batch converts `authorized → pending_void` (type=VOID)<br>3. `participant.left` is consumed → guarded UPDATE parks the order `authorized → pending_void` (type=VOID) |
| `Order.created` | `(order_id, user_id, items)` | 1. Order Service consumes `payment.charged` (NORMAL, `pending_charge → confirmed`)<br>2. Order Service consumes `payment.captured` (DEAL, `pending_capture → confirmed`) |
| `Order.authorized` | `(deal_id, participant_id)` | 1. Order Service consumes `payment.authorized` (`pending_authorization → authorized`) |
| `Order.normal_order_cancelled` | `(order_id, user_id, [{product_id, quantity}])` | 1. Inventory Service unreachable mid-reservation<br>2. Insufficient stock on a line item<br>3. Order Service consumes `payment.failed` on a NORMAL order (`pending_charge`)<br>4. Sweep force-cancels a NORMAL order stuck in `pending_charge` (payment timeout)<br>5. Sweep force-cancels a NORMAL order stuck in `reserving` (crash mid-reservation) |
| `Order.deal_order_cancelled` | `(order_id, deal_id, participant_id, user_id, items, reason)` | 1. Order Service consumes `payment.failed` on a DEAL order in `pending_authorization`<br>2. Sweep force-cancels a DEAL order stuck in `pending_authorization` (no payment outcome, or a late `payment.authorized` arriving after the sweep already fired)<br>3. `deal.failed` batch: order voided (`pending_void → cancelled`, reason `deal_failed`)<br>4. `participant.left` consumed and the void completes (`authorized → cancelled`, reason `participant_left`) |
---

## 5. Subscribers of Order Service's Events

| Event | Subscriber | Reaction |
|---|---|---|
| `Order.created` | Notification Service | Send confirmation email |
| `Order.authorized` | Notification Service | Push: "You are in — pending deal outcome" — Notification is this event's **only** subscriber; Deal Service's `authorized_count++` happens via the sync `authorize-slot` RPC, not by consuming this event (see correction below) |
| `Order.normal_order_cancelled` | Inventory Service | Release stock using the `items` list in the payload |
| `Order.normal_order_cancelled` | Notification Service | Send email |
| `Order.normal_order_cancelled` | Payment Service | Safety net: if a charge for this `order_id` actually went through before the cancel (event race), void/cancel it *(drawn in the no-outcome-sweep flow, missing from your original list)* |
| `Order.deal_order_cancelled` | Notification Service | Push notify buyer of the outcome |
| `Order.deal_order_cancelled` | Participation Service | Convert participation row to `removed` |
| `Order.deal_order_cancelled` | Payment Service | Safety net: if a PaymentIntent exists for this order (authorize succeeded but the event was lost, or a hold exists for a now-cancelled order), void it *(drawn in the sweep-timeout flows, missing from your original list)* |
| `Order.payment_initiation_requested` | Payment Service | Create + confirm the Stripe PaymentIntent identified by `payment_intent_id` (`type=CHARGE`: capture automatic, `type=AUTHORIZE`: capture manual) given `user_id` + `order_id` + `amount` |
| `Order.payment_settlement_requested` | Payment Service | Capture (`type=CAPTURE`) or cancel (`type=VOID`) the **existing** PaymentIntent looked up by `payment_id` |

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
| `Participant.left` | `(participant_id, deal_id, user_id, order_id)` | 1. Find the **existing** order `WHERE deal_id = ? AND participant_id = ? AND status = authorized` — no new row is created<br>2. `UPDATE status = pending_void WHERE status = authorized` (guard — parks the order so deal-resolution batches skip it)<br>3. Fire `Order.payment_settlement_requested(payment_id, 'VOID')` |
| `Deal.succeeded` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: `SELECT ... WHERE deal_id = ? AND status = authorized FOR UPDATE SKIP LOCKED` → `status = pending_capture`<br>2. Fire `Order.payment_settlement_requested(payment_id, 'CAPTURE')` per order |
| `Deal.failed` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: same pattern → `status = pending_void`<br>2. Fire `Order.payment_settlement_requested(order_id, payment_id, 'VOID')` per order |

---

## Order Service — Endpoints

### 1. `GET /users/{id}/orders`

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

### 2. `GET /orders/{id}`

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

### 3. `POST /orders` — Normal checkout

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
