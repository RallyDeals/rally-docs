# Order Service — Data Model, Events & Purchase Flows

## 1. Order Data Model (ERD)

```mermaid
erDiagram
    ORDERS ||--o{ ORDER_PRODUCTS : "has line items"
    ORDERS ||..o{ OUTBOX_EVENTS : "aggregate_id (logical, no FK)"

    ORDERS {
        uuid id PK
        uuid user_id
        varchar order_type "NORMAL | DEAL"
        uuid deal_id "required if DEAL"
        uuid participant_id "required if DEAL"
        varchar status
        numeric total_price
        text address
        uuid payment_id "nullable, set by Payment Service"
        varchar card_last4 "nullable, snapshot fetched sync from Payment Service at order-creation time"
        varchar card_brand "nullable, same snapshot"
        varchar card_exp_month "nullable, same snapshot; VARCHAR since V12 — Payment Service returns it as a string"
        varchar card_exp_year "nullable, same snapshot; VARCHAR since V12"
        varchar cancel_reason
        varchar payment_error_code "nullable, added V14; Payment Service's declined-reason code"
        text payment_error_message "nullable, added V14; populated only when payment_error_code = card_declined"
        int version
        timestamptz created_at
        timestamptz updated_at
        timestamptz status_updated_at
    }

    ORDER_PRODUCTS {
        uuid id PK
        uuid order_id FK
        uuid product_id
        int quantity "always 1 for DEAL"
        numeric unit_price
        varchar product_name "nullable, snapshot at order-creation time"
        varchar product_image_url "nullable, snapshot at order-creation time"
        timestamptz created_at
    }

    OUTBOX_EVENTS {
        uuid id PK
        uuid aggregate_id
        varchar aggregate_type
        varchar event_type
        varchar topic
        jsonb payload
        varchar status "PENDING | ..."
        int attempts
        text last_error
        uuid correlation_id
        uuid causation_id
        uuid trace_id
        timestamptz created_at
        timestamptz published_at
    }

    PROCESSED_EVENTS {
        varchar event_id PK
        varchar source_topic PK
        varchar event_type
        timestamptz processed_at
    }
```

**Constraints & indexes not shown above:**
- `orders.status` is one of `RESERVING, PENDING_CHARGE, PENDING_AUTHORIZATION, AUTHORIZED,
  PENDING_CAPTURE, PENDING_VOID, CONFIRMED, CANCELLED`; `cancel_reason` is one of
  `INSUFFICIENT_STOCK, INVENTORY_UNREACHABLE, RESERVATION_INCOMPLETE, PAYMENT_DECLINED,
  PAYMENT_TIMEOUT, DEAL_FAILED, DEAL_RESOLVED, PARTICIPANT_LEFT`.
- `deal_fields_consistency` check: `deal_id`/`participant_id` set together for `DEAL`,
  both null for `NORMAL`.
- Unique index on `(deal_id, participant_id) WHERE order_type = 'DEAL'` — guards against a
  redelivered `Participant.Joined` creating two orders for the same slot.
- Index on `(status, status_updated_at)` — sweep jobs key off `status_updated_at`, not
  `updated_at`, since `updated_at` bumps on any column change (e.g. setting `payment_id`
  without a status transition).
- Two triggers keep `updated_at` and `status_updated_at` accurate automatically, so sweep
  jobs never depend on application code remembering to set them by hand.
- `outbox_events` is the transactional outbox for everything Order Service publishes — the
  publish is part of the same commit as the business write; a relay process polls
  `WHERE status = 'PENDING'` and delivers to Kafka at-least-once.
- `processed_events` is inbound Kafka dedup — every consumer checks/inserts here in the
  same transaction as its business write, so at-least-once redelivery is a no-op.
  Primary key is `(event_id, source_topic)`, not `event_id` alone.

---

## 2. Order Status & State Machine

Two independent flows share `orders`. `reserving`/`pending_charge` exist only for
`order_type = NORMAL`; `pending_authorization`/`authorized`/`pending_capture`/`pending_void`
exist only for `order_type = DEAL`. `confirmed` and `cancelled` are terminal for both.
Every transition into a terminal or intermediate state is a conditional
`UPDATE ... WHERE status = <expected>` — this guard makes a reconciliation-sweep cancel
and a late payment event mutually exclusive: whichever commits first wins, the other is a no-op.

### 2.1 NORMAL flow

```mermaid
stateDiagram-v2
    [*] --> reserving : POST /orders (catalog lookup ok)

    reserving --> cancelled : inventory-reserve batch response has any reserved=false\n[insufficient_stock]
    reserving --> cancelled : Inventory Service unreachable\n[inventory_unreachable]
    reserving --> cancelled : sweep, stuck > 14 sec\n[reservation_incomplete]
    reserving --> pending_charge : batch response — all items reserved=true

    pending_charge --> confirmed : Payment.Charged
    pending_charge --> cancelled : Payment.Failed
    pending_charge --> cancelled : sweep, stuck > 5 min

    confirmed --> [*]
    cancelled --> [*]
```

### 2.2 DEAL flow

```mermaid
stateDiagram-v2
    [*] --> pending_authorization : Participant.Joined

    pending_authorization --> authorized : Payment.Authorized
    pending_authorization --> cancelled : Payment.Failed
    pending_authorization --> cancelled : sweep, stuck > 60s

    authorized --> pending_capture : Deal.Succeeded batch(guarded UPDATE)
    authorized --> pending_void : Deal.Failed batch(guarded UPDATE)
    authorized --> pending_void : Participant.Left(guarded UPDATE)
    authorized --> pending_void : late Payment.Authorized, deal already resolved\n[deal_resolved]

    pending_capture --> confirmed : Payment.Captured

    pending_void --> cancelled : Payment.Voided

    confirmed --> [*]
    cancelled --> [*]
```

**Sweep only covers `pending_authorization`**: orders stuck there > 60s are
**force-cancelled** (`release-slot`, `Order.DealCancelled`, `Payment.Timeout`).

Three paths park an order in `pending_void`, all via a guarded `UPDATE ... WHERE status =
'authorized'`: the `Deal.Failed` batch (§6 step 3), the participant-leave path (on
`Participant.Left`), and a **late-authorization** race — `Payment.Authorized` is consumed and
the order reaches `authorized`, but the following `authorize-slot` call is rejected because
the deal already resolved. That order is immediately re-parked `authorized → pending_void`
with `cancel_reason = deal_resolved`, `release-slot` is called and `Order.Authorized` is not fired.

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
        { "productId": "8a2c...", "name": "Wireless Mouse", "imageUrl": "https://cdn.../mouse.jpg", "quantity": 2, "price": 39.99 },
        { "productId": "c091...", "name": "USB-C Hub", "imageUrl": "https://cdn.../hub.jpg", "quantity": 1, "price": 49.99 }
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

Returns a single order and its line items. Response shape mirrors `DetailedOrderResponse`
field-for-field (see Appendix for the underlying columns).

**Response — 200**
```json
{
  "orderId": "c4d2...",
  "userId": "b3f1...",
  "orderType": "NORMAL",
  "dealId": null,
  "participantId": null,
  "status": "CONFIRMED",
  "cancelReason": null,
  "paymentErrorCode": null,
  "paymentErrorMessage": null,
  "totalPrice": 129.97,
  "address": "123 Main St, Springfield",
  "paymentId": "9f4e...",
  "cardLast4": "4242",
  "cardBrand": "visa",
  "cardExpMonth": "12",
  "cardExpYear": "2030",
  "orderProducts": [
    { "productId": "8a2c...", "productName": "Wireless Mouse", "productImageUrl": "https://cdn.../mouse.jpg", "quantity": 2, "unitPrice": 39.99 },
    { "productId": "c091...", "productName": "USB-C Hub", "productImageUrl": "https://cdn.../hub.jpg", "quantity": 1, "unitPrice": 49.99 }
  ],
  "createdAt": "2026-07-12T10:15:00Z",
  "updatedAt": "2026-07-12T10:15:04Z",
  "statusUpdatedAt": "2026-07-12T10:15:04Z"
}
```

A cancelled order (e.g. a card decline) additionally populates `cancelReason`,
`paymentErrorCode`, and — only when `paymentErrorCode` is `card_declined` —
`paymentErrorMessage`:
```json
{
  "status": "CANCELLED",
  "cancelReason": "PAYMENT_DECLINED",
  "paymentErrorCode": "card_declined",
  "paymentErrorMessage": "Your card has insufficient funds."
}
```

**Errors**
| Status | Cause |
|---|---|
| 404 | no order with this `id` |
| 401/403 | requester is neither the order's `userId` nor an admin/seller role owning the underlying product |

---

### 3.3 `POST /orders` — Normal checkout

**Request**
```json
{
  "userId": "b3f1...",
  "paymentMethodId": "pm_...",
  "address": "123 Main St, Springfield",
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
  "address": "123 Main St, Springfield",
  "cardLast4": "4242",
  "cardBrand": "visa",
  "cardExpMonth": "12",
  "cardExpYear": "2030",
  "items": [
    { "productId": "8a2c...", "name": "Wireless Mouse", "imageUrl": "https://cdn.../mouse.jpg", "quantity": 2, "price": 39.99 },
    { "productId": "c091...", "name": "USB-C Hub", "imageUrl": "https://cdn.../hub.jpg", "quantity": 1, "price": 49.99 }
  ],
  "createdAt": "2026-07-12T10:15:00Z"
}
```

**Errors**
| Status | Cause | Body |
|---|---|---|
| 400 | empty cart, bad quantity, missing `address`/`paymentMethodId`, unknown field | `{ "error": "..." }` |
| 404 | `productId` doesn't exist | `{ "error": "..." }` |
| 402 | Payment Service declined authorize/capture | `{ "error": "card_declined" }` |
| 503 | Payment Service or Catalog Service unreachable | `{ "error": "..." }` — no order persisted as `confirmed`; see order-service-spec.md §9.2 for the fail-fast-vs-reconcile decision |

---

## 4. Service Contracts

### 4.1 Catalog Service

| Request Endpoint | Request Body | Response Body |
|---|---|---|
| `POST /products/lookup` | `{ "productIds": ["8a2c...", "c091..."] }` | `{ "found": { "8a2c...": { "productId": "8a2c...", "name": "Wireless Mouse", "imageUrl": "https://cdn.../mouse.jpg", "price": 39.99 } }, "notFound": ["c091..."] }` |

### 4.2 Inventory Service

Published events are on `order.lifecycle` topic.

Reservation is a single batch call — one request per order, covering every line item,
not one request per product. The batch is **atomic in effect**: if any item can't be
reserved, Inventory Service releases whatever it had already reserved for the other items
in that same call before responding, so a failed batch never leaves a partial reservation
behind. The per-item `reserved: true/false` flags are diagnostic only — they tell Order
Service which product(s) caused the failure; they don't indicate items Order Service needs
to release itself.

| Request Endpoint | Request Body | Response Body |
|---|---|---|
| `POST /inventory/order-reserve` | `{ "orderId": "c4d2...", "items": [{"productId": "8a2c...", "quantity": 2}, ...] }` | `{ "orderId": "c4d2...", "items": [{"productId": "8a2c...", "available": 10, "reserved": true}, ...] }` |

| Published Event | Payload |
|---|---|
| `Order.NormalCancelled` | `(order_id, user_id, cancelReason, [{product_id, quantity}])` |

### 4.3 Deal Service

| Request Endpoint | Request Body | Expected Action |
|---|---|---|
| `POST /deals/{deal_id}/authorize-slot` | None | increment `authorized_count`, or reject (returns non-2xx) if the deal has already resolved |
| `POST /deals/{deal_id}/release-slot` | None | decrement `reserved_stock` |
| `POST /deals/{deal_id}/release-authorized-slot` | None | decrement `reserved_stock` & `authorized_count` |

| Received Event | Payload | Reaction |
|---|---|---|
| `Deal.Succeeded` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: `SELECT ... WHERE deal_id = ? AND status = 'authorized'`, then per order a guarded `UPDATE ... WHERE id = ? AND status = 'authorized'` → `status = pending_capture`<br>2. Fire `Order.payment_settlement_requested(payment_id, 'CAPTURE')` per order whose guarded update affected a row |
| `Deal.Failed` | `(deal_id, deal_stock, authorized_count)` | 1. Batch: same pattern → `status = pending_void`<br>2. Fire `Order.payment_settlement_requested(order_id, payment_id, 'VOID')` per order whose guarded update affected a row |

### 4.4 Payment Service

- Order Service publishes payment-related events on `order.payments_requested`.
- Order Service receives payment-related events on `Payment.events`.
- Order Service additionally makes one **synchronous** call to Payment Service to fetch a
  card snapshot for display, keyed by `(user_id, payment_method_id)`. The
  call happens once, at order-creation time (DEAL: `Participant.Joined`; NORMAL: checkout).

| Published Event | Payload | Reaction |
|---|---|---|
| `Payment.InitRequired.Charge` | `{user_id, order_id, amount, payment_method_id}` | Charge the amount using `paymentMethodId` |
| `Payment.InitRequired.Authorize` | `{user_id, order_id, amount, payment_method_id}` | Authorize the amount using `paymentMethodId` |
| `Payment.SettlementRequired.Capture` | `{order_id, payment_id}` | Capture the held amount |
| `Payment.SettlementRequired.Void` | `{order_id, payment_id}` | Release the held amount |
| `Payment.Timeout` | `{order_id}` | Cancel a stuck order — fired when no payment outcome ever arrived |

| Sync Call | Request | Response | Reaction |
|---|---|---|---|
| `GET /api/users/{user_id}/payment-methods/{payment_method_id}` | — | `{id, user_id, type, isDefault, card_last4, card_brand, card_exp_month, card_exp_year, card_fingerprint}` | Called once at order-creation time `card_last4`/`card_brand`/`card_exp_month`/`card_exp_year` are stored on the order as part of the same insert. |

| Received Event | Payload | Reaction |
|---|---|---|
| `Payment.Failed` | `(payment_id, order_id, amount, errorMessage, errorCode)` | 1. Get order by `order_id`<br>2. `UPDATE status = cancelled WHERE status IN (pending_charge, pending_authorization)` (guard)<br>3. Set `payment_id` — no card-snapshot fetch here, it was already captured at order-creation time<br>4. Set `payment_error_code`/`payment_error_message` from the event, verbatim — `errorCode` is one of `card_declined`, `incorrect_cvc`, `processing_error`, `expired_card`; `errorMessage` is only populated by Payment Service when `errorCode = card_declined`, otherwise `null`<br>5. DEAL path only: call `release-slot` (sync — `reserved_stock--`; order never reached `authorized`, so `authorized_count` is untouched)<br>6. Fire `Order.NormalCancelled` (NORMAL) or `Order.DealCancelled` (DEAL) |
| `Payment.Charged` | `(payment_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = confirmed WHERE status = pending_charge` (guard)<br>3. Set `payment_id`<br>4. Fire `Order.Created` |
| `Payment.Authorized` | `(payment_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = authorized WHERE status = pending_authorization` (guard)<br>3. Set `payment_id`<br>4. Call `authorize-slot` (sync — `authorized_count++`; deal may flip `succeeded` here)<br>5a. Slot claimed → fire `Order.Authorized`<br>5b. Slot rejected (deal already resolved before this call landed — **late authorization**) → `UPDATE status = pending_void WHERE status = authorized` (guard), `cancel_reason = deal_resolved`, call `release-slot` (not `release-authorized-slot` — Deal Service never counted this slot), fire `Payment.SettlementRequired.Void`; `Order.Authorized` is **not** fired |
| `Payment.Captured` | `(payment_id, order_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = confirmed WHERE status = pending_capture` (guard)<br>3. Set `payment_id` — no card-snapshot re-fetch, same reasoning as above<br>4. Fire `Order.Created` |
| `Payment.Voided` | `(order_id, payment_id, amount)` | 1. Get order by `order_id`<br>2. `UPDATE status = cancelled WHERE status = pending_void` (guard), `cancel_reason` = `deal_failed`, `participant_left`, or `deal_resolved` depending on which path parked the order<br>3. Set `payment_id` — no card-snapshot re-fetch<br>4. Leave path only: call `release-authorized-slot` (sync — `reserved_stock--` and `authorized_count--` atomically; order had reached `authorized` before parking)<br>5. Fire `Order.DealCancelled` |

No sweep re-publishes `Payment.SettlementRequired.Capture`/`Void` for orders stuck in
`pending_capture`/`pending_void` (the settlement sweeper was removed — see §6). Resolution
of those two states depends entirely on Payment Service eventually delivering
`Payment.Captured`/`Payment.Voided` on its own.

### 4.5 Participation Service

Published events are on `order.lifecycle` topic.

| Published Event | Payload |
|---|---|
| `Order.DealCancelled` | `(order_id, deal_id, participant_id, user_id, reason)` |

| Received Event | Payload | Reaction |
|---|---|---|
| `Participant.Joined` | `(participant_id, deal_id, user_id, product_id, price, payment_method_id, address)` | 1. Synchronously call `GET /api/users/{user_id}/payment-methods/{payment_method_id}` on Payment Service and capture the card snapshot (`card_last4`/`card_brand`/`card_exp_month`/`card_exp_year`) — failure aborts the whole handler, no order row is created, message is redelivered<br>2. Create order row, `status = pending_authorization` (+ `order_products` row), with the card snapshot set from step 1 and `address` from the event (`''` if absent — Participation Service doesn't send it yet)<br>3. Fire `Payment.InitRequired.Authorize(user_id, order_id, amount, payment_method_id)` |
| `Participant.Left` | `(participant_id, deal_id)` | 1. Find existing order `WHERE deal_id = ? AND participant_id = ? AND status = authorized` — no new row created<br>2. `UPDATE status = pending_void WHERE status = authorized` (guard — parks the order so deal-resolution batches skip it)<br>3. Fire `Payment.SettlementRequired.Void(order_id, payment_id)` |

### 4.6 Notification Service

Published events are on `order.lifecycle` topic.

| Published Event | Payload |
|---|---|
| `Order.Created` | `{order_id, user_id, items}` |
| `Order.Authorized` | `{deal_id, user_id}` |
| `Order.NormalCancelled` | `(order_id, user_id, cancelReason, [{product_id, quantity}])` |
| `Order.DealCancelled` | `(order_id, deal_id, participant_id, user_id, reason)` |

---

## 5. Normal Order Flow

1. **Receive & validate.** `POST /orders` payload: non-empty `items`, all `quantity > 0`.
   Merge duplicate `productId` entries by summing quantities. Malformed request → `400`,
   nothing else touched.

2. **Catalog lookup.** `POST /products/lookup` with all merged `productIds` in one call.
    - Any `notFound` → `400`. No order row, no reservation, no charge event.
    - `found` entries give the authoritative `price` per line item (never trust a
      client-supplied price).
    - Catalog Service unreachable → `503`, order row never created.

3. **Create order.** Synchronously call `GET /api/users/{user_id}/payment-methods/{payment_method_id}`
   on Payment Service and capture the card snapshot (`card_last4`/`card_brand`/`card_exp_month`/
   `card_exp_year`) — failure aborts checkout entirely, no order row created. One DB
   transaction: insert `orders` (`status='reserving'`, `order_type='NORMAL'`,
   `total_price = Σ(unitPrice × qty)`, `address` from the request (required, `@NotNull`),
   card snapshot from above) + one `order_products` row per merged item (prices from step 2).

4. **Inventory reservation.** Single batch call: `POST /inventory/order-reserve` with the
   `orderId` from step 3 and every merged line item (`{productId, quantity}`) in one
   request — not one call per product.
    - Inventory Service unreachable (no response at all):
      `UPDATE orders SET status='cancelled', cancel_reason='inventory_unreachable' WHERE id=?`,
      write `Order.NormalCancelled` to the outbox (all originally-requested items), commit,
      return `503` immediately.
    - Response received but any item has `reserved: false`: Inventory Service has already
      released any items it reserved for this call before responding,
      so no reservation is left outstanding on Inventory Service's side.
      `UPDATE orders SET status='cancelled', cancel_reason='insufficient_stock' WHERE id=?`,
      write `Order.NormalCancelled` to the outbox listing all originally-requested items,
      commit, return `409` immediately.
    - All items come back `reserved: true` → proceed to step 5.

5. **Charge.** `UPDATE orders SET status='pending_charge' WHERE id=? AND status='reserving'`,
   write `Payment.InitRequired.Charge` to the outbox (`{user_id, order_id, amount,
   payment_method_id}`), commit. Order Service never calls
   Payment Service directly to *initiate* the charge — Payment Service consumes this event,
   charges the card, and publishes `Payment.Charged` or `Payment.Failed` asynchronously.
    - If Order Service's own consumer resolves the order within a short in-request wait
      → return `201` (confirmed) or `402` (declined) synchronously.
    - Otherwise → leave the order as `pending_charge`. Do not cancel/release yet — the
      charge may still succeed on Payment Service's side; releasing now risks confirming a
      payment against stock already sold to someone else. Return `202` immediately.

Resolution is either an inbound event or a sweep-fired outbound event.

**Event consumption:**
- **Case 1 — `Payment.Charged`:** `UPDATE orders SET status='confirmed', payment_id=?
  WHERE id=? AND status='pending_charge'`; if the update affected a row, fire `Order.Created`.
- **Case 2 — `Payment.Failed`:** `UPDATE orders SET status='cancelled',
  cancel_reason='payment_declined' WHERE id=? AND status='pending_charge'`; if the update
  affected a row, set `payment_id`, then fire `Order.NormalCancelled` (`{order_id, user_id,
  cancelReason, [{product_id, quantity}]}`). Inventory Service reacts the same way it reacts
  to every other `Order.NormalCancelled` (§4.2) — Order Service doesn't call `order-release`
  itself.

**Reconciliation sweep**, every ~30s:
- Orders in `pending_charge` past 5 min → fire `Payment.Timeout`.
- Orders in `reserving` past 2 sec → fire `Order.NormalCancelled` with the full item list.

---

## 6. Deal Order Flow

1. **Join.** Consuming `Participant.Joined`: synchronously call
   `GET /api/users/{user_id}/payment-methods/{payment_method_id}` on Payment Service (using
   `payment_method_id` from the event) and capture the card snapshot (`card_last4`/
   `card_brand`/`card_exp_month`/`card_exp_year`) — failure aborts the handler entirely, no
   order row is created and the inbound message is redelivered. Insert `orders`
   (`status='pending_authorization'`, `order_type='DEAL'`, `deal_id`, `participant_id` from
   the event, `address` from the event's `address` field, card snapshot from
   above) + one `order_products` row (`quantity=1`,
   `unit_price` = event's `price`). Write `Payment.InitRequired.Authorize`
   (`{user_id, order_id, amount, payment_method_id}`) to the outbox and commit. No further
   synchronous call to Payment Service after this step — it consumes this event, authorizes
   the hold, and publishes `Payment.Authorized` or `Payment.Failed` asynchronously.

2. **Authorization resolves.**
    - **`Payment.Authorized` consumed:** `UPDATE orders SET status='authorized', payment_id=?
     WHERE id=? AND status='pending_authorization'` (guard). No card-snapshot fetch here — it
      was already captured at join time (step 1). Call `authorize-slot` on Deal Service
      synchronously (`authorized_count++`; this call may flip the deal to `succeeded` on Deal
      Service's side).
        - Slot claimed → fire `Order.Authorized`.
        - Slot rejected (**late authorization** — the deal already resolved before this call
          landed) → `UPDATE orders SET status='pending_void', cancel_reason='deal_resolved'
          WHERE id=? AND status='authorized'` (guard), call `release-slot` (not
          `release-authorized-slot` — Deal Service never counted this slot as authorized),
          fire `Payment.SettlementRequired.Void`. `Order.Authorized` is not fired.
    - **`Payment.Failed` consumed:** `UPDATE orders SET status='cancelled',
     cancel_reason='payment_declined' WHERE id=? AND status='pending_authorization'` (guard).
      Set `payment_id` — no card-snapshot fetch, same reasoning as above. Call `release-slot`
      (`reserved_stock--`; the order never reached `authorized`, so `authorized_count` is
      untouched). Fire `Order.DealCancelled`.

3. **Deal resolves.** Deal Service batches over every order still `authorized` for a
   `deal_id`: `SELECT ... WHERE deal_id = ? AND status = 'authorized'`, then per order a
   guarded `UPDATE ... WHERE id = ? AND status = 'authorized'`:
    - **`Deal.Succeeded` consumed:** per order, guarded `status='authorized' → 'pending_capture'`;
      fire `Payment.SettlementRequired.Capture` (`{order_id, payment_id}`) for each order
      whose update affected a row.
    - **`Deal.Failed` consumed:** per order, guarded `status='authorized' → 'pending_void'`;
      fire `Payment.SettlementRequired.Void` (`{order_id, payment_id}`) for each order whose
      update affected a row.

4. **Participant leaves (concurrent path).** Consuming `Participant.Left`: find the
   existing order `WHERE deal_id=? AND participant_id=? AND status='authorized'` — no new
   row is created. `UPDATE status='pending_void' WHERE status='authorized'` (guard) parks
   the order so a concurrent `Deal.Succeeded`/`Deal.Failed` batch skips it. Fire
   `Payment.SettlementRequired.Void` (`{order_id, payment_id}`).

5. **Settlement resolves.**
    - **`Payment.Captured` consumed:** `UPDATE orders SET status='confirmed', payment_id=?
     WHERE id=? AND status='pending_capture'` (guard). Fire `Order.Created`.
    - **`Payment.Voided` consumed:** `UPDATE orders SET status='cancelled',
     cancel_reason=<'deal_failed'|'participant_left'|'deal_resolved'> WHERE id=? AND
      status='pending_void'` (guard; reason depends on which path parked the order). Leave
      path only: call `release-authorized-slot` (`reserved_stock--` and `authorized_count--`
      atomically — the order had reached `authorized` before parking, unlike the plain
      `release-slot` case used for `deal_resolved`/`Payment.Failed`). Fire
      `Order.DealCancelled`.

**Reconciliation sweep**, every ~30s, threshold is 60s for `pending_authorization` (vs. 5
min/2 sec for NORMAL — deals settle on a much tighter clock):
- Orders in `pending_authorization` past 60s → **force-cancel**: `UPDATE status='cancelled',
  cancel_reason='payment_timeout' WHERE status='pending_authorization'` (guard), call
  `release-slot`, fire `Order.DealCancelled` and `Payment.Timeout` (`{order_id}`).
---

## Appendix: SQL Schema

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
                        address           TEXT NOT NULL, 
                        payment_id        UUID NULL,               
                        card_last4        VARCHAR(4) NULL,        
                        card_brand        VARCHAR(20) NULL,
                        card_exp_month    VARCHAR(2) NULL,          
                        card_exp_year     VARCHAR(4) NULL,          
                        cancel_reason     VARCHAR(30) NULL CHECK (cancel_reason IN (
                                                                                    'INSUFFICIENT_STOCK', 'INVENTORY_UNREACHABLE', 'RESERVATION_INCOMPLETE',
                                                                                    'PAYMENT_DECLINED', 'PAYMENT_TIMEOUT', 'DEAL_FAILED', 'DEAL_RESOLVED',
                                                                                    'PARTICIPANT_LEFT'
                            )),
                        payment_error_code    VARCHAR(50) NULL,    
                        payment_error_message TEXT NULL,            

                        version           INT NOT NULL DEFAULT 0,  
                        created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                        status_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),  

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
-- Sweep jobs key off status_updated_at, not updated_at — updated_at bumps on
-- any column change, e.g. setting payment_id without a status transition.
CREATE INDEX idx_orders_status_status_updated_at ON orders (status, status_updated_at);

-- Keeps updated_at accurate automatically so sweep jobs don't depend on
-- application code remembering to set it on every status change.
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
                                id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                order_id           UUID NOT NULL REFERENCES orders(id),
                                product_id         UUID NOT NULL,
                                quantity           INT NOT NULL CHECK (quantity > 0),   -- always 1 for DEAL, can be >1 for NORMAL
                                unit_price         NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
                                product_name       VARCHAR(255) NULL,    
                                product_image_url VARCHAR(1000) NULL,     
                                created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_products_order_id ON order_products (order_id);

-- =====================================================================
-- outbox_events
--
-- Transactional outbox for every event Order Service publishes: the publish
-- is part of the same commit as the business write. A relay process polls
-- WHERE status = 'PENDING' and delivers to Kafka at-least-once.
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
                               correlation_id  UUID NOT NULL,             -- added V6
                               causation_id    UUID NOT NULL,             -- added V8
                               trace_id        UUID NOT NULL              -- added V8
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