# Order Service — Normal Purchase Flow (Finalized)

## 1. New/Changed Service Contracts

These are additions beyond the original architecture doc, agreed while designing this flow.

### 1.1 Catalog Service — `POST /products/lookup`
```json
// Request
{ "productIds": ["8a2c...", "c091..."] }

// 200 Response
{
  "found": [
    { "id": "8a2c...", "basePrice": 39.99, "sellerId": "sel_..." }
  ],
  "notFound": ["c091..."]
}
```
Any non-empty `notFound` fails the order before any order row, reservation, or payment side
effect occurs — cheapest failure to check first.

### 1.2 Inventory Service — `POST /inventory/{productId}/order-reserve`
Hard-decrements real stock immediately

```json
// Request
{ "orderId": "ord_...", "quantity": 2 }

// 200 Response
{ "productId": "8a2c...", "availableStock": 14 }

// 409 Response
{ "error": "insufficient_stock", "available": 1 }
```
```sql
UPDATE inventory SET stock = stock - ? WHERE product_id = ? AND stock >= ?
```
`orderId` is included specifically so Inventory Service can dedupe `(orderId, productId)` —
if Order Service retries this call after a network timeout without knowing whether the
first attempt landed, dedup prevents a double-decrement. Inventory Service persists a
reservation record per successful `(orderId, productId)`

### 1.3 Inventory Service — `POST /inventory/{productId}/order-release`
```json
// Request
{ "orderId": "ord_...", "quantity": 2 }

// 200 Response
{ "productId": "8a2c...", "availableStock": 16 }
```
**Trigger: event-driven, not a direct call from Order Service.** Inventory Service calls
this itself (internally) whenever it consumes an `order.cancelled` event (§4), for every
`{product_id, quantity}` in that event's payload.

Two idempotency guarantees make this safe against at-least-once event delivery and against
`order.cancelled` covering items that were *requested* but never actually reserved:
- **No matching reservation record** for `(orderId, productId)` → no-op, returns current
  `availableStock` unchanged.
- **Reservation record already released** → increment `availableStock` with the quantity (release)
### 1.4 Payment Service — `POST /payments/charge`
Auth + capture in one atomic call from Order Service's point of view.

```json
// Request
{ "orderId": "ord_...", "amount": 129.97, "idempotencyKey": "ord_..." }

// 200 Response
{ "paymentId": "pay_...", "status": "charged" }

// 402 Response
{ "error": "card_declined", "declineCode": "insufficient_funds" }
```
`idempotencyKey = orderId` always

Publishes `payment.charged` on success, `payment.failed` on decline.

### 1.5 Payment Service — `GET /payments/by-order/{orderId}`
Added specifically for the reconciliation sweep (§4) to resolve orders that timed out with
no event ever arriving.

```json
// 200 Response — intent exists
{ "paymentId": "pay_...", "status": "processing" }
// or
{ "paymentId": "pay_...", "status": "charged" }
// or
{ "paymentId": "pay_...", "status": "declined" }

// 404 Response — no intent exists for this orderId
{ "error": "not_found" }
```


### 1.6 Payment Service — `POST /payments/{paymentId}/cancel`
Added for the reconciliation sweep to force-resolve a payment intent that's been stuck in
`processing` past an acceptable threshold.

```json
// 200 Response
{ "paymentId": "pay_...", "status": "cancelled" }
```
Voids the in-flight intent so it can never later resolve to `charged`. Idempotent — calling
it again on an already-cancelled intent returns the same `200`.

---

## 2. Request/Response Shapes

### `POST /orders`
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

**202 — payment timed out, order left pending**
```json
{
  "id": "ord_...",
  "status": "pending_payment",
  "message": "Order created; payment is still processing."
}
```


**Error responses**
| Status | Cause | Order row |
|---|---|---|
| 400 | empty cart, bad quantity, unknown field, or any `productId` in Catalog's `notFound` | never created |
| 409 | inventory reservation failed for one or more items (insufficient stock) | created, then `cancelled` (`cancel_reason='insufficient_stock'`) |
| 402 | `POST /payments/charge` returned a decline within the timeout window | created, then `cancelled` (`cancel_reason='payment_declined'`) |
| 503 | Catalog Service unreachable during lookup (step 2, before the order row exists) | never created |
| 503 | Inventory Service unreachable during reservation (step 4, after the order row exists) | created, then `cancelled` (`cancel_reason='inventory_unreachable'`) |

---

## 3. Flow — Step by Step

1. **Receive & validate.** `POST /orders` payload: non-empty `items`, all `quantity > 0`.
   Merge any duplicate `productId` entries by summing their quantities. Malformed request
   → `400`, nothing else touched.

2. **Catalog lookup.** `POST /products/lookup` with all (merged) `productIds` in one call.
   - Any `notFound` → `400`. Stop here — no order row, no reservation, no payment call yet.
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
     write `order.cancelled` to the outbox (payload: all originally-requested items —
     Inventory Service's dedup, §1.3, no-ops the ones that were never actually reserved),
     commit, return `409`/`503` immediately. No synchronous rollback call is made.

5. **Charge.** `UPDATE orders SET status='pending_payment' WHERE id=? AND status='reserving'`,
   then `POST /payments/charge` with `idempotencyKey = orderId`.
   - **`200` within timeout** → `UPDATE orders SET status='confirmed', payment_id=? WHERE id=? AND status='pending_payment'`, write `order.created` to the outbox, commit, return `201`.
   - **`402` within timeout** → `UPDATE orders SET status='cancelled', cancel_reason='payment_declined' WHERE id=? AND status='pending_payment'`, write `order.cancelled` to the outbox (payload: all reserved items), commit, return `402`.
   - **No definitive response in time** (timeout, 5xx, or connection failure — see §1.4) → leave the order as `pending_payment`. Do **not** cancel/release yet — the charge may still succeed server-side; releasing now risks confirming a payment against stock that's already been sold to someone else. Return `202` immediately. Resolution moves to the async path (§4).

---

## 4. Async Reconciliation

Order Service subscribes to `payment.charged` and `payment.failed`, and also runs a
periodic sweep — together these resolve orders that timed out at step 5, and orders
orphaned by a crash mid-flow.

**Event consumption**:
- **Case1** — consuming `payment.charged`: `UPDATE orders SET status='confirmed', payment_id=? WHERE id=? AND status='pending_payment'`; if the update affected a row, fire `order.created`.
- **Case2** — consuming `payment.failed`: `UPDATE orders SET status='cancelled', cancel_reason='payment_declined' WHERE id=? AND status='pending_payment'`; if the update affected a row, fire `order.cancelled` (payload: `{order_id, user_id, [{product_id, quantity}]}`). Inventory Service reacts to this the same way it reacts to every other `order.cancelled` (§1.3) — Order Service does not call `order-release` itself here either.

**Reconciliation sweep**, run every ~30s:
- For orders in `pending_payment`: `GET /payments/by-order/{orderId}` (§1.5).
  - `404` (no record found) → the original charge call never landed → retrigger `POST /payments/charge` with the same `idempotencyKey`.
  - `200` with `status=charged`/`declined` → run Case1/Case2 above (the `WHERE status='pending_payment'` guard makes this safe even if the event handler is processing the same order concurrently).
  - `200` with `status=processing` and the order has been `pending_payment` past an agreed staleness threshold *(number TBD, §6)* → `POST /payments/{paymentId}/cancel` (§1.6), then run Case2 with `cancel_reason='payment_stuck'`.
- For orders in `reserving` past a short grace threshold *(number TBD, §6 — this state is normally only held for the duration of step 4 within a single request; persisting past that threshold means the process crashed mid-reservation-loop)*: `UPDATE orders SET status='cancelled', cancel_reason='reservation_incomplete' WHERE id=? AND status='reserving'`, fire `order.cancelled` with the order's full item list. Inventory Service's dedup (§1.3) safely no-ops any item that was never actually reserved before the crash.

---

## 5. Full Branch Summary

| Branch | Order status | Inventory | Payment | Client response | Async event |
|---|---|---|---|---|---|
| Happy path | `confirmed` | committed | charged | `201` | `order.created` |
| Unknown product | never created | untouched | not called | `400` | — |
| Catalog unreachable | never created | untouched | not called | `503` | — |
| Insufficient stock (any item) | created → `cancelled` (`insufficient_stock`) | released async (no-op for never-reserved items) | not called | `409` | `order.cancelled` |
| Inventory Service unreachable mid-reservation | created → `cancelled` (`inventory_unreachable`) | released async | not called | `503` | `order.cancelled` |
| Card declined (within timeout) | created → `cancelled` (`payment_declined`) | released async | declined | `402` | `order.cancelled` |
| Payment timeout → later charged | `pending_payment` → `confirmed` | stays committed | charged (async) | `202` then resolved async | `order.created` |
| Payment timeout → later declined | `pending_payment` → `cancelled` (`payment_declined`) | released async | declined (async) | `202` then resolved async | `order.cancelled` |
| Payment stuck in `processing` past threshold | `pending_payment` → `cancelled` (`payment_stuck`) | released async | force-cancelled (async) | `202` then resolved async | `order.cancelled` |
| Crash mid-reservation loop | `reserving` → `cancelled` (`reservation_incomplete`), caught by sweep | released async | not called | original request already failed/disconnected — no live response | `order.cancelled` |

---

## 6. Open Items Carried Over

- Staleness threshold for force-cancelling a payment intent stuck in `processing` (§4) —
  not yet a number, just a requirement.
- Grace threshold for treating a `reserving` order as crashed/orphaned (§4) — not yet a
  number; needs to comfortably exceed the normal step-4 reservation-loop duration to avoid
  false positives against a merely-slow-but-healthy request.
- General alerting policy for orders stuck in `pending_payment` or `reserving` beyond their
  respective thresholds — not yet defined.
- `payment_id` and `cancel_reason` columns on `orders` (from the base spec) are both used
  throughout this flow and should be treated as required, not optional additions.
  `cancel_reason` now has five values: `insufficient_stock`, `inventory_unreachable`,
  `payment_declined`, `payment_stuck`, `reservation_incomplete`.
- Dedup ownership is now settled as part of this update: Inventory Service is the sole
  source of truth for `(orderId, productId)` state (§1.2/§1.3) — Order Service keeps no
  local outbound-call idempotency table.
