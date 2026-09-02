# Payment Service — Data Model, Broker Contracts & Workflow

This document describes the payment-service contract for Rally in the same style as `order-service-details.md`.

It focuses on the broker-driven payment flow:

- Order Service publishes payment requests.
- Payment Service consumes those requests.
- Payment Service persists state in PostgreSQL.
- Payment Service uses outbox tables for reliability.
- Payment Service publishes payment outcome events back to Kafka.

The selected integration model is:

- `order.payments_requested` for the first step.

The payment service does not need to be called directly by Order over HTTP for the main checkout flow.

---

## 1. Payment DB Schema

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =====================================================================
-- payment_methods
-- =====================================================================

CREATE TABLE payment_methods (
    id               UUID NOT NULL PRIMARY KEY,
    user_id          UUID NOT NULL,
    type             VARCHAR(50) NOT NULL,     -- CARD, WALLET, etc.
    token            VARCHAR(500) NOT NULL,    -- Stripe payment method id or vault token
    is_default       BOOLEAN NOT NULL DEFAULT FALSE,
    card_brand       VARCHAR(50),
    card_last4       VARCHAR(4),
    card_exp_month   VARCHAR(2),
    card_exp_year    VARCHAR(4),
    card_fingerprint VARCHAR(255),
    version          BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_payment_methods_user_id ON payment_methods (user_id);

-- Only one default payment method per user.
CREATE UNIQUE INDEX uq_payment_methods_one_default_per_user
    ON payment_methods (user_id)
    WHERE is_default = TRUE;

-- =====================================================================
-- payments
-- =====================================================================

CREATE TABLE payments (
    id                 UUID NOT NULL PRIMARY KEY,
    user_id            UUID NOT NULL,
    order_id           UUID NOT NULL,
    payment_method_id  UUID REFERENCES payment_methods(id) ON DELETE SET NULL,
    payment_intent_id  VARCHAR(255),
    amount             NUMERIC(18,2) NOT NULL,
    status             VARCHAR(50) NOT NULL,
    failure_reason     VARCHAR(500),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    authorized_at      TIMESTAMPTZ,
    captured_at        TIMESTAMPTZ,
    charged_at         TIMESTAMPTZ,
    failed_at          TIMESTAMPTZ,
    voided_at          TIMESTAMPTZ,
    version            BIGINT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX uq_payments_order_id ON payments (order_id);
CREATE INDEX idx_payments_user_id ON payments (user_id);
CREATE INDEX idx_payments_payment_method_id ON payments (payment_method_id);
CREATE INDEX idx_payments_payment_intent_id ON payments (payment_intent_id);
CREATE INDEX idx_payments_status ON payments (status);

-- =====================================================================
-- outbox_messages
-- =====================================================================

CREATE TABLE outbox_messages (
    message_id      UUID PRIMARY KEY,
    aggregate_id    UUID NOT NULL,
    aggregate_type  VARCHAR(100) NOT NULL,
    topic           VARCHAR(100) NOT NULL,
    message_key     VARCHAR(255), -- not used yet; single partition setup
    message_type    VARCHAR(100) NOT NULL,
    correlation_id  UUID,
    causation_id    VARCHAR(255),
    trace_id        VARCHAR(64),
    payload         JSONB NOT NULL,
    headers         JSONB,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX idx_outbox_status_created_at
    ON outbox_messages (status, created_at);

CREATE INDEX idx_outbox_topic
    ON outbox_messages (topic);

CREATE INDEX idx_outbox_aggregate_id
    ON outbox_messages (aggregate_id);

CREATE INDEX idx_outbox_correlation
    ON outbox_messages (correlation_id);

-- =====================================================================
-- inbox_messages
-- =====================================================================

CREATE TABLE inbox_messages (
    message_id      VARCHAR(255) PRIMARY KEY,
    topic           VARCHAR(100) NOT NULL,
    message_type    VARCHAR(100) NOT NULL,
    correlation_id  UUID,
    causation_id    VARCHAR(255),
    trace_id        VARCHAR(64),
    payload         JSONB NOT NULL,
    headers         JSONB,
    status          VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX idx_inbox_status_received_at
    ON inbox_messages (status, received_at);

CREATE INDEX idx_inbox_processed_at
    ON inbox_messages (processed_at)
    WHERE processed_at IS NOT NULL;

CREATE INDEX idx_inbox_topic
    ON inbox_messages (topic);

CREATE INDEX idx_inbox_correlation
    ON inbox_messages (correlation_id);

-- =====================================================================
-- payment_profiles
-- =====================================================================

CREATE TABLE payment_profiles (
    id                 UUID PRIMARY KEY,
    user_id            UUID NOT NULL UNIQUE,
    stripe_customer_id VARCHAR(255) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    version            BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_payment_profiles_user_id
    ON payment_profiles (user_id);
```

---

## 2. Payment Domain Model

### 2.1 `Payment`

The `Payment` aggregate owns the lifecycle of a payment for a single order.

Key fields:

- `id`
- `userId`
- `orderId`
- `paymentMethodId`
- `paymentIntentId`
- `stripeCustomerId`
- `amount`
- `status`
- `failureReason`
- timestamps for key transitions
- optimistic locking `version`

Key behaviors:

- `initialize(...)`
- `charge(...)`
- `authorize(...)`
- `capture()`
- `voidPayment(...)`
- `fail(...)`
- `requireAdditionalAction(...)`
- `linkToStripeCustomer(...)`
- `linkToStripePaymentIntent(...)`
- `suppressDomainEvents(...)`

### 2.2 `PaymentMethod`

Represents a buyer-owned payment method.

Key fields:

- `id`
- `userId`
- `type`
- `token`
- `isDefault`
- `cardFingerprint`
- embedded card metadata
- `version`

The service stores only non-sensitive metadata such as brand, last four digits, and the card fingerprint, plus the Stripe payment method id (`token`).

### 2.3 `OutboxMessage`

Stores payment outcome events that will later be published to Kafka by the outbox relay.

### 2.4 `InboxMessage`

Stores incoming broker messages for idempotent processing.

Key fields:

- `messageId` — unique message identifier (`X-Id` header value) used for deduplication
- `topic` — Kafka topic the message was consumed from
- `messageType` — message type (`X-Type` header value)
- `correlationId` — links the message to the business flow
- `causationId` — always null; the payment service neither consumes nor populates a causation id
- `traceId` — distributed tracing identifier
- `payload` — the deserialized message payload (JSONB)
- `headers` — the raw Kafka record headers (JSONB)
- `status` — `RECEIVED` or `PROCESSED`
- `receivedAt` / `processedAt` — lifecycle timestamps
- `retryCount` / `maxRetries` — retry tracking
- `lastError` — error message if processing failed

### 2.5 `PaymentProfile`

Maps a buyer to their Stripe Customer ID. Created lazily on first payment interaction.

Key fields:

- `id`
- `userId` — unique; one profile per buyer
- `stripeCustomerId` — the Stripe Customer object ID (`cus_...`)
- `version`

---

## 3. Payment Status Machine

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> AUTHORIZED: init / AUTHORIZE
    PENDING --> CHARGED: init / CHARGE
    AUTHORIZED --> CAPTURED: settlement / CAPTURE
    AUTHORIZED --> VOIDED: settlement / VOID
    PENDING --> FAILED: gateway failure
    AUTHORIZED --> FAILED: gateway failure
    CHARGED --> [*]
    CAPTURED --> [*]
    VOIDED --> [*]
    FAILED --> [*]
    AUTHORIZED --> REQUIRES_ACTION: gateway says 3DS / action needed
    CHARGED --> REFUNDED: refund.created (full)
    CHARGED --> PARTIALLY_REFUNDED: refund.created (partial)
    CAPTURED --> REFUNDED: refund.created (full)
    CAPTURED --> PARTIALLY_REFUNDED: refund.created (partial)
```

Practical meaning:

- `PENDING` means the payment has been created locally but is not yet resolved.
- `AUTHORIZED` means the card is held and ready for settlement.
- `CHARGED` means immediate purchase succeeded.
- `CAPTURED` means a previously authorized payment has been settled.
- `VOIDED` means a held payment was cancelled before capture.
- `FAILED` means the payment attempt did not complete successfully.
- `REQUIRES_ACTION` means customer action is required before completion.
- `REFUNDED` means a fully refunded (captured or charged) payment.
- `PARTIALLY_REFUNDED` means a partially refunded (captured or charged) payment.

---

## 4. Kafka Topic Contracts

The order service publishes payment requests. The payment service consumes those requests and publishes outcome events.

### 4.1 Message envelope rule

Every brokered message, whether consumed or published, must include these headers:

| Header | Required | Purpose |
|---|---|---|
| `X-Id` | yes | Unique message identifier used for message dedupe |
| `X-Type` | yes | Message type and operation selector |
| `X-Correlation-Id` | yes | Links the message to the business flow |
| `traceparent` | no | W3C trace context used for distributed tracing |

The payload must not contain any of those fields.

For payment requests and payment outcome events, the operation or message type must be selected by `X-Type`, not by a payload field.

The payload must never carry `status` or any enum value in either inbound or outbound messages.

Recommended `X-Type` values for request messages:

- `Payment.InitRequired.Charge`
- `Payment.InitRequired.Authorize`
- `Payment.SettlementRequired.Capture`
- `Payment.SettlementRequired.Void`
- `Payment.Timeout`

Recommended `X-Type` values for outcome messages (the ones actually published to `payment.events`):

- `Payment.Authorized`
- `Payment.Charged`
- `Payment.Captured`
- `Payment.Failed`
- `Payment.Voided`

Note: `Payment.Initialized` and `Payment.Refunded` are raised as internal domain events but are **not** published to Kafka. `Payment.RequiresAction` is mapped to `Payment.Failed` at orchestration time and is not emitted as its own outbox event.

### 4.2 Order Service -> Payment Service

| Topic | Purpose | `X-Type` selector |
|---|---|---|
| `order.payments_requested` | First step of checkout | `X-Type = Payment.InitRequired.Charge` or `Payment.InitRequired.Authorize` |
| `order.payments_requested` | Final settlement step | `X-Type = Payment.SettlementRequired.Capture` or `Payment.SettlementRequired.Void` |
| `order.payments_requested` | Order timed out and payment must be cancelled | `X-Type = Payment.Timeout` |

### 4.3 Payment Service -> Order Service

| Topic | Purpose |
|---|---|
| `payment.events` | Payment outcome events consumed by Order Service |

---

## 5. Message Payload Schemas

### 5.1 Payment Initiation Requested

The order service sends this when a payment should start.

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.InitRequired.Charge` or `Payment.InitRequired.Authorize` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "userId": "uuid",
  "orderId": "uuid",
  "paymentMethodId": "uuid",
  "amount": 125.50
}
```

This payload intentionally contains no `status` or enum field.

Interpretation:

- `X-Type = Payment.InitRequired.Charge` for normal purchase
- `X-Type = Payment.InitRequired.Authorize` for deal participation or delayed settlement flows

### 5.2 Payment Settlement Requested

The order service sends this when a resolved payment must be finalized.

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.SettlementRequired.Capture` or `Payment.SettlementRequired.Void` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "paymentId": "uuid",
  "orderId": "uuid"
}
```

This payload intentionally contains no `status` or enum field.

Interpretation:

- `X-Type = Payment.SettlementRequired.Capture`
- `X-Type = Payment.SettlementRequired.Void`

### 5.3 Payment Timeout Received

The order service sends this when the order has timed out and the payment service must cancel the payment for that order.

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.Timeout` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "orderId": "uuid"
}
```

This payload intentionally contains no `status` or enum field.

Interpretation:

- `X-Type = Payment.Timeout` means the payment service must cancel the payment / Stripe PaymentIntent for the specified `orderId`.

### 5.4 Payment Outcome Events

#### `payment.authorized`

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.Authorized` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "paymentId": "uuid",
  "orderId": "uuid",
  "paymentIntentId": "pi_...",
  "amount": 125.50,
}
```

`X-Type` declares the message type; the payload does not carry a `status` or enum.

#### `payment.charged`

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.Charged` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "paymentId": "uuid",
  "orderId": "uuid",
  "paymentIntentId": "pi_...",
  "amount": 125.50,
}
```

`X-Type` declares the message type; the payload does not carry a `status` or enum.

#### `payment.captured`

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.Captured` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "paymentId": "uuid",
  "orderId": "uuid",
  "paymentIntentId": "pi_...",
  "amount": 125.50,
}
```

`X-Type` declares the message type; the payload does not carry a `status` or enum.

#### `payment.failed`

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.Failed` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "paymentId": "uuid",
  "orderId": "uuid",
  "paymentIntentId": "pi_...",
  "amount": 125.50,
  "errorCode": "card_declined",
  "errorMessage": "The card was declined"
}
```

#### `payment.voided`

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.Voided` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload:

```json
{
  "paymentId": "uuid",
  "orderId": "uuid",
  "paymentIntentId": "pi_...",
  "amount": 125.50,
}
```

`X-Type` declares the message type; the payload does not carry a `status` or enum.

#### `payment.requires_action`

> Not emitted as its own outbox event. When Stripe returns `requires_action` (3DS/SCA), the payment service maps the payment directly to `FAILED` in the orchestration flow (3DS/SCA is not supported in this version) and publishes `payment.failed` instead.

For reference, the internal `PaymentRequiresAction` domain event is raised but never written to the outbox:

Headers:

| Header | Example |
|---|---|
| `X-Type` | `Payment.RequiresAction` |
| `X-Id` | `uuid` |
| `X-Correlation-Id` | `uuid` |
| `traceparent` | W3C trace context (optional, used for distributed tracing) |

Payload (internal domain event only; never published):

```json
{
  "paymentId": "uuid",
  "orderId": "uuid",
  "paymentIntentId": "pi_..."
}
```

`X-Type` declares the message type; the payload does not carry a `status` or enum.

### 5.5 Required Headers

Both inbound and outbound messages use the same set of headers:

| Header | Purpose |
|---|---|
| `X-Id` | Unique message id for message dedupe |
| `X-Type` | Event or command type |
| `X-Correlation-Id` | Correlates payment request with the order flow |
| `traceparent` | Optional W3C trace context (distributed tracing) |

The payment service does **not use an `X-Causation-Id` header** at all — neither the messages it consumes nor the events it publishes carry one.

The persistence models mirror these headers:

- `message_id` stores `X-Id`
- `correlation_id` stores `X-Correlation-Id`
- `causation_id` is always left null (the service does not consume or produce a causation header)
- `trace_id` stores the trace id (derived from `traceparent` on inbound, or the current Micrometer trace on outbound)

### 5.6 Payload rule

The payload for every message must contain only business data.

The payload must not contain:

- message id
- correlation id
- causation id
- trace id
- type

Those belong only in headers.

---

## 6. Flows

### 6.1 Normal Purchase

```mermaid
sequenceDiagram
    participant O as Order Service
    participant P as Payment Service
    participant S as Stripe
    participant X as Outbox

    O->>P: order.payments_requested [X-Type=Payment.InitRequired.Charge]
    P->>S: create and confirm PaymentIntent
    S-->>P: succeeded or failed
    P->>P: update Payment
    P->>X: write payment.charged or payment.failed
    P-->>O: Kafka outcome event
    O->>O: confirm or cancel order
```

Typical sequence:

1. Order Service creates the order and publishes `order.payments_requested` with `X-Type=Payment.InitRequired.Charge`.
2. Payment Service loads the payment method and creates the Stripe PaymentIntent.
3. If Stripe confirms the payment, Payment Service marks the payment as `CHARGED`.
4. Payment Service writes `payment.charged` to the outbox.
5. Order Service consumes `payment.charged` and confirms the order.

If Stripe declines:

1. Payment Service marks the payment `FAILED`.
2. Payment Service writes `payment.failed` to the outbox.
3. Order Service cancels the order and releases inventory.

### 6.2 Deal Authorization

```mermaid
sequenceDiagram
    participant O as Order Service
    participant P as Payment Service
    participant S as Stripe
    participant X as Outbox

    O->>P: order.payments_requested [X-Type=Payment.InitRequired.Authorize]
    P->>P: save inbox message
    P->>S: create manual-capture PaymentIntent
    S-->>P: authorized or failed
    P->>P: update Payment
    P->>X: write payment.authorized or payment.failed
    P-->>O: Kafka outcome event
```

Typical sequence:

1. Order Service creates a deal order in pending authorization state.
2. It publishes `order.payments_requested` with `X-Type=Payment.InitRequired.Authorize`.
3. Payment Service authorizes the amount with Stripe.
4. Payment Service publishes `payment.authorized`.
5. Order Service moves the order to `AUTHORIZED`.

### 6.3 Deal Settlement

```mermaid
sequenceDiagram
    participant O as Order Service
    participant P as Payment Service
    participant S as Stripe
    participant X as Outbox

    O->>P: order.payments_requested [X-Type=Payment.SettlementRequired.Capture]
    P->>P: save inbox message
    P->>S: capture PaymentIntent
    S-->>P: succeeded or failed
    P->>P: update Payment
    P->>X: write payment.captured or payment.failed
    P-->>O: Kafka outcome event
```

For void:

1. Order Service publishes `order.payments_requested` with `X-Type=Payment.SettlementRequired.Void`.
2. Payment Service cancels the authorized intent.
3. Payment Service publishes `payment.voided`.
4. Order Service cancels the order.

### 6.4 Payment Timeout Flow

```mermaid
sequenceDiagram
    participant O as Order Service
    participant P as Payment Service
    participant S as Stripe
    participant X as Outbox

    O->>P: order.payments_requested [X-Type=Payment.Timeout]
    P->>P: save inbox message
    P->>S: cancel PaymentIntent (if active)
    S-->>P: cancelled
    P->>P: update Payment status to FAILED/VOIDED
    P->>X: write payment.voided or payment.failed (if outcome event needed)
```

Typical sequence:

1. Order Service's reconciliation sweeper identifies an order stuck in `pending_charge` (> 5 min) or `pending_authorization` (> 60s).
2. Order Service publishes an event to `order.payments_requested` with header `X-Type = Payment.Timeout` and payload `{ "orderId": "uuid" }`.
3. Payment Service receives the message and persists it in `inbox_messages` for deduplication.
4. Payment Service looks up the payment for `orderId`. If a Stripe PaymentIntent exists and is active, Payment Service cancels it with Stripe.
5. Payment Service updates the local payment status to `VOIDED` or `FAILED`.

### 6.5 Inbox Reliability

```mermaid
flowchart LR
    A[Kafka message] --> B[Read X-Id header]
    B --> C{Message already in inbox?}
    C -- yes --> D[No-op]
    C -- no --> E[Insert inbox row]
    E --> F[Apply payment state change]
    F --> G[Write outbox event]
    G --> H[Mark inbox processed]
```

Rules:

- A message is processed at most once per `X-Id`.
- Duplicate redelivery must be a no-op.
- Payment state changes and outbox writes happen in the same transaction.
- Inbox and outbox entries must preserve the header envelope fields.

### 6.6 Outbox Relay

```mermaid
flowchart LR
    A[Outbox row PENDING] --> B[Relay polls batch]
    B --> C[Send to Kafka]
    C --> D{acknowledged?}
    D -- yes --> E[Mark PUBLISHED]
    D -- no --> F[Increment retry_count]
    F --> G{max retries reached?}
    G -- no --> A
    G -- yes --> H[Mark FAILED]
```

---

## 7. API Surface

The broker-driven flow is the primary path, but the service still exposes supporting APIs for setup and inspection. All buyer-scoped endpoints read the caller identity from the `X-User-Id` request header.

### 7.1 Payment Methods

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/payment-methods` | List the buyer's saved payment methods (masked) |
| `GET` | `/api/payment-methods/{methodId}` | Get one of the buyer's saved payment methods |
| `POST` | `/api/payment-methods/setup-intent` | Start adding a card via a Stripe SetupIntent (returns client secret) |
| `POST` | `/api/payment-methods` | Confirm a completed SetupIntent and save the payment method (returns `201 Created`, or `200 OK` if the card was already saved) |
| `PUT` | `/api/payment-methods/{methodId}/default` | Set the buyer's default payment method |
| `DELETE` | `/api/payment-methods/{methodId}/default` | Clear the default flag (no silent promotion) |
| `DELETE` | `/api/payment-methods/{methodId}` | Hard-delete a saved payment method |

### 7.2 Payment Status

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/payments` | Create a payment locally (resolve it via the broker `order.payments_requested` flow) |
| `GET` | `/api/payments/{paymentId}` | Inspect payment state |
| `GET` | `/api/payments/user/{userId}` | Inspect payments for a user (caller `X-User-Id`, with path override available) |
| `GET` | `/api/payments/order/{orderId}` | Inspect the payment(s) for an order |
| `POST` | `/api/payments/{paymentId}/authorize` | Internal or operator-assisted authorization |
| `POST` | `/api/payments/{paymentId}/capture` | Internal or operator-assisted settlement capture |
| `POST` | `/api/payments/{paymentId}/fail` | Internal recovery endpoint to mark a payment failed |
| `POST` | `/api/payments/{paymentId}/void` | Internal or operator-assisted void |

### 7.3 Broker Intake

| Method | Path | Purpose |
|---|---|---|
| `POST` | `${stripe.webhook-path}` (default `/api/v1/payments/webhook`) | Verify Stripe signatures and ingest webhook events |

### 7.4 Reliability and Ops (Actuator)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/actuator/health` | Application and dependency health |
| `GET` | `/actuator/info` | Build and deployment metadata |
| `GET` | `/actuator/metrics` | Runtime metrics |
| `GET` | `/actuator/prometheus` | Prometheus scrape endpoint |

### 7.5 API documentation

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api-docs` | OpenAPI JSON description |
| `GET` | `/swagger-ui.html` | Swagger UI for interactive API exploration |

---

## 8. Payment Service Responsibilities

The payment service owns:

- payment method persistence
- payment state transitions
- inbox deduplication
- outbox publishing
- Stripe interaction
- payment outcome publication to Order Service

The payment service does not own:

- order state transitions
- inventory reservation
- deal lifecycle rules
- user-facing checkout orchestration

---

## 9. Notes on Naming

For Rally, keep the contract names aligned with Order Service.

### Topic Names

- `order.payments_requested`
- `payment.events`

### Inbound Message `X-Type` Values

- `Payment.InitRequired.Charge`
- `Payment.InitRequired.Authorize`
- `Payment.SettlementRequired.Capture`
- `Payment.SettlementRequired.Void`
- `Payment.Timeout`


### Outbound Message `X-Type` Values

- `Payment.Authorized`
- `Payment.Charged`
- `Payment.Captured`
- `Payment.Failed`
- `Payment.Voided`

(Note: `Payment.Initialized`, `Payment.RequiresAction`, and `Payment.Refunded` are internal-only domain events and are not published to `payment.events`.)

Keep topic names lowercase and domain-oriented. Keep `X-Type` values as the message contract identifiers, and keep enum casing only in code-level models where needed.

---

## 10. Summary

The payment service is a broker-driven, Stripe-backed, inbox/outbox protected microservice.

Its job is to:

1. receive payment commands from Order,
2. resolve them against Stripe,
3. store the outcome safely,
4. publish canonical payment events,
5. and let Order finish the checkout lifecycle.
