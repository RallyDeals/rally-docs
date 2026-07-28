# Inventory Service Design

## 1. Overview

### Purpose

The **Inventory Service** is responsible for managing product inventory across the platform.

It maintains the total, reserved, and available stock for each product, reserves inventory when new deals are created, releases reserved inventory when deals are cancelled or expire, deducts stock after successful deals or completed orders, handles product restocking, and publishes inventory-related events to keep other services synchronized.

The Inventory Service is the **single source of truth for product inventory**, maintaining the current inventory state while synchronizing with the Deal Service through events to reflect reservation activities.

---

### Responsibilities

- Create an inventory record for newly created products.
- Maintain the total, reserved, and available stock for each product.
- Reserve inventory when a new deal is created.
- Release reserved inventory when a deal is cancelled or expires.
- Deduct reserved stock after a successful deal.
- Deduct stock after a completed direct order.
- Handle product restocking.
- Maintain inventory movement history for auditing and tracking.
- Publish inventory-related events to notify other services of stock changes.

---


## 2. Database Schema

### Inventory

Stores the inventory status for each product, including total, reserved, and available stock.

| Field | Type | Description |
|-------|------|-------------|
| productId | UUID / Long | Product identifier (Primary Key) |
| totalStock | Integer | Total quantity of the product in inventory |
| reservedStock | Integer | Quantity currently reserved by active deals |
| availableStock | Integer | Quantity available for new reservations (`totalStock - reservedStock`) |
| updatedAt | Timestamp | Last inventory update timestamp |
| version | Long | Version used for optimistic locking |

#### Entity

```text
Inventory
-----------------------------------
productId         PK
totalStock
reservedStock
availableStock
updatedAt
version
```

---

### InventoryHistory

Stores every inventory movement for auditing and tracking purposes.

| Field | Type | Description |
|-------|------|-------------|
| historyId | UUID | History record identifier (Primary Key) |
| productId | UUID / Long | Related product |
| quantityChange | Integer | Positive or negative quantity change |
| movementType | Enum | Type of inventory movement |
| referenceId | UUID / Long | Related Deal ID, Order ID, or Restock ID |
| createdAt | Timestamp | Time of the inventory change |

#### Entity

```text
InventoryHistory
-------------------------------------
historyId         PK
productId         FK
quantityChange
movementType
referenceId
createdAt
```

---

### Movement Types

The `movementType` field can have one of the following values:

| Value | Description |
|-------|-------------|
| `PRODUCT_CREATED` | Initial inventory created when a product is added |
| `DEAL_RESERVED` | Inventory reserved for a newly created deal |
| `DEAL_RELEASED` | Reserved inventory released after deal cancellation or expiration |
| `DEAL_SUCCESS` | Reserved inventory converted into a completed sale |
| `ORDER_COMPLETED` | Stock deducted after a completed direct order |
| `RESTOCK` | Stock increased after a restock operation |
| `MANUAL_ADJUSTMENT` | Manual inventory correction by an administrator |

---

### Relationship

```text
                 +----------------------------------+
                 |            Inventory             |
                 +----------------------------------+
                 | productId (PK)                  |
                 | totalStock                      |
                 | reservedStock                   |
                 | availableStock                  |
                 | updatedAt                       |
                 | version                         |
                 +---------------+-----------------+
                                 |
                    1            |             *
                                 |
                 +---------------v----------------+
                 |       InventoryHistory         |
                 +--------------------------------+
                 | historyId (PK)                |
                 | productId (FK)                |
                 | quantityChange                |
                 | movementType                  |
                 | referenceId                   |
                 | createdAt                     |
                 +--------------------------------+
```

## 3. Consumed Events

The Inventory Service subscribes to the following events published by other microservices.

---

### 3.1 ProductCreated

**Publisher:** Product Service

**Purpose**

Create an inventory record for a newly created product.

#### Payload

```json
{
  "productId": 101,
  "initialStock": 100
}
```

#### Inventory Action

- Create a new inventory record.
- Initialize:
  - `totalStock = initialStock`
  - `reservedStock = 0`
  - `availableStock = initialStock`
- Add a `PRODUCT_CREATED` record to `InventoryHistory`.

---

### 3.2 DealCreated

**Publisher:** Deal Service

**Purpose**

Reserve inventory for a newly created deal.

#### Payload

```json
{
  "dealId": 25,
  "productId": 101,
  "quantity": 40
}
```

#### Inventory Action

- Validate that `availableStock >= quantity`.
- Increase `reservedStock` by `quantity`.
- Decrease `availableStock` by `quantity`.
- Add a `DEAL_RESERVED` record to `InventoryHistory`.
- Publish `InventoryUpdated`.

---

### 3.3 DealCancelled

**Publisher:** Deal Service

**Purpose**

Release the inventory reserved for a cancelled deal.

#### Payload

```json
{
  "dealId": 25,
  "productId": 101,
  "quantity": 40
}
```

#### Inventory Action

- Decrease `reservedStock` by `quantity`.
- Increase `availableStock` by `quantity`.
- Add a `DEAL_RELEASED` record to `InventoryHistory`.
- Publish `InventoryUpdated`.
- Publish `InventoryRestocked`.

---

### 3.4 DealExpired

**Publisher:** Deal Service

**Purpose**

Release the inventory reserved for an expired deal.

#### Payload

```json
{
  "dealId": 25,
  "productId": 101,
  "quantity": 40
}
```

#### Inventory Action

- Decrease `reservedStock` by `quantity`.
- Increase `availableStock` by `quantity`.
- Add a `DEAL_RELEASED` record to `InventoryHistory`.
- Publish `InventoryUpdated`.
- Publish `InventoryRestocked`.

---

### 3.5 DealSucceeded

**Publisher:** Deal Service

**Purpose**

Convert reserved inventory into a completed sale.

#### Payload

```json
{
  "dealId": 25,
  "productId": 101,
  "quantity": 40
}
```

#### Inventory Action

- Decrease `totalStock` by `quantity`.
- Decrease `reservedStock` by `quantity`.
- Keep `availableStock` unchanged.
- Add a `DEAL_SUCCESS` record to `InventoryHistory`.
- Publish `InventoryUpdated`.
- Publish `InventoryOutOfStock` if `availableStock == 0`.

---

### 3.6 OrderCreated

**Publisher:** Order Service

**Purpose**

Reserve inventory for a newly created order until the payment process is completed.

#### Payload

```json
{
  "orderId": 70,
  "productId": 101,
  "quantity": 2
}
```

#### Inventory Action

- Validate that `availableStock >= quantity`.
- Increase `reservedStock` by `quantity`.
- Decrease `availableStock` by `quantity`.
- Add an `ORDER_RESERVED` record to `InventoryHistory`.
- Publish `InventoryUpdated`.

---

### 3.7 OrderCancelled

**Publisher:** Order Service

**Purpose**

Release the inventory reserved for an order that was cancelled or whose payment failed.

#### Payload

```json
{
  "orderId": 70,
  "productId": 101,
  "quantity": 2
}
```

#### Inventory Action

- Decrease `reservedStock` by `quantity`.
- Increase `availableStock` by `quantity`.
- Add an `ORDER_RELEASED` record to `InventoryHistory`.
- Publish `InventoryUpdated`.
- Publish `InventoryRestocked`.

---

### 3.8 OrderCompleted

**Publisher:** Order Service

**Purpose**

Convert reserved inventory into a completed purchase after successful payment.

#### Payload

```json
{
  "orderId": 70,
  "productId": 101,
  "quantity": 2
}
```

#### Inventory Action

- Decrease `totalStock` by `quantity`.
- Decrease `reservedStock` by `quantity`.
- Keep `availableStock` unchanged.
- Add an `ORDER_COMPLETED` record to `InventoryHistory`.
- Publish `InventoryUpdated`.
- Publish `InventoryOutOfStock` if `availableStock == 0`.

---

## 4. Produced Events

The Inventory Service publishes the following events to notify other services about inventory changes.

---

### 4.1 InventoryUpdated

**Consumers**

- Deal Service
- Product Service *(optional)*
- Search Service *(optional)*
- Analytics Service *(optional)*

**Purpose**

Notify other services whenever the inventory state changes due to a reservation, reservation release, completed sale, restock, or manual adjustment.

#### Payload

```json
{
  "productId": 101,
  "totalStock": 100,
  "reservedStock": 40,
  "availableStock": 60,
  "updatedAt": "2026-07-15T18:30:00Z"
}
```

**Published After**

- DealCreated
- DealCancelled
- DealExpired
- DealSucceeded
- OrderCompleted
- Inventory Restock
- Manual Inventory Adjustment

---

### 4.2 InventoryOutOfStock

**Consumers**

- Deal Service
- Product Service

**Purpose**

Notify that no inventory is currently available for new reservations or purchases.

#### Payload

```json
{
  "productId": 101,
  "availableStock": 0,
  "updatedAt": "2026-07-15T18:30:00Z"
}
```

**Published When**

- `availableStock == 0`

---

### 4.3 InventoryRestocked

**Consumers**

- Deal Service

**Purpose**

Notify that inventory has increased, allowing pending or previously unavailable deals to reserve stock again.

#### Payload

```json
{
  "productId": 101,
  "addedQuantity": 50,
  "totalStock": 150,
  "reservedStock": 40,
  "availableStock": 110,
  "updatedAt": "2026-07-15T19:00:00Z"
}
```

**Published After**

- Successful inventory restock operation.


## 5. REST APIs

The Inventory Service exposes the following REST APIs for inventory management.

---

### 5.1 Get Product Inventory

Retrieve the current inventory information for a specific product.

| Method | Endpoint |
|--------|----------|
| GET | `/api/v1/inventory/{productId}` |

#### Path Parameters

| Parameter | Type | Description |
|----------|------|-------------|
| productId | Long / UUID | Product identifier |

#### Response (200 OK)

```json
{
  "productId": 101,
  "totalStock": 150,
  "reservedStock": 40,
  "availableStock": 110,
  "updatedAt": "2026-07-15T18:30:00Z"
}
```

---

### 5.2 Restock Inventory

Increase the total inventory of a product.

| Method | Endpoint |
|--------|----------|
| PATCH | `/api/v1/inventory/{productId}/restock` |

#### Path Parameters

| Parameter | Type | Description |
|----------|------|-------------|
| productId | Long / UUID | Product identifier |

#### Request Body

```json
{
  "quantity": 50
}
```

#### Response (200 OK)

```json
{
  "message": "Inventory restocked successfully."
}
```

#### Inventory Action

- Increase `totalStock` by `quantity`.
- Increase `availableStock` by `quantity`.
- Add a `RESTOCK` record to `InventoryHistory`.

#### Events Published

- `InventoryUpdated`
- `InventoryRestocked`

---

### 5.3 Adjust Inventory

Manually adjust inventory quantities. Intended for administrative operations such as correcting inventory discrepancies or removing damaged items.

| Method | Endpoint |
|--------|----------|
| PATCH | `/api/v1/inventory/{productId}/adjust` |

#### Path Parameters

| Parameter | Type | Description |
|----------|------|-------------|
| productId | Long / UUID | Product identifier |

#### Request Body

```json
{
  "adjustment": -5,
  "reason": "DAMAGED_ITEMS"
}
```

> **Note:** `adjustment` can be positive or negative.

#### Response (200 OK)

```json
{
  "message": "Inventory adjusted successfully."
}
```

#### Inventory Action

- Update `totalStock`.
- Recalculate `availableStock = totalStock - reservedStock`.
- Add a `MANUAL_ADJUSTMENT` record to `InventoryHistory`.

#### Events Published

- `InventoryUpdated`
- `InventoryOutOfStock` *(if `availableStock == 0`)*

---

## API Summary

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/inventory/{productId}` | Retrieve inventory details for a product |
| PATCH | `/api/v1/inventory/{productId}/restock` | Increase the inventory of a product |
| PATCH | `/api/v1/inventory/{productId}/adjust` | Manually adjust inventory (Admin only) |