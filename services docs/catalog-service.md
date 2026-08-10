# Catalog Service — Data Model, API & Admin Moderation Flow

## 1. Catalog DB Schema

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =====================================================================
-- categories
-- =====================================================================

CREATE TABLE categories (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =====================================================================
-- products
-- =====================================================================

CREATE TABLE products (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seller_id         UUID NOT NULL,
    name              VARCHAR(255) NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    category_id       UUID REFERENCES categories(id),
    base_price        NUMERIC(10,2) NOT NULL CHECK (base_price >= 0),
    image_url         VARCHAR(500),

    status            VARCHAR(20) NOT NULL DEFAULT 'PENDING_APPROVAL'
                          CHECK (status IN (
                              'PENDING_APPROVAL',
                              'APPROVED',
                              'REJECTED'
                          )),
    rejection_reason  TEXT,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Soft-delete: set when seller deletes a product not tied to an active deal.
    deleted_at        TIMESTAMPTZ
);

CREATE INDEX idx_products_seller_id    ON products (seller_id);
CREATE INDEX idx_products_category_id  ON products (category_id);
CREATE INDEX idx_products_status       ON products (status);
CREATE INDEX idx_products_deleted_at   ON products (deleted_at);

-- Full-text search index for name + description
CREATE INDEX idx_products_fts
    ON products
    USING GIN (to_tsvector('english', name || ' ' || description));

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();


```

---

## 2. Domain Model

### 2.1 `Product`

The `Product` aggregate owns a seller's listing in the catalog.

Key fields:

- `id`
- `sellerId` — owning seller
- `name`, `description`
- `category` — reference to a `Category`
- `basePrice` — the non-deal list price
- `imageUrl` — primary product image URL
- `status` — moderation gate (`PENDING_APPROVAL` / `APPROVED` / `REJECTED`)
- `rejectionReason` — set by admin when rejecting
- `deletedAt` — soft-delete timestamp; non-null means hidden

Visibility rules:

- **Buyers** see only products where `status = 'APPROVED' AND deleted_at IS NULL`.
- **Sellers** see all of their own products regardless of status or deletion.
- **Admins** see all products including deleted ones.

### 2.2 `Category`

A grouping label for products. Managed by admins.

Key fields:

- `id`
- `name` — unique display name
- `description`

---

## 3. Product Status State Machine

```
                 (seller creates product)
                          |
                          ▼
                  PENDING_APPROVAL
                    /           \
                   /             \
          (admin approves)    (admin rejects)
                 |                  |
                 ▼                  ▼
             APPROVED           REJECTED
                 |
                 |  (seller updates — stays APPROVED)
                 |  (deal created on this product — no status change)
                 |
                 ▼
          (soft-delete: seller deletes
           only if no active deal exists)
```

- Products are **always** created in `PENDING_APPROVAL`.
- Only an admin can move a product to `APPROVED` or `REJECTED`.
- Once approved, a product stays approved. Updating metadata (name, description, price, etc.) does not reset the status — there is no re-approval cycle.
- A rejected product can be edited and resubmitted by the seller, which transitions it back to `PENDING_APPROVAL`.
- Deletion is a soft-delete (`deleted_at` set). A product tied to an active deal cannot be deleted (rejected by the API).

---

## 4. Events

Catalog Service does **not** publish or consume any asynchronous events. All communication is synchronous HTTP.

The only cross-service contract is the internal lookup endpoint (§5.6) called by Order Service during checkout.

---

## 5. REST API

### 5.1 `POST /products` — Seller creates a product

**Request**

```json
{
  "sellerId": "uuid",
  "name": "Wireless Headphones",
  "description": "Noise-cancelling Bluetooth headphones",
  "categoryId": "uuid",
  "basePrice": 79.99,
  "imageUrl": "https://cdn.example.com/img1.jpg"
}
```

**Response — 201**

```json
{
  "id": "uuid",
  "sellerId": "uuid",
  "name": "Wireless Headphones",
  "description": "Noise-cancelling Bluetooth headphones",
  "category": { "id": "uuid", "name": "Electronics", "description": "Gadgets, devices, and accessories", "createdAt": "2026-07-28T09:00:00Z" },
  "basePrice": 79.99,
  "imageUrl": "https://cdn.example.com/img1.jpg",
  "status": "PENDING_APPROVAL",
  "rejectionReason": null,
  "createdAt": "2026-07-28T10:15:00Z",
  "updatedAt": "2026-07-28T10:15:00Z"
}
```

> **Note (spec aligned with implementation):** the response embeds the full
> `category` object (`{id, name, description, createdAt}`) — not a flat `categoryId`.
> It also carries `rejectionReason` and `updatedAt`.

**Errors**
| Status | Cause |
|---|---|
| 400 | missing required field, negative price, invalid categoryId |
| 404 | `categoryId` does not exist |

---

### 5.2 `GET /products` — Browse/search (buyer-facing)

Returns only `APPROVED` and non-deleted products.

**Query params**
| Param | Type | Required | Notes |
|---|---|---|---|
| `q` | string | no | Full-text search on name + description |
| `categoryId` | UUID | no | Filter by category |
| `sellerId` | UUID | no | Filter by seller |
| `minPrice` | numeric | no | Minimum base price |
| `maxPrice` | numeric | no | Maximum base price |
| `sort` | string | no | Sort field and direction — see §7 |
| `page` | int | no | Default `1` |
| `limit` | int | no | Default `20`, max `100` |

**Response — 200**

```json
{
  "items": [
    {
      "id": "uuid",
      "sellerId": "uuid",
      "name": "Wireless Headphones",
      "description": "Noise-cancelling Bluetooth headphones",
      "category": { "id": "uuid", "name": "Electronics", "description": "Gadgets, devices, and accessories", "createdAt": "2026-07-28T09:00:00Z" },
      "basePrice": 79.99,
      "imageUrl": "https://cdn.example.com/img1.jpg",
      "status": "APPROVED",
      "rejectionReason": null,
      "createdAt": "2026-07-28T10:15:00Z",
      "updatedAt": "2026-07-28T10:20:00Z"
    }
  ],
  "page": 1,
  "limit": 20,
  "total": 1
}
```

> **Note (spec aligned with implementation):** the envelope key is `items`, not
> `products`. Items are full `ProductResponse` objects (same shape as §5.3).

**Errors**
| Status | Cause |
|---|---|
| 400 | invalid filter value, bad `page`/`limit` |

---

### 5.3 `GET /products/{id}` — Product detail

**Response — 200**

```json
{
  "id": "uuid",
  "sellerId": "uuid",
  "name": "Wireless Headphones",
  "description": "Noise-cancelling Bluetooth headphones",
  "category": { "id": "uuid", "name": "Electronics", "description": "Gadgets, devices, and accessories", "createdAt": "2026-07-28T09:00:00Z" },
  "basePrice": 79.99,
  "imageUrl": "https://cdn.example.com/img1.jpg",
  "status": "APPROVED",
  "rejectionReason": null,
  "createdAt": "2026-07-28T10:15:00Z",
  "updatedAt": "2026-07-28T10:20:00Z"
}
```

Visibility depends on caller role:

- **Buyer:** returns `404` if product is not `APPROVED` or is deleted.
- **Seller:** returns the full detail if the caller owns the product, regardless of status.
- **Admin:** returns the full detail regardless of ownership or status.

**Errors**
| Status | Cause |
|---|---|
| 404 | not found, or not visible to the caller's role |

---

### 5.4 `PATCH /products/{id}` — Seller updates a product

**Request** (all fields optional — partial update)

```json
{
  "name": "Wireless Headphones Pro",
  "basePrice": 89.99,
  "imageUrl": "https://cdn.example.com/new-img.jpg"
}
```

**Behavior by current status:**

- `APPROVED` — updates are accepted; **status stays `APPROVED`**. No re-approval cycle.
- `REJECTED` — updates are accepted; status resets to `PENDING_APPROVAL` so the admin can review the revised listing.
- `PENDING_APPROVAL` — updates are accepted; status stays `PENDING_APPROVAL`.

**Response — 200** (mirrors `GET /products/{id}` shape with updated fields)

**Errors**
| Status | Cause |
|---|---|
| 400 | invalid field value |
| 403 | caller is not the product's `seller_id` |
| 404 | product not found |

---

### 5.5 `DELETE /products/{id}` — Seller deletes a product

Soft-deletes a product (sets `deleted_at`).

**Errors**
| Status | Cause |
|---|---|
| 403 | caller is not the product's `seller_id` |
| 404 | product not found |
| 409 | product is tied to an active deal — deletion rejected |
| 410 | product is already deleted |

> **Status of the 409:** NOT yet enforced. Currently the service soft-deletes regardless
> of any active deal. It requires the **Deal Service contract** described in §10
> (blocked until Deal Service exists).

---

### 5.6 `POST /products/lookup` — Internal bulk lookup

**Called by Order Service** during checkout to resolve product IDs into prices and to validate existence (§8.1 of order-service.md).

**Request**

```json
{
  "productIds": ["uuid", "uuid"]
}
```

**Response — 200**

```json
{
  "found": {
    "uuid": {
      "id": "uuid",
      "name": "Wireless Earbuds",
      "basePrice": 39.99,
      "imageUrl": "https://cdn.example.com/img1.jpg"
    }
  },
  "notFound": ["uuid"]
}
```

`found` is a map keyed by product ID; each item includes `name`, `basePrice`, and `imageUrl` so Order Service can snapshot the product into the order. Only returns approved, non-deleted products. Any `productId` not matching an approved product is returned in `notFound`.

**Errors**
| Status | Cause |
|---|---|
| 400 | empty `productIds` array, or more than 50 IDs in one call |
| 503 | temporary database or service failure |

---

### 5.7 `GET /products/sellers/{sellerId}` — Seller's own products

Returns all products owned by the seller, regardless of status (including rejected, pending, and soft-deleted).

**Query params**
| Param | Type | Required | Notes |
|---|---|---|---|
| `status` | string | no | Filter by moderation status: `PENDING_APPROVAL` \| `APPROVED` \| `REJECTED` |
| `includeDeleted` | bool | no | Default `false` |
| `sort` | string | no | Sort field and direction — see §7 |
| `page` | int | no | Default `1` |
| `limit` | int | no | Default `20`, max `100` |

**Response — 200** (same shape as `GET /products`)

**Errors**
| Status | Cause |
|---|---|
| 403 | caller is not `{sellerId}` — enforced service-side (deferred until Auth exists, §10.2) |

---

### 5.8 Admin Endpoints

> **Note (spec aligned with implementation):** admin routes live under `/products/admin/...`
> (not `/admin/products/...`). `ADMIN` role enforcement is done by the **service itself**
> via `AdminRoleFilter` (reads `X-User-Role` header injected by the gateway). The filter is
> **currently disabled** — see §10.2.

#### `GET /products/admin` — Admin views all products

Returns all products regardless of status, including soft-deleted.

**Query params**
| Param | Type | Required | Notes |
|---|---|---|---|
| `status` | string | no | `PENDING_APPROVAL` \| `APPROVED` \| `REJECTED` |
| `includeDeleted` | bool | no | Default `false` |
| `page` | int | no | Default `1` |
| `limit` | int | no | Default `20`, max `100` |

**Response — 200** (same shape as `GET /products`, but includes `status` and `rejectionReason` for every product)

---

#### `PATCH /products/admin/{id}/approve` — Admin approves a product

Transitions `PENDING_APPROVAL → APPROVED`.

**Response — 200**

```json
{
  "id": "uuid",
  "status": "APPROVED"
}
```

**Errors**
| Status | Cause |
|---|---|
| 400 | product is not in `PENDING_APPROVAL` |
| 404 | product not found |

---

#### `PATCH /products/admin/{id}/reject` — Admin rejects a product

Transitions `PENDING_APPROVAL → REJECTED`.

**Request**

```json
{
  "reason": "Missing required warranty information"
}
```

**Response — 200**

```json
{
  "id": "uuid",
  "status": "REJECTED",
  "rejectionReason": "Missing required warranty information"
}
```

**Errors**
| Status | Cause |
|---|---|
| 400 | product is not in `PENDING_APPROVAL` |
| 404 | product not found |

---

### 5.9 Category Endpoints

| Method   | Path               | Purpose                                                   |
| -------- | ------------------ | --------------------------------------------------------- |
| `POST`   | `/categories`      | Admin creates a category                                  |
| `GET`    | `/categories`      | List all categories                                       |
| `GET`    | `/categories/{id}` | Get one category                                          |
| `PATCH`  | `/categories/{id}` | Admin updates a category                                  |
| `DELETE` | `/categories/{id}` | Admin deletes a category (fails if products reference it) |

#### `POST /categories`

**Request**

```json
{
  "name": "Electronics",
  "description": "Gadgets, devices, and accessories"
}
```

**Response — 201**

```json
{
  "id": "uuid",
  "name": "Electronics",
  "description": "Gadgets, devices, and accessories",
  "createdAt": "2026-07-28T10:15:00Z"
}
```

#### `GET /categories`

Returns all categories (no pagination needed — bounded set).

**Response — 200**

```json
[
  { "id": "uuid", "name": "Electronics", "description": "Gadgets, devices, and accessories" }
]
```

> **Note (spec aligned with implementation):** returns a bare array — no `categories` envelope.

---

## 6. Admin Moderation Flow

```
Seller creates product
        │
        ▼
  POST /products
  status = PENDING_APPROVAL
        │
        ▼
  Admin reviews via GET /admin/products?status=PENDING_APPROVAL
        │
        ├── approve ──► PATCH /admin/products/{id}/approve
        │                     status → APPROVED
        │                     visible to buyers immediately
        │
        └── reject  ──► PATCH /admin/products/{id}/reject
                              status → REJECTED
                              rejection_reason set
                              hidden from buyers

  ── On resubmission ──
  Seller PATCHes the rejected product
  → status resets to PENDING_APPROVAL
  → admin reviews again
```

---

## 7. Search & Filtering

The `GET /products` endpoint supports full-text search and fixed filter params (consistent with the hardcoded-param style used across other services).

### 7.1 Search (`q`)

**Current implementation (spec aligned with code):** `q` is matched with a
case-insensitive `LIKE '%q%'` on `name` and `description` via JPA Criteria
(`lower(name) LIKE %q% OR lower(description) LIKE %q%`). Portable across databases, no
raw SQL, but substring-based — `"run"` does **not** match `"running"`.

> **Optional future enhancement:** switch to PostgreSQL full-text search
> (`to_tsvector('english', name || ' ' || description) @@ plainto_tsquery('english', :q)`)
> so `"run"` matches `"run"`/`"running"`/`"runner"`. The `idx_products_fts` GIN index
> already exists in the schema (§1) and is currently unused. If implemented, register a
> custom Hibernate SQL function for the `@@` operator rather than reverting to raw SQL.

### 7.2 Fixed Filter Params

| Param        | SQL clause                  | Notes       |
| ------------ | --------------------------- | ----------- |
| `categoryId` | `category_id = :categoryId` | Exact match |
| `sellerId`   | `seller_id = :sellerId`     | Exact match |
| `minPrice`   | `base_price >= :minPrice`   | Inclusive   |
| `maxPrice`   | `base_price <= :maxPrice`   | Inclusive   |

All params are optional and AND-ed together.

**Examples:**

```
# Electronics category
GET /products?categoryId=3a1f2b8e-...

# Seller's products
GET /products?sellerId=b3f1a2c8-...

# Price range
GET /products?minPrice=10&maxPrice=100

# Text search + category + sort
GET /products?q=headphones&categoryId=3a1f2b8e-...&sort=basePrice:asc

# Combined filters
GET /products?sellerId=b3f1a2c8-...&minPrice=50&sort=createdAt:desc
```

### 7.3 Sort

```
GET /products?sort=basePrice:asc
GET /products?sort=createdAt:desc
```

Defaults to `createdAt:desc`.

**Sortable fields:** `createdAt`, `basePrice`, `name`.

### 7.4 Base Constraints

Every buyer-facing query additionally enforces:

```sql
AND status = 'APPROVED'
AND deleted_at IS NULL
```

---

## 8. Integration Points

| Consumer          | Endpoint                | When called                                                                                                          |
| ----------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Order Service     | `POST /products/lookup` | During normal checkout to validate product IDs and fetch base prices (§8.1 of order-service.md)                      |
| Deal Service      | `GET /products/{id}`    | At deal creation to verify the product exists and is approved, and to reference `base_price` for discount validation |
| Inventory Service | `GET /products/{id}`    | At deal creation to verify the product exists before reserving stock                                                 |

---

## 9. Catalog Service Responsibilities

The catalog service owns:

- product listing creation and metadata management
- category taxonomy
- product moderation lifecycle (pending → approved/rejected)
- product search and browse for buyers
- internal product lookup for Order Service checkout

The catalog service does **not** own:

- deal pricing or lifecycle rules
- inventory stock levels or reservation
- order state or payment processing
- user authentication or authorization (relies on API Gateway for JWT validation)

---

## 10. Cross-Service Contracts Catalog Needs (deferred)

### 10.1 Deal Service contract — reject deleting a product with an active deal

`DELETE /products/{id}` must return **409 Conflict** when the product is tied to an
active deal. Catalog does not own deal state, so it cannot decide this on its own.

**What Deal Service must provide (choose one):**

- **Sync endpoint (recommended):** `GET /internal/deals?productId={id}&active=true` →
  `200 { "hasActiveDeal": true|false }`, or an `exists`-style internal endpoint. Called
  by Catalog Service inside `deleteProduct` before soft-deleting.
- **Async alternative:** Deal Service publishes a `deal.created` / `deal.succeeded` /
  `deal.cancelled` / `deal.failed` event; Catalog keeps a local `product_deal_refs`
  table (product_id + deal_status) to answer "has an active deal?" without a sync call.

**Contract note:** `active` means the deal is in `pending` or `active` state (not yet
`succeeded`/`failed`/`cancelled`). Exact state set to be confirmed when Deal Service's
state machine is implemented (`rally-docs/services docs/deal-service.md`).

### 10.2 Role enforcement — service-side filter (deferred)

Catalog does its **own** role filtering; it does not depend on the API Gateway for
authorization logic.

- **Implementation:** `AdminRoleFilter` (`com.rally.catalog.config`) — a servlet filter
  matching `/products/admin/*` that requires the `X-User-Role` header to be `ADMIN`
  (403 `UnauthorizedException` otherwise).
- **Status: DISABLED.** The filter is written but NOT registered (`@Component` on the
  class and the `adminRoleFilter()` bean in `SecurityConfig` are commented out).
- **Blocking dependency:** the Auth service. `X-User-Role` is only trustworthy once a JWT
  is validated and the header is injected by an authenticated caller. Until Auth exists,
  headers can be forged, so the check stays off to keep endpoints testable.
- **Also deferred to the same filter/flow:** `SELLER` ownership on
  `PATCH`/`DELETE /products/{id}` and `GET /products/sellers/{sellerId}` (currently the
  ownership checks run in `ProductService` using the `X-User-Id` header).

