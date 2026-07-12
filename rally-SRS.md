# Software Requirements Specification (SRS)
## GroupDeal — Group-Buying E-Commerce Platform

---

# 1. Background

GroupDeal is a group-buying e-commerce platform that enables sellers to offer products through two purchasing channels: a group-deal channel, where a discounted price unlocks only once a minimum number of buyers commit within a defined time window, and a normal-listing channel, where products are available at a fixed base price for immediate purchase.

The platform's core differentiator is a risk-free group-deal mechanic: buyers who join a deal are not charged at the time of joining. Instead, a hold is placed on their payment method, and they are only charged if the deal successfully reaches its minimum participation threshold before its time window expires. If the deal fails to reach that threshold, every participant's hold is released and no one is charged.

This model addresses two connected business needs. It gives sellers a low-risk way to validate demand for bulk or promotional pricing before committing to it, supported by referral-driven participant growth to extend reach. It gives buyers transparent, risk-free access to group-negotiated discounts, while preserving the option to purchase immediately at base price through the normal-listing channel for buyers who are unwilling or unable to wait for a deal to resolve.

---

# 2. Objectives

- Enable sellers to validate product demand before committing to bulk or promotional pricing.
- Enable buyers to access group-negotiated discount prices without upfront financial risk.
- Ensure buyers are never charged for a deal that does not succeed.
- Automate deal resolution (success/failure) with no manual intervention by sellers or administrators.
- Enable referral-driven growth of deals through shareable referral links.
- Provide buyers a no-wait purchasing alternative alongside the group-deal mechanic.
- Provide administrators with visibility into deal outcomes and control over listing quality through an approval workflow.
- Maintain accurate, near real-time visibility into deal progress for buyers.
- Guarantee correctness of stock-cap enforcement under concurrent join attempts.

---

# 3. Stakeholders

| Stakeholder | Role | Responsibilities |
|---|---|---|
| Buyer | End customer | Browses products, joins/leaves group deals, checks out normal purchases, generates/shares referral links, views order history |
| Seller | End customer (merchant) | Creates and manages product listings, creates and cancels deals, views outcomes of past deals |
| Admin | Platform operator | Views all deals and their status, approves/rejects product listings, changes user roles |
| Platform (system actor) | Automated system behavior | Executes deal resolution on timer expiry or stock fill, orchestrates payment/order workflows, dispatches notifications |

**Note:** A single account may hold both Buyer and Seller roles simultaneously; Admin is a distinct, platform-assigned role.

---

# 4. Epics

| Field | Value |
|---|---|
| Epic Name | Account & Access Management |
| Description | Covers user registration, authentication, and role management, allowing individuals to access the platform as buyers and/or sellers, and allowing admins to manage role assignments. |
| Business Value | Establishes the identity and access foundation required for all buyer, seller, and admin activity on the platform. |
| Actors | Buyer, Seller, Admin |
| Acceptance Criteria | - Users can register and log in using JWT-based authentication.<br>- A single account can hold both buyer and seller roles.<br>- Admins can change a user's role. |

| Field | Value |
|---|---|
| Epic Name | Product Catalog Management |
| Description | Covers how sellers create and manage product listings, how newly created listings are gated behind admin approval, and how buyers discover approved products through browsing and search. |
| Business Value | Provides the product inventory foundation on which both normal purchases and group deals are built, while ensuring only compliant listings reach buyers. |
| Actors | Seller, Buyer, Admin |
| Acceptance Criteria | - Sellers can create product listings with name, description, category, and base price; new listings enter a pending-approval state.<br>- Sellers can update products they own at any time; a product tied to an active deal cannot be deleted, but can still be updated.<br>- Buyers can browse and search (by name, seller, category, price range) only admin-approved products. |

| Field | Value |
|---|---|
| Epic Name | Deal Management (Seller-Facing) |
| Description | Covers a seller's ability to create a group deal on a product they own, cancel a deal before anyone has joined, and review outcomes of past deals. |
| Business Value | Lets sellers validate demand and access group-buy pricing dynamics without committing to bulk discounts until sufficient buyer interest is confirmed. |
| Actors | Seller |
| Acceptance Criteria | - A seller can create a deal on an owned product with a discount price, stock cap, minimum participant count, and duration.<br>- The minimum participant count can never exceed the stock cap.<br>- A seller can cancel a deal only if no participant has joined yet.<br>- A seller can view outcomes of their past deals. |

| Field | Value |
|---|---|
| Epic Name | Group Participation (Buyer-Facing) |
| Description | Covers a buyer's ability to join and leave open deals, track live deal progress, and grow a deal's participation through referral links. |
| Business Value | Delivers the core buyer-facing value proposition of GroupDeal: access to discount pricing through group participation, with transparency and no upfront financial risk. |
| Actors | Buyer |
| Acceptance Criteria | - A buyer can join an open deal if capacity allows and the deal has not resolved, provided they do not already hold an active participation in that deal.<br>- A buyer can view live deal progress (participants joined vs. target, time remaining).<br>- A buyer can generate and share a referral link for a deal they joined.<br>- A buyer can leave a deal only while it is open and more than 10 minutes remain before timer expiry. |

| Field | Value |
|---|---|
| Epic Name | Deal Resolution (Automated) |
| Description | Covers the platform's automatic, system-triggered resolution of deals to success or failure, and the resulting order and payment side effects, with no manual intervention. |
| Business Value | Guarantees buyers and sellers a reliable, tamper-free, and timely outcome for every deal, and ensures the "never charge for a failed deal" guarantee is enforced automatically. |
| Actors | Platform (system actor) |
| Acceptance Criteria | - A deal succeeds when its stock cap fills, or when its timer expires with at least the minimum required participants joined.<br>- A deal fails when its timer expires with fewer than the minimum required participants joined.<br>- On success, all pending orders are confirmed, all held payments are captured, and all participants are notified.<br>- On failure, all pending orders are cancelled, all held payments are voided, reserved inventory is released, and all participants are notified. |

| Field | Value |
|---|---|
| Epic Name | Payment Processing |
| Description | Covers authorization (holds), capture, and voiding of buyer payments for deal purchases, and immediate authorize-and-capture for normal purchases, via Stripe in test mode. |
| Business Value | Ensures buyers are financially protected — never charged unless a deal actually succeeds — while giving sellers and the platform a reliable settlement mechanism. |
| Actors | Buyer, Platform (system actor) |
| Acceptance Criteria | - A pending order is created before payment authorization is requested; a payment method is then authorized (held, not charged) for the discount price.<br>- A held payment is captured only if the deal succeeds.<br>- A held payment is voided if the deal fails or the participant leaves.<br>- Normal purchases are authorized and captured in the same step, with no hold period.<br>- A declined authorization at join time cancels the pending order and releases the reserved slot; no void occurs, since no hold was ever successfully placed. |

| Field | Value |
|---|---|
| Epic Name | Normal Purchase (Cart Checkout) |
| Description | Covers the non-deal purchase path, where buyers add products to a cart and check out immediately at base price without any waiting period. |
| Business Value | Gives buyers an immediate-purchase alternative for those unwilling or unable to wait for a group deal to resolve. |
| Actors | Buyer |
| Acceptance Criteria | - A buyer can add multiple products to a cart.<br>- Checkout creates a single order with one line item per product.<br>- Payment is authorized and captured immediately, and the order is confirmed immediately. |

| Field | Value |
|---|---|
| Epic Name | Order Management |
| Description | Covers a buyer's ability to view their order history and the status of individual orders, covering both deal orders and normal purchases. |
| Business Value | Gives buyers transparency into their purchase history and current order status across both purchase paths. |
| Actors | Buyer |
| Acceptance Criteria | - A buyer can view a history of all their orders (deal and normal).<br>- A buyer can view the status of each order (pending, confirmed, cancelled). |

| Field | Value |
|---|---|
| Epic Name | Notifications |
| Description | Covers system-generated email notifications sent to buyers at key points: join confirmation, deal outcome, and order confirmation. |
| Business Value | Keeps buyers informed in near real time of the status of their commitments and purchases, reinforcing trust in the platform's automated processes. |
| Actors | Platform (system actor), Buyer |
| Acceptance Criteria | - A buyer receives a join confirmation (email) distinct from a deal-success notification, immediately upon joining a deal.<br>- A buyer receives a deal outcome notification (email) using a success- or failure-specific template when a deal resolves.<br>- A buyer receives an order confirmation (email), using a distinct plain-order template, for normal purchases. |

| Field | Value |
|---|---|
| Epic Name | Admin Moderation & Analytics |
| Description | Covers platform administrator capabilities to view all deals and their statuses, and to approve or reject product listings at creation time so that only compliant listings become visible to buyers. |
| Business Value | Gives the platform operational visibility into deal outcomes and a gate that enforces listing quality/compliance before a product reaches buyers. |
| Actors | Admin |
| Acceptance Criteria | - An admin can view all deals and their current status.<br>- Every newly created product listing is queued for admin approval before it is visible to buyers.<br>- An admin can approve or reject a pending product listing.<br>- An admin can change a user's role. |

---

# 5. User Stories

| Field | Value |
|---|---|
| ID | US-001 |
| Title | User Registration |
| Epic | Account & Access Management |
| Story | As a prospective user, I want to register an account, so that I can access the platform as a buyer and/or seller. |
| Acceptance Criteria | - Given a prospective user provides Name, Email, Phone Number, Password, and a self-selected Role (Buyer or Seller), when they submit registration, then a new account is created with the selected role(s).<br>- Given a prospective user omits a required field or provides invalid data, when they submit registration, then the registration is rejected. |
| Priority | High |
| Dependencies | None |
| Notes | Registration fields are Name, Email, Phone Number, Password, and Role; Role is self-selected. Field-level validation rules (password complexity, phone format, duplicate-email handling) are not specified. |

| Field | Value |
|---|---|
| ID | US-002 |
| Title | User Login |
| Epic | Account & Access Management |
| Story | As a registered user, I want to log in, so that I can access my account and platform features. |
| Acceptance Criteria | - Given a registered user provides valid credentials, when they submit a login request, then the system authenticates them and issues a JWT.<br>- Given a registered user provides invalid credentials, when they submit a login request, then authentication is rejected. |
| Priority | High |
| Dependencies | US-001 |
| Notes | Authentication is JWT-based. |

| Field | Value |
|---|---|
| ID | US-003 |
| Title | Hold Buyer and/or Seller Roles on a Single Account |
| Epic | Account & Access Management |
| Story | As a registered user, I want to hold both buyer and seller roles on my account, so that I can both purchase products and list products for sale without maintaining separate accounts. |
| Acceptance Criteria | - Given a registered user account, when the user acts as a buyer or as a seller, then the same account supports both roles concurrently. |
| Priority | Medium |
| Dependencies | US-001 |
| Notes | Role is self-selected at registration (US-001). The mechanism by which an existing single-role account later acquires a second role is not specified. |

| Field | Value |
|---|---|
| ID | US-004 |
| Title | Admin Changes User Role |
| Epic | Account & Access Management |
| Story | As an admin, I want to change a user's role, so that I can manage platform access appropriately. |
| Acceptance Criteria | - Given an admin is viewing a user account, when the admin assigns a new role, then the user's role is updated accordingly. |
| Priority | Medium |
| Dependencies | US-001 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-005 |
| Title | Seller Creates a Product Listing |
| Epic | Product Catalog Management |
| Story | As a seller, I want to create a product listing, so that buyers can discover and purchase my product. |
| Acceptance Criteria | - Given a seller provides a product name, description, category, and base price, when the seller submits the listing, then a new product listing is created, associated with that seller, and placed in a pending-approval state. |
| Priority | High |
| Dependencies | US-001, US-002 |
| Notes | Product creation triggers an admin approval check (US-035); the product is not visible to buyers (US-007) until approved. |

| Field | Value |
|---|---|
| ID | US-006 |
| Title | Seller Manages Product Listings |
| Epic | Product Catalog Management |
| Story | As a seller, I want to manage my product listings, so that I can keep my catalog accurate and up to date. |
| Acceptance Criteria | - Given a seller owns a product listing, when the seller updates listing details, then the changes are reflected in the catalog, regardless of whether the product is tied to an active deal.<br>- Given a seller owns a product listing tied to an active deal, when the seller attempts to delete it, then the deletion is rejected.<br>- Given a seller owns a product listing not tied to any active deal, when the seller deletes it, then the listing is removed from the catalog. |
| Priority | Medium |
| Dependencies | US-005 |
| Notes | A product tied to an active deal can be updated but not deleted. |

| Field | Value |
|---|---|
| ID | US-007 |
| Title | Buyer Browses Products |
| Epic | Product Catalog Management |
| Story | As a buyer, I want to browse available products, so that I can discover items to purchase. |
| Acceptance Criteria | - Given approved products exist in the catalog, when a buyer views the catalog, then approved products are displayed.<br>- Given a product is pending approval or has been rejected, when a buyer views the catalog, then that product is not displayed. |
| Priority | High |
| Dependencies | US-005, US-035 |
| Notes | Only admin-approved products are visible to buyers (see US-035). |

| Field | Value |
|---|---|
| ID | US-008 |
| Title | Buyer Searches Products |
| Epic | Product Catalog Management |
| Story | As a buyer, I want to search for products, so that I can quickly find items I'm interested in. |
| Acceptance Criteria | - Given approved products exist in the catalog, when a buyer submits a search query by name, seller, category, and/or price range, then matching approved products are returned. |
| Priority | Medium |
| Dependencies | US-005, US-035 |
| Notes | Search fields are name, seller, category, and price range. |

| Field | Value |
|---|---|
| ID | US-009 |
| Title | Seller Creates a Deal |
| Epic | Deal Management (Seller-Facing) |
| Story | As a seller, I want to create a group deal on a product I own, so that I can validate demand and offer a discounted price if enough buyers commit. |
| Acceptance Criteria | - Given a seller owns a product and specifies a discount price, stock cap, minimum participant count, and duration, when the seller submits the deal and the minimum participant count is ≤ the stock cap, then the deal is created, the needed stock is reserved from the product's overall inventory, and the deal becomes visible and joinable but not yet running (no timer).<br>- Given the specified minimum participant count exceeds the stock cap, when the seller submits the deal, then the deal creation is rejected. |
| Priority | High |
| Dependencies | US-005 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-010 |
| Title | Seller Cancels a Deal |
| Epic | Deal Management (Seller-Facing) |
| Story | As a seller, I want to cancel a deal I created, so that I can withdraw an offer before any buyer has committed to it. |
| Acceptance Criteria | - Given a deal has zero participants joined, when the seller cancels the deal, then the deal is cancelled.<br>- Given a deal has one or more participants joined, when the seller attempts to cancel the deal, then the cancellation is rejected. |
| Priority | Medium |
| Dependencies | US-009 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-011 |
| Title | Seller Views Deal Outcomes |
| Epic | Deal Management (Seller-Facing) |
| Story | As a seller, I want to view the outcomes of my past deals, so that I can understand how my offers performed. |
| Acceptance Criteria | - Given a seller has one or more resolved deals, when the seller views deal history, then the outcome (success/failure) of each past deal is displayed. |
| Priority | Low |
| Dependencies | US-009 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-012 |
| Title | Buyer Joins an Open Deal |
| Epic | Group Participation (Buyer-Facing) |
| Story | As a buyer, I want to join an open deal, so that I can access the discounted group price without being charged unless the deal succeeds. |
| Acceptance Criteria | - Given a deal has open capacity and has not resolved, when a buyer requests to join, then the system checks for a free slot; if available, the buyer is listed as a participant, a slot is reserved, a pending order is created, — if this is the first participant — the deal's timer starts, and a payment authorization hold is triggered (US-022) for the discount price, requested only after the participant is listed and the pending order exists.<br>- Given a deal is full or already resolved, when a buyer requests to join, then the join is rejected with no side effects.<br>- Given the payment authorization triggered by this join is declined, when the decline is reported back, then the order-cancellation-and-slot-release behavior in US-025 is invoked.<br>- Given a buyer already has an active participation in a deal, when that buyer attempts to join the same deal again, then the second join is rejected.<br>- Given a buyer previously left a deal, or previously purchased the same product through a normal order, when that buyer requests to join the deal, then the join is allowed. |
| Priority | High |
| Dependencies | US-009, US-022, US-025 |
| Notes | Join sequence: (1) check for a free slot, (2) list the buyer as a participant and reserve the slot, (3) create the pending order, (4) request payment authorization. A decline cancels the order and frees the slot rather than voiding a hold, since no hold was ever placed (see US-025). A buyer may hold only one active participation per deal at a time, but may leave and rejoin, and may join a deal for a product already purchased normally. **Ownership boundary:** this story owns capacity checking, slot reservation, timer start, and pending-order creation; authorization-hold logic is owned by US-022, and decline handling is owned by US-025. |

| Field | Value |
|---|---|
| ID | US-013 |
| Title | Buyer Views Live Deal Progress |
| Epic | Group Participation (Buyer-Facing) |
| Story | As a buyer, I want to view live progress of a deal, so that I can decide whether to join and track how close it is to succeeding. |
| Acceptance Criteria | - Given an active deal, when a buyer views the deal, then current participant count vs. target and time remaining are displayed, updated in near real time. |
| Priority | High |
| Dependencies | US-009 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-014 |
| Title | Buyer Generates and Shares a Referral Link |
| Epic | Group Participation (Buyer-Facing) |
| Story | As a participant in a deal, I want to generate and share a referral link, so that I can invite others to join and help the deal succeed. |
| Acceptance Criteria | - Given a buyer has joined a deal, when the buyer requests a referral link, then a shareable link tied to that specific deal is generated. |
| Priority | Medium |
| Dependencies | US-012 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-015 |
| Title | New Buyer Joins via Referral Link |
| Epic | Group Participation (Buyer-Facing) |
| Story | As a buyer who received a referral link, I want to join the deal through that link, so that I can participate, with the referrer credited for the referral. |
| Acceptance Criteria | - Given a valid referral link for an open deal, when a buyer opens the link and joins, then the buyer goes through the standard join flow, and the system records who referred them for tracking purposes. |
| Priority | Medium |
| Dependencies | US-012, US-014 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-016 |
| Title | Buyer Leaves a Deal |
| Epic | Group Participation (Buyer-Facing) |
| Story | As a participant in a deal, I want to leave a deal I previously joined, so that I can withdraw my commitment if I change my mind, while the deal is still open. |
| Acceptance Criteria | - Given a deal is still open and more than 10 minutes remain before timer expiry, when the participant requests to leave, then their slot is released, their order is cancelled, and the void-hold behavior in US-024 is triggered.<br>- Given the deal has already resolved, or fewer than 10 minutes remain before timer expiry, when the participant requests to leave, then the request is rejected. |
| Priority | High |
| Dependencies | US-012, US-024 |
| Notes | **Ownership boundary:** this story owns eligibility checking, slot release, and order cancellation; the actual payment-void logic is owned by US-024. |

| Field | Value |
|---|---|
| ID | US-017 |
| Title | Automatic Deal Success on Stock Cap Fill |
| Epic | Deal Resolution (Automated) |
| Story | As the platform, I want to automatically resolve a deal as successful the instant its stock cap fills, so that buyers and sellers get a timely, reliable outcome without manual intervention. |
| Acceptance Criteria | - Given a deal's stock cap becomes fully filled, when the last slot is taken, then the deal resolves as successful immediately, even if time remains. |
| Priority | High |
| Dependencies | US-012 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-018 |
| Title | Automatic Deal Success on Timer Expiry with Minimum Met |
| Epic | Deal Resolution (Automated) |
| Story | As the platform, I want to automatically resolve a deal as successful when its timer expires with at least the minimum required participants, so that the deal succeeds as intended even if the stock cap was never fully filled. |
| Acceptance Criteria | - Given a deal's timer expires, when the number of joined participants is at least the minimum required, then the deal resolves as successful. |
| Priority | High |
| Dependencies | US-012 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-019 |
| Title | Automatic Deal Failure on Timer Expiry Below Minimum |
| Epic | Deal Resolution (Automated) |
| Story | As the platform, I want to automatically resolve a deal as failed when its timer expires with fewer than the minimum required participants, so that buyers are protected from being charged for an under-subscribed deal. |
| Acceptance Criteria | - Given a deal's timer expires, when the number of joined participants is fewer than the minimum required, then the deal resolves as failed. |
| Priority | High |
| Dependencies | US-012 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-020 |
| Title | System Processes Deal-Success Side Effects |
| Epic | Deal Resolution (Automated) |
| Story | As the platform, I want to confirm orders when a deal succeeds and trigger payment capture and participant notification, so that buyers and sellers receive the correct outcome consistently. |
| Acceptance Criteria | - Given a deal resolves as successful, when resolution is processed, then every pending order tied to the deal is confirmed.<br>- Given orders have been confirmed for a successful deal, when the success workflow completes, then the capture behavior in US-023 and the notification behavior in US-032 are triggered. |
| Priority | High |
| Dependencies | US-017, US-018, US-023, US-032 |
| Notes | **Ownership boundary:** this story owns order confirmation only; capture is owned by US-023 and notification by US-032. |

| Field | Value |
|---|---|
| ID | US-021 |
| Title | System Processes Deal-Failure Side Effects |
| Epic | Deal Resolution (Automated) |
| Story | As the platform, I want to cancel orders and release inventory when a deal fails, and trigger payment voiding and participant notification, so that no buyer is charged and reserved inventory becomes available again. |
| Acceptance Criteria | - Given a deal resolves as failed, when resolution is processed, then every pending order tied to the deal is cancelled and reserved inventory is released back to general stock.<br>- Given orders have been cancelled and inventory released for a failed deal, when the failure workflow completes, then the void behavior in US-024 and the notification behavior in US-032 are triggered. |
| Priority | High |
| Dependencies | US-019, US-024, US-032 |
| Notes | **Ownership boundary:** this story owns order cancellation and inventory release only; voiding is owned by US-024 and notification by US-032. |

| Field | Value |
|---|---|
| ID | US-022 |
| Title | Authorize Payment Hold at Join |
| Epic | Payment Processing |
| Story | As the platform, I want to place a payment authorization hold when a buyer joins a deal, so that funds are reserved without charging the buyer upfront. |
| Acceptance Criteria | - Given a buyer successfully joins a deal, when the join is processed, then a payment hold (authorization) for the discount price is placed via Stripe test mode, without a charge occurring. |
| Priority | High |
| Dependencies | US-012 |
| Notes | Uses Stripe in test mode (test API keys, test card numbers); no real money moves. |

| Field | Value |
|---|---|
| ID | US-023 |
| Title | Capture Payment on Deal Success |
| Epic | Payment Processing |
| Story | As the platform, I want to capture a held payment when a deal succeeds, so that the buyer is charged only after the deal is confirmed to succeed. |
| Acceptance Criteria | - Given a deal resolves as successful, when resolution is processed, then each associated payment hold is captured, actually charging the buyer. |
| Priority | High |
| Dependencies | US-020 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-024 |
| Title | Void Payment Hold |
| Epic | Payment Processing |
| Story | As the platform, I want to void a held payment when a deal fails or a participant leaves, so that a buyer is never charged for a commitment that did not result in a purchase. |
| Acceptance Criteria | - Given a deal fails (triggered by US-021) or a participant leaves a deal (triggered by US-016), when the relevant event occurs, then the associated payment hold is voided and no charge is made. |
| Priority | High |
| Dependencies | US-016, US-021 |
| Notes | A declined join-time authorization is handled by US-025 (order cancellation and slot release), not by this story, since no hold is ever successfully placed before a decline. |

| Field | Value |
|---|---|
| ID | US-025 |
| Title | Declined Authorization Cancels Order and Releases Reserved Slot |
| Epic | Payment Processing |
| Story | As the platform, I want to cancel the pending order and release the reserved slot when a payment authorization is declined at join time, so that the slot doesn't become permanently unavailable and no orphaned pending order remains. |
| Acceptance Criteria | - Given a buyer's payment authorization is declined during a join attempt, when the decline is detected, then the pending order created for that join is set to cancelled, and the reserved slot is released back to the pool for other buyers. |
| Priority | High |
| Dependencies | US-012 |
| Notes | Because the pending order is created before authorization is requested (US-012), a decline cancels that order in addition to freeing the slot. No payment void occurs here. |

| Field | Value |
|---|---|
| ID | US-026 |
| Title | Immediate Authorize and Capture for Normal Purchase |
| Epic | Payment Processing |
| Story | As a buyer, I want my payment to be authorized and captured immediately at checkout for a normal purchase, so that I don't have to wait for a deal to resolve. |
| Acceptance Criteria | - Given a buyer checks out a cart of normal (non-deal) products, when checkout is processed, then payment is authorized and captured in the same step, and the order is immediately confirmed. |
| Priority | High |
| Dependencies | US-028 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-027 |
| Title | Buyer Adds Products to Cart |
| Epic | Normal Purchase (Cart Checkout) |
| Story | As a buyer, I want to add multiple products to a cart, so that I can purchase several items in a single transaction. |
| Acceptance Criteria | - Given products available at base price, when a buyer adds one or more products to the cart, then the cart reflects the selected products. |
| Priority | High |
| Dependencies | US-007 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-028 |
| Title | Buyer Checks Out Cart |
| Epic | Normal Purchase (Cart Checkout) |
| Story | As a buyer, I want to check out my cart as a single order, so that I can complete a normal purchase without waiting. |
| Acceptance Criteria | - Given a buyer's cart contains one or more products, when the buyer checks out, then a single order is created with one line item per product, and the immediate authorize-and-capture behavior in US-026 is triggered.<br>- Given the triggered payment succeeds, when authorization and capture complete, then the order is confirmed. |
| Priority | High |
| Dependencies | US-027, US-026 |
| Notes | **Ownership boundary:** this story owns cart-to-order translation and order confirmation; authorize-and-capture logic is owned by US-026. |

| Field | Value |
|---|---|
| ID | US-029 |
| Title | Buyer Views Order History |
| Epic | Order Management |
| Story | As a buyer, I want to view my order history, so that I can review my past deal orders and normal purchases. |
| Acceptance Criteria | - Given a buyer has one or more orders (deal or normal), when the buyer views order history, then all their orders are listed. |
| Priority | Medium |
| Dependencies | US-012, US-028 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-030 |
| Title | Buyer Views Order Details |
| Epic | Order Management |
| Story | As a buyer, I want to view the details of an order, so that I can review its information and current status. |
| Acceptance Criteria | - Given a buyer selects an order, when the order details page opens, then the order status is displayed. <br> - Then purchased items are displayed. <br> - Then quantities and prices are displayed. <br> - Then order date and total amount are displayed. |
| Priority | Medium |
| Dependencies | US-029 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-031 |
| Title | Join Confirmation Notification |
| Epic | Notifications |
| Story | As a buyer, I want to receive a join confirmation when I join a deal, so that I know my join was recorded, without assuming the deal has succeeded. |
| Acceptance Criteria | - Given a buyer successfully joins a deal, when the join is processed, then the buyer receives an email join confirmation distinct from a deal-success notification. |
| Priority | Medium |
| Dependencies | US-012 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-032 |
| Title | Deal Outcome Notification |
| Epic | Notifications |
| Story | As a buyer, I want to receive a notification when a deal I joined resolves, so that I know whether it succeeded or failed. |
| Acceptance Criteria | - Given a deal resolves as successful, when resolution is processed, then all participants receive an email outcome notification, near real time, using a success-specific template that includes order information plus a success acknowledgement.<br>- Given a deal resolves as failed, when resolution is processed, then all participants receive an email outcome notification, near real time, using a failure-specific template that includes order information plus an explanation that the deal did not succeed. |
| Priority | Medium |
| Dependencies | US-020, US-021 |
| Notes | The deal outcome notification uses a distinct template from the order confirmation (US-033): both include order information, but this one adds success/failure-specific messaging. |

| Field | Value |
|---|---|
| ID | US-033 |
| Title | Order Confirmation Notification |
| Epic | Notifications |
| Story | As a buyer, I want to receive an order confirmation, so that I know my purchase was successfully completed. |
| Acceptance Criteria | - Given a normal purchase order is confirmed, when checkout completes, then the buyer receives an email order confirmation. |
| Priority | Medium |
| Dependencies | US-028 |
| Notes | This is a plain order-information email, distinct from the deal outcome notification (US-032). Deal-order confirmations are covered by US-032's success template rather than a separate confirmation email. |

| Field | Value |
|---|---|
| ID | US-034 |
| Title | Admin Views All Deals |
| Epic | Admin Moderation & Analytics |
| Story | As an admin, I want to view all deals and their status, so that I have visibility into platform deal activity. |
| Acceptance Criteria | - Given deals exist on the platform, when an admin views the deals list, then all deals and their current status are displayed. |
| Priority | Medium |
| Dependencies | US-009 |
| Notes | None |

| Field | Value |
|---|---|
| ID | US-035 |
| Title | Admin Approves or Rejects Product Listings |
| Epic | Admin Moderation & Analytics |
| Story | As an admin, I want to approve or reject a product listing when it is created, so that only compliant listings become visible to buyers. |
| Acceptance Criteria | - Given a seller creates a new product listing (US-005), when the listing is submitted, then it enters a pending-approval state and is queued for admin review.<br>- Given an admin reviews a pending product listing, when the admin approves it, then the listing becomes visible to buyers (US-007, US-008).<br>- Given an admin reviews a pending product listing, when the admin rejects it, then the listing remains hidden from buyers. |
| Priority | Medium |
| Dependencies | US-005 |
| Notes | Moderation is an approve/reject workflow triggered automatically at product-creation time. Whether a rejected listing can be edited and resubmitted, and whether the seller is notified of rejection, is not specified. |

---

# 6. Functional Requirements

| FR ID | Requirement | Traceability |
|---|---|---|
| FR-001 | The system shall allow a prospective user to register a new account by providing Name, Email, Phone Number, Password, and a self-selected Role (Buyer or Seller). | US-001 |
| FR-002 | The system shall authenticate registered users using JWT-based login. | US-002 |
| FR-003 | The system shall support a single account holding both buyer and seller roles concurrently. | US-003 |
| FR-004 | The system shall allow an admin to change a user's role. | US-004 |
| FR-005 | The system shall allow a seller to create a product listing with name, description, category, and base price. | US-005 |
| FR-005a | The system shall place a newly created product listing into a pending-approval state, triggering the admin approval workflow. | Owner: US-035; triggered by US-005 |
| FR-006 | The system shall allow a seller to update a product listing they own at any time, regardless of whether it is tied to an active deal. | US-006 |
| FR-006a | The system shall reject deletion of a product listing that is tied to an active deal. | US-006 |
| FR-006b | The system shall allow deletion of a product listing that is not tied to any active deal. | US-006 |
| FR-007 | The system shall allow a buyer to browse only admin-approved product listings in the catalog. | US-007 |
| FR-008 | The system shall allow a buyer to search the catalog of admin-approved products by name, seller, category, and/or price range. | US-008 |
| FR-009 | The system shall allow a seller to create a deal on a product they own, specifying discount price, stock cap, minimum participant count, and duration. | US-009 |
| FR-010 | The system shall reject deal creation if the minimum participant count exceeds the stock cap. | US-009 |
| FR-011 | The system shall reserve the deal's needed stock from the product's overall inventory at deal creation. | US-009 |
| FR-012 | The system shall make a newly created deal visible and joinable, without starting its timer, until the first join occurs. | US-009 |
| FR-013 | The system shall allow a seller to cancel a deal only if no participant has joined it. | US-010 |
| FR-014 | The system shall reject a seller's cancellation request for a deal that has one or more participants. | US-010 |
| FR-015 | The system shall allow a seller to view the outcomes of their past deals. | US-011 |
| FR-016 | The system shall allow a buyer to join an open, unresolved deal with available capacity. | US-012 |
| FR-017 | The system shall check for a free slot, then reserve one slot and list the buyer as a participant, before creating that participant's pending order. | US-012 |
| FR-018 | The system shall start a deal's timer at the moment its first participant joins. | US-012 |
| FR-019 | The system shall create a pending order for the joining buyer before requesting payment authorization. | US-012 |
| FR-019a | The system shall authorize (hold, not charge) a buyer's payment method for the discount price, triggered after the pending order is created for that join. | Owner: US-022; triggered by US-012 |
| FR-020 | The system shall reject a join request with no side effects if the deal is full or already resolved. | US-012 |
| FR-021 | The system shall reject a second, concurrent join request from a buyer who already has an active participation in that deal. | US-012 |
| FR-021a | The system shall allow a buyer to join a deal for a product they have already purchased through a normal order, and to rejoin a deal they previously left. | US-012 |
| FR-022 | The system shall cancel the pending order and release the reserved slot back to the pool if the payment authorization for that join is declined; no payment void occurs. | Owner: US-025; triggered by US-012 |
| FR-023 | The system shall display live deal progress (participants joined vs. target, time remaining) to buyers viewing an active deal, updated in near real time. | US-013 |
| FR-024 | The system shall allow a participant in a deal to generate a shareable referral link tied to that specific deal. | US-014 |
| FR-025 | The system shall route new participants who join via a referral link through the standard join flow. | US-015 |
| FR-026 | The system shall record the referrer for a buyer who joins via a referral link, for tracking purposes. | US-015 |
| FR-027 | The system shall allow a participant to leave a deal only while it is open and more than 10 minutes remain before timer expiry. | US-016 |
| FR-028 | The system shall reject a leave request if the deal has resolved or fewer than 10 minutes remain before timer expiry. | US-016 |
| FR-029 | The system shall, upon a valid leave request, release the participant's slot and cancel their order. | US-016 |
| FR-029a | The system shall void a participant's payment hold, triggered when they leave a deal. | Owner: US-024; triggered by US-016 |
| FR-030 | The system shall automatically resolve a deal as successful the instant its stock cap fills, even if time remains. | US-017 |
| FR-031 | The system shall automatically resolve a deal as successful when its timer expires and at least the minimum required participants have joined. | US-018 |
| FR-032 | The system shall automatically resolve a deal as failed when its timer expires with fewer than the minimum required participants joined. | US-019 |
| FR-033 | The system shall, upon deal success, confirm every pending order tied to the deal. | US-020 |
| FR-034 | The system shall capture every associated payment hold, triggered upon deal success. | Owner: US-023; triggered by US-020 |
| FR-035 | The system shall notify all participants of the outcome, triggered upon deal success. | Owner: US-032; triggered by US-020 |
| FR-036 | The system shall, upon deal failure, cancel every pending order tied to the deal. | US-021 |
| FR-037 | The system shall void every associated payment hold, triggered upon deal failure. | Owner: US-024; triggered by US-021 |
| FR-038 | The system shall, upon deal failure, release the deal's reserved inventory back to general stock. | US-021 |
| FR-039 | The system shall notify all participants of the outcome, triggered upon deal failure. | Owner: US-032; triggered by US-021 |
| FR-040 | The system shall authorize and capture payment in the same step for normal (non-deal) purchases. | US-026 |
| FR-041 | The system shall allow a buyer to add one or more products to a cart. | US-027 |
| FR-042 | The system shall allow a buyer to check out a cart as a single order with one line item per product. | US-028 |
| FR-043 | The system shall immediately confirm a normal purchase order upon successful checkout. | US-028 |
| FR-044 | The system shall allow a buyer to view their order history, covering both deal orders and normal purchases. | US-029 |
| FR-045 | The system shall allow a buyer to view the status of an individual order (pending, confirmed, or cancelled). | US-030 |
| FR-046 | The system shall send a buyer an email join confirmation upon successfully joining a deal, distinct from a deal-success notification. | US-031 |
| FR-047 | The system shall send all participants an email deal outcome notification when a deal resolves, using a success-specific template on success and a failure-specific template on failure, each including order information plus outcome-specific messaging. | US-032 |
| FR-048 | The system shall send a buyer an email order confirmation, using a plain order-information template distinct from the deal outcome templates, for normal purchases. | US-033 |
| FR-049 | The system shall allow an admin to view all deals and their current status. | US-034 |
| FR-050 | The system shall queue every newly created product listing for admin review before it becomes visible to buyers. | US-035 |
| FR-050a | The system shall allow an admin to approve a pending product listing, making it visible to buyers. | US-035 |
| FR-050b | The system shall allow an admin to reject a pending product listing, keeping it hidden from buyers. | US-035 |
| FR-051 | The system shall ensure each buyer receives their own individual unit of a product on deal success; group participation aggregates demand only, and nothing is split or shared between participants. | Section 7, Rules & Constraints |

---

# 7. Non-Functional Requirements

## Performance
**NFR-001** Live deal progress (participant count, time remaining) shall update in near real time for anyone viewing an active deal.
**NFR-002** Buyers shall receive deal outcome notifications (success/failure) close to real time.

## Reliability
**NFR-003** The system shall never charge a buyer for a deal that did not succeed.
**NFR-004** Deal outcomes shall resolve automatically, with no manual trigger required from a seller or admin.

## Data Integrity / Concurrency
**NFR-005** The system shall never allow more participants to join a deal than its stock cap, even under concurrent join attempts.

## Security
**NFR-006** The system shall use JWT-based authentication for user access.
**NFR-007** Security requirements beyond JWT-based authentication (password policy, encryption standards, session/token expiry, rate limiting) are not currently defined.

## Scalability
**NFR-008** Concrete scalability targets (concurrent users, deals, or transactions per second) are not currently defined.

## Availability
**NFR-009** Availability/uptime targets are not currently defined.

## Usability
**NFR-010** Usability requirements (accessibility standards, supported devices/browsers, UI response-time expectations) are not currently defined.

## Maintainability
**NFR-011** The system's payment/order/inventory workflows shall be implemented using sagas — sequences of coordinated steps across services with defined compensating actions if a later step fails — to support maintainable, recoverable distributed transactions.

---