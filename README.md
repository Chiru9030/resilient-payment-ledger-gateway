# Resilient Payment & Ledger Gateway Simulator

A microservices-ready payment gateway and double-entry ledger system built in TypeScript/Node.js, designed around financial correctness, idempotency, and reliable asynchronous webhook delivery.

## Architecture

- **API Service (`SERVICE_ROLE=api`)**: REST endpoints for wallets, transfers, invoices, subscriptions, and webhook management.
- **Billing Worker (`SERVICE_ROLE=billing-worker`)**: Periodically bills due subscriptions inside isolated ACID transactions, transitioning delinquent accounts to `PAST_DUE`.
- **Webhook Worker (`SERVICE_ROLE=webhook-worker`)**: Polls the transactional outbox, signs payloads with Stripe-style `HMAC-SHA256` (`X-Webhook-Signature: t=...,v1=...`), retries failures with bounded exponential backoff + jitter, and moves exhausted deliveries to a Dead-Letter Queue (`DEAD`).

## Key Engineering Guarantees

1. **Idempotency (`Idempotency-Key`)**:
   - Atomically claims `(key, endpoint)` with `IN_PROGRESS` state.
   - Canonicalizes and hashes request payloads (`SHA-256`): returns `422 Unprocessable Entity` if reused with different parameters, `409 Conflict` if a concurrent request is in-flight, and replays cached status/body (`X-Idempotency-Replayed: true`) on safe retries.
2. **ACID Transactions & Concurrency Control**:
   - All monetary values are stored as integer minor units (cents) — zero floating-point drift.
   - Double-entry invariant (`SUM(DEBIT) === SUM(CREDIT)`) enforced on every posted transaction.
   - Deterministic sorted account locking (`ORDER BY id ASC`) + Optimistic Concurrency Control (`version` CAS) prevents deadlocks and lost updates under concurrent transfers.
3. **Transactional Outbox & Webhook Delivery**:
   - Outbox events are written inside the *same* ACID database transaction as the ledger mutation, preventing dual-write inconsistencies.
   - Constant-time HMAC verification (`verifyWebhookSignature`), exponential backoff with jitter, DLQ, and manual replay.

## Quick Start

```bash
npm install
npm test        # Runs unit & concurrency test suite (zero external deps via SQLite WAL)
npm run build   # Compiles TypeScript to dist/
npm start       # Starts monolith API + workers on :3000
```

Or run the full distributed microservices stack with PostgreSQL + Redis:

```bash
docker compose up --build
```

