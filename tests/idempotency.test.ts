import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { IdempotencyService } from '../src/services/idempotency.service.js';
import { LedgerService } from '../src/services/ledger.service.js';
import { IdempotencyInProgressError, IdempotencyMismatchError } from '../src/shared/errors.js';
import { SqliteStorage } from '../src/storage/sqlite.js';

describe('IdempotencyService', () => {
  let storage: SqliteStorage;
  let ledger: LedgerService;
  let idempotency: IdempotencyService;

  beforeEach(async () => {
    storage = new SqliteStorage(':memory:');
    await storage.migrate();
    ledger = new LedgerService(storage);
    idempotency = new IdempotencyService(storage);
  });

  afterEach(async () => {
    await storage.close();
  });

  it('never charges twice when the same Idempotency-Key is retried', async () => {
    const alice = await ledger.createAccount({
      ownerId: 'alice',
      type: 'USER_WALLET',
      currency: 'USD',
      initialBalance: 10_000,
    });
    const bob = await ledger.createAccount({ ownerId: 'bob', type: 'MERCHANT_WALLET', currency: 'USD' });

    const payload = { fromAccountId: alice.id, toAccountId: bob.id, amount: 2_500, currency: 'USD' as const };

    const first = await idempotency.execute('key-123', 'POST /v1/transfers', payload, async () => ({
      statusCode: 201,
      body: await ledger.transfer(payload),
    }));
    expect(first.replayed).toBe(false);
    expect(first.statusCode).toBe(201);

    // Retry with identical key & payload
    const second = await idempotency.execute('key-123', 'POST /v1/transfers', payload, async () => ({
      statusCode: 201,
      body: await ledger.transfer(payload),
    }));
    expect(second.replayed).toBe(true);
    expect(second.body.id).toBe(first.body.id);

    // Balance was only debited once
    expect((await ledger.getAccount(alice.id)).balance).toBe(7_500);
    expect((await ledger.getAccount(bob.id)).balance).toBe(2_500);
  });

  it('rejects with 422 when an Idempotency-Key is reused with a different payload', async () => {
    const payloadA = { amount: 1000, to: 'acct_1' };
    const payloadB = { amount: 2000, to: 'acct_1' };

    await idempotency.execute('key-abc', 'POST /v1/transfers', payloadA, async () => ({
      statusCode: 201,
      body: { ok: true },
    }));

    await expect(
      idempotency.execute('key-abc', 'POST /v1/transfers', payloadB, async () => ({
        statusCode: 201,
        body: { ok: true },
      })),
    ).rejects.toBeInstanceOf(IdempotencyMismatchError);
  });

  it('rejects concurrent in-flight duplicate requests with 409 Conflict', async () => {
    const payload = { amount: 500 };
    let releaseHandler!: () => void;
    const gate = new Promise<void>((res) => {
      releaseHandler = res;
    });

    const p1 = idempotency.execute('key-inflight', 'POST /v1/transfers', payload, async () => {
      await gate;
      return { statusCode: 201, body: { done: true } };
    });

    // Yield one tick so p1 claims the key
    await new Promise((r) => setTimeout(r, 15));

    await expect(
      idempotency.execute('key-inflight', 'POST /v1/transfers', payload, async () => ({
        statusCode: 201,
        body: { done: true },
      })),
    ).rejects.toBeInstanceOf(IdempotencyInProgressError);

    releaseHandler();
    const res1 = await p1;
    expect(res1.statusCode).toBe(201);
  });

  it('reclaims a stale IN_PROGRESS lock after lockTimeoutMs expires', async () => {
    const claim1 = await storage.withTransaction((tx) =>
      tx.claimIdempotencyKey('stale-key', 'POST /v1/transfers', 'hash1', 1000, 60_000),
    );
    expect(claim1.claimed).toBe(true);

    // With lockTimeoutMs = -1 (already expired), a subsequent claim recovers the lock
    const claim2 = await storage.withTransaction((tx) =>
      tx.claimIdempotencyKey('stale-key', 'POST /v1/transfers', 'hash1', -1, 60_000),
    );
    expect(claim2.claimed).toBe(true);
  });
});

