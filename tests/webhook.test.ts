import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { LedgerService } from '../src/services/ledger.service.js';
import { computeExponentialBackoffMs, WebhookService } from '../src/services/webhook.service.js';
import { verifyWebhookSignature } from '../src/shared/crypto.js';
import { SqliteStorage } from '../src/storage/sqlite.js';

describe('WebhookService — HMAC Signing, Exponential Backoff & DLQ', () => {
  let storage: SqliteStorage;
  let ledger: LedgerService;

  beforeEach(async () => {
    storage = new SqliteStorage(':memory:');
    await storage.migrate();
    ledger = new LedgerService(storage);
  });

  afterEach(async () => {
    await storage.close();
  });

  it('computes bounded exponential backoff with jitter', () => {
    const opts = { baseDelayMs: 1000, maxDelayMs: 10_000, jitterRatio: 0.2 };
    expect(computeExponentialBackoffMs(1, opts, () => 0)).toBe(1000);
    expect(computeExponentialBackoffMs(2, opts, () => 0)).toBe(2000);
    expect(computeExponentialBackoffMs(3, opts, () => 0)).toBe(4000);
    expect(computeExponentialBackoffMs(10, opts, () => 0)).toBe(10_000);
    expect(computeExponentialBackoffMs(1, opts, () => 0.5)).toBe(1100);
  });

  it('delivers signed webhooks and verifies HMAC-SHA256 signature', async () => {
    let capturedSig = '';
    let capturedPayload = '';

    const webhooks = new WebhookService(storage, async (_url, payload, headers) => {
      capturedPayload = payload;
      capturedSig = headers['X-Webhook-Signature'];
      return { statusCode: 200 };
    });

    const ep = await webhooks.registerEndpoint({
      url: 'https://merchant.example/webhooks',
      secret: 'whsec_test_secret_123',
      events: ['transfer.succeeded'],
    });

    const a = await ledger.createAccount({ ownerId: 'a', type: 'USER_WALLET', currency: 'USD', initialBalance: 5000 });
    const b = await ledger.createAccount({ ownerId: 'b', type: 'MERCHANT_WALLET', currency: 'USD' });
    await ledger.transfer({ fromAccountId: a.id, toAccountId: b.id, amount: 1500, currency: 'USD' });

    const res = await webhooks.processDueEvents();
    expect(res.delivered).toBe(1);
    expect(verifyWebhookSignature(capturedPayload, capturedSig, ep.secret)).toBe(true);
    expect(verifyWebhookSignature(capturedPayload, capturedSig, 'wrong_secret')).toBe(false);
  });

  it('retries failed deliveries with backoff and moves to DEAD after maxAttempts', async () => {
    let calls = 0;
    const webhooks = new WebhookService(
      storage,
      async () => {
        calls++;
        return { statusCode: 503, error: 'Service Unavailable' };
      },
      { baseDelayMs: 1000, maxDelayMs: 30_000, jitterRatio: 0 },
    );

    await webhooks.registerEndpoint({
      url: 'https://flaky.example/hook',
      events: ['transfer.succeeded'],
    });

    const a = await ledger.createAccount({ ownerId: 'a', type: 'USER_WALLET', currency: 'USD', initialBalance: 5000 });
    const b = await ledger.createAccount({ ownerId: 'b', type: 'MERCHANT_WALLET', currency: 'USD' });
    await ledger.transfer({ fromAccountId: a.id, toAccountId: b.id, amount: 500, currency: 'USD' });

    // Simulate advancing time across 5 attempts (maxAttempts = 5)
    let cursorMs = Date.now() + 100;
    for (let attempt = 1; attempt <= 5; attempt++) {
      const r = await webhooks.processDueEvents(new Date(cursorMs).toISOString());
      expect(r.attempted).toBe(1);
      cursorMs += 60_000; // advance past maxDelayMs
    }

    expect(calls).toBe(5);
    const dead = await webhooks.listEvents('DEAD');
    expect(dead).toHaveLength(1);
    expect(dead[0].attemptCount).toBe(5);
    expect(dead[0].lastStatusCode).toBe(503);

    // Replay dead event
    const replayed = await webhooks.replayDeadEvent(dead[0].id);
    expect(replayed.status).toBe('PENDING');
    expect(replayed.attemptCount).toBe(0);
  });
});
