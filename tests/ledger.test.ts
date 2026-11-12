import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { LedgerService } from '../src/services/ledger.service.js';
import { InsufficientFundsError, ValidationError } from '../src/shared/errors.js';
import { SqliteStorage } from '../src/storage/sqlite.js';

describe('LedgerService — ACID, Double-Entry & Concurrency', () => {
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

  it('maintains double-entry invariant (debits == credits) across deposits and transfers', async () => {
    const alice = await ledger.createAccount({ ownerId: 'alice', type: 'USER_WALLET', currency: 'USD' });
    const bob = await ledger.createAccount({ ownerId: 'bob', type: 'USER_WALLET', currency: 'USD' });

    const depTx = await ledger.deposit(alice.id, 10_000, 'USD');
    expect(depTx.entries).toHaveLength(2);
    expect(depTx.entries.find((e) => e.accountId === alice.id)?.direction).toBe('CREDIT');

    const xferTx = await ledger.transfer({
      fromAccountId: alice.id,
      toAccountId: bob.id,
      amount: 3_500,
      currency: 'USD',
    });

    const debits = xferTx.entries.filter((e) => e.direction === 'DEBIT').reduce((s, e) => s + e.amount, 0);
    const credits = xferTx.entries.filter((e) => e.direction === 'CREDIT').reduce((s, e) => s + e.amount, 0);
    expect(debits).toBe(3_500);
    expect(credits).toBe(3_500);

    const aliceAfter = await ledger.getAccount(alice.id);
    const bobAfter = await ledger.getAccount(bob.id);
    expect(aliceAfter.balance).toBe(6_500);
    expect(bobAfter.balance).toBe(3_500);
    expect(aliceAfter.version).toBe(3); // create(1) -> deposit(2) -> transfer(3)
    expect(bobAfter.version).toBe(2);
  });

  it('prevents overdraft under high concurrency (20 parallel transfers from $100 wallet)', async () => {
    const sender = await ledger.createAccount({
      ownerId: 'sender',
      type: 'USER_WALLET',
      currency: 'USD',
      initialBalance: 10_000, // $100.00
    });
    const receiver = await ledger.createAccount({
      ownerId: 'receiver',
      type: 'MERCHANT_WALLET',
      currency: 'USD',
    });

    // Fire 20 concurrent $15 transfers ($300 total attempted against $100 balance)
    const results = await Promise.allSettled(
      Array.from({ length: 20 }, (_, i) =>
        ledger.transfer({
          fromAccountId: sender.id,
          toAccountId: receiver.id,
          amount: 1_500,
          currency: 'USD',
          description: `concurrent-${i}`,
        }),
      ),
    );

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');

    // Only floor(10000 / 1500) = 6 transfers can succeed
    expect(fulfilled).toHaveLength(6);
    expect(rejected).toHaveLength(14);
    for (const r of rejected) {
      if (r.status === 'rejected') {
        expect(r.reason).toBeInstanceOf(InsufficientFundsError);
      }
    }

    const senderAfter = await ledger.getAccount(sender.id);
    const receiverAfter = await ledger.getAccount(receiver.id);
    expect(senderAfter.balance).toBe(1_000);
    expect(receiverAfter.balance).toBe(9_000);
    expect(senderAfter.balance + receiverAfter.balance).toBe(10_000);
  });

  it('rejects non-integer or negative amounts and currency mismatches', async () => {
    const usd = await ledger.createAccount({ ownerId: 'u1', type: 'USER_WALLET', currency: 'USD', initialBalance: 5000 });
    const eur = await ledger.createAccount({ ownerId: 'u2', type: 'USER_WALLET', currency: 'EUR' });

    await expect(
      ledger.transfer({ fromAccountId: usd.id, toAccountId: eur.id, amount: 10.5, currency: 'USD' }),
    ).rejects.toBeInstanceOf(ValidationError);

    await expect(
      ledger.transfer({ fromAccountId: usd.id, toAccountId: eur.id, amount: 1000, currency: 'USD' }),
    ).rejects.toBeInstanceOf(ValidationError);
  });
});

