import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { BillingService } from '../src/services/billing.service.js';
import { LedgerService } from '../src/services/ledger.service.js';
import { InvalidStateError } from '../src/shared/errors.js';
import { SqliteStorage } from '../src/storage/sqlite.js';

describe('BillingService — Invoices & Subscriptions', () => {
  let storage: SqliteStorage;
  let ledger: LedgerService;
  let billing: BillingService;

  beforeEach(async () => {
    storage = new SqliteStorage(':memory:');
    await storage.migrate();
    ledger = new LedgerService(storage);
    billing = new BillingService(storage, ledger);
  });

  afterEach(async () => {
    await storage.close();
  });

  it('creates, pays, and prevents double-payment of an invoice', async () => {
    const customer = await ledger.createAccount({
      ownerId: 'cust_1',
      type: 'USER_WALLET',
      currency: 'USD',
      initialBalance: 10_000,
    });
    const merchant = await ledger.createAccount({
      ownerId: 'merch_1',
      type: 'MERCHANT_WALLET',
      currency: 'USD',
    });

    const inv = await billing.createInvoice({
      customerAccountId: customer.id,
      merchantAccountId: merchant.id,
      amount: 4_200,
      currency: 'USD',
      description: 'Pro API Plan',
    });
    expect(inv.status).toBe('OPEN');

    const paid = await billing.payInvoice(inv.id);
    expect(paid.status).toBe('PAID');
    expect(paid.transactionId).toBeTruthy();

    await expect(billing.payInvoice(inv.id)).rejects.toBeInstanceOf(InvalidStateError);
    expect((await ledger.getAccount(customer.id)).balance).toBe(5_800);
    expect((await ledger.getAccount(merchant.id)).balance).toBe(4_200);
  });

  it('bills due subscriptions and marks PAST_DUE when funds are exhausted', async () => {
    const customer = await ledger.createAccount({
      ownerId: 'cust_2',
      type: 'USER_WALLET',
      currency: 'USD',
      initialBalance: 3_000,
    });
    const merchant = await ledger.createAccount({
      ownerId: 'merch_2',
      type: 'MERCHANT_WALLET',
      currency: 'USD',
    });

    const pastDate = new Date(Date.now() - 60_000).toISOString();
    const sub = await billing.createSubscription({
      customerAccountId: customer.id,
      merchantAccountId: merchant.id,
      planName: 'Starter',
      amount: 2_000,
      currency: 'USD',
      interval: 'MONTHLY',
      firstBillAt: pastDate,
    });

    // Cycle 1 succeeds ($20 charged, $10 left)
    const cycle1 = await billing.runBillingCycle();
    expect(cycle1.succeeded).toEqual([sub.id]);
    expect((await billing.getSubscription(sub.id)).cyclesBilled).toBe(1);

    // Advance clock 32 days -> Cycle 2 fails with InsufficientFunds -> PAST_DUE
    const futureIso = new Date(Date.now() + 32 * 86_400_000).toISOString();
    const cycle2 = await billing.runBillingCycle(futureIso);
    expect(cycle2.pastDue).toEqual([sub.id]);
    expect((await billing.getSubscription(sub.id)).status).toBe('PAST_DUE');
  });
});

