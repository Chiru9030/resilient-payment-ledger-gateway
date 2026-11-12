import { randomUUID } from 'node:crypto';
import {
  InsufficientFundsError,
  InvalidStateError,
  NotFoundError,
  ValidationError,
} from '../shared/errors.js';
import type { BillingInterval, Currency, Invoice, Subscription } from '../shared/types.js';
import type { StorageAdapter } from '../storage/interface.js';
import { LedgerService } from './ledger.service.js';

export class BillingService {
  constructor(
    private readonly storage: StorageAdapter,
    private readonly ledger: LedgerService,
  ) {}

  async createInvoice(input: {
    customerAccountId: string;
    merchantAccountId: string;
    amount: number;
    currency: Currency;
    description: string;
    dueAt?: string;
  }): Promise<Invoice> {
    if (!Number.isSafeInteger(input.amount) || input.amount <= 0) {
      throw new ValidationError('Invoice amount must be a positive integer in minor units');
    }
    if (input.customerAccountId === input.merchantAccountId) {
      throw new ValidationError('Customer and merchant accounts must differ');
    }

    const dueAt = input.dueAt ?? new Date(Date.now() + 7 * 86_400_000).toISOString();

    return this.storage.withTransaction(async (tx) => {
      const customer = await tx.getAccount(input.customerAccountId);
      if (!customer) throw new NotFoundError('Account', input.customerAccountId);
      const merchant = await tx.getAccount(input.merchantAccountId);
      if (!merchant) throw new NotFoundError('Account', input.merchantAccountId);

      const invoice = await tx.createInvoice({
        id: `inv_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
        customerAccountId: input.customerAccountId,
        merchantAccountId: input.merchantAccountId,
        amount: input.amount,
        currency: input.currency,
        description: input.description,
        dueAt,
      });

      await this.ledger.enqueueOutboxInTx(tx, 'invoice.created', {
        invoiceId: invoice.id,
        customerAccountId: invoice.customerAccountId,
        merchantAccountId: invoice.merchantAccountId,
        amount: invoice.amount,
        currency: invoice.currency,
      });

      return invoice;
    });
  }

  async getInvoice(id: string): Promise<Invoice> {
    const inv = await this.storage.withTransaction((tx) => tx.getInvoice(id));
    if (!inv) throw new NotFoundError('Invoice', id);
    return inv;
  }

  async payInvoice(invoiceId: string): Promise<Invoice> {
    return this.storage.withTransaction(async (tx) => {
      const inv = await tx.getInvoice(invoiceId);
      if (!inv) throw new NotFoundError('Invoice', invoiceId);
      if (inv.status !== 'OPEN') {
        throw new InvalidStateError(`Invoice '${invoiceId}' cannot be paid in status '${inv.status}'`, {
          status: inv.status,
        });
      }

      const ledgerTx = await this.ledger.postTransferInTx(tx, {
        fromAccountId: inv.customerAccountId,
        toAccountId: inv.merchantAccountId,
        amount: inv.amount,
        currency: inv.currency,
        description: `Payment for invoice ${inv.id}: ${inv.description}`,
        referenceType: 'INVOICE_PAYMENT',
        referenceId: inv.id,
      });

      const paid = await tx.updateInvoiceStatus(inv.id, 'PAID', ledgerTx.id, new Date().toISOString());

      await this.ledger.enqueueOutboxInTx(tx, 'invoice.paid', {
        invoiceId: paid.id,
        transactionId: ledgerTx.id,
        amount: paid.amount,
        currency: paid.currency,
      });

      return paid;
    });
  }

  async voidInvoice(invoiceId: string): Promise<Invoice> {
    return this.storage.withTransaction(async (tx) => {
      const inv = await tx.getInvoice(invoiceId);
      if (!inv) throw new NotFoundError('Invoice', invoiceId);
      if (inv.status !== 'OPEN' && inv.status !== 'DRAFT') {
        throw new InvalidStateError(`Invoice '${invoiceId}' cannot be voided in status '${inv.status}'`);
      }
      return tx.updateInvoiceStatus(inv.id, 'VOID');
    });
  }

  async createSubscription(input: {
    customerAccountId: string;
    merchantAccountId: string;
    planName: string;
    amount: number;
    currency: Currency;
    interval: BillingInterval;
    firstBillAt?: string;
  }): Promise<Subscription> {
    if (!Number.isSafeInteger(input.amount) || input.amount <= 0) {
      throw new ValidationError('Subscription amount must be a positive integer in minor units');
    }

    const firstBillAt = input.firstBillAt ?? this.advancePeriod(new Date().toISOString(), input.interval);

    return this.storage.withTransaction(async (tx) => {
      const customer = await tx.getAccount(input.customerAccountId);
      if (!customer) throw new NotFoundError('Account', input.customerAccountId);
      const merchant = await tx.getAccount(input.merchantAccountId);
      if (!merchant) throw new NotFoundError('Account', input.merchantAccountId);

      const sub = await tx.createSubscription({
        id: `sub_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
        customerAccountId: input.customerAccountId,
        merchantAccountId: input.merchantAccountId,
        planName: input.planName,
        amount: input.amount,
        currency: input.currency,
        interval: input.interval,
        currentPeriodEnd: firstBillAt,
      });

      await this.ledger.enqueueOutboxInTx(tx, 'subscription.created', {
        subscriptionId: sub.id,
        planName: sub.planName,
        amount: sub.amount,
        currency: sub.currency,
      });

      return sub;
    });
  }

  async getSubscription(id: string): Promise<Subscription> {
    const sub = await this.storage.withTransaction((tx) => tx.getSubscription(id));
    if (!sub) throw new NotFoundError('Subscription', id);
    return sub;
  }

  async cancelSubscription(id: string): Promise<Subscription> {
    return this.storage.withTransaction(async (tx) => {
      const sub = await tx.getSubscription(id);
      if (!sub) throw new NotFoundError('Subscription', id);
      if (sub.status === 'CANCELED') return sub;
      const updated = await tx.cancelSubscription(id);
      await this.ledger.enqueueOutboxInTx(tx, 'subscription.canceled', { subscriptionId: id });
      return updated;
    });
  }

  /**
   * Processes all ACTIVE subscriptions due as of `asOfIso`.
   * Each subscription is billed in its own ACID transaction; insufficient funds
   * transitions the subscription to PAST_DUE instead of failing the batch.
   */
  async runBillingCycle(asOfIso = new Date().toISOString()): Promise<{
    processed: number;
    succeeded: string[];
    pastDue: string[];
  }> {
    const dueSubs = await this.storage.withTransaction((tx) => tx.listDueSubscriptions(asOfIso));
    const succeeded: string[] = [];
    const pastDue: string[] = [];

    for (const sub of dueSubs) {
      try {
        await this.storage.withTransaction(async (tx) => {
          const fresh = await tx.getSubscription(sub.id);
          if (!fresh || fresh.status !== 'ACTIVE' || fresh.currentPeriodEnd > asOfIso) {
            return;
          }

          await this.ledger.postTransferInTx(tx, {
            fromAccountId: fresh.customerAccountId,
            toAccountId: fresh.merchantAccountId,
            amount: fresh.amount,
            currency: fresh.currency,
            description: `Subscription renewal (${fresh.planName}) cycle #${fresh.cyclesBilled + 1}`,
            referenceType: 'SUBSCRIPTION_CHARGE',
            referenceId: `${fresh.id}_cycle_${fresh.cyclesBilled + 1}`,
          });

          const nextPeriodEnd = this.advancePeriod(fresh.currentPeriodEnd, fresh.interval);
          await tx.updateSubscriptionBilling(
            fresh.id,
            'ACTIVE',
            nextPeriodEnd,
            fresh.cyclesBilled + 1,
          );

          await this.ledger.enqueueOutboxInTx(tx, 'subscription.renewed', {
            subscriptionId: fresh.id,
            cycle: fresh.cyclesBilled + 1,
            nextPeriodEnd,
          });
        });
        succeeded.push(sub.id);
      } catch (err) {
        if (err instanceof InsufficientFundsError) {
          await this.storage.withTransaction(async (tx) => {
            await tx.updateSubscriptionBilling(
              sub.id,
              'PAST_DUE',
              sub.currentPeriodEnd,
              sub.cyclesBilled,
            );
            await this.ledger.enqueueOutboxInTx(tx, 'subscription.payment_failed', {
              subscriptionId: sub.id,
              reason: 'insufficient_funds',
            });
          });
          pastDue.push(sub.id);
        } else {
          throw err;
        }
      }
    }

    return { processed: dueSubs.length, succeeded, pastDue };
  }

  private advancePeriod(isoDate: string, interval: BillingInterval): string {
    const d = new Date(isoDate);
    if (interval === 'DAILY') d.setUTCDate(d.getUTCDate() + 1);
    else if (interval === 'WEEKLY') d.setUTCDate(d.getUTCDate() + 7);
    else d.setUTCMonth(d.getUTCMonth() + 1);
    return d.toISOString();
  }
}
