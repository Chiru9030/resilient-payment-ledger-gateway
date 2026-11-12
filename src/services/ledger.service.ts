import { randomUUID } from 'node:crypto';
import {
  ConcurrencyConflictError,
  InsufficientFundsError,
  NotFoundError,
  ValidationError,
} from '../shared/errors.js';
import type {
  Account,
  AccountType,
  Currency,
  LedgerEntry,
  LedgerTransaction,
} from '../shared/types.js';
import type { StorageAdapter, TxContext } from '../storage/interface.js';

export interface TransferInput {
  fromAccountId: string;
  toAccountId: string;
  amount: number;
  currency: Currency;
  description?: string;
  referenceType?: LedgerTransaction['referenceType'];
  referenceId?: string;
}

export class LedgerService {
  private readonly maxOccRetries = 3;

  constructor(private readonly storage: StorageAdapter) {}

  private validateAmount(amount: number): void {
    if (!Number.isSafeInteger(amount) || amount <= 0) {
      throw new ValidationError('Amount must be a positive integer in minor units (cents)', {
        amount,
      });
    }
  }

  async createAccount(input: {
    id?: string;
    ownerId: string;
    type: AccountType;
    currency: Currency;
    initialBalance?: number;
  }): Promise<Account> {
    if (!input.ownerId || input.ownerId.trim() === '') {
      throw new ValidationError('ownerId is required');
    }
    const initial = input.initialBalance ?? 0;
    if (!Number.isSafeInteger(initial) || initial < 0) {
      throw new ValidationError('initialBalance must be a non-negative integer in minor units');
    }

    const id = input.id ?? `acct_${randomUUID().replace(/-/g, '').slice(0, 16)}`;
    return this.storage.withTransaction(async (tx) => {
      return tx.createAccount({
        id,
        ownerId: input.ownerId,
        type: input.type,
        currency: input.currency,
        initialBalance: initial,
      });
    });
  }

  async getAccount(id: string): Promise<Account> {
    const acct = await this.storage.withTransaction((tx) => tx.getAccount(id));
    if (!acct) throw new NotFoundError('Account', id);
    return acct;
  }

  async listAccountEntries(accountId: string, limit = 50): Promise<LedgerEntry[]> {
    return this.storage.withTransaction(async (tx) => {
      const acct = await tx.getAccount(accountId);
      if (!acct) throw new NotFoundError('Account', accountId);
      return tx.listAccountEntries(accountId, limit);
    });
  }

  async deposit(
    accountId: string,
    amount: number,
    currency: Currency,
    description = 'Wallet deposit',
  ): Promise<LedgerTransaction> {
    this.validateAmount(amount);
    const treasuryId = `sys_treasury_${currency.toLowerCase()}`;

    return this.executeWithOccRetry(async (tx) => {
      let treasury = await tx.getAccount(treasuryId);
      if (!treasury) {
        treasury = await tx.createAccount({
          id: treasuryId,
          ownerId: 'system',
          type: 'SYSTEM_TREASURY',
          currency,
          initialBalance: 0,
        });
      }
      return this.postTransferInTx(tx, {
        fromAccountId: treasuryId,
        toAccountId: accountId,
        amount,
        currency,
        description,
        referenceType: 'DEPOSIT',
        referenceId: `dep_${randomUUID().slice(0, 12)}`,
      });
    });
  }

  async transfer(input: TransferInput): Promise<LedgerTransaction> {
    this.validateAmount(input.amount);
    if (input.fromAccountId === input.toAccountId) {
      throw new ValidationError('Source and destination accounts must differ');
    }

    return this.executeWithOccRetry((tx) => this.postTransferInTx(tx, input));
  }

  /**
   * Executes a double-entry transfer inside an existing ACID transaction context
   * and atomically enqueues outbox webhook events.
   */
  async postTransferInTx(tx: TxContext, input: TransferInput): Promise<LedgerTransaction> {
    this.validateAmount(input.amount);
    if (input.fromAccountId === input.toAccountId) {
      throw new ValidationError('Source and destination accounts must differ');
    }

    // Lock accounts in deterministic ID order to prevent deadlocks
    const accounts = await tx.getAccountsForUpdate([input.fromAccountId, input.toAccountId]);
    const fromAcct = accounts.find((a) => a.id === input.fromAccountId);
    const toAcct = accounts.find((a) => a.id === input.toAccountId);

    if (!fromAcct) throw new NotFoundError('Account', input.fromAccountId);
    if (!toAcct) throw new NotFoundError('Account', input.toAccountId);

    if (fromAcct.currency !== input.currency || toAcct.currency !== input.currency) {
      throw new ValidationError('Currency mismatch between accounts and transfer', {
        fromCurrency: fromAcct.currency,
        toCurrency: toAcct.currency,
        transferCurrency: input.currency,
      });
    }

    // System treasury accounts are contra-asset accounts allowed to go negative
    if (fromAcct.type !== 'SYSTEM_TREASURY' && fromAcct.balance < input.amount) {
      throw new InsufficientFundsError(fromAcct.id, fromAcct.balance, input.amount);
    }

    const newFromBalance = fromAcct.balance - input.amount;
    const newToBalance = toAcct.balance + input.amount;

    const fromUpdated = await tx.updateAccountBalanceOCC(
      fromAcct.id,
      newFromBalance,
      fromAcct.version,
    );
    if (!fromUpdated) {
      throw new ConcurrencyConflictError(`OCC conflict updating account ${fromAcct.id}`);
    }

    const toUpdated = await tx.updateAccountBalanceOCC(toAcct.id, newToBalance, toAcct.version);
    if (!toUpdated) {
      throw new ConcurrencyConflictError(`OCC conflict updating account ${toAcct.id}`);
    }

    const txId = `txn_${randomUUID().replace(/-/g, '').slice(0, 16)}`;
    const ledgerTx = await tx.insertTransaction({
      id: txId,
      referenceType: input.referenceType ?? 'TRANSFER',
      referenceId: input.referenceId ?? txId,
      description: input.description ?? 'Funds transfer',
      entries: [
        {
          id: `ent_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
          transactionId: txId,
          accountId: fromAcct.id,
          direction: 'DEBIT',
          amount: input.amount,
          currency: input.currency,
          balanceAfter: newFromBalance,
        },
        {
          id: `ent_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
          transactionId: txId,
          accountId: toAcct.id,
          direction: 'CREDIT',
          amount: input.amount,
          currency: input.currency,
          balanceAfter: newToBalance,
        },
      ],
    });

    // Double-entry invariant assertion
    const debits = ledgerTx.entries
      .filter((e) => e.direction === 'DEBIT')
      .reduce((s, e) => s + e.amount, 0);
    const credits = ledgerTx.entries
      .filter((e) => e.direction === 'CREDIT')
      .reduce((s, e) => s + e.amount, 0);
    if (debits !== credits) {
      throw new Error(`Double-entry invariant violated: debits=${debits} credits=${credits}`);
    }

    // Transactional Outbox: emit webhook event inside the same DB transaction
    const eventType =
      input.referenceType === 'DEPOSIT' ? 'wallet.deposit.succeeded' : 'transfer.succeeded';
    await this.enqueueOutboxInTx(tx, eventType, {
      transactionId: ledgerTx.id,
      referenceType: ledgerTx.referenceType,
      referenceId: ledgerTx.referenceId,
      fromAccountId: fromAcct.id,
      toAccountId: toAcct.id,
      amount: input.amount,
      currency: input.currency,
    });

    return ledgerTx;
  }

  async enqueueOutboxInTx(
    tx: TxContext,
    eventType: string,
    data: Record<string, unknown>,
  ): Promise<void> {
    const endpoints = await tx.listActiveWebhookEndpointsForEvent(eventType);
    if (endpoints.length === 0) return;

    const now = new Date().toISOString();
    const payload = JSON.stringify({
      id: `evt_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
      type: eventType,
      createdAt: now,
      data,
    });

    for (const ep of endpoints) {
      await tx.enqueueWebhookEvent({
        id: `whk_msg_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
        endpointId: ep.id,
        eventType,
        payload,
        maxAttempts: 5,
        nextAttemptAt: now,
      });
    }
  }

  private async executeWithOccRetry<T>(fn: (tx: TxContext) => Promise<T>): Promise<T> {
    let attempt = 0;
    while (true) {
      attempt++;
      try {
        return await this.storage.withTransaction(fn);
      } catch (err) {
        if (err instanceof ConcurrencyConflictError && attempt < this.maxOccRetries) {
          continue;
        }
        throw err;
      }
    }
  }
}

