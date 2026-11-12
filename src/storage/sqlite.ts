import { createRequire } from 'node:module';
import type { DatabaseSync as DatabaseSyncType } from 'node:sqlite';

const require = createRequire(import.meta.url);
const { DatabaseSync } = require('node:sqlite') as typeof import('node:sqlite');
import type {
  Account,
  IdempotencyRecord,
  Invoice,
  InvoiceStatus,
  LedgerEntry,
  LedgerTransaction,
  Subscription,
  SubscriptionStatus,
  WebhookDeliveryStatus,
  WebhookEndpoint,
  WebhookOutboxEvent,
} from '../shared/types.js';
import type {
  CreateAccountInput,
  CreateInvoiceInput,
  CreateSubscriptionInput,
  CreateTransactionInput,
  CreateWebhookEndpointInput,
  EnqueueWebhookInput,
  StorageAdapter,
  TxContext,
} from './interface.js';

export class SqliteStorage implements StorageAdapter {
  private readonly db: DatabaseSyncType;
  private txMutex: Promise<void> = Promise.resolve();

  constructor(dbPath = ':memory:') {
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec('PRAGMA foreign_keys = ON;');
    this.db.exec('PRAGMA busy_timeout = 5000;');
  }

  async migrate(): Promise<void> {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS accounts (
        id TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        type TEXT NOT NULL,
        currency TEXT NOT NULL,
        balance INTEGER NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        reference_type TEXT NOT NULL,
        reference_id TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS ledger_entries (
        id TEXT PRIMARY KEY,
        transaction_id TEXT NOT NULL REFERENCES transactions(id),
        account_id TEXT NOT NULL REFERENCES accounts(id),
        direction TEXT NOT NULL CHECK(direction IN ('DEBIT', 'CREDIT')),
        amount INTEGER NOT NULL CHECK(amount > 0),
        currency TEXT NOT NULL,
        balance_after INTEGER NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_ledger_entries_account ON ledger_entries(account_id, created_at);

      CREATE TABLE IF NOT EXISTS idempotency_records (
        key TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        status TEXT NOT NULL,
        response_status INTEGER,
        response_body TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (key, endpoint)
      );

      CREATE TABLE IF NOT EXISTS invoices (
        id TEXT PRIMARY KEY,
        customer_account_id TEXT NOT NULL REFERENCES accounts(id),
        merchant_account_id TEXT NOT NULL REFERENCES accounts(id),
        amount INTEGER NOT NULL CHECK(amount > 0),
        currency TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        transaction_id TEXT,
        due_at TEXT NOT NULL,
        paid_at TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS subscriptions (
        id TEXT PRIMARY KEY,
        customer_account_id TEXT NOT NULL REFERENCES accounts(id),
        merchant_account_id TEXT NOT NULL REFERENCES accounts(id),
        plan_name TEXT NOT NULL,
        amount INTEGER NOT NULL CHECK(amount > 0),
        currency TEXT NOT NULL,
        interval TEXT NOT NULL,
        status TEXT NOT NULL,
        current_period_end TEXT NOT NULL,
        cycles_billed INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS webhook_endpoints (
        id TEXT PRIMARY KEY,
        url TEXT NOT NULL,
        secret TEXT NOT NULL,
        events TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS webhook_outbox (
        id TEXT PRIMARY KEY,
        endpoint_id TEXT NOT NULL REFERENCES webhook_endpoints(id),
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 5,
        next_attempt_at TEXT NOT NULL,
        last_error TEXT,
        last_status_code INTEGER,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_webhook_outbox_due ON webhook_outbox(status, next_attempt_at);
    `);
  }

  async withTransaction<T>(fn: (tx: TxContext) => Promise<T>): Promise<T> {
    let release!: () => void;
    const prev = this.txMutex;
    this.txMutex = new Promise<void>((res) => {
      release = res;
    });
    await prev;

    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const ctx = new SqliteTxContext(this.db);
      const result = await fn(ctx);
      this.db.exec('COMMIT;');
      return result;
    } catch (err) {
      try {
        this.db.exec('ROLLBACK;');
      } catch {
        // Ignore rollback errors if already rolled back
      }
      throw err;
    } finally {
      release();
    }
  }

  async close(): Promise<void> {
    this.db.close();
  }
}

class SqliteTxContext implements TxContext {
  constructor(private readonly db: DatabaseSyncType) {}

  private mapAccount(row: Record<string, unknown>): Account {
    return {
      id: String(row.id),
      ownerId: String(row.owner_id),
      type: row.type as Account['type'],
      currency: row.currency as Account['currency'],
      balance: Number(row.balance),
      version: Number(row.version),
      createdAt: String(row.created_at),
      updatedAt: String(row.updated_at),
    };
  }

  async getAccount(id: string): Promise<Account | null> {
    const row = this.db.prepare('SELECT * FROM accounts WHERE id = ?').get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? this.mapAccount(row) : null;
  }

  async getAccountsForUpdate(ids: string[]): Promise<Account[]> {
    if (ids.length === 0) return [];
    const sorted = [...new Set(ids)].sort();
    const placeholders = sorted.map(() => '?').join(',');
    const rows = this.db
      .prepare(`SELECT * FROM accounts WHERE id IN (${placeholders}) ORDER BY id ASC`)
      .all(...sorted) as Record<string, unknown>[];
    return rows.map((r) => this.mapAccount(r));
  }

  async createAccount(input: CreateAccountInput): Promise<Account> {
    const now = new Date().toISOString();
    const balance = input.initialBalance ?? 0;
    this.db
      .prepare(
        `INSERT INTO accounts (id, owner_id, type, currency, balance, version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, 1, ?, ?)`,
      )
      .run(input.id, input.ownerId, input.type, input.currency, balance, now, now);
    return {
      id: input.id,
      ownerId: input.ownerId,
      type: input.type,
      currency: input.currency,
      balance,
      version: 1,
      createdAt: now,
      updatedAt: now,
    };
  }

  async updateAccountBalanceOCC(
    id: string,
    newBalance: number,
    expectedVersion: number,
  ): Promise<boolean> {
    const now = new Date().toISOString();
    const info = this.db
      .prepare(
        `UPDATE accounts
         SET balance = ?, version = version + 1, updated_at = ?
         WHERE id = ? AND version = ?`,
      )
      .run(newBalance, now, id, expectedVersion);
    return Number(info.changes) === 1;
  }

  async insertTransaction(input: CreateTransactionInput): Promise<LedgerTransaction> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `INSERT INTO transactions (id, reference_type, reference_id, description, status, created_at)
         VALUES (?, ?, ?, ?, 'POSTED', ?)`,
      )
      .run(input.id, input.referenceType, input.referenceId, input.description, now);

    const insertEntry = this.db.prepare(
      `INSERT INTO ledger_entries (id, transaction_id, account_id, direction, amount, currency, balance_after, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    );

    const entries: LedgerEntry[] = input.entries.map((e) => {
      insertEntry.run(
        e.id,
        input.id,
        e.accountId,
        e.direction,
        e.amount,
        e.currency,
        e.balanceAfter,
        now,
      );
      return { ...e, createdAt: now };
    });

    return {
      id: input.id,
      referenceType: input.referenceType,
      referenceId: input.referenceId,
      description: input.description,
      status: 'POSTED',
      entries,
      createdAt: now,
    };
  }

  async getTransaction(id: string): Promise<LedgerTransaction | null> {
    const row = this.db.prepare('SELECT * FROM transactions WHERE id = ?').get(id) as
      | Record<string, unknown>
      | undefined;
    if (!row) return null;
    const entryRows = this.db
      .prepare('SELECT * FROM ledger_entries WHERE transaction_id = ? ORDER BY created_at ASC')
      .all(id) as Record<string, unknown>[];
    return {
      id: String(row.id),
      referenceType: row.reference_type as LedgerTransaction['referenceType'],
      referenceId: String(row.reference_id),
      description: String(row.description),
      status: row.status as LedgerTransaction['status'],
      createdAt: String(row.created_at),
      entries: entryRows.map((e) => ({
        id: String(e.id),
        transactionId: String(e.transaction_id),
        accountId: String(e.account_id),
        direction: e.direction as LedgerEntry['direction'],
        amount: Number(e.amount),
        currency: e.currency as LedgerEntry['currency'],
        balanceAfter: Number(e.balance_after),
        createdAt: String(e.created_at),
      })),
    };
  }

  async listAccountEntries(accountId: string, limit = 50): Promise<LedgerEntry[]> {
    const rows = this.db
      .prepare(
        'SELECT * FROM ledger_entries WHERE account_id = ? ORDER BY created_at DESC LIMIT ?',
      )
      .all(accountId, limit) as Record<string, unknown>[];
    return rows.map((e) => ({
      id: String(e.id),
      transactionId: String(e.transaction_id),
      accountId: String(e.account_id),
      direction: e.direction as LedgerEntry['direction'],
      amount: Number(e.amount),
      currency: e.currency as LedgerEntry['currency'],
      balanceAfter: Number(e.balance_after),
      createdAt: String(e.created_at),
    }));
  }

  async claimIdempotencyKey(
    key: string,
    endpoint: string,
    requestHash: string,
    lockTimeoutMs = 30_000,
    ttlMs = 86_400_000,
  ): Promise<{ claimed: true } | { claimed: false; existing: IdempotencyRecord }> {
    const nowMs = Date.now();
    const now = new Date(nowMs).toISOString();
    const info = this.db
      .prepare(
        `INSERT OR IGNORE INTO idempotency_records
         (key, endpoint, request_hash, status, response_status, response_body, created_at, updated_at)
         VALUES (?, ?, ?, 'IN_PROGRESS', NULL, NULL, ?, ?)`,
      )
      .run(key, endpoint, requestHash, now, now);

    if (Number(info.changes) === 1) {
      return { claimed: true };
    }

    const existing = this.db
      .prepare('SELECT * FROM idempotency_records WHERE key = ? AND endpoint = ?')
      .get(key, endpoint) as Record<string, unknown>;

    const updatedMs = new Date(String(existing.updated_at)).getTime();
    const status = String(existing.status);
    const isStaleLock = status === 'IN_PROGRESS' && nowMs - updatedMs > lockTimeoutMs;
    const isExpired = status === 'COMPLETED' && nowMs - updatedMs > ttlMs;

    if (isStaleLock || isExpired) {
      this.db
        .prepare(
          `UPDATE idempotency_records
           SET request_hash = ?, status = 'IN_PROGRESS', response_status = NULL, response_body = NULL, updated_at = ?
           WHERE key = ? AND endpoint = ?`,
        )
        .run(requestHash, now, key, endpoint);
      return { claimed: true };
    }

    return {
      claimed: false,
      existing: {
        key: String(existing.key),
        endpoint: String(existing.endpoint),
        requestHash: String(existing.request_hash),
        status: existing.status as IdempotencyRecord['status'],
        responseStatus: existing.response_status !== null ? Number(existing.response_status) : null,
        responseBody: existing.response_body !== null ? String(existing.response_body) : null,
        createdAt: String(existing.created_at),
        updatedAt: String(existing.updated_at),
      },
    };
  }

  async completeIdempotencyKey(
    key: string,
    endpoint: string,
    statusCode: number,
    responseBody: string,
  ): Promise<void> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `UPDATE idempotency_records
         SET status = 'COMPLETED', response_status = ?, response_body = ?, updated_at = ?
         WHERE key = ? AND endpoint = ?`,
      )
      .run(statusCode, responseBody, now, key, endpoint);
  }

  async failIdempotencyKey(key: string, endpoint: string): Promise<void> {
    this.db
      .prepare('DELETE FROM idempotency_records WHERE key = ? AND endpoint = ?')
      .run(key, endpoint);
  }

  private mapInvoice(row: Record<string, unknown>): Invoice {
    return {
      id: String(row.id),
      customerAccountId: String(row.customer_account_id),
      merchantAccountId: String(row.merchant_account_id),
      amount: Number(row.amount),
      currency: row.currency as Invoice['currency'],
      description: String(row.description),
      status: row.status as InvoiceStatus,
      transactionId: row.transaction_id !== null ? String(row.transaction_id) : null,
      dueAt: String(row.due_at),
      paidAt: row.paid_at !== null ? String(row.paid_at) : null,
      createdAt: String(row.created_at),
    };
  }

  async createInvoice(input: CreateInvoiceInput): Promise<Invoice> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `INSERT INTO invoices (id, customer_account_id, merchant_account_id, amount, currency, description, status, transaction_id, due_at, paid_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, 'OPEN', NULL, ?, NULL, ?)`,
      )
      .run(
        input.id,
        input.customerAccountId,
        input.merchantAccountId,
        input.amount,
        input.currency,
        input.description,
        input.dueAt,
        now,
      );
    return (await this.getInvoice(input.id))!;
  }

  async getInvoice(id: string): Promise<Invoice | null> {
    const row = this.db.prepare('SELECT * FROM invoices WHERE id = ?').get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? this.mapInvoice(row) : null;
  }

  async updateInvoiceStatus(
    id: string,
    status: InvoiceStatus,
    transactionId: string | null = null,
    paidAt: string | null = null,
  ): Promise<Invoice> {
    this.db
      .prepare(
        `UPDATE invoices SET status = ?, transaction_id = COALESCE(?, transaction_id), paid_at = COALESCE(?, paid_at) WHERE id = ?`,
      )
      .run(status, transactionId, paidAt, id);
    return (await this.getInvoice(id))!;
  }

  private mapSubscription(row: Record<string, unknown>): Subscription {
    return {
      id: String(row.id),
      customerAccountId: String(row.customer_account_id),
      merchantAccountId: String(row.merchant_account_id),
      planName: String(row.plan_name),
      amount: Number(row.amount),
      currency: row.currency as Subscription['currency'],
      interval: row.interval as Subscription['interval'],
      status: row.status as SubscriptionStatus,
      currentPeriodEnd: String(row.current_period_end),
      cyclesBilled: Number(row.cycles_billed),
      createdAt: String(row.created_at),
    };
  }

  async createSubscription(input: CreateSubscriptionInput): Promise<Subscription> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `INSERT INTO subscriptions (id, customer_account_id, merchant_account_id, plan_name, amount, currency, interval, status, current_period_end, cycles_billed, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, 0, ?)`,
      )
      .run(
        input.id,
        input.customerAccountId,
        input.merchantAccountId,
        input.planName,
        input.amount,
        input.currency,
        input.interval,
        input.currentPeriodEnd,
        now,
      );
    return (await this.getSubscription(input.id))!;
  }

  async getSubscription(id: string): Promise<Subscription | null> {
    const row = this.db.prepare('SELECT * FROM subscriptions WHERE id = ?').get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? this.mapSubscription(row) : null;
  }

  async listDueSubscriptions(asOfIso: string): Promise<Subscription[]> {
    const rows = this.db
      .prepare(
        `SELECT * FROM subscriptions WHERE status = 'ACTIVE' AND current_period_end <= ? ORDER BY current_period_end ASC`,
      )
      .all(asOfIso) as Record<string, unknown>[];
    return rows.map((r) => this.mapSubscription(r));
  }

  async updateSubscriptionBilling(
    id: string,
    status: SubscriptionStatus,
    currentPeriodEnd: string,
    cyclesBilled: number,
  ): Promise<Subscription> {
    this.db
      .prepare(
        `UPDATE subscriptions SET status = ?, current_period_end = ?, cycles_billed = ? WHERE id = ?`,
      )
      .run(status, currentPeriodEnd, cyclesBilled, id);
    return (await this.getSubscription(id))!;
  }

  async cancelSubscription(id: string): Promise<Subscription> {
    this.db.prepare(`UPDATE subscriptions SET status = 'CANCELED' WHERE id = ?`).run(id);
    return (await this.getSubscription(id))!;
  }

  async createWebhookEndpoint(input: CreateWebhookEndpointInput): Promise<WebhookEndpoint> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `INSERT INTO webhook_endpoints (id, url, secret, events, active, created_at)
         VALUES (?, ?, ?, ?, 1, ?)`,
      )
      .run(input.id, input.url, input.secret, JSON.stringify(input.events), now);
    return {
      id: input.id,
      url: input.url,
      secret: input.secret,
      events: input.events,
      active: true,
      createdAt: now,
    };
  }

  async getWebhookEndpoint(id: string): Promise<WebhookEndpoint | null> {
    const row = this.db.prepare('SELECT * FROM webhook_endpoints WHERE id = ?').get(id) as
      | Record<string, unknown>
      | undefined;
    if (!row) return null;
    return {
      id: String(row.id),
      url: String(row.url),
      secret: String(row.secret),
      events: JSON.parse(String(row.events)) as string[],
      active: Number(row.active) === 1,
      createdAt: String(row.created_at),
    };
  }

  async listActiveWebhookEndpointsForEvent(eventType: string): Promise<WebhookEndpoint[]> {
    const rows = this.db
      .prepare('SELECT * FROM webhook_endpoints WHERE active = 1')
      .all() as Record<string, unknown>[];
    return rows
      .map((row) => ({
        id: String(row.id),
        url: String(row.url),
        secret: String(row.secret),
        events: JSON.parse(String(row.events)) as string[],
        active: true,
        createdAt: String(row.created_at),
      }))
      .filter((ep) => ep.events.includes('*') || ep.events.includes(eventType));
  }

  private mapOutboxEvent(row: Record<string, unknown>): WebhookOutboxEvent {
    return {
      id: String(row.id),
      endpointId: String(row.endpoint_id),
      eventType: String(row.event_type),
      payload: String(row.payload),
      status: row.status as WebhookDeliveryStatus,
      attemptCount: Number(row.attempt_count),
      maxAttempts: Number(row.max_attempts),
      nextAttemptAt: String(row.next_attempt_at),
      lastError: row.last_error !== null ? String(row.last_error) : null,
      lastStatusCode: row.last_status_code !== null ? Number(row.last_status_code) : null,
      createdAt: String(row.created_at),
      updatedAt: String(row.updated_at),
    };
  }

  async enqueueWebhookEvent(input: EnqueueWebhookInput): Promise<WebhookOutboxEvent> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `INSERT INTO webhook_outbox
         (id, endpoint_id, event_type, payload, status, attempt_count, max_attempts, next_attempt_at, last_error, last_status_code, created_at, updated_at)
         VALUES (?, ?, ?, ?, 'PENDING', 0, ?, ?, NULL, NULL, ?, ?)`,
      )
      .run(
        input.id,
        input.endpointId,
        input.eventType,
        input.payload,
        input.maxAttempts,
        input.nextAttemptAt,
        now,
        now,
      );
    return (await this.getWebhookEvent(input.id))!;
  }

  async claimDueWebhookEvents(asOfIso: string, limit: number): Promise<WebhookOutboxEvent[]> {
    const rows = this.db
      .prepare(
        `SELECT * FROM webhook_outbox
         WHERE status = 'PENDING' AND next_attempt_at <= ?
         ORDER BY next_attempt_at ASC LIMIT ?`,
      )
      .all(asOfIso, limit) as Record<string, unknown>[];
    return rows.map((r) => this.mapOutboxEvent(r));
  }

  async updateWebhookEventAttempt(
    id: string,
    status: WebhookDeliveryStatus,
    attemptCount: number,
    nextAttemptAt: string,
    lastStatusCode: number | null,
    lastError: string | null,
  ): Promise<void> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `UPDATE webhook_outbox
         SET status = ?, attempt_count = ?, next_attempt_at = ?, last_status_code = ?, last_error = ?, updated_at = ?
         WHERE id = ?`,
      )
      .run(status, attemptCount, nextAttemptAt, lastStatusCode, lastError, now, id);
  }

  async getWebhookEvent(id: string): Promise<WebhookOutboxEvent | null> {
    const row = this.db.prepare('SELECT * FROM webhook_outbox WHERE id = ?').get(id) as
      | Record<string, unknown>
      | undefined;
    return row ? this.mapOutboxEvent(row) : null;
  }

  async listWebhookEvents(status?: WebhookDeliveryStatus): Promise<WebhookOutboxEvent[]> {
    const rows = status
      ? (this.db
          .prepare('SELECT * FROM webhook_outbox WHERE status = ? ORDER BY created_at DESC')
          .all(status) as Record<string, unknown>[])
      : (this.db
          .prepare('SELECT * FROM webhook_outbox ORDER BY created_at DESC')
          .all() as Record<string, unknown>[]);
    return rows.map((r) => this.mapOutboxEvent(r));
  }
}
