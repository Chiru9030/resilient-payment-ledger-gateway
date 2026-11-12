import { Pool, type PoolClient } from 'pg';
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

export class PostgresStorage implements StorageAdapter {
  private readonly pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async migrate(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        type TEXT NOT NULL,
        currency TEXT NOT NULL,
        balance BIGINT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      );

      CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        reference_type TEXT NOT NULL,
        reference_id TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
      );

      CREATE TABLE IF NOT EXISTS ledger_entries (
        id TEXT PRIMARY KEY,
        transaction_id TEXT NOT NULL REFERENCES transactions(id),
        account_id TEXT NOT NULL REFERENCES accounts(id),
        direction TEXT NOT NULL CHECK(direction IN ('DEBIT', 'CREDIT')),
        amount BIGINT NOT NULL CHECK(amount > 0),
        currency TEXT NOT NULL,
        balance_after BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_ledger_entries_account ON ledger_entries(account_id, created_at);

      CREATE TABLE IF NOT EXISTS idempotency_records (
        key TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        status TEXT NOT NULL,
        response_status INTEGER,
        response_body TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (key, endpoint)
      );

      CREATE TABLE IF NOT EXISTS invoices (
        id TEXT PRIMARY KEY,
        customer_account_id TEXT NOT NULL REFERENCES accounts(id),
        merchant_account_id TEXT NOT NULL REFERENCES accounts(id),
        amount BIGINT NOT NULL CHECK(amount > 0),
        currency TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        transaction_id TEXT,
        due_at TIMESTAMPTZ NOT NULL,
        paid_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL
      );

      CREATE TABLE IF NOT EXISTS subscriptions (
        id TEXT PRIMARY KEY,
        customer_account_id TEXT NOT NULL REFERENCES accounts(id),
        merchant_account_id TEXT NOT NULL REFERENCES accounts(id),
        plan_name TEXT NOT NULL,
        amount BIGINT NOT NULL CHECK(amount > 0),
        currency TEXT NOT NULL,
        interval TEXT NOT NULL,
        status TEXT NOT NULL,
        current_period_end TIMESTAMPTZ NOT NULL,
        cycles_billed INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL
      );

      CREATE TABLE IF NOT EXISTS webhook_endpoints (
        id TEXT PRIMARY KEY,
        url TEXT NOT NULL,
        secret TEXT NOT NULL,
        events JSONB NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL
      );

      CREATE TABLE IF NOT EXISTS webhook_outbox (
        id TEXT PRIMARY KEY,
        endpoint_id TEXT NOT NULL REFERENCES webhook_endpoints(id),
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 5,
        next_attempt_at TIMESTAMPTZ NOT NULL,
        last_error TEXT,
        last_status_code INTEGER,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_webhook_outbox_due ON webhook_outbox(status, next_attempt_at);
    `);
  }

  async withTransaction<T>(fn: (tx: TxContext) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const ctx = new PostgresTxContext(client);
      const result = await fn(ctx);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

class PostgresTxContext implements TxContext {
  constructor(private readonly client: PoolClient) {}

  private mapAccount(row: Record<string, unknown>): Account {
    return {
      id: String(row.id),
      ownerId: String(row.owner_id),
      type: row.type as Account['type'],
      currency: row.currency as Account['currency'],
      balance: Number(row.balance),
      version: Number(row.version),
      createdAt: new Date(String(row.created_at)).toISOString(),
      updatedAt: new Date(String(row.updated_at)).toISOString(),
    };
  }

  async getAccount(id: string): Promise<Account | null> {
    const res = await this.client.query('SELECT * FROM accounts WHERE id = $1', [id]);
    return res.rows[0] ? this.mapAccount(res.rows[0]) : null;
  }

  async getAccountsForUpdate(ids: string[]): Promise<Account[]> {
    if (ids.length === 0) return [];
    const sorted = [...new Set(ids)].sort();
    // Sorted ORDER BY id ASC + FOR UPDATE prevents deadlocks on concurrent multi-account transfers
    const res = await this.client.query(
      'SELECT * FROM accounts WHERE id = ANY($1::text[]) ORDER BY id ASC FOR UPDATE',
      [sorted],
    );
    return res.rows.map((r) => this.mapAccount(r));
  }

  async createAccount(input: CreateAccountInput): Promise<Account> {
    const now = new Date().toISOString();
    const balance = input.initialBalance ?? 0;
    await this.client.query(
      `INSERT INTO accounts (id, owner_id, type, currency, balance, version, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, 1, $6, $6)`,
      [input.id, input.ownerId, input.type, input.currency, balance, now],
    );
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
    const res = await this.client.query(
      `UPDATE accounts SET balance = $1, version = version + 1, updated_at = $2
       WHERE id = $3 AND version = $4`,
      [newBalance, now, id, expectedVersion],
    );
    return (res.rowCount ?? 0) === 1;
  }

  async insertTransaction(input: CreateTransactionInput): Promise<LedgerTransaction> {
    const now = new Date().toISOString();
    await this.client.query(
      `INSERT INTO transactions (id, reference_type, reference_id, description, status, created_at)
       VALUES ($1, $2, $3, $4, 'POSTED', $5)`,
      [input.id, input.referenceType, input.referenceId, input.description, now],
    );
    const entries: LedgerEntry[] = [];
    for (const e of input.entries) {
      await this.client.query(
        `INSERT INTO ledger_entries (id, transaction_id, account_id, direction, amount, currency, balance_after, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [e.id, input.id, e.accountId, e.direction, e.amount, e.currency, e.balanceAfter, now],
      );
      entries.push({ ...e, createdAt: now });
    }
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
    const txRes = await this.client.query('SELECT * FROM transactions WHERE id = $1', [id]);
    if (!txRes.rows[0]) return null;
    const row = txRes.rows[0];
    const entryRes = await this.client.query(
      'SELECT * FROM ledger_entries WHERE transaction_id = $1 ORDER BY created_at ASC',
      [id],
    );
    return {
      id: String(row.id),
      referenceType: row.reference_type,
      referenceId: String(row.reference_id),
      description: String(row.description),
      status: row.status,
      createdAt: new Date(String(row.created_at)).toISOString(),
      entries: entryRes.rows.map((e) => ({
        id: String(e.id),
        transactionId: String(e.transaction_id),
        accountId: String(e.account_id),
        direction: e.direction,
        amount: Number(e.amount),
        currency: e.currency,
        balanceAfter: Number(e.balance_after),
        createdAt: new Date(String(e.created_at)).toISOString(),
      })),
    };
  }

  async listAccountEntries(accountId: string, limit = 50): Promise<LedgerEntry[]> {
    const res = await this.client.query(
      'SELECT * FROM ledger_entries WHERE account_id = $1 ORDER BY created_at DESC LIMIT $2',
      [accountId, limit],
    );
    return res.rows.map((e) => ({
      id: String(e.id),
      transactionId: String(e.transaction_id),
      accountId: String(e.account_id),
      direction: e.direction,
      amount: Number(e.amount),
      currency: e.currency,
      balanceAfter: Number(e.balance_after),
      createdAt: new Date(String(e.created_at)).toISOString(),
    }));
  }

  async claimIdempotencyKey(
    key: string,
    endpoint: string,
    requestHash: string,
  ): Promise<{ claimed: true } | { claimed: false; existing: IdempotencyRecord }> {
    const now = new Date().toISOString();
    const res = await this.client.query(
      `INSERT INTO idempotency_records
       (key, endpoint, request_hash, status, response_status, response_body, created_at, updated_at)
       VALUES ($1, $2, $3, 'IN_PROGRESS', NULL, NULL, $4, $4)
       ON CONFLICT (key, endpoint) DO NOTHING`,
      [key, endpoint, requestHash, now],
    );
    if ((res.rowCount ?? 0) === 1) return { claimed: true };
    const existingRes = await this.client.query(
      'SELECT * FROM idempotency_records WHERE key = $1 AND endpoint = $2',
      [key, endpoint],
    );
    const ex = existingRes.rows[0];
    return {
      claimed: false,
      existing: {
        key: String(ex.key),
        endpoint: String(ex.endpoint),
        requestHash: String(ex.request_hash),
        status: ex.status,
        responseStatus: ex.response_status !== null ? Number(ex.response_status) : null,
        responseBody: ex.response_body !== null ? String(ex.response_body) : null,
        createdAt: new Date(String(ex.created_at)).toISOString(),
        updatedAt: new Date(String(ex.updated_at)).toISOString(),
      },
    };
  }

  async completeIdempotencyKey(
    key: string,
    endpoint: string,
    statusCode: number,
    responseBody: string,
  ): Promise<void> {
    await this.client.query(
      `UPDATE idempotency_records SET status = 'COMPLETED', response_status = $1, response_body = $2, updated_at = $3
       WHERE key = $4 AND endpoint = $5`,
      [statusCode, responseBody, new Date().toISOString(), key, endpoint],
    );
  }

  async failIdempotencyKey(key: string, endpoint: string): Promise<void> {
    await this.client.query('DELETE FROM idempotency_records WHERE key = $1 AND endpoint = $2', [
      key,
      endpoint,
    ]);
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
      dueAt: new Date(String(row.due_at)).toISOString(),
      paidAt: row.paid_at !== null ? new Date(String(row.paid_at)).toISOString() : null,
      createdAt: new Date(String(row.created_at)).toISOString(),
    };
  }

  async createInvoice(input: CreateInvoiceInput): Promise<Invoice> {
    const now = new Date().toISOString();
    await this.client.query(
      `INSERT INTO invoices (id, customer_account_id, merchant_account_id, amount, currency, description, status, transaction_id, due_at, paid_at, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, 'OPEN', NULL, $7, NULL, $8)`,
      [
        input.id,
        input.customerAccountId,
        input.merchantAccountId,
        input.amount,
        input.currency,
        input.description,
        input.dueAt,
        now,
      ],
    );
    return (await this.getInvoice(input.id))!;
  }

  async getInvoice(id: string): Promise<Invoice | null> {
    const res = await this.client.query('SELECT * FROM invoices WHERE id = $1', [id]);
    return res.rows[0] ? this.mapInvoice(res.rows[0]) : null;
  }

  async updateInvoiceStatus(
    id: string,
    status: InvoiceStatus,
    transactionId: string | null = null,
    paidAt: string | null = null,
  ): Promise<Invoice> {
    await this.client.query(
      `UPDATE invoices SET status = $1, transaction_id = COALESCE($2, transaction_id), paid_at = COALESCE($3, paid_at) WHERE id = $4`,
      [status, transactionId, paidAt, id],
    );
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
      currentPeriodEnd: new Date(String(row.current_period_end)).toISOString(),
      cyclesBilled: Number(row.cycles_billed),
      createdAt: new Date(String(row.created_at)).toISOString(),
    };
  }

  async createSubscription(input: CreateSubscriptionInput): Promise<Subscription> {
    const now = new Date().toISOString();
    await this.client.query(
      `INSERT INTO subscriptions (id, customer_account_id, merchant_account_id, plan_name, amount, currency, interval, status, current_period_end, cycles_billed, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'ACTIVE', $8, 0, $9)`,
      [
        input.id,
        input.customerAccountId,
        input.merchantAccountId,
        input.planName,
        input.amount,
        input.currency,
        input.interval,
        input.currentPeriodEnd,
        now,
      ],
    );
    return (await this.getSubscription(input.id))!;
  }

  async getSubscription(id: string): Promise<Subscription | null> {
    const res = await this.client.query('SELECT * FROM subscriptions WHERE id = $1', [id]);
    return res.rows[0] ? this.mapSubscription(res.rows[0]) : null;
  }

  async listDueSubscriptions(asOfIso: string): Promise<Subscription[]> {
    const res = await this.client.query(
      `SELECT * FROM subscriptions WHERE status = 'ACTIVE' AND current_period_end <= $1 ORDER BY current_period_end ASC FOR UPDATE SKIP LOCKED`,
      [asOfIso],
    );
    return res.rows.map((r) => this.mapSubscription(r));
  }

  async updateSubscriptionBilling(
    id: string,
    status: SubscriptionStatus,
    currentPeriodEnd: string,
    cyclesBilled: number,
  ): Promise<Subscription> {
    await this.client.query(
      `UPDATE subscriptions SET status = $1, current_period_end = $2, cycles_billed = $3 WHERE id = $4`,
      [status, currentPeriodEnd, cyclesBilled, id],
    );
    return (await this.getSubscription(id))!;
  }

  async cancelSubscription(id: string): Promise<Subscription> {
    await this.client.query(`UPDATE subscriptions SET status = 'CANCELED' WHERE id = $1`, [id]);
    return (await this.getSubscription(id))!;
  }

  async createWebhookEndpoint(input: CreateWebhookEndpointInput): Promise<WebhookEndpoint> {
    const now = new Date().toISOString();
    await this.client.query(
      `INSERT INTO webhook_endpoints (id, url, secret, events, active, created_at) VALUES ($1, $2, $3, $4, TRUE, $5)`,
      [input.id, input.url, input.secret, JSON.stringify(input.events), now],
    );
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
    const res = await this.client.query('SELECT * FROM webhook_endpoints WHERE id = $1', [id]);
    if (!res.rows[0]) return null;
    const row = res.rows[0];
    return {
      id: String(row.id),
      url: String(row.url),
      secret: String(row.secret),
      events: row.events as string[],
      active: Boolean(row.active),
      createdAt: new Date(String(row.created_at)).toISOString(),
    };
  }

  async listActiveWebhookEndpointsForEvent(eventType: string): Promise<WebhookEndpoint[]> {
    const res = await this.client.query('SELECT * FROM webhook_endpoints WHERE active = TRUE');
    return res.rows
      .map((row) => ({
        id: String(row.id),
        url: String(row.url),
        secret: String(row.secret),
        events: row.events as string[],
        active: true,
        createdAt: new Date(String(row.created_at)).toISOString(),
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
      nextAttemptAt: new Date(String(row.next_attempt_at)).toISOString(),
      lastError: row.last_error !== null ? String(row.last_error) : null,
      lastStatusCode: row.last_status_code !== null ? Number(row.last_status_code) : null,
      createdAt: new Date(String(row.created_at)).toISOString(),
      updatedAt: new Date(String(row.updated_at)).toISOString(),
    };
  }

  async enqueueWebhookEvent(input: EnqueueWebhookInput): Promise<WebhookOutboxEvent> {
    const now = new Date().toISOString();
    await this.client.query(
      `INSERT INTO webhook_outbox
       (id, endpoint_id, event_type, payload, status, attempt_count, max_attempts, next_attempt_at, last_error, last_status_code, created_at, updated_at)
       VALUES ($1, $2, $3, $4, 'PENDING', 0, $5, $6, NULL, NULL, $7, $7)`,
      [
        input.id,
        input.endpointId,
        input.eventType,
        input.payload,
        input.maxAttempts,
        input.nextAttemptAt,
        now,
      ],
    );
    return (await this.getWebhookEvent(input.id))!;
  }

  async claimDueWebhookEvents(asOfIso: string, limit: number): Promise<WebhookOutboxEvent[]> {
    const res = await this.client.query(
      `SELECT * FROM webhook_outbox WHERE status = 'PENDING' AND next_attempt_at <= $1
       ORDER BY next_attempt_at ASC LIMIT $2 FOR UPDATE SKIP LOCKED`,
      [asOfIso, limit],
    );
    return res.rows.map((r) => this.mapOutboxEvent(r));
  }

  async updateWebhookEventAttempt(
    id: string,
    status: WebhookDeliveryStatus,
    attemptCount: number,
    nextAttemptAt: string,
    lastStatusCode: number | null,
    lastError: string | null,
  ): Promise<void> {
    await this.client.query(
      `UPDATE webhook_outbox SET status = $1, attempt_count = $2, next_attempt_at = $3, last_status_code = $4, last_error = $5, updated_at = $6 WHERE id = $7`,
      [status, attemptCount, nextAttemptAt, lastStatusCode, lastError, new Date().toISOString(), id],
    );
  }

  async getWebhookEvent(id: string): Promise<WebhookOutboxEvent | null> {
    const res = await this.client.query('SELECT * FROM webhook_outbox WHERE id = $1', [id]);
    return res.rows[0] ? this.mapOutboxEvent(res.rows[0]) : null;
  }

  async listWebhookEvents(status?: WebhookDeliveryStatus): Promise<WebhookOutboxEvent[]> {
    const res = status
      ? await this.client.query(
          'SELECT * FROM webhook_outbox WHERE status = $1 ORDER BY created_at DESC',
          [status],
        )
      : await this.client.query('SELECT * FROM webhook_outbox ORDER BY created_at DESC');
    return res.rows.map((r) => this.mapOutboxEvent(r));
  }
}
