import type {
  Account,
  AccountType,
  BillingInterval,
  Currency,
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

export interface CreateAccountInput {
  id: string;
  ownerId: string;
  type: AccountType;
  currency: Currency;
  initialBalance?: number;
}

export interface CreateTransactionInput {
  id: string;
  referenceType: LedgerTransaction['referenceType'];
  referenceId: string;
  description: string;
  entries: Omit<LedgerEntry, 'createdAt'>[];
}

export interface CreateInvoiceInput {
  id: string;
  customerAccountId: string;
  merchantAccountId: string;
  amount: number;
  currency: Currency;
  description: string;
  dueAt: string;
}

export interface CreateSubscriptionInput {
  id: string;
  customerAccountId: string;
  merchantAccountId: string;
  planName: string;
  amount: number;
  currency: Currency;
  interval: BillingInterval;
  currentPeriodEnd: string;
}

export interface CreateWebhookEndpointInput {
  id: string;
  url: string;
  secret: string;
  events: string[];
}

export interface EnqueueWebhookInput {
  id: string;
  endpointId: string;
  eventType: string;
  payload: string;
  maxAttempts: number;
  nextAttemptAt: string;
}

export interface TxContext {
  getAccount(id: string): Promise<Account | null>;
  getAccountsForUpdate(ids: string[]): Promise<Account[]>;
  createAccount(input: CreateAccountInput): Promise<Account>;
  /**
   * Updates account balance using Optimistic Concurrency Control (`version = expectedVersion`).
   * Returns false if the version did not match (concurrent modification).
   */
  updateAccountBalanceOCC(id: string, newBalance: number, expectedVersion: number): Promise<boolean>;

  insertTransaction(input: CreateTransactionInput): Promise<LedgerTransaction>;
  getTransaction(id: string): Promise<LedgerTransaction | null>;
  listAccountEntries(accountId: string, limit?: number): Promise<LedgerEntry[]>;

  // Idempotency
  claimIdempotencyKey(
    key: string,
    endpoint: string,
    requestHash: string,
    lockTimeoutMs?: number,
    ttlMs?: number,
  ): Promise<
    | { claimed: true }
    | { claimed: false; existing: IdempotencyRecord }
  >;
  completeIdempotencyKey(
    key: string,
    endpoint: string,
    statusCode: number,
    responseBody: string,
  ): Promise<void>;
  failIdempotencyKey(key: string, endpoint: string): Promise<void>;

  // Invoices
  createInvoice(input: CreateInvoiceInput): Promise<Invoice>;
  getInvoice(id: string): Promise<Invoice | null>;
  updateInvoiceStatus(
    id: string,
    status: InvoiceStatus,
    transactionId?: string | null,
    paidAt?: string | null,
  ): Promise<Invoice>;

  // Subscriptions
  createSubscription(input: CreateSubscriptionInput): Promise<Subscription>;
  getSubscription(id: string): Promise<Subscription | null>;
  listDueSubscriptions(asOfIso: string): Promise<Subscription[]>;
  updateSubscriptionBilling(
    id: string,
    status: SubscriptionStatus,
    currentPeriodEnd: string,
    cyclesBilled: number,
  ): Promise<Subscription>;
  cancelSubscription(id: string): Promise<Subscription>;

  // Webhooks
  createWebhookEndpoint(input: CreateWebhookEndpointInput): Promise<WebhookEndpoint>;
  getWebhookEndpoint(id: string): Promise<WebhookEndpoint | null>;
  listActiveWebhookEndpointsForEvent(eventType: string): Promise<WebhookEndpoint[]>;
  enqueueWebhookEvent(input: EnqueueWebhookInput): Promise<WebhookOutboxEvent>;
  claimDueWebhookEvents(asOfIso: string, limit: number): Promise<WebhookOutboxEvent[]>;
  updateWebhookEventAttempt(
    id: string,
    status: WebhookDeliveryStatus,
    attemptCount: number,
    nextAttemptAt: string,
    lastStatusCode: number | null,
    lastError: string | null,
  ): Promise<void>;
  getWebhookEvent(id: string): Promise<WebhookOutboxEvent | null>;
  listWebhookEvents(status?: WebhookDeliveryStatus): Promise<WebhookOutboxEvent[]>;
}

export interface StorageAdapter {
  migrate(): Promise<void>;
  withTransaction<T>(fn: (tx: TxContext) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

