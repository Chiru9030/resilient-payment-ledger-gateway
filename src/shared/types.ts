export type Currency = 'USD' | 'EUR' | 'GBP';

export type AccountType = 'USER_WALLET' | 'MERCHANT_WALLET' | 'SYSTEM_TREASURY' | 'REVENUE';

export interface Account {
  id: string;
  ownerId: string;
  type: AccountType;
  currency: Currency;
  /** Balance in minor units (e.g., cents). Stored as integer to prevent float drift. */
  balance: number;
  /** Optimistic concurrency control version. */
  version: number;
  createdAt: string;
  updatedAt: string;
}

export type EntryDirection = 'DEBIT' | 'CREDIT';

export interface LedgerEntry {
  id: string;
  transactionId: string;
  accountId: string;
  direction: EntryDirection;
  /** Positive integer in minor units (cents). */
  amount: number;
  currency: Currency;
  balanceAfter: number;
  createdAt: string;
}

export type TransactionStatus = 'POSTED' | 'REVERSED';

export interface LedgerTransaction {
  id: string;
  referenceType: 'TRANSFER' | 'DEPOSIT' | 'INVOICE_PAYMENT' | 'SUBSCRIPTION_CHARGE';
  referenceId: string;
  description: string;
  status: TransactionStatus;
  entries: LedgerEntry[];
  createdAt: string;
}

export type IdempotencyStatus = 'IN_PROGRESS' | 'COMPLETED' | 'FAILED';

export interface IdempotencyRecord {
  key: string;
  endpoint: string;
  requestHash: string;
  status: IdempotencyStatus;
  responseStatus: number | null;
  responseBody: string | null;
  createdAt: string;
  updatedAt: string;
}

export type InvoiceStatus = 'DRAFT' | 'OPEN' | 'PAID' | 'VOID';

export interface Invoice {
  id: string;
  customerAccountId: string;
  merchantAccountId: string;
  amount: number;
  currency: Currency;
  description: string;
  status: InvoiceStatus;
  transactionId: string | null;
  dueAt: string;
  paidAt: string | null;
  createdAt: string;
}

export type SubscriptionStatus = 'ACTIVE' | 'PAST_DUE' | 'CANCELED';
export type BillingInterval = 'DAILY' | 'WEEKLY' | 'MONTHLY';

export interface Subscription {
  id: string;
  customerAccountId: string;
  merchantAccountId: string;
  planName: string;
  amount: number;
  currency: Currency;
  interval: BillingInterval;
  status: SubscriptionStatus;
  currentPeriodEnd: string;
  cyclesBilled: number;
  createdAt: string;
}

export interface WebhookEndpoint {
  id: string;
  url: string;
  secret: string;
  events: string[];
  active: boolean;
  createdAt: string;
}

export type WebhookDeliveryStatus = 'PENDING' | 'DELIVERED' | 'DEAD';

export interface WebhookOutboxEvent {
  id: string;
  endpointId: string;
  eventType: string;
  payload: string;
  status: WebhookDeliveryStatus;
  attemptCount: number;
  maxAttempts: number;
  nextAttemptAt: string;
  lastError: string | null;
  lastStatusCode: number | null;
  createdAt: string;
  updatedAt: string;
}
