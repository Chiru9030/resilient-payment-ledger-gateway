import { randomUUID, timingSafeEqual } from 'node:crypto';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { BillingService } from '../services/billing.service.js';
import { IdempotencyService } from '../services/idempotency.service.js';
import { LedgerService } from '../services/ledger.service.js';
import { WebhookService } from '../services/webhook.service.js';
import { GatewayError, PayloadTooLargeError, UnauthorizedError, ValidationError } from '../shared/errors.js';
import type { AccountType, BillingInterval, Currency, WebhookDeliveryStatus } from '../shared/types.js';

const MAX_BODY_BYTES = 1_048_576; // 1 MiB

export interface GatewayDependencies {
  ledger: LedgerService;
  idempotency: IdempotencyService;
  billing: BillingService;
  webhooks: WebhookService;
  apiKey?: string;
}

export function createGatewayServer(deps: GatewayDependencies): Server {
  return createServer(async (req, res) => {
    const requestId = (req.headers['x-request-id'] as string | undefined) || `req_${randomUUID().slice(0, 12)}`;
    res.setHeader('X-Request-ID', requestId);

    try {
      const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
      const method = req.method ?? 'GET';
      const path = url.pathname;

      if (method === 'GET' && path === '/health') {
        return sendJson(res, 200, { status: 'ok', service: 'resilient-payment-ledger-gateway' });
      }

      if (deps.apiKey) {
        verifyApiKey(req, deps.apiKey);
      }

      // Accounts
      if (method === 'POST' && path === '/v1/accounts') {
        const body = await readJsonBody<{
          ownerId: string;
          type: AccountType;
          currency: Currency;
          initialBalance?: number;
        }>(req);
        const key = getIdempotencyKey(req);
        const result = await deps.idempotency.execute(key, 'POST /v1/accounts', body, async () => ({
          statusCode: 201,
          body: await deps.ledger.createAccount(body),
        }));
        if (result.replayed) res.setHeader('X-Idempotency-Replayed', 'true');
        return sendJson(res, result.statusCode, result.body);
      }

      const acctMatch = path.match(/^\/v1\/accounts\/([^/]+)$/);
      if (method === 'GET' && acctMatch) {
        const account = await deps.ledger.getAccount(acctMatch[1]);
        return sendJson(res, 200, account);
      }

      const entriesMatch = path.match(/^\/v1\/accounts\/([^/]+)\/entries$/);
      if (method === 'GET' && entriesMatch) {
        const limit = Number(url.searchParams.get('limit') ?? '50');
        const entries = await deps.ledger.listAccountEntries(entriesMatch[1], limit);
        return sendJson(res, 200, { entries });
      }

      const depositMatch = path.match(/^\/v1\/accounts\/([^/]+)\/deposit$/);
      if (method === 'POST' && depositMatch) {
        const body = await readJsonBody<{ amount: number; currency: Currency; description?: string }>(
          req,
        );
        const key = getIdempotencyKey(req);
        const accountId = depositMatch[1];
        const result = await deps.idempotency.execute(
          key,
          `POST /v1/accounts/${accountId}/deposit`,
          body,
          async () => ({
            statusCode: 201,
            body: await deps.ledger.deposit(accountId, body.amount, body.currency, body.description),
          }),
        );
        if (result.replayed) res.setHeader('X-Idempotency-Replayed', 'true');
        return sendJson(res, result.statusCode, result.body);
      }

      // Transfers
      if (method === 'POST' && path === '/v1/transfers') {
        const body = await readJsonBody<{
          fromAccountId: string;
          toAccountId: string;
          amount: number;
          currency: Currency;
          description?: string;
        }>(req);
        const key = getIdempotencyKey(req);
        const result = await deps.idempotency.execute(key, 'POST /v1/transfers', body, async () => ({
          statusCode: 201,
          body: await deps.ledger.transfer(body),
        }));
        if (result.replayed) res.setHeader('X-Idempotency-Replayed', 'true');
        return sendJson(res, result.statusCode, result.body);
      }

      // Invoices
      if (method === 'POST' && path === '/v1/invoices') {
        const body = await readJsonBody<{
          customerAccountId: string;
          merchantAccountId: string;
          amount: number;
          currency: Currency;
          description: string;
          dueAt?: string;
        }>(req);
        const key = getIdempotencyKey(req);
        const result = await deps.idempotency.execute(key, 'POST /v1/invoices', body, async () => ({
          statusCode: 201,
          body: await deps.billing.createInvoice(body),
        }));
        if (result.replayed) res.setHeader('X-Idempotency-Replayed', 'true');
        return sendJson(res, result.statusCode, result.body);
      }

      const invMatch = path.match(/^\/v1\/invoices\/([^/]+)$/);
      if (method === 'GET' && invMatch) {
        return sendJson(res, 200, await deps.billing.getInvoice(invMatch[1]));
      }

      const invPayMatch = path.match(/^\/v1\/invoices\/([^/]+)\/pay$/);
      if (method === 'POST' && invPayMatch) {
        const key = getIdempotencyKey(req);
        const invoiceId = invPayMatch[1];
        const result = await deps.idempotency.execute(
          key,
          `POST /v1/invoices/${invoiceId}/pay`,
          { invoiceId },
          async () => ({
            statusCode: 200,
            body: await deps.billing.payInvoice(invoiceId),
          }),
        );
        if (result.replayed) res.setHeader('X-Idempotency-Replayed', 'true');
        return sendJson(res, result.statusCode, result.body);
      }

      const invVoidMatch = path.match(/^\/v1\/invoices\/([^/]+)\/void$/);
      if (method === 'POST' && invVoidMatch) {
        return sendJson(res, 200, await deps.billing.voidInvoice(invVoidMatch[1]));
      }

      // Subscriptions
      if (method === 'POST' && path === '/v1/subscriptions') {
        const body = await readJsonBody<{
          customerAccountId: string;
          merchantAccountId: string;
          planName: string;
          amount: number;
          currency: Currency;
          interval: BillingInterval;
          firstBillAt?: string;
        }>(req);
        const key = getIdempotencyKey(req);
        const result = await deps.idempotency.execute(
          key,
          'POST /v1/subscriptions',
          body,
          async () => ({
            statusCode: 201,
            body: await deps.billing.createSubscription(body),
          }),
        );
        if (result.replayed) res.setHeader('X-Idempotency-Replayed', 'true');
        return sendJson(res, result.statusCode, result.body);
      }

      const subMatch = path.match(/^\/v1\/subscriptions\/([^/]+)$/);
      if (method === 'GET' && subMatch) {
        return sendJson(res, 200, await deps.billing.getSubscription(subMatch[1]));
      }

      const subCancelMatch = path.match(/^\/v1\/subscriptions\/([^/]+)\/cancel$/);
      if (method === 'POST' && subCancelMatch) {
        return sendJson(res, 200, await deps.billing.cancelSubscription(subCancelMatch[1]));
      }

      if (method === 'POST' && path === '/v1/subscriptions/run-billing') {
        const body = await readJsonBody<{ asOfIso?: string }>(req).catch(() => ({ asOfIso: undefined }));
        return sendJson(res, 200, await deps.billing.runBillingCycle(body.asOfIso));
      }

      // Webhooks
      if (method === 'POST' && path === '/v1/webhooks/endpoints') {
        const body = await readJsonBody<{ url: string; secret?: string; events?: string[] }>(req);
        return sendJson(res, 201, await deps.webhooks.registerEndpoint(body));
      }

      if (method === 'GET' && path === '/v1/webhooks/events') {
        const status = url.searchParams.get('status') as WebhookDeliveryStatus | null;
        return sendJson(res, 200, { events: await deps.webhooks.listEvents(status ?? undefined) });
      }

      if (method === 'POST' && path === '/v1/webhooks/dispatch') {
        return sendJson(res, 200, await deps.webhooks.processDueEvents());
      }

      const whReplayMatch = path.match(/^\/v1\/webhooks\/events\/([^/]+)\/replay$/);
      if (method === 'POST' && whReplayMatch) {
        return sendJson(res, 200, await deps.webhooks.replayDeadEvent(whReplayMatch[1]));
      }

      return sendJson(res, 404, {
        error: { code: 'not_found', message: `Route ${method} ${path} not found`, request_id: requestId },
      });
    } catch (err) {
      if (err instanceof GatewayError) {
        return sendJson(res, err.statusCode, err.toJSON(requestId));
      }
      const message = err instanceof Error ? err.message : 'Internal server error';
      return sendJson(res, 500, {
        error: { code: 'internal_error', message, request_id: requestId },
      });
    }
  });
}

function getIdempotencyKey(req: IncomingMessage): string | undefined {
  const val = req.headers['idempotency-key'];
  if (Array.isArray(val)) return val[0];
  return val;
}

function verifyApiKey(req: IncomingMessage, expected: string): void {
  const headerKey = (req.headers['x-api-key'] as string | undefined)?.trim();
  const auth = (req.headers['authorization'] as string | undefined)?.trim();
  const bearer = auth?.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : undefined;
  const provided = headerKey || bearer || '';

  const a = Buffer.from(provided);
  const b = Buffer.from(expected);
  if (a.length === 0 || a.length !== b.length || !timingSafeEqual(a, b)) {
    throw new UnauthorizedError();
  }
}

async function readJsonBody<T>(req: IncomingMessage): Promise<T> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    totalBytes += buf.length;
    if (totalBytes > MAX_BODY_BYTES) {
      throw new PayloadTooLargeError(MAX_BODY_BYTES);
    }
    chunks.push(buf);
  }
  const raw = Buffer.concat(chunks).toString('utf8').trim();
  if (!raw) return {} as T;
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new ValidationError('Invalid JSON request body');
  }
}

function sendJson(res: ServerResponse, statusCode: number, data: unknown): void {
  const body = JSON.stringify(data);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

