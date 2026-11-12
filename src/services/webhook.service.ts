import { randomUUID } from 'node:crypto';
import { signWebhookPayload } from '../shared/crypto.js';
import { NotFoundError, ValidationError } from '../shared/errors.js';
import type {
  WebhookDeliveryStatus,
  WebhookEndpoint,
  WebhookOutboxEvent,
} from '../shared/types.js';
import type { StorageAdapter } from '../storage/interface.js';

export type WebhookHttpDeliverer = (
  url: string,
  payload: string,
  headers: Record<string, string>,
) => Promise<{ statusCode: number; error?: string }>;

export interface BackoffOptions {
  baseDelayMs: number;
  maxDelayMs: number;
  jitterRatio?: number;
}

const DEFAULT_BACKOFF: BackoffOptions = {
  baseDelayMs: 1000,
  maxDelayMs: 60_000,
  jitterRatio: 0.2,
};

export function computeExponentialBackoffMs(
  attempt: number,
  opts: BackoffOptions = DEFAULT_BACKOFF,
  rng: () => number = Math.random,
): number {
  const exp = Math.min(opts.maxDelayMs, opts.baseDelayMs * Math.pow(2, Math.max(0, attempt - 1)));
  const jitter = exp * (opts.jitterRatio ?? 0.2) * rng();
  return Math.round(exp + jitter);
}

const defaultDeliverer: WebhookHttpDeliverer = async (url, payload, headers) => {
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers,
      body: payload,
      signal: AbortSignal.timeout(5000),
    });
    return { statusCode: res.status, error: res.ok ? undefined : `HTTP ${res.status}` };
  } catch (err) {
    return {
      statusCode: 0,
      error: err instanceof Error ? err.message : 'Network delivery error',
    };
  }
};

export class WebhookService {
  constructor(
    private readonly storage: StorageAdapter,
    private readonly deliverer: WebhookHttpDeliverer = defaultDeliverer,
    private readonly backoffOpts: BackoffOptions = DEFAULT_BACKOFF,
  ) {}

  async registerEndpoint(input: {
    url: string;
    secret?: string;
    events?: string[];
  }): Promise<WebhookEndpoint> {
    if (!input.url || !input.url.startsWith('http')) {
      throw new ValidationError('Webhook url must be a valid http(s) URL');
    }
    const secret = input.secret ?? `whsec_${randomUUID().replace(/-/g, '')}`;
    const events = input.events && input.events.length > 0 ? input.events : ['*'];

    return this.storage.withTransaction((tx) =>
      tx.createWebhookEndpoint({
        id: `we_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
        url: input.url,
        secret,
        events,
      }),
    );
  }

  async listEvents(status?: WebhookDeliveryStatus): Promise<WebhookOutboxEvent[]> {
    return this.storage.withTransaction((tx) => tx.listWebhookEvents(status));
  }

  async replayDeadEvent(eventId: string): Promise<WebhookOutboxEvent> {
    return this.storage.withTransaction(async (tx) => {
      const ev = await tx.getWebhookEvent(eventId);
      if (!ev) throw new NotFoundError('WebhookEvent', eventId);
      const now = new Date().toISOString();
      await tx.updateWebhookEventAttempt(ev.id, 'PENDING', 0, now, null, null);
      return (await tx.getWebhookEvent(ev.id))!;
    });
  }

  /**
   * Processes due outbox events, signing each payload with HMAC-SHA256.
   * Failed deliveries are retried with exponential backoff until `maxAttempts`,
   * after which they move to the Dead-Letter Queue (`DEAD`).
   */
  async processDueEvents(
    asOfIso = new Date().toISOString(),
    batchSize = 25,
  ): Promise<{ attempted: number; delivered: number; retried: number; deadLettered: number }> {
    const due = await this.storage.withTransaction((tx) => tx.claimDueWebhookEvents(asOfIso, batchSize));
    let delivered = 0;
    let retried = 0;
    let deadLettered = 0;

    for (const event of due) {
      const endpoint = await this.storage.withTransaction((tx) =>
        tx.getWebhookEndpoint(event.endpointId),
      );
      if (!endpoint || !endpoint.active) {
        await this.storage.withTransaction((tx) =>
          tx.updateWebhookEventAttempt(
            event.id,
            'DEAD',
            event.attemptCount + 1,
            event.nextAttemptAt,
            null,
            'Endpoint missing or inactive',
          ),
        );
        deadLettered++;
        continue;
      }

      const signature = signWebhookPayload(event.payload, endpoint.secret);
      const result = await this.deliverer(endpoint.url, event.payload, {
        'Content-Type': 'application/json',
        'X-Webhook-Id': event.id,
        'X-Webhook-Event': event.eventType,
        'X-Webhook-Signature': signature,
      });

      const nextAttemptCount = event.attemptCount + 1;
      const succeeded = result.statusCode >= 200 && result.statusCode < 300;

      if (succeeded) {
        await this.storage.withTransaction((tx) =>
          tx.updateWebhookEventAttempt(
            event.id,
            'DELIVERED',
            nextAttemptCount,
            asOfIso,
            result.statusCode,
            null,
          ),
        );
        delivered++;
      } else if (nextAttemptCount >= event.maxAttempts) {
        await this.storage.withTransaction((tx) =>
          tx.updateWebhookEventAttempt(
            event.id,
            'DEAD',
            nextAttemptCount,
            asOfIso,
            result.statusCode || null,
            result.error ?? `HTTP ${result.statusCode}`,
          ),
        );
        deadLettered++;
      } else {
        const delayMs = computeExponentialBackoffMs(nextAttemptCount, this.backoffOpts);
        const nextRetryIso = new Date(new Date(asOfIso).getTime() + delayMs).toISOString();
        await this.storage.withTransaction((tx) =>
          tx.updateWebhookEventAttempt(
            event.id,
            'PENDING',
            nextAttemptCount,
            nextRetryIso,
            result.statusCode || null,
            result.error ?? `HTTP ${result.statusCode}`,
          ),
        );
        retried++;
      }
    }

    return { attempted: due.length, delivered, retried, deadLettered };
  }
}
