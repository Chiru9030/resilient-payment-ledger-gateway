import { createHash, createHmac, timingSafeEqual } from 'node:crypto';

/**
 * Produces a deterministic SHA-256 hash of a JSON-serializable payload
 * by sorting object keys recursively before serialization.
 */
export function hashPayload(payload: unknown): string {
  const canonical = canonicalStringify(payload);
  return createHash('sha256').update(canonical).digest('hex');
}

function canonicalStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return '[' + value.map(canonicalStringify).join(',') + ']';
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return (
    '{' +
    keys
      .filter((k) => obj[k] !== undefined)
      .map((k) => `${JSON.stringify(k)}:${canonicalStringify(obj[k])}`)
      .join(',') +
    '}'
  );
}

/**
 * Signs a webhook payload using Stripe-style `t=<timestamp>,v1=<hmac_hex>` format.
 */
export function signWebhookPayload(payload: string, secret: string, timestampSec?: number): string {
  const ts = timestampSec ?? Math.floor(Date.now() / 1000);
  const signedContent = `${ts}.${payload}`;
  const sig = createHmac('sha256', secret).update(signedContent).digest('hex');
  return `t=${ts},v1=${sig}`;
}

/**
 * Verifies a Stripe-style `X-Webhook-Signature` header in constant time.
 */
export function verifyWebhookSignature(
  payload: string,
  header: string,
  secret: string,
  toleranceSeconds = 300,
): boolean {
  const parts = Object.fromEntries(
    header.split(',').map((part) => {
      const idx = part.indexOf('=');
      return idx === -1 ? [part, ''] : [part.slice(0, idx).trim(), part.slice(idx + 1).trim()];
    }),
  );

  const ts = Number(parts['t']);
  const providedSig = parts['v1'];
  if (!Number.isFinite(ts) || !providedSig) return false;

  const age = Math.abs(Math.floor(Date.now() / 1000) - ts);
  if (age > toleranceSeconds) return false;

  const expectedSig = createHmac('sha256', secret).update(`${ts}.${payload}`).digest('hex');
  const a = Buffer.from(providedSig, 'hex');
  const b = Buffer.from(expectedSig, 'hex');
  if (a.length !== b.length || a.length === 0) return false;
  return timingSafeEqual(a, b);
}

