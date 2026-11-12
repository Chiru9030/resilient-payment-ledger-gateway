import { hashPayload } from '../shared/crypto.js';
import {
  IdempotencyInProgressError,
  IdempotencyMismatchError,
  ValidationError,
} from '../shared/errors.js';
import type { StorageAdapter } from '../storage/interface.js';

export interface IdempotentResult<T> {
  statusCode: number;
  body: T;
  replayed: boolean;
}

/**
 * Coordinates Idempotency-Key execution across HTTP endpoints.
 * Guarantees at-most-once execution of side-effecting mutations even under
 * network retries or concurrent duplicate requests.
 */
export class IdempotencyService {
  constructor(private readonly storage: StorageAdapter) {}

  async execute<T>(
    key: string | undefined,
    endpoint: string,
    requestPayload: unknown,
    handler: () => Promise<{ statusCode: number; body: T }>,
  ): Promise<IdempotentResult<T>> {
    if (!key) {
      const result = await handler();
      return { ...result, replayed: false };
    }

    if (key.trim().length === 0 || key.length > 255) {
      throw new ValidationError('Idempotency-Key must be a non-empty string (max 255 chars)');
    }

    const requestHash = hashPayload(requestPayload);

    const claim = await this.storage.withTransaction(async (tx) => {
      return tx.claimIdempotencyKey(key, endpoint, requestHash);
    });

    if (!claim.claimed) {
      const { existing } = claim;
      if (existing.requestHash !== requestHash) {
        throw new IdempotencyMismatchError(key);
      }
      if (existing.status === 'IN_PROGRESS') {
        throw new IdempotencyInProgressError(key);
      }
      if (existing.status === 'COMPLETED' && existing.responseBody !== null) {
        return {
          statusCode: existing.responseStatus ?? 200,
          body: JSON.parse(existing.responseBody) as T,
          replayed: true,
        };
      }
    }

    try {
      const result = await handler();
      await this.storage.withTransaction(async (tx) => {
        await tx.completeIdempotencyKey(
          key,
          endpoint,
          result.statusCode,
          JSON.stringify(result.body),
        );
      });
      return { ...result, replayed: false };
    } catch (err) {
      // Release lock on failure so the client can safely retry with the same key
      await this.storage
        .withTransaction(async (tx) => {
          await tx.failIdempotencyKey(key, endpoint);
        })
        .catch(() => undefined);
      throw err;
    }
  }
}
