export class GatewayError extends Error {
  public readonly statusCode: number;
  public readonly code: string;
  public readonly details?: Record<string, unknown>;

  constructor(
    statusCode: number,
    code: string,
    message: string,
    details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = new.target.name;
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }

  toJSON(requestId?: string) {
    return {
      error: {
        code: this.code,
        message: this.message,
        ...(this.details ? { details: this.details } : {}),
        ...(requestId ? { request_id: requestId } : {}),
      },
    };
  }
}

export class ValidationError extends GatewayError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(400, 'validation_error', message, details);
  }
}

export class NotFoundError extends GatewayError {
  constructor(resource: string, id: string) {
    super(404, 'resource_not_found', `${resource} '${id}' not found`, { resource, id });
  }
}

export class InsufficientFundsError extends GatewayError {
  constructor(accountId: string, available: number, required: number) {
    super(402, 'insufficient_funds', 'Account has insufficient funds for this operation', {
      accountId,
      available,
      required,
    });
  }
}

export class ConcurrencyConflictError extends GatewayError {
  constructor(message = 'Concurrent modification detected; retry transaction') {
    super(409, 'concurrency_conflict', message);
  }
}

export class IdempotencyInProgressError extends GatewayError {
  constructor(key: string) {
    super(
      409,
      'idempotency_in_progress',
      `A request with Idempotency-Key '${key}' is currently in progress`,
      { idempotencyKey: key },
    );
  }
}

export class IdempotencyMismatchError extends GatewayError {
  constructor(key: string) {
    super(
      422,
      'idempotency_key_reused',
      `Idempotency-Key '${key}' was already used with different request parameters`,
      { idempotencyKey: key },
    );
  }
}

export class InvalidStateError extends GatewayError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(409, 'invalid_state_transition', message, details);
  }
}

export class PayloadTooLargeError extends GatewayError {
  constructor(maxBytes: number) {
    super(413, 'payload_too_large', `Request body exceeds maximum size of ${maxBytes} bytes`, {
      maxBytes,
    });
  }
}

export class UnauthorizedError extends GatewayError {
  constructor(message = 'Missing or invalid API key') {
    super(401, 'unauthorized', message);
  }
}

