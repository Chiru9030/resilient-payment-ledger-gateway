import { createGatewayServer } from './api/server.js';
import { BillingService } from './services/billing.service.js';
import { IdempotencyService } from './services/idempotency.service.js';
import { LedgerService } from './services/ledger.service.js';
import { WebhookService } from './services/webhook.service.js';
import type { StorageAdapter } from './storage/interface.js';
import { PostgresStorage } from './storage/postgres.js';
import { SqliteStorage } from './storage/sqlite.js';

async function bootstrap() {
  const port = Number(process.env.PORT ?? 3000);
  const serviceRole = process.env.SERVICE_ROLE ?? 'all';
  const databaseUrl = process.env.DATABASE_URL;
  const sqlitePath = process.env.SQLITE_PATH ?? './ledger.db';

  const storage: StorageAdapter = databaseUrl
    ? new PostgresStorage(databaseUrl)
    : new SqliteStorage(sqlitePath);

  await storage.migrate();

  const ledger = new LedgerService(storage);
  const idempotency = new IdempotencyService(storage);
  const billing = new BillingService(storage, ledger);
  const webhooks = new WebhookService(storage);

  let webhookTimer: NodeJS.Timeout | undefined;
  let billingTimer: NodeJS.Timeout | undefined;

  if (serviceRole === 'all' || serviceRole === 'webhook-worker') {
    webhookTimer = setInterval(() => {
      webhooks.processDueEvents().catch((err) => {
        console.error('[webhook-worker] error:', err);
      });
    }, 2000);
  }

  if (serviceRole === 'all' || serviceRole === 'billing-worker') {
    billingTimer = setInterval(() => {
      billing.runBillingCycle().catch((err) => {
        console.error('[billing-worker] error:', err);
      });
    }, 10_000);
  }

  let server: ReturnType<typeof createGatewayServer> | undefined;
  if (serviceRole === 'all' || serviceRole === 'api') {
    server = createGatewayServer({ ledger, idempotency, billing, webhooks });
    server.listen(port, () => {
      console.log(`[gateway:${serviceRole}] listening on :${port}`);
    });
  } else {
    console.log(`[gateway:${serviceRole}] worker started`);
  }

  const shutdown = async () => {
    if (webhookTimer) clearInterval(webhookTimer);
    if (billingTimer) clearInterval(billingTimer);
    if (server) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    await storage.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

bootstrap().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
