import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { Database } from './infrastructure/database.js';
import { Queues } from './infrastructure/sqs.js';
import { Observability, logger } from './infrastructure/observability.js';
import { Publisher, QueueConsumer, ReferenceWorker, type LoopWorker } from './infrastructure/workers.js';
import { Wagering } from './application/wagering.js';
import { MAX_IDENTIFIER_LENGTH } from './application/contracts.js';
import { ApiErrorFilter, ApiModule } from './http/api.js';

if ((process.env.AUTH_MODE ?? 'development') !== 'development')
  throw new Error('Configure an IdentityPort adapter before enabling another AUTH_MODE');
const roles = new Set(
  (process.env.APP_ROLES ?? 'api,consumer,references,publisher,dlq').split(',').filter(Boolean),
);
for (const role of roles)
  if (!['api', 'consumer', 'references', 'publisher', 'dlq', 'events'].includes(role))
    throw new Error(`Unknown APP_ROLES value: ${role}`);
const db = await Database.connect();
await db.query('SELECT 1');
const queues = new Queues();
const metrics = new Observability();
const wagering = new Wagering(db, metrics);
const workers: LoopWorker[] = [];
if (roles.has('consumer')) workers.push(new QueueConsumer(db, queues, metrics, 'input', wagering));
if (roles.has('references')) workers.push(new ReferenceWorker(wagering));
if (roles.has('publisher')) workers.push(new Publisher(db, queues, metrics));
if (roles.has('dlq')) workers.push(new QueueConsumer(db, queues, metrics, 'dlq'));
if (roles.has('events')) workers.push(new QueueConsumer(db, queues, metrics, 'events'));
let app: NestFastifyApplication | undefined;
let port: number | undefined;
if (roles.has('api')) {
  const adapter = new FastifyAdapter({
    bodyLimit: 32768,
    // Let business validation handle oversized identifiers, including multibyte paths.
    routerOptions: { maxParamLength: MAX_IDENTIFIER_LENGTH * 2 },
    requestIdHeader: false,
    genReqId: (req: IncomingMessage) => {
      const value = req.headers['x-correlation-id'];
      return typeof value === 'string' && /^[a-zA-Z0-9._:-]{1,128}$/.test(value) ? value : randomUUID();
    },
  });
  adapter.getInstance().addHook('onRequest', async (req, reply) => {
    reply.header('X-Correlation-Id', req.id);
  });
  app = await NestFactory.create<NestFastifyApplication>(
    ApiModule.configure(db, queues, metrics, wagering),
    adapter,
    {
      logger: {
        log: (message: unknown) => logger.debug({ component: 'nest' }, String(message)),
        error: (message: unknown) => logger.error({ component: 'nest' }, String(message)),
        warn: (message: unknown) => logger.warn({ component: 'nest' }, String(message)),
      },
      abortOnError: false,
    },
  );
  app.useGlobalFilters(new ApiErrorFilter());
  const document = SwaggerModule.createDocument(
    app,
    new DocumentBuilder()
      .setTitle('Jungle Wagering API')
      .setVersion('1.0.0')
      .setDescription(
        'Dinheiro exato; desenvolvimento sem autenticação. Reenvie a mesma chave/payload após 503. Estados terminais são imutáveis.',
      )
      .build(),
  );
  SwaggerModule.setup('docs', app, document, { jsonDocumentUrl: 'docs-json' });
  await app.listen(Number(process.env.PORT ?? 3000), '0.0.0.0');
  port = (app.getHttpServer().address() as { port: number }).port;
}
for (const worker of workers) worker.start();
logger.info({ roles: [...roles], port, authMode: 'development' }, 'application_ready');
if (process.send) process.send({ type: 'ready', port, roles: [...roles] });
let shuttingDown = false;
async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info({ roles: [...roles] }, 'shutdown_started');
  const grace = Number(process.env.SHUTDOWN_GRACE_MS ?? 25000);
  await Promise.race([
    Promise.allSettled([
      Promise.race([app?.close(), Bun.sleep(grace)]),
      ...workers.map((worker) => worker.stop()),
    ]),
    // Keep the SQS client alive for the bounded release after financial work stops draining.
    Bun.sleep(grace + 1500),
  ]);
  queues.close();
  await Promise.race([db.close(), Bun.sleep(1000)]);
  metrics.registry.clear();
  logger.info('shutdown_completed');
  process.exit(0);
}
process.on('SIGTERM', () => {
  void shutdown();
});
process.on('SIGINT', () => {
  void shutdown();
});
// A worker-only process must remain alive between polls.
setInterval(() => {}, 60000);
