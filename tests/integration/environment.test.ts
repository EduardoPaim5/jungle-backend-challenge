import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Database } from '../../src/infrastructure/database.js';
import { SQSClient, DeleteQueueCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs';

async function run(args: string[], env: Record<string, string>) {
  const child = Bun.spawn([process.execPath, ...args], {
    env: { ...process.env, ...env },
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const timer = setTimeout(() => child.kill('SIGKILL'), 20000);
  try {
    const [stdout, stderr, code] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    return { code, stdout, stderr };
  } finally {
    clearTimeout(timer);
  }
}

function closedPort() {
  const server = Bun.serve({ port: 0, fetch: () => new Response('unused') });
  const port = server.port;
  server.stop(true);
  return port;
}

test('carga não inicia com dependências paradas; diagnóstico não expõe credenciais nem stack trace', async () => {
  const port = closedPort();
  const password = 'preflight-password-must-not-be-logged';
  const result = await run(['run', 'test:load'], {
    DATABASE_URL: `postgresql://jungle_app:${password}@127.0.0.1:${port}/jungle`,
    AWS_ENDPOINT_URL: `http://127.0.0.1:${port}`,
  });
  expect(result.code).toBe(1);
  expect(result.stderr).toContain('[POSTGRES_UNAVAILABLE]');
  expect(result.stderr).toContain('[SQS_UNAVAILABLE]');
  expect(result.stderr).toContain('docker compose');
  expect(result.stderr).toContain('bun run db:migrate');
  expect(result.stderr).toContain('bun run queues:init');
  expect(result.stderr).not.toContain(password);
  expect(result.stderr).not.toContain('DriverException');
  expect(result.stderr).not.toContain('at convertException');
  expect(result.stderr).not.toContain('tsc -p');
  expect(result.stdout).not.toContain('"throughput"');
}, 30000);

test('verificação detecta schema e filas ausentes, broker parado e ambiente pronto sem criar dados', async () => {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `jungle_preflight_${suffix}`;
  const prefix = `preflight-${suffix}-`;
  const rootUrl =
    process.env.MIGRATION_DATABASE_URL ?? 'postgresql://jungle_owner:jungle_owner@localhost:55432/jungle';
  const root = await Database.connect(rootUrl);
  const ownerUrl = new URL(rootUrl);
  ownerUrl.pathname = `/${databaseName}`;
  const appUrl = new URL(ownerUrl);
  appUrl.username = 'jungle_app';
  appUrl.password = 'jungle_app';
  const env = { DATABASE_URL: appUrl.toString(), QUEUE_PREFIX: prefix };
  let owner: Database | undefined;
  const client = new SQSClient({
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    region: process.env.AWS_REGION ?? 'us-east-1',
    maxAttempts: 1,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
  try {
    await root.query(`CREATE DATABASE ${databaseName}`);
    owner = await Database.connect(ownerUrl.toString());
    const missingSchema = await run(['run', 'environment:check'], env);
    expect(missingSchema.code).toBe(1);
    expect(missingSchema.stderr).toContain('[SCHEMA_NOT_READY]');
    expect(missingSchema.stderr).toContain('[QUEUES_NOT_READY]');
    const [untouched] = await owner.query<{ relation: string | null }>(
      "SELECT to_regclass('public.wallets')::text AS relation",
    );
    expect(untouched!.relation).toBeNull();

    await owner.orm.migrator.up();
    await owner.orm.migrator.down();
    const outdatedSchema = await run(['run', 'environment:check'], env);
    expect(outdatedSchema.code).toBe(1);
    expect(outdatedSchema.stderr).toContain('[SCHEMA_NOT_READY]');
    await owner.orm.migrator.up();
    const missingQueues = await run(['run', 'environment:check'], env);
    expect(missingQueues.code).toBe(1);
    expect(missingQueues.stderr).not.toContain('[SCHEMA_NOT_READY]');
    expect(missingQueues.stderr).toContain('[QUEUES_NOT_READY]');

    const unavailableBroker = await run(['run', 'environment:check'], {
      ...env,
      AWS_ENDPOINT_URL: `http://127.0.0.1:${closedPort()}`,
    });
    expect(unavailableBroker.code).toBe(1);
    expect(unavailableBroker.stderr).toContain('[SQS_UNAVAILABLE]');
    expect(unavailableBroker.stderr).not.toContain('[POSTGRES_UNAVAILABLE]');

    const initialized = await run(['run', 'queues:init'], env);
    expect(initialized.code).toBe(0);
    const ready = await run(['run', 'environment:check'], env);
    expect(ready.code).toBe(0);
    expect(ready.stdout).toContain('Ambiente pronto');
    const [data] = await owner.query<{ wallets: string; transactions: string; outbox: string }>(
      `SELECT (SELECT count(*)::text FROM wallets) AS wallets,
        (SELECT count(*)::text FROM wager_transactions) AS transactions,
        (SELECT count(*)::text FROM outbox_messages) AS outbox`,
    );
    expect(data).toEqual({ wallets: '0', transactions: '0', outbox: '0' });
  } finally {
    await Promise.allSettled(
      ['wager-transactions.fifo', 'wager-transactions-dlq.fifo', 'wager-events.fifo'].map(async (name) => {
        const queue = await client.send(new GetQueueUrlCommand({ QueueName: `${prefix}${name}` }), {
          abortSignal: AbortSignal.timeout(5000),
        });
        const endpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';
        const url = new URL(new URL(queue.QueueUrl!).pathname, endpoint).toString();
        await client.send(new DeleteQueueCommand({ QueueUrl: url }), {
          abortSignal: AbortSignal.timeout(5000),
        });
      }),
    );
    client.destroy();
    if (owner) await owner.close();
    await root.query(`DROP DATABASE IF EXISTS ${databaseName} WITH (FORCE)`);
    await root.close();
  }
}, 60000);
