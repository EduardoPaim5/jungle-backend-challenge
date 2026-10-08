import { randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';

const broker = process.env.TEST_BROKER ?? 'localstack';
if (!['localstack', 'ministack'].includes(broker)) throw new Error('Invalid TEST_BROKER');
if (broker === 'localstack' && !process.env.LOCALSTACK_AUTH_TOKEN)
  throw new Error('Configure LOCALSTACK_AUTH_TOKEN in .env.local before running this check');
const root = resolve(import.meta.dir, '..');
const project = `jungle-verify-${randomUUID().slice(0, 8)}`;
const directory = resolve(root, 'artifacts', project);
await mkdir(directory, { recursive: true });
// Select unused loopback ports and keep their mappings fixed across container restarts.
const reservations = Array.from({ length: 2 }, () =>
  Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    fetch: () => new Response('Reserved for isolated verification'),
  }),
);
const [databasePort, brokerPort] = reservations.map((server) => server.port);
await Promise.all(reservations.map((server) => server.stop(true)));
const override = resolve(directory, 'compose.override.yaml');
await Bun.write(
  override,
  `services:\n  postgres:\n    ports: !override ['127.0.0.1:${databasePort}:5432']\n  ${broker}:\n    ports: !override ['127.0.0.1:${brokerPort}:4566']\n`,
);
const env = {
  ...process.env,
  COMPOSE_PROJECT_NAME: project,
  COMPOSE_FILE: `${resolve(root, 'compose.yaml')}:${override}`,
  TEST_BROKER: broker,
  DATABASE_URL: `postgresql://jungle_app:jungle_app@127.0.0.1:${databasePort}/jungle`,
  MIGRATION_DATABASE_URL: `postgresql://jungle_owner:jungle_owner@127.0.0.1:${databasePort}/jungle`,
  AWS_ENDPOINT_URL: `http://127.0.0.1:${brokerPort}`,
  AWS_ACCESS_KEY_ID: 'test',
  AWS_SECRET_ACCESS_KEY: 'test',
};
const run = async (args: string[]) => {
  const child = Bun.spawn(args, { cwd: root, env, stdout: 'inherit', stderr: 'inherit' });
  const code = await child.exited;
  if (code !== 0) throw new Error(`Command failed with exit ${code}: ${args.join(' ')}`);
};
const compose = (...args: string[]) =>
  run([
    'docker',
    'compose',
    '-p',
    project,
    '--profile',
    broker === 'localstack' ? 'reference' : 'portable',
    ...args,
  ]);
const started = performance.now();
const report = {
  recordedAt: new Date().toISOString(),
  project,
  broker,
  methodology:
    'Entire verification suite against a fresh Compose project with isolated PostgreSQL/SQS volumes and fixed loopback ports. Real restart tests target this project through COMPOSE_FILE and COMPOSE_PROJECT_NAME. All owned containers and volumes are removed in finally.',
  success: false,
  failure: undefined as string | undefined,
  seconds: 0,
};
try {
  await compose('up', '-d', '--wait', 'postgres', broker);
  console.log(JSON.stringify({ project, broker, isolated: true }));
  await run([process.execPath, 'run', 'verify']);
  report.success = true;
} catch (error) {
  report.failure = error instanceof Error ? error.message : 'Unknown isolated verification failure';
  process.exitCode = 1;
} finally {
  try {
    await compose('down', '-v');
  } catch (error) {
    report.success = false;
    report.failure = error instanceof Error ? error.message : 'Resource cleanup failed';
    process.exitCode = 1;
  }
  report.seconds = (performance.now() - started) / 1000;
  await Bun.write(resolve(directory, 'report.json'), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify({
      report: `artifacts/${project}/report.json`,
      success: report.success,
      ...(report.failure ? { failure: report.failure } : {}),
    }),
  );
}
