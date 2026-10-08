import { randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { CreateQueueCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { Queues } from '../src/infrastructure/sqs.js';

const broker = process.env.RECOVERY_BROKER ?? 'localstack';
if (!['localstack', 'ministack'].includes(broker)) throw new Error('Invalid RECOVERY_BROKER');
const cycles = Number(process.env.RECOVERY_CYCLES ?? 5);
if (!Number.isInteger(cycles) || cycles < 1 || cycles > 20)
  throw new Error('RECOVERY_CYCLES must be an integer between 1 and 20');
if (broker === 'localstack' && !process.env.LOCALSTACK_AUTH_TOKEN)
  throw new Error('Configure LOCALSTACK_AUTH_TOKEN in .env.local before running this check');

const root = resolve(import.meta.dir, '..');
const project = `jungle-recovery-${randomUUID().slice(0, 8)}`;
const directory = resolve(root, 'artifacts', project);
await mkdir(directory, { recursive: true });
const override = resolve(directory, 'compose.override.yaml');
await Bun.write(override, `services:\n  ${broker}:\n    ports: !override ['127.0.0.1::4566']\n`);
const profile = broker === 'localstack' ? 'reference' : 'portable';
const configuredToken = process.env.LOCALSTACK_AUTH_TOKEN;
const redact = (value: string) => (configuredToken ? value.replaceAll(configuredToken, '[REDACTED]') : value);
const command = async (args: string[]) => {
  const child = Bun.spawn(args, { cwd: root, stdout: 'pipe', stderr: 'pipe' });
  const [output, error, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (code !== 0) throw new Error(redact(`Command failed: ${args.join(' ')}\n${error}`));
  return output.trim();
};
const compose = (...args: string[]) =>
  command([
    'docker',
    'compose',
    '-p',
    project,
    '-f',
    resolve(root, 'compose.yaml'),
    '-f',
    override,
    '--profile',
    profile,
    ...args,
  ]);
const selectEndpoint = async () => {
  const mapping = await compose('port', broker, '4566');
  const port = mapping.match(/:(\d+)$/)?.[1];
  if (!port) throw new Error('Cannot discover isolated broker port');
  process.env.AWS_ENDPOINT_URL = `http://127.0.0.1:${port}`;
  return Number(port);
};
const report: {
  recordedAt: string;
  broker: string;
  project: string;
  image?: string;
  methodology: string;
  cycles: Record<string, unknown>[];
  success: boolean;
  failure?: string;
} = {
  recordedAt: new Date().toISOString(),
  broker,
  project,
  methodology:
    'Fresh isolated Compose project and volume; same pinned image and persistence configuration as the application. Each graceful restart checks input, events and DLQ identities plus all acknowledged sends, including unacknowledged receives. Later cycles keep four empty-queue long polls in flight during shutdown. Queues are created once, never recreated after a restart. Resources are removed after recording evidence.',
  cycles: [],
  success: false,
};
let queues: Queues | undefined;
try {
  await compose('up', '-d', broker);
  const container = await compose('ps', '-q', broker);
  report.image = await command(['docker', 'inspect', '--format', '{{.Config.Image}}', container]);
  await selectEndpoint();
  process.env.AWS_ACCESS_KEY_ID = 'test';
  process.env.AWS_SECRET_ACCESS_KEY = 'test';
  process.env.QUEUE_PREFIX = `${project}-`;
  process.env.SQS_WAIT_SECONDS = '1';
  process.env.SQS_VISIBILITY_SECONDS = '2';
  queues = new Queues();
  const eventually = async (work: () => Promise<void>, deadlineMs = 60000) => {
    const end = Date.now() + deadlineMs;
    let lastName = '';
    while (Date.now() < end) {
      try {
        await work();
        return;
      } catch (error) {
        lastName = error instanceof Error ? redact(`${error.name}: ${error.message}`) : 'unknown';
      }
      await Bun.sleep(250);
    }
    throw new Error(`Broker condition timed out: ${lastName}`);
  };
  await eventually(() => queues!.bootstrap());
  const emptyQueue = await queues.client.send(
    new CreateQueueCommand({
      QueueName: `${project}-empty.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
    }),
  );
  const emptyPath = new URL(emptyQueue.QueueUrl!).pathname;

  for (let cycle = 1; cycle <= cycles; cycle++) {
    const expected = new Map<string, Set<string>>();
    for (const name of Object.values(queues.names)) {
      const values = new Set<string>();
      for (let sequence = 0; sequence < 12; sequence++) {
        const body = JSON.stringify({ cycle, name, sequence, nonce: randomUUID() });
        await queues.send(name, body, `group-${sequence}`, randomUUID());
        values.add(body);
      }
      expected.set(name, values);
    }
    const inFlight = await queues.receive(queues.names.input, AbortSignal.timeout(5000));
    if (!inFlight.length) throw new Error('No message received before restart');
    let settledPolls = 0;
    const polling =
      cycle > 1
        ? Array.from({ length: 4 }, () =>
            queues!.client
              .send(
                new ReceiveMessageCommand({
                  QueueUrl: new URL(emptyPath, queues!.endpoint).toString(),
                  WaitTimeSeconds: 20,
                }),
                {
                  abortSignal: AbortSignal.timeout(30000),
                },
              )
              .then(
                () => {
                  settledPolls++;
                  return 'completed';
                },
                (error: unknown) => {
                  settledPolls++;
                  return error instanceof Error ? error.name : 'unknown';
                },
              ),
          )
        : [];
    if (polling.length) {
      await Bun.sleep(250);
      if (settledPolls !== 0) throw new Error('Long polling was not active before shutdown');
    }
    const started = performance.now();
    await compose('stop', broker);
    const state = JSON.parse(
      await command(['docker', 'inspect', '--format', '{{json .State}}', container]),
    ) as { ExitCode: number; OOMKilled: boolean };
    const stopSeconds = (performance.now() - started) / 1000;
    const logs = redact(await compose('logs', '--no-color', broker));
    await Bun.write(resolve(directory, `cycle-${cycle}.log`), logs);
    const evidence: Record<string, unknown> = {
      cycle,
      activeLongPolls: polling.length,
      inFlightReceives: inFlight.length,
      exitCode: state.ExitCode,
      oomKilled: state.OOMKilled,
      stopSeconds,
      acknowledgedSends: 36,
      recoveredUniqueMessages: 0,
    };
    report.cycles.push(evidence);
    if (state.ExitCode !== 0 || state.OOMKilled) throw new Error('Broker did not stop gracefully');
    await Promise.all(polling);
    queues.close();
    await compose('up', '-d', broker);
    // Docker may allocate a different ephemeral host port when the stopped container starts.
    evidence.brokerPort = await selectEndpoint();
    queues = new Queues();
    const restartStarted = performance.now();
    // Readiness uses a new SDK client and resolves names; it cannot create missing queues.
    await eventually(() => queues!.ready());
    evidence.readySeconds = (performance.now() - restartStarted) / 1000;
    for (const [name, values] of expected) {
      const recovered = new Set<string>();
      await eventually(async () => {
        const messages = await queues!.receive(name, AbortSignal.timeout(5000));
        for (const message of messages) {
          if (!values.has(message.Body!)) throw new Error('Unexpected restored message');
          recovered.add(message.Body!);
          await queues!.ack(name, message.ReceiptHandle!);
        }
        if (recovered.size !== values.size) throw new Error('Messages not fully restored');
      }, 20000);
      evidence.recoveredUniqueMessages = Number(evidence.recoveredUniqueMessages) + recovered.size;
    }
    console.log(JSON.stringify(evidence));
  }
  report.success = true;
} catch (error) {
  report.failure = redact(error instanceof Error ? error.message : 'Unknown recovery failure');
  process.exitCode = 1;
} finally {
  queues?.close();
  try {
    await Bun.write(resolve(directory, 'final.log'), redact(await compose('logs', '--no-color', broker)));
  } catch {
    /* Preserve the original failure if the container never started. */
  }
  await Bun.write(resolve(directory, 'report.json'), JSON.stringify(report, null, 2));
  try {
    await compose('down', '-v');
  } catch (error) {
    report.success = false;
    report.failure = redact(error instanceof Error ? error.message : 'Resource cleanup failed');
    process.exitCode = 1;
    await Bun.write(resolve(directory, 'report.json'), JSON.stringify(report, null, 2));
  }
  console.log(
    JSON.stringify({
      report: `artifacts/${project}/report.json`,
      success: report.success,
      ...(report.failure ? { failure: report.failure } : {}),
    }),
  );
}
