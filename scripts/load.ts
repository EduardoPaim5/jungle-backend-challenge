import { mkdir } from 'node:fs/promises';
import os from 'node:os';
import { withInstances, open, command, request, type WalletView } from './runtime.js';
import { Database } from '../src/infrastructure/database.js';

const requests = Number(process.env.LOAD_REQUESTS ?? 600),
  concurrency = Number(process.env.LOAD_CONCURRENCY ?? 24),
  repetitions = Number(process.env.LOAD_REPETITIONS ?? 3);
for (const [name, value] of Object.entries({ requests, concurrency, repetitions }))
  if (!Number.isInteger(value) || value < 1 || value > 100000) throw new Error(`Invalid load ${name}`);
function percentiles(samples: number[]) {
  const sorted = [...samples].sort((a, b) => a - b);
  const p = (q: number) => sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * q) - 1)] ?? 0;
  return { p50: p(0.5), p95: p(0.95), p99: p(0.99) };
}
const db = await Database.connect();
try {
  const results = await withInstances(3, 'api,publisher', async (urls) => {
    async function metricsSnapshot() {
      const snapshots = await Promise.all(
        urls.map(async (url) => {
          const response = await fetch(`${url}/metrics`);
          if (!response.ok) throw new Error('Metrics unavailable');
          const text = await response.text();
          const read = (name: string) =>
            Number(text.match(new RegExp(`^${name} ([0-9.eE+-]+)$`, 'm'))?.[1] ?? 0);
          return {
            locks: read('wager_lock_conflicts_total'),
            databaseRetries: read('wager_retries_total\\{component="database"\\}'),
            lockSum: read('wager_wallet_lock_seconds_sum'),
            lockCount: read('wager_wallet_lock_seconds_count'),
          };
        }),
      );
      return snapshots.reduce(
        (sum, value) => ({
          locks: sum.locks + value.locks,
          databaseRetries: sum.databaseRetries + value.databaseRetries,
          lockSum: sum.lockSum + value.lockSum,
          lockCount: sum.lockCount + value.lockCount,
        }),
        { locks: 0, databaseRetries: 0, lockSum: 0, lockCount: 0 },
      );
    }
    async function drain() {
      const started = performance.now(),
        deadline = Date.now() + Number(process.env.LOAD_DRAIN_SECONDS ?? 180) * 1000;
      let pending = 0;
      do {
        pending = Number(
          (
            await db.query<{ count: string }>(
              'SELECT count(*)::text AS count FROM outbox_messages WHERE published_at IS NULL',
            )
          )[0]!.count,
        );
        if (pending === 0) break;
        await Bun.sleep(100);
      } while (Date.now() < deadline);
      if (pending !== 0) throw new Error(`Outbox did not drain; ${pending} events retained for retry`);
      return { outboxPendingAfterDrain: pending, drainSeconds: (performance.now() - started) / 1000 };
    }
    await drain();
    async function run(
      mode: 'concentrated' | 'distributed',
      size: number,
      iteration: number,
      warmup = false,
    ) {
      const wallets: WalletView[] = [];
      for (let i = 0; i < (mode === 'concentrated' ? 1 : 24); i++)
        wallets.push(await open(urls[i % urls.length]!, '1000000.00'));
      const rejectedWallet = await open(urls[0]!, '0.00');
      let index = 0,
        processed = 0,
        rejected = 0,
        technicalErrors = 0,
        maxLag = 0,
        maxPending = 0,
        samplingError: string | undefined;
      const latencies: number[] = [],
        failures: Record<string, number> = {};
      const sample = async () => {
        try {
          const [r] = await db.query<{
            lag: string;
            pending: string;
          }>(`SELECT COALESCE(extract(epoch FROM clock_timestamp()-min(occurred_at)),0)::text AS lag,
        count(*)::text AS pending FROM outbox_messages WHERE published_at IS NULL`);
          maxLag = Math.max(maxLag, Number(r!.lag));
          maxPending = Math.max(maxPending, Number(r!.pending));
        } catch {
          samplingError = 'Unable to sample outbox';
        }
      };
      const beforeMetrics = await metricsSnapshot();
      const sampler = setInterval(() => {
        void sample();
      }, 200);
      const start = performance.now();
      await Promise.all(
        Array.from({ length: concurrency }, async () => {
          while (index < size) {
            const current = index++;
            const expectedRejection = current % 10 === 0;
            const wallet = expectedRejection ? rejectedWallet : wallets[current % wallets.length]!;
            const { idempotencyKey, ...body } = command(wallet, 'BET', '0.01');
            const begin = performance.now();
            try {
              const response = await fetch(`${urls[current % urls.length]}/wagering/transactions`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
                body: JSON.stringify(body),
                signal: AbortSignal.timeout(10000),
              });
              const result = (await response.json()) as {
                status?: string;
                failureCode?: string;
                code?: string;
              };
              if (response.status === 200 && !expectedRejection && result.status === 'PROCESSED') processed++;
              else if (
                response.status === 422 &&
                expectedRejection &&
                result.failureCode === 'INSUFFICIENT_FUNDS'
              )
                rejected++;
              else {
                technicalErrors++;
                const key = `${response.status}:${result.code ?? result.failureCode ?? result.status}`;
                failures[key] = (failures[key] ?? 0) + 1;
              }
            } catch {
              technicalErrors++;
              failures.NETWORK_ERROR = (failures.NETWORK_ERROR ?? 0) + 1;
            } finally {
              latencies.push(performance.now() - begin);
            }
          }
        }),
      );
      const seconds = (performance.now() - start) / 1000;
      clearInterval(sampler);
      await sample();
      const afterMetrics = await metricsSnapshot();
      const reconciliations = await Promise.all(
        [...wallets, rejectedWallet].map((w) =>
          request<{ consistent: boolean }>(urls[0]!, `/wallets/${w.id}/reconciliation`, {}),
        ),
      );
      const consistent = reconciliations.every((r) => r.consistent);
      const result = {
        mode,
        iteration,
        warmup,
        instances: urls.length,
        wallets: wallets.length,
        requests: size,
        concurrency,
        seconds,
        throughput: size / seconds,
        latencyMs: percentiles(latencies),
        processed,
        expectedRejections: rejected,
        technicalErrors,
        technicalErrorRate: technicalErrors / size,
        failures,
        lockConflicts: afterMetrics.locks - beforeMetrics.locks,
        databaseRetries: afterMetrics.databaseRetries - beforeMetrics.databaseRetries,
        averageWalletLockWaitMs:
          (1000 * (afterMetrics.lockSum - beforeMetrics.lockSum)) /
          Math.max(1, afterMetrics.lockCount - beforeMetrics.lockCount),
        maxOutboxLagSeconds: maxLag,
        maxOutboxPending: maxPending,
        consistent,
        ...(samplingError ? { samplingError } : {}),
      };
      console.log(JSON.stringify(result));
      if (!consistent || technicalErrors > 0)
        throw new Error('Load validation failed; inspect the reported errors');
      return result;
    }
    await run('concentrated', 100, 0, true);
    await run('distributed', 100, 0, true);
    const measured = [];
    for (let i = 1; i <= repetitions; i++) {
      measured.push(await run('concentrated', requests, i));
      measured.push(await run('distributed', requests, i));
    }
    return { measurements: measured, ...(await drain()) };
  });
  const [pg] = await db.query<{ version: string }>('SELECT version() AS version');
  const pkg = (await Bun.file('package.json').json()) as { dependencies: Record<string, string> };
  const report = {
    recordedAt: new Date().toISOString(),
    environment: {
      bun: Bun.version,
      nest: pkg.dependencies['@nestjs/core'],
      mikroORM: pkg.dependencies['@mikro-orm/core'],
      postgres: pg!.version,
      platform: os.platform(),
      release: os.release(),
      cpu: os.cpus()[0]?.model,
      logicalCPUs: os.cpus().length,
      totalMemoryBytes: os.totalmem(),
      broker:
        process.env.BROKER_LABEL ?? new URL(process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566').host,
    },
    methodology:
      'Three processes by default, HTTP round-robin, fixed concurrency, 100-request warmup per topology, repeated measurements, 10% deliberate insufficient-funds rejections. Latencies include all responses. Local development benchmark, no production capacity claim.',
    ...results,
  };
  await mkdir('artifacts', { recursive: true });
  const path = process.env.LOAD_REPORT ?? 'artifacts/load.json';
  await Bun.write(path, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ report: path, success: true }));
} finally {
  await db.close();
}
