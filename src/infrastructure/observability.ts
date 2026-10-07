import pino from 'pino';
import { hostname } from 'node:os';
import { Registry, Counter, Histogram, Gauge, collectDefaultMetrics } from 'prom-client';

export const logger = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  base: { instance: process.env.INSTANCE_ID ?? `${hostname()}:${process.pid}` },
  redact: ['req.headers.authorization', 'req.headers.cookie', 'body', 'payload', 'money', 'balance'],
});
export class Observability {
  readonly registry = new Registry();
  readonly transactions = new Counter({
    name: 'wager_transactions_total',
    help: 'Committed transitions',
    labelNames: ['status', 'kind'],
    registers: [this.registry],
  });
  readonly duplicates = new Counter({
    name: 'wager_duplicates_total',
    help: 'Persistent duplicates',
    labelNames: ['source'],
    registers: [this.registry],
  });
  readonly retries = new Counter({
    name: 'wager_retries_total',
    help: 'Retries',
    labelNames: ['component'],
    registers: [this.registry],
  });
  readonly dlq = new Counter({
    name: 'wager_dlq_total',
    help: 'Messages archived from DLQ',
    registers: [this.registry],
  });
  readonly locks = new Counter({
    name: 'wager_lock_conflicts_total',
    help: 'Lock timeout/deadlock/serialization failures',
    registers: [this.registry],
  });
  readonly lockWait = new Histogram({
    name: 'wager_wallet_lock_seconds',
    help: 'Wallet lock acquisition duration',
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2],
    registers: [this.registry],
  });
  readonly latency = new Histogram({
    name: 'wager_processing_seconds',
    help: 'Processing including database commit',
    labelNames: ['source'],
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15],
    registers: [this.registry],
  });
  readonly lag = new Gauge({
    name: 'wager_outbox_lag_seconds',
    help: 'Age of oldest unpublished event',
    registers: [this.registry],
  });
  readonly pending = new Gauge({
    name: 'wager_outbox_pending',
    help: 'Unpublished events',
    registers: [this.registry],
  });
  readonly reconciliations = new Counter({
    name: 'wager_reconciliation_divergences_total',
    help: 'Detected divergences',
    registers: [this.registry],
  });
  constructor() {
    collectDefaultMetrics({ register: this.registry });
  }
}
