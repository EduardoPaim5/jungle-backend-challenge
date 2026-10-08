import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
} from '@aws-sdk/client-sqs';
import { Database } from '../../src/infrastructure/database.js';
import { Queues } from '../../src/infrastructure/sqs.js';
import type { ProcessingResult } from '../../src/application/contracts.js';
import type { WagerCommand } from '../../src/domain/transaction.js';
import type { IdentityPort } from '../../src/http/api.js';

const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
const rootUrl =
  process.env.MIGRATION_DATABASE_URL ?? 'postgresql://jungle_owner:jungle_owner@localhost:55432/jungle';
const databaseName = `jungle_test_${suffix}`;
const ownerUrl = new URL(rootUrl);
ownerUrl.pathname = `/${databaseName}`;
const appUrl = new URL(ownerUrl);
appUrl.username = 'jungle_app';
appUrl.password = 'jungle_app';
process.env.QUEUE_PREFIX = `test-${suffix}-`;
process.env.SQS_WAIT_SECONDS = '1';
process.env.SQS_VISIBILITY_SECONDS = '2';
let root: Database;
let owner: Database;
let db: Database;
let queues: Queues;
interface App {
  child: Bun.Subprocess<'ignore', 'ignore', 'pipe'>;
  url: string;
  wait(type: string): Promise<Record<string, unknown> | undefined>;
  stop(signal?: 'SIGTERM' | 'SIGKILL'): Promise<void>;
}
const processes: App[] = [];
const apis: App[] = [];
interface WalletView {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}
async function eventually<T>(
  check: () => Promise<T>,
  accept: (value: T) => boolean,
  timeout = 15000,
): Promise<T> {
  const end = Date.now() + timeout;
  let latest: T | undefined;
  while (Date.now() < end) {
    latest = await check();
    if (accept(latest)) return latest;
    await Bun.sleep(30);
  }
  throw new Error(`Condition not reached: ${JSON.stringify(latest)}`);
}
async function start(roles: string, extra: Record<string, string> = {}): Promise<App> {
  const messages: Record<string, unknown>[] = [];
  const child = Bun.spawn([process.execPath, 'dist/src/main.js'], {
    env: {
      ...process.env,
      NODE_ENV: 'test',
      DATABASE_URL: appUrl.toString(),
      PORT: '0',
      LOG_LEVEL: 'error',
      APP_ROLES: roles,
      REFERENCE_POLL_MS: '25',
      REFERENCE_BACKOFF_MS: '25',
      OUTBOX_POLL_MS: '25',
      SQS_HEARTBEAT_MS: '500',
      WORK_LEASE_MS: '1500',
      SHUTDOWN_GRACE_MS: '1000',
      ...extra,
    },
    stdin: 'ignore',
    stdout: 'ignore',
    stderr: 'pipe',
    ipc(message: unknown) {
      if (message && typeof message === 'object') messages.push(message as Record<string, unknown>);
    },
  });
  async function wait(type: string) {
    return eventually(
      async () => {
        if (child.exitCode !== null)
          throw new Error(`Child exited ${child.exitCode}: ${await new Response(child.stderr).text()}`);
        return messages.find((message) => message.type === type);
      },
      Boolean,
      10000,
    );
  }
  const ready = await wait('ready');
  const instance = {
    child,
    url: `http://127.0.0.1:${ready!.port}`,
    wait,
    async stop(signal: 'SIGTERM' | 'SIGKILL' = 'SIGTERM') {
      if (child.exitCode === null) child.kill(signal);
      await child.exited;
    },
  };
  processes.push(instance);
  return instance;
}
async function http<T>(app: App, path: string, body?: unknown, key?: string) {
  const response = await fetch(`${app.url}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  return { status: response.status, data: (await response.json()) as T };
}
async function open(amount = '100.00') {
  const r = await http<WalletView>(apis[0]!, '/wallets', {
    playerId: randomUUID(),
    initialBalance: { amount, currency: 'BRL' },
  });
  if (r.status !== 201) throw new Error(`Opening failed: ${JSON.stringify(r)}`);
  return r.data;
}
function command(
  wallet: WalletView,
  kind: WagerCommand['kind'] = 'BET',
  amount = '10.00',
  reference?: string,
): WagerCommand {
  const external = randomUUID();
  return {
    providerId: 'provider-a',
    externalTransactionId: external,
    idempotencyKey: `key-${external}`,
    walletId: wallet.id,
    playerId: wallet.playerId,
    roundId: 'round-1',
    gameId: 'game-1',
    kind,
    money: { amount, currency: 'BRL' },
    ...(reference ? { referenceExternalTransactionId: reference } : {}),
  };
}
async function submit(c: WagerCommand, index = 0) {
  const { idempotencyKey, ...body } = c;
  return http<ProcessingResult>(apis[index % apis.length]!, '/wagering/transactions', body, idempotencyKey);
}
async function balance(w: WalletView) {
  return (await http<WalletView>(apis[0]!, `/wallets/${w.id}`)).data;
}
async function scalar(sql: string, params: unknown[] = []) {
  return Number((await db.query<{ count: string }>(sql, params))[0]!.count);
}
async function ledgerCount(wallet: WalletView) {
  return scalar('SELECT count(*)::text AS count FROM wallet_ledger WHERE wallet_id=?', [wallet.id]);
}
async function integrity() {
  const broken = await db.query(`SELECT w.id FROM wallets w LEFT JOIN LATERAL
    (SELECT COALESCE(sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END),0) AS balance,
      COALESCE(max(wallet_version),1) AS version FROM wallet_ledger WHERE wallet_id=w.id) l ON true
    WHERE w.balance<>l.balance OR w.version<>l.version OR w.balance<0`);
  expect(broken).toEqual([]);
  expect(
    await scalar(`SELECT count(*)::text AS count FROM wager_transactions t WHERE t.status='PENDING'
    OR (t.status='PROCESSED' AND t.kind<>'LOSS' AND (SELECT count(*) FROM wallet_ledger l WHERE l.transaction_id=t.id)<>1)
    OR ((t.status<>'PROCESSED' OR t.kind='LOSS') AND EXISTS(SELECT 1 FROM wallet_ledger l WHERE l.transaction_id=t.id))`),
  ).toBe(0);
  const chain =
    await db.query(`WITH entries AS (SELECT *,lag(balance_after) OVER(PARTITION BY wallet_id ORDER BY wallet_version) AS previous_balance,
      lag(wallet_version) OVER(PARTITION BY wallet_id ORDER BY wallet_version) AS previous_version FROM wallet_ledger)
    SELECT id FROM entries WHERE (previous_version IS NOT NULL AND (wallet_version<>previous_version+1 OR balance_before<>previous_balance))
      OR (previous_version IS NULL AND (wallet_version NOT IN (1,2) OR balance_before<>0))`);
  expect(chain).toEqual([]);
}
async function envelope(c: WagerCommand, id = randomUUID()) {
  const body = JSON.stringify({
    messageId: id,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: c,
  });
  await queues.send(queues.names.input, body, c.walletId, randomUUID());
  return { id, body };
}
async function waitTransaction(c: WagerCommand, status = 'PROCESSED') {
  return eventually(
    () =>
      db.query<{ status: string; id: string }>(
        'SELECT id,status FROM wager_transactions WHERE idempotency_key=?',
        [c.idempotencyKey],
      ),
    (rows) => rows[0]?.status === status,
  );
}
async function inputDrained() {
  await eventually(
    async () => {
      const r = await queues.client.send(
        new GetQueueAttributesCommand({
          QueueUrl: await queues.url(queues.names.input),
          AttributeNames: [
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed',
          ],
        }),
      );
      return Object.values(r.Attributes ?? {}).reduce((total, value) => total + Number(value), 0);
    },
    (x) => x === 0,
  );
}

beforeAll(async () => {
  root = await Database.connect(rootUrl);
  await root.query(`CREATE DATABASE ${databaseName}`);
  owner = await Database.connect(ownerUrl.toString());
  await owner.orm.migrator.up();
  await owner.orm.migrator.down({ to: 0 });
  await owner.orm.migrator.up();
  db = await Database.connect(appUrl.toString());
  queues = new Queues();
  await eventually(
    async () => {
      try {
        await queues.bootstrap();
        return true;
      } catch {
        return false;
      }
    },
    Boolean,
    60000,
  );
  for (let i = 0; i < 3; i++) apis.push(await start('api'));
}, 90000);
afterAll(async () => {
  await Promise.allSettled(processes.map((app) => app.stop('SIGKILL')));
  if (db) {
    await integrity();
    await db.close();
  }
  if (queues) {
    await Promise.allSettled(
      Object.values(queues.names).map(async (name) =>
        queues.client.send(new DeleteQueueCommand({ QueueUrl: await queues.url(name) })),
      ),
    );
    queues.close();
  }
  if (owner) await owner.close();
  if (root) {
    await root.query(`DROP DATABASE ${databaseName} WITH (FORCE)`);
    await root.close();
  }
}, 20000);

test('abertura positiva/zerada, saúde, OpenAPI e constraints reais', async () => {
  const positive = await open();
  const zero = await open('0.00');
  expect(positive.version).toBe(1);
  expect(zero.version).toBe(1);
  expect(await ledgerCount(positive)).toBe(1);
  expect(await ledgerCount(zero)).toBe(0);
  expect((await http(apis[0]!, '/health/ready')).status).toBe(200);
  expect((await http(apis[0]!, '/docs-json')).status).toBe(200);
  await expect(db.query('UPDATE wallets SET balance=-1 WHERE id=?', [positive.id])).rejects.toThrow();
  await expect(
    db.query("UPDATE wallets SET balance='NaN'::numeric WHERE id=?", [positive.id]),
  ).rejects.toThrow();
  await expect(
    db.query('UPDATE wallets SET balance=90,version=version+1 WHERE id=?', [positive.id]),
  ).rejects.toThrow();
  await expect(db.query('UPDATE wallets SET version=version+1 WHERE id=?', [positive.id])).rejects.toThrow();
  await expect(
    owner.query('UPDATE wallet_ledger SET amount=amount WHERE wallet_id=?', [positive.id]),
  ).rejects.toThrow('append-only');
  await expect(db.query('TRUNCATE wallet_ledger')).rejects.toThrow();
  const c = command(positive);
  const result = await submit(c);
  await expect(
    db.query("UPDATE wager_transactions SET status='PENDING',processed_at=NULL WHERE id=?", [
      result.data.transactionId,
    ]),
  ).rejects.toThrow();
  await expect(
    db.query('UPDATE wager_transactions SET amount=11 WHERE id=?', [result.data.transactionId]),
  ).rejects.toThrow();
  await expect(
    db.query(
      "UPDATE outbox_messages SET payload=jsonb_set(payload,'{data,status}','\"FORGED\"') WHERE aggregate_id=?",
      [result.data.transactionId],
    ),
  ).rejects.toThrow('immutable');
  await integrity();
});
test('50 submissões concorrentes em três processos: um débito e snapshot original no replay', async () => {
  const wallet = await open();
  const c = command(wallet, 'BET', '25.00');
  const results = await Promise.all(Array.from({ length: 50 }, (_, index) => submit(c, index)));
  expect(results.every((r) => r.status === 200)).toBe(true);
  expect(new Set(results.map((r) => r.data.transactionId)).size).toBe(1);
  expect(results.filter((r) => !r.data.idempotentReplay)).toHaveLength(1);
  expect((await balance(wallet)).balance.amount).toBe('75.00');
  expect(await ledgerCount(wallet)).toBe(2);
  await submit(command(wallet, 'WIN', '20.00'));
  const replay = await submit(c, 2);
  expect(replay.data.balance!.amount).toBe('75.00');
  expect(replay.data.walletVersion).toBe(2);
  expect((await balance(wallet)).balance.amount).toBe('95.00');
  expect((await submit({ ...c, money: { amount: '26.00', currency: 'BRL' } })).status).toBe(409);
  expect((await submit({ ...c, idempotencyKey: 'another-key' })).status).toBe(409);
  await integrity();
});
test('apostas disputam saldo; rejeição é persistente e LOSS não cria ledger/versão', async () => {
  const wallet = await open();
  const bets = [command(wallet, 'BET', '80.00'), command(wallet, 'BET', '80.00')];
  const results = await Promise.all(bets.map((c, i) => submit(c, i)));
  expect(results.map((r) => r.status).sort()).toEqual([200, 422]);
  expect((await balance(wallet)).balance.amount).toBe('20.00');
  const rejectedIndex = results.findIndex((r) => r.status === 422);
  await submit(command(wallet, 'WIN', '100.00'));
  const replay = await submit(bets[rejectedIndex]!);
  expect(replay.status).toBe(422);
  expect(replay.data.balance!.amount).toBe('20.00');
  const before = await balance(wallet);
  const count = await ledgerCount(wallet);
  expect((await submit(command(wallet, 'LOSS', '0.00'))).status).toBe(200);
  expect((await balance(wallet)).version).toBe(before.version);
  expect(await ledgerCount(wallet)).toBe(count);
  await integrity();
});
test('todas as reversões, exclusividade por tipo, referências e rejeições sem efeito', async () => {
  const w = await open();
  const bet = command(w, 'BET', '30.00');
  await submit(bet);
  const refund = command(w, 'REFUND', '30.00', bet.externalTransactionId);
  expect((await submit(refund)).status).toBe(200);
  expect((await submit(command(w, 'REFUND', '30.00', bet.externalTransactionId))).data.failureCode).toBe(
    'REVERSAL_ALREADY_APPLIED',
  );
  expect((await submit(command(w, 'ROLLBACK', '30.00', bet.externalTransactionId))).status).toBe(200);
  expect((await submit(command(w, 'ROLLBACK', '30.00', refund.externalTransactionId))).status).toBe(200);
  const win = command(w, 'WIN', '45.00', bet.externalTransactionId);
  expect((await submit(win)).status).toBe(200);
  expect((await submit(command(w, 'ROLLBACK', '45.00', win.externalTransactionId))).status).toBe(200);
  expect((await balance(w)).balance.amount).toBe('100.00');
  expect((await submit(command(w, 'REFUND', '29.00', bet.externalTransactionId))).data.failureCode).toBe(
    'REFERENCE_AMOUNT_MISMATCH',
  );
  expect(
    (await submit({ ...command(w, 'WIN', '10.00', bet.externalTransactionId), roundId: 'other' })).data
      .failureCode,
  ).toBe('REFERENCE_CONTEXT_MISMATCH');
  expect((await submit({ ...command(w), playerId: randomUUID() })).data.failureCode).toBe('PLAYER_MISMATCH');
  expect((await submit({ ...command(w), walletId: randomUUID() })).data.failureCode).toBe('WALLET_NOT_FOUND');
  const low = await open('0.00');
  const credit = command(low, 'WIN', '10.00');
  await submit(credit);
  await submit(command(low, 'BET', '10.00'));
  expect(
    (await submit(command(low, 'ROLLBACK', '10.00', credit.externalTransactionId))).data.failureCode,
  ).toBe('REVERSAL_INSUFFICIENT_FUNDS');
  const max = await open(`${'9'.repeat(36)}.99`);
  expect((await submit(command(max, 'WIN', '0.01'))).data.failureCode).toBe('MONEY_LIMIT_EXCEEDED');
  await integrity();
});
test('HTTP e SQS simultâneos compartilham efeito; inbox detecta envelope divergente', async () => {
  const w = await open();
  const c = command(w, 'BET', '15.00');
  const worker = await start('consumer,dlq');
  const { id, body } = await envelope(c);
  await submit(c, 1);
  await waitTransaction(c);
  await eventually(
    () =>
      scalar(
        'SELECT count(*)::text AS count FROM inbox_messages WHERE message_id=? AND processed_at IS NOT NULL',
        [id],
      ),
    (x) => x === 1,
  );
  await queues.send(queues.names.input, body, w.id, randomUUID());
  await queues.send(
    queues.names.input,
    JSON.stringify({
      messageId: randomUUID(),
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: c,
    }),
    w.id,
    randomUUID(),
  );
  await queues.send(
    queues.names.input,
    JSON.stringify({ ...JSON.parse(body), data: { ...c, money: { amount: '16.00', currency: 'BRL' } } }),
    w.id,
    randomUUID(),
  );
  await eventually(
    () => scalar('SELECT count(*)::text AS count FROM dead_letter_records WHERE message_id=?', [id]),
    (x) => x === 1,
  );
  expect((await balance(w)).balance.amount).toBe('85.00');
  expect(await ledgerCount(w)).toBe(2);
  await worker.stop();
  await integrity();
});
test('REFUNDs concorrentes e wallets distribuídas preservam efeitos e versões', async () => {
  const w = await open();
  const bet = command(w, 'BET', '30.00');
  await submit(bet);
  const reversals = await Promise.all([
    submit(command(w, 'REFUND', '30.00', bet.externalTransactionId), 1),
    submit(command(w, 'REFUND', '30.00', bet.externalTransactionId), 2),
  ]);
  expect(reversals.map((r) => r.status).sort()).toEqual([200, 422]);
  expect((await balance(w)).balance.amount).toBe('100.00');
  const wallets = await Promise.all(Array.from({ length: 12 }, () => open()));
  expect(
    (await Promise.all(wallets.map((wallet, i) => submit(command(wallet, 'BET', '80.00'), i)))).every(
      (r) => r.status === 200,
    ),
  ).toBe(true);
  for (const wallet of wallets) {
    expect((await balance(wallet)).balance.amount).toBe('20.00');
    expect((await balance(wallet)).version).toBe(2);
  }
  await integrity();
});
test('SQS confirma REFUND pendente para liberar BET posterior do mesmo grupo', async () => {
  const w = await open();
  const bet = command(w, 'BET', '20.00');
  const refund = command(w, 'REFUND', '20.00', bet.externalTransactionId);
  const workers = await start('consumer,references');
  await envelope(refund);
  await waitTransaction(refund, 'PENDING_REFERENCE');
  await envelope(bet);
  await waitTransaction(bet);
  await waitTransaction(refund);
  await inputDrained();
  await workers.stop();
  expect((await balance(w)).balance.amount).toBe('100.00');
  expect(await ledgerCount(w)).toBe(3);
  await integrity();
});
test('falha permanente de permissão é FAILED, auditada na DLQ e não pode ser reaberta', async () => {
  const w = await open();
  const c = command(w);
  const { id } = await envelope(c);
  await owner.query('REVOKE INSERT ON wallet_ledger FROM jungle_app');
  const worker = await start('consumer,dlq');
  try {
    await waitTransaction(c, 'FAILED');
    await eventually(
      () => scalar('SELECT count(*)::text AS count FROM dead_letter_records WHERE message_id=?', [id]),
      (x) => x === 1,
    );
  } finally {
    await owner.query('GRANT INSERT ON wallet_ledger TO jungle_app');
    await worker.stop();
  }
  const replay = await submit(c);
  expect(replay.status).toBe(500);
  expect(replay.data.status).toBe('FAILED');
  expect((await balance(w)).balance.amount).toBe('100.00');
  expect(await ledgerCount(w)).toBe(1);
  await integrity();
});
test('crash após enviar DLQ retoma encaminhamento persistido sem mudar FAILED', async () => {
  const w = await open();
  const c = command(w);
  const { id } = await envelope(c);
  await owner.query('REVOKE INSERT ON wallet_ledger FROM jungle_app');
  const doomed = await start('consumer', { TEST_FAULT_POINT: 'after-dlq-send', TEST_FAULT_MESSAGE_ID: id });
  try {
    await doomed.wait('barrier');
    await doomed.stop('SIGKILL');
  } finally {
    await owner.query('GRANT INSERT ON wallet_ledger TO jungle_app');
  }
  const [pending] = await db.query<{ disposition: string; dead_letter_sent_at: Date | null }>(
    'SELECT disposition,dead_letter_sent_at FROM inbox_messages WHERE message_id=?',
    [id],
  );
  expect(pending!.disposition).toBe('DLQ');
  expect(pending!.dead_letter_sent_at).toBeNull();
  const recovered = await start('consumer,dlq');
  await eventually(
    () =>
      scalar(
        'SELECT count(*)::text AS count FROM inbox_messages WHERE message_id=? AND dead_letter_sent_at IS NOT NULL',
        [id],
      ),
    (x) => x === 1,
  );
  await eventually(
    () => scalar('SELECT count(*)::text AS count FROM dead_letter_records WHERE message_id=?', [id]),
    (x) => x === 1,
  );
  await inputDrained();
  await recovered.stop();
  expect((await submit(c)).data.status).toBe('FAILED');
  expect((await balance(w)).balance.amount).toBe('100.00');
  await integrity();
});
test('referência fora de ordem persiste, não bloqueia mensagens posteriores e sobrevive a reinício', async () => {
  const w = await open();
  const bet = command(w, 'BET', '40.00');
  const refund = command(w, 'REFUND', '40.00', bet.externalTransactionId);
  const first = await submit(refund);
  expect(first.status).toBe(202);
  await submit(command(w, 'WIN', '5.00'));
  const pendingReplay = await submit(refund);
  expect(pendingReplay.data.balance!.amount).toBe('100.00');
  let workers = await start('consumer,references');
  await envelope(bet);
  await waitTransaction(bet);
  await waitTransaction(refund);
  await workers.stop();
  workers = await start('references');
  const replay = await submit(refund);
  expect(replay.status).toBe(200);
  expect(replay.data.idempotentReplay).toBe(true);
  expect((await balance(w)).balance.amount).toBe('105.00');
  expect(
    await scalar(
      "SELECT count(*)::text AS count FROM outbox_messages WHERE aggregate_id=? AND event_type='WagerTransactionPendingReference'",
      [first.data.transactionId],
    ),
  ).toBe(1);
  await workers.stop();
  await integrity();
});
test('referências terminais inválidas e TTL: rejeição auditável sem lançamento', async () => {
  const w = await open('0.00');
  const rejectedBet = command(w, 'BET', '1.00');
  await submit(rejectedBet);
  const invalid = await submit(command(w, 'REFUND', '1.00', rejectedBet.externalTransactionId));
  expect(invalid.status).toBe(422);
  const missing = command(w, 'ROLLBACK', '1.00', randomUUID());
  await submit(missing);
  const workers = await start('references', { REFERENCE_TTL_SECONDS: '0.2' });
  await waitTransaction(missing, 'REJECTED');
  const replay = await submit(missing);
  expect(replay.data.failureCode).toBe('REFERENCE_NOT_FOUND');
  expect(await ledgerCount(w)).toBe(0);
  await workers.stop();
  await integrity();
});
test('wallet bloqueada não bloqueia outras; timeout retorna 503 recuperável com mesma chave', async () => {
  const blocked = await open();
  const free = await open();
  const c = command(blocked);
  const known = command(blocked);
  await submit(known);
  let release!: () => void;
  let acquired!: () => void;
  const acquiredPromise = new Promise<void>((r) => {
    acquired = r;
  });
  const held = owner.transaction(async (em) => {
    await owner.query('SELECT id FROM wallets WHERE id=? FOR UPDATE', [blocked.id], em);
    acquired();
    await new Promise<void>((r) => {
      release = r;
    });
  });
  await acquiredPromise;
  const replayStart = performance.now();
  expect((await submit(known, 1)).data.idempotentReplay).toBe(true);
  expect(performance.now() - replayStart).toBeLessThan(900);
  const wait = submit(c);
  const startTime = performance.now();
  const other = await submit(command(free), 2);
  expect(other.status).toBe(200);
  expect(performance.now() - startTime).toBeLessThan(900);
  expect((await wait).status).toBe(503);
  release();
  await held;
  expect((await submit(c)).status).toBe(200);
  expect((await balance(blocked)).balance.amount).toBe('80.00');
  await integrity();
}, 10000);
test('cursor mantém limite superior durante novos movimentos; reconciliação detecta corrupção administrativa', async () => {
  const w = await open();
  for (let i = 0; i < 4; i++) await submit(command(w, 'WIN', '1.00'));
  type Page = { items: { walletVersion: number }[]; nextCursor: string | null };
  const first = await http<Page>(apis[0]!, `/wallets/${w.id}/ledger?limit=2`);
  expect(first.data.items.map((x) => x.walletVersion)).toEqual([5, 4]);
  await submit(command(w, 'WIN', '1.00'));
  const second = await http<Page>(
    apis[1]!,
    `/wallets/${w.id}/ledger?limit=2&cursor=${first.data.nextCursor}`,
  );
  expect(second.data.items.map((x) => x.walletVersion)).toEqual([3, 2]);
  expect(
    (await http<{ consistent: boolean }>(apis[0]!, `/wallets/${w.id}/reconciliation`, {})).data.consistent,
  ).toBe(true);
  await owner.query('ALTER TABLE wallets DISABLE TRIGGER wallet_movement');
  try {
    await owner.query('UPDATE wallets SET balance=balance+1 WHERE id=?', [w.id]);
    const check = await http<{ consistent: boolean; difference: { amount: string } }>(
      apis[0]!,
      `/wallets/${w.id}/reconciliation`,
      {},
    );
    expect(check.data.consistent).toBe(false);
    expect(check.data.difference.amount).toBe('1.00');
    await owner.query('UPDATE wallets SET balance=balance-1 WHERE id=?', [w.id]);
  } finally {
    await owner.query('ALTER TABLE wallets ENABLE TRIGGER wallet_movement');
  }
  await integrity();
});
for (const point of ['before-commit', 'after-commit', 'before-ack'])
  test(`morte em ${point}: redelivery mantém atomicidade e efeito único`, async () => {
    const w = await open();
    const c = command(w, 'BET', '25.00');
    const { id } = await envelope(c);
    const doomed = await start('consumer', { TEST_FAULT_POINT: point, TEST_FAULT_MESSAGE_ID: id });
    const barrier = await doomed.wait('barrier');
    if (point === 'before-commit') {
      expect(
        await scalar(
          "SELECT count(*)::text AS count FROM outbox_messages WHERE payload->'data'->>'transactionId'=?",
          [barrier!.transactionId],
        ),
      ).toBe(0);
      expect((await balance(w)).balance.amount).toBe('100.00');
    }
    await doomed.stop('SIGKILL');
    if (point === 'before-commit') {
      expect((await balance(w)).balance.amount).toBe('100.00');
      expect(
        await scalar('SELECT count(*)::text AS count FROM wager_transactions WHERE idempotency_key=?', [
          c.idempotencyKey,
        ]),
      ).toBe(0);
      expect(
        await scalar('SELECT count(*)::text AS count FROM inbox_messages WHERE message_id=?', [id]),
      ).toBe(0);
    } else {
      expect((await balance(w)).balance.amount).toBe('75.00');
    }
    const recovered = await start('consumer');
    await waitTransaction(c);
    await eventually(
      () =>
        scalar(
          'SELECT count(*)::text AS count FROM inbox_messages WHERE message_id=? AND processed_at IS NOT NULL',
          [id],
        ),
      (x) => x === 1,
    );
    await inputDrained();
    await recovered.stop();
    expect((await balance(w)).balance.amount).toBe('75.00');
    expect(await ledgerCount(w)).toBe(2);
    const tx = await submit(c);
    expect(tx.data.idempotentReplay).toBe(true);
    await integrity();
  });
test('shutdown no prazo devolve visibilidade imediatamente, sem aguardar expiração de 30 segundos', async () => {
  const visibilityStarted = Promise.withResolvers<void>();
  const allowVisibility = Promise.withResolvers<void>();
  const upstream = new URL(queues.endpoint);
  const proxy = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      const text = await request.text();
      const body = text ? JSON.parse(text) : {};
      if (body.VisibilityTimeout === 0) {
        visibilityStarted.resolve();
        await allowVisibility.promise;
      }
      const destination = new URL(new URL(request.url).pathname, upstream);
      try {
        return await fetch(destination, {
          method: request.method,
          headers: request.headers,
          body: text,
          signal: request.signal,
        });
      } catch {
        return new Response('Forwarding cancelled', { status: 503 });
      }
    },
  });
  const queueUrl = await queues.url(queues.names.input);
  await queues.client.send(
    new SetQueueAttributesCommand({ QueueUrl: queueUrl, Attributes: { VisibilityTimeout: '30' } }),
  );
  try {
    const w = await open();
    const c = command(w);
    await envelope(c);
    const doomed = await start('consumer', {
      TEST_FAULT_POINT: 'before-commit',
      SHUTDOWN_GRACE_MS: '200',
      SQS_VISIBILITY_SECONDS: '30',
      AWS_ENDPOINT_URL: proxy.url.origin,
    });
    await doomed.wait('barrier');
    const startTime = performance.now();
    const stopped = doomed.stop();
    await Promise.race([
      visibilityStarted.promise,
      Bun.sleep(2000).then(() => {
        throw new Error('Visibility return never started');
      }),
    ]);
    // Delay a real SQS request past the drain deadline while preserving its network cancellation.
    await Bun.sleep(300);
    expect(doomed.child.exitCode).toBeNull();
    allowVisibility.resolve();
    await stopped;
    expect(performance.now() - startTime).toBeLessThan(2000);
    expect((await balance(w)).balance.amount).toBe('100.00');
    const returned = await queues.receive(queues.names.input);
    expect(returned).toHaveLength(1);
    expect(JSON.parse(returned[0]!.Body!).data.externalTransactionId).toBe(c.externalTransactionId);
    await queues.visibility(queues.names.input, returned[0]!.ReceiptHandle!, 0);
    const recovered = await start('consumer');
    await waitTransaction(c);
    await inputDrained();
    await recovered.stop();
    expect((await balance(w)).balance.amount).toBe('90.00');
    await integrity();
  } finally {
    allowVisibility.resolve();
    await proxy.stop(true);
    await queues.client.send(
      new SetQueueAttributesCommand({ QueueUrl: queueUrl, Attributes: { VisibilityTimeout: '2' } }),
    );
  }
});
test('DLQ persiste mensagens permanentes e esgotadas pelo broker antes de removê-las', async () => {
  const worker = await start('consumer,dlq');
  await queues.send(queues.names.input, 'invalid-json', 'invalid', randomUUID());
  await eventually(
    () => scalar("SELECT count(*)::text AS count FROM dead_letter_records WHERE body='invalid-json'"),
    (x) => x === 1,
  );
  await worker.stop();
  const poison = `retry-exhausted-${suffix}`;
  await queues.send(queues.names.input, poison, 'exhaustion', randomUUID());
  for (let i = 0; i < 6; i++) {
    const messages = await queues.receive(queues.names.input);
    for (const message of messages) await queues.visibility(queues.names.input, message.ReceiptHandle!, 0);
  }
  const audit = await start('dlq');
  await eventually(
    () => scalar('SELECT count(*)::text AS count FROM dead_letter_records WHERE body=?', [poison]),
    (x) => x === 1,
  );
  await audit.stop();
  await integrity();
});
test('dois publishers, crash após envio e lease expirada: eventId estável e confirmação protegida', async () => {
  const [event] = await db.query<{ id: string; wallet_id: string; payload: Record<string, unknown> }>(
    'SELECT * FROM outbox_messages ORDER BY occurred_at,id LIMIT 1',
  );
  const receipts = new Map<string, string>();
  const acknowledgements = new Set<string>();
  const upstream = new URL(queues.endpoint);
  const proxy = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      const text = await request.text();
      const command = text ? JSON.parse(text) : {};
      try {
        const response = await fetch(new URL(new URL(request.url).pathname, upstream), {
          method: request.method,
          headers: request.headers,
          body: text,
          signal: request.signal,
        });
        const body = await response.text();
        if (response.ok && request.headers.get('x-amz-target')?.endsWith('.ReceiveMessage')) {
          let delayed = false;
          for (const message of JSON.parse(body).Messages ?? []) {
            if (JSON.parse(message.Body).eventId === event!.id) {
              receipts.set(message.ReceiptHandle, message.MessageId);
              delayed = true;
            }
          }
          // Exercise real consumption with latency beyond the old fixed 100 ms assertion.
          if (delayed) await Bun.sleep(250);
        }
        if (response.ok && request.headers.get('x-amz-target')?.endsWith('.DeleteMessage')) {
          const messageId = receipts.get(command.ReceiptHandle);
          if (messageId) acknowledgements.add(messageId);
        }
        return new Response(body, { status: response.status, headers: response.headers });
      } catch {
        return new Response('Forwarding cancelled', { status: 503 });
      }
    },
  });
  const participants: App[] = [];
  const track = async (roles: string, extra: Record<string, string> = {}) => {
    const app = await start(roles, extra);
    participants.push(app);
    return app;
  };
  try {
    const doomed = await track('publisher', { TEST_FAULT_POINT: 'after-publish', WORK_LEASE_MS: '500' });
    await doomed.wait('barrier');
    await doomed.stop('SIGKILL');
    const first = await track('publisher,events', { AWS_ENDPOINT_URL: proxy.url.origin });
    const second = await track('publisher');
    await eventually(
      () => scalar('SELECT count(*)::text AS count FROM outbox_messages WHERE published_at IS NULL'),
      (x) => x === 0,
      20000,
    );
    await eventually(
      () =>
        scalar('SELECT count(*)::text AS count FROM integration_event_receipts WHERE event_id=?', [
          event!.id,
        ]),
      (count) => count === 1,
    );
    await queues.send(queues.names.events, JSON.stringify(event!.payload), event!.wallet_id, randomUUID());
    await queues.send(queues.names.events, JSON.stringify(event!.payload), event!.wallet_id, randomUUID());
    await eventually(
      async () => acknowledgements.size,
      (count) => count >= 3,
    );
    expect(
      await scalar('SELECT count(*)::text AS count FROM integration_event_receipts WHERE event_id=?', [
        event!.id,
      ]),
    ).toBe(1);
    await first.stop();
    await second.stop();
    const w = await open();
    const stale = await track('publisher', {
      TEST_FAULT_POINT: 'publisher-before-send',
      WORK_LEASE_MS: '300',
    });
    await stale.wait('barrier');
    await Bun.sleep(400);
    const replacement = await track('publisher');
    await eventually(
      () =>
        scalar(
          'SELECT count(*)::text AS count FROM outbox_messages WHERE wallet_id=? AND published_at IS NULL',
          [w.id],
        ),
      (x) => x === 0,
    );
    const snapshots = await db.query(
      'SELECT id,published_at,attempts FROM outbox_messages WHERE wallet_id=? ORDER BY id',
      [w.id],
    );
    stale.child.send({ type: 'release-fault' });
    await Bun.sleep(100);
    expect(
      await db.query('SELECT id,published_at,attempts FROM outbox_messages WHERE wallet_id=? ORDER BY id', [
        w.id,
      ]),
    ).toEqual(snapshots);
    await stale.stop();
    await replacement.stop();
    await integrity();
  } finally {
    await Promise.all(participants.map((app) => app.stop()));
    await proxy.stop(true);
  }
}, 30000);
test('indisponibilidade de SQS conserva outbox e retomada publica eventos confirmados', async () => {
  const originalEndpoint = process.env.AWS_ENDPOINT_URL;
  process.env.AWS_ENDPOINT_URL = 'http://127.0.0.1:9';
  const failing = await start('publisher');
  if (originalEndpoint === undefined) delete process.env.AWS_ENDPOINT_URL;
  else process.env.AWS_ENDPOINT_URL = originalEndpoint;
  const w = await open();
  await submit(command(w));
  await eventually(
    () =>
      scalar(
        'SELECT count(*)::text AS count FROM outbox_messages WHERE wallet_id=? AND attempts>0 AND published_at IS NULL',
        [w.id],
      ),
    (x) => x > 0,
  );
  await failing.stop();
  const recovered = await start('publisher');
  await eventually(
    () => scalar('SELECT count(*)::text AS count FROM outbox_messages WHERE published_at IS NULL'),
    (x) => x === 0,
  );
  await recovered.stop();
  await integrity();
});
async function compose(...args: string[]) {
  const envFile = (await Bun.file('.env.local').exists()) ? ['--env-file', '.env.local'] : [];
  const child = Bun.spawn(['docker', 'compose', ...envFile, ...args], {
    stdout: 'ignore',
    stderr: 'inherit',
  });
  if ((await child.exited) !== 0) throw new Error(`Docker Compose failed: ${args.join(' ')}`);
}
test.skipIf(!process.env.TEST_BROKER)(
  'broker real parado/reiniciado: readiness, retry, persistência SQS e outbox',
  async () => {
    const broker = process.env.TEST_BROKER!;
    if (!['localstack', 'ministack'].includes(broker)) throw new Error('Invalid TEST_BROKER');
    const profile = broker === 'localstack' ? 'reference' : 'portable';
    const w = await open();
    const queued = command(w, 'BET', '10.00');
    await envelope(queued);
    await compose('--profile', profile, 'stop', broker);
    let running: App | undefined;
    try {
      running = await start('api,consumer,publisher');
      expect((await http(running, '/health/live')).status).toBe(200);
      expect((await http(running, '/health/ready')).status).toBe(503);
      const during = command(w, 'BET', '20.00');
      expect((await submit(during)).status).toBe(200);
      expect(
        await scalar(
          'SELECT count(*)::text AS count FROM outbox_messages WHERE wallet_id=? AND published_at IS NULL',
          [w.id],
        ),
      ).toBeGreaterThan(0);
    } finally {
      await compose('--profile', profile, 'up', '-d', broker);
      await eventually(
        async () => {
          try {
            await queues.ready();
            return true;
          } catch {
            return false;
          }
        },
        Boolean,
        60000,
      );
    }
    await waitTransaction(queued);
    await inputDrained();
    // Outbox retries can wait up to five minutes after a prolonged real broker outage.
    await eventually(
      () => scalar('SELECT count(*)::text AS count FROM outbox_messages WHERE published_at IS NULL'),
      (x) => x === 0,
      330000,
    );
    expect((await balance(w)).balance.amount).toBe('70.00');
    expect((await http(running!, '/health/ready')).status).toBe(200);
    await running!.stop();
    await integrity();
  },
  450000,
);
test.skipIf(!process.env.TEST_BROKER)(
  'PostgreSQL real indisponível: 503, liveness e retry com mesma chave após reinício',
  async () => {
    const w = await open();
    const c = command(w);
    await compose('stop', 'postgres');
    try {
      expect((await http(apis[0]!, '/health/live')).status).toBe(200);
      expect((await submit(c)).status).toBe(503);
    } finally {
      await compose('up', '-d', 'postgres');
      await eventually(
        async () => {
          try {
            await db.query('SELECT 1');
            return true;
          } catch {
            return false;
          }
        },
        Boolean,
        20000,
      );
    }
    expect((await submit(c)).status).toBe(200);
    expect((await balance(w)).balance.amount).toBe('90.00');
    await integrity();
  },
  30000,
);

test('contratos HTTP adversariais rejeitam entradas inválidas sem persistir efeitos', async () => {
  const w = await open();
  const { idempotencyKey, ...valid } = command(w);
  const invalid: unknown[] = [
    null,
    {},
    { ...valid, kind: 'OPENING' },
    { ...valid, providerId: '__system__' },
    { ...valid, providerId: ' \t' },
    { ...valid, externalTransactionId: 'invalid\u0000id' },
    { ...valid, externalTransactionId: '\ud800' },
    { ...valid, roundId: '\udfff' },
    { ...valid, playerId: 'not-a-uuid' },
    { ...valid, walletId: 'not-a-uuid' },
    { ...valid, kind: 'REFUND' },
    { ...valid, kind: 'ROLLBACK' },
    { ...valid, referenceExternalTransactionId: 'unwanted' },
    { ...valid, kind: 'LOSS', referenceExternalTransactionId: 'unwanted' },
    ...['-1.00', '1e2', 'NaN', 'Infinity', '', '1.001', '1', '1.0', '+1.00', ' 1.00'].map((amount) => ({
      ...valid,
      money: { amount, currency: 'BRL' },
    })),
    { ...valid, money: { amount: 10, currency: 'BRL' } },
    { ...valid, money: { amount: '1.00', currency: 'USD' } },
    { ...valid, money: { amount: `${'9'.repeat(37)}.00`, currency: 'BRL' } },
  ];
  for (const body of invalid)
    expect((await http(apis[0]!, '/wagering/transactions', body, idempotencyKey)).status).toBe(400);
  expect((await http(apis[1]!, '/wagering/transactions', valid)).status).toBe(400);
  const first = await submit({ ...valid, idempotencyKey });
  expect(first.status).toBe(200);
  expect(first.data.idempotentReplay).toBe(false);
  for (const kind of ['BET', 'WIN', 'REFUND', 'ROLLBACK'] as const) {
    const zero = command(
      w,
      kind,
      '0.00',
      ['REFUND', 'ROLLBACK'].includes(kind) ? valid.externalTransactionId : undefined,
    );
    const result = await submit(zero);
    expect(result.status).toBe(422);
    expect(result.data.failureCode).toBe('AMOUNT_MUST_BE_POSITIVE');
  }
  expect((await balance(w)).balance.amount).toBe('90.00');
  expect(await ledgerCount(w)).toBe(2);
  expect(
    await scalar(
      "SELECT count(*)::text AS count FROM wager_transactions WHERE wallet_id=? AND status='PROCESSED'",
      [w.id],
    ),
  ).toBe(2);
  await integrity();
});

test('JSON malformado, mídia incompatível e payload grande preservam HTTP 4xx e correlação', async () => {
  const w = await open();
  for (const sample of [
    { type: 'application/json', body: '{"broken":', status: 400 },
    { type: 'application/xml', body: '<transaction/>', status: 415 },
    { type: 'application/json', body: JSON.stringify({ padding: 'x'.repeat(32769) }), status: 413 },
  ]) {
    const correlationId = `contract-${randomUUID()}`;
    const response = await fetch(`${apis[0]!.url}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': sample.type,
        'X-Correlation-Id': correlationId,
        'Idempotency-Key': randomUUID(),
      },
      body: sample.body,
    });
    expect(response.status).toBe(sample.status);
    expect(response.headers.get('X-Correlation-Id')).toBe(correlationId);
    const error = (await response.json()) as { correlationId: string; retryable: boolean };
    expect(error.correlationId).toBe(correlationId);
    expect(error.retryable).toBe(false);
  }
  expect((await balance(w)).balance.amount).toBe('100.00');
  expect(await ledgerCount(w)).toBe(1);
  await integrity();
});

test('dinheiro acima de 2^53 atravessa HTTP, PostgreSQL e ledger sem perder centavos', async () => {
  const w = await open('9007199254740993.01');
  const c = command(w, 'BET', '0.01');
  const { idempotencyKey, ...body } = c;
  const first = await http<ProcessingResult>(
    apis[0]!,
    '/wagering/transactions',
    {
      ...body,
      playerId: body.playerId.toUpperCase(),
      walletId: body.walletId.toUpperCase(),
      money: { amount: '0000.01', currency: 'BRL' },
      idempotencyKey: 'ignored-body-key',
    },
    idempotencyKey,
  );
  expect(first.status).toBe(200);
  expect(first.data.balance!.amount).toBe('9007199254740993.00');
  await submit(command(w, 'WIN', '0.03'), 1);
  expect((await balance(w)).balance.amount).toBe('9007199254740993.03');
  expect((await submit(c, 2)).data).toEqual({ ...first.data, idempotentReplay: true });
  expect(
    await db.query<{ balance_after: string }>(
      'SELECT balance_after::text FROM wallet_ledger WHERE wallet_id=? ORDER BY wallet_version',
      [w.id],
    ),
  ).toEqual([
    { balance_after: '9007199254740993.01' },
    { balance_after: '9007199254740993.00' },
    { balance_after: '9007199254740993.03' },
  ]);
  await integrity();
});

test('identidades globais em wallets distintas conflitam sem bloquear o escopo de outro provedor', async () => {
  const wallets = [await open(), await open()];
  for (const mode of ['key', 'external'] as const) {
    const external = randomUUID();
    const commands = wallets.map((w, i) => ({
      ...command(w),
      externalTransactionId: mode === 'external' ? external : randomUUID(),
      idempotencyKey: mode === 'key' ? `global-${external}` : `external-${external}-${i}`,
    }));
    const responses = await Promise.all(commands.map((c, i) => submit(c, i)));
    expect(responses.map((r) => r.status).sort()).toEqual([200, 409]);
    const conflict = responses.find((r) => r.status === 409)!;
    expect((conflict.data as unknown as { code: string }).code).toBe(
      mode === 'key' ? 'IDEMPOTENCY_CONFLICT' : 'EXTERNAL_TRANSACTION_CONFLICT',
    );
  }
  const scoped = randomUUID();
  const independent = wallets.map((w, i) => ({
    ...command(w),
    providerId: `scope-${i}`,
    externalTransactionId: scoped,
  }));
  expect((await Promise.all(independent.map((c, i) => submit(c, i)))).every((r) => r.status === 200)).toBe(
    true,
  );
  const balances = await Promise.all(wallets.map(balance));
  expect(balances.map((w) => BigInt(w.balance.amount.replace('.', ''))).reduce((a, b) => a + b)).toBe(16000n);
  await integrity();
});

test('WIN, REFUNDs e ROLLBACK fora de ordem disputam a referência com dois workers', async () => {
  const w = await open();
  const bet = command(w, 'BET', '40.00');
  const dependents = [
    command(w, 'WIN', '60.00', bet.externalTransactionId),
    command(w, 'REFUND', '40.00', bet.externalTransactionId),
    command(w, 'REFUND', '40.00', bet.externalTransactionId),
    command(w, 'ROLLBACK', '40.00', bet.externalTransactionId),
  ];
  const pending = await Promise.all(dependents.map((c, i) => submit(c, i)));
  expect(pending.every((r) => r.status === 202)).toBe(true);
  const workers = [await start('references'), await start('references')];
  try {
    expect((await submit(bet, 2)).status).toBe(200);
    await eventually(
      () =>
        scalar(
          "SELECT count(*)::text AS count FROM wager_transactions WHERE wallet_id=? AND status='PENDING_REFERENCE'",
          [w.id],
        ),
      (n) => n === 0,
    );
    const results = await Promise.all(dependents.map((c, i) => submit(c, i)));
    expect(results[0]!.status).toBe(200);
    expect(results[3]!.status).toBe(200);
    expect(
      results
        .slice(1, 3)
        .map((r) => r.status)
        .sort(),
    ).toEqual([200, 422]);
    expect(results.slice(1, 3).find((r) => r.status === 422)!.data.failureCode).toBe(
      'REVERSAL_ALREADY_APPLIED',
    );
    expect((await balance(w)).balance.amount).toBe('200.00');
    expect((await balance(w)).version).toBe(5);
    expect(await ledgerCount(w)).toBe(5);
    for (const initial of pending)
      expect(
        await scalar(
          "SELECT count(*)::text AS count FROM outbox_messages WHERE aggregate_id=? AND event_type='WagerTransactionPendingReference'",
          [initial.data.transactionId],
        ),
      ).toBe(1);
    await submit(command(w, 'BET', '1.00'));
    for (let i = 0; i < dependents.length; i++)
      expect((await submit(dependents[i]!, 2)).data).toEqual(results[i]!.data);
  } finally {
    await Promise.all(workers.map((worker) => worker.stop()));
  }
  await integrity();
});

test('worker de referência com lease vencida não sobrescreve decisão do sucessor', async () => {
  const w = await open();
  const bet = command(w, 'BET', '20.00');
  const refund = command(w, 'REFUND', '20.00', bet.externalTransactionId);
  const pending = await submit(refund);
  const stale = await start('api,references', {
    TEST_FAULT_POINT: 'reference-before-process',
    WORK_LEASE_MS: '200',
  });
  try {
    const claimed = await stale.wait('barrier');
    expect(claimed!.transactionId).toBe(pending.data.transactionId);
    expect((await submit(bet)).status).toBe(200);
    await eventually(
      () =>
        scalar(
          'SELECT count(*)::text AS count FROM wager_transactions WHERE id=? AND lease_until<=clock_timestamp()',
          [pending.data.transactionId],
        ),
      (n) => n === 1,
    );
    const replacement = await start('references');
    try {
      await waitTransaction(refund);
    } finally {
      await replacement.stop();
    }
    await submit(command(w, 'WIN', '5.00'));
    const snapshot = await db.query('SELECT * FROM wager_transactions WHERE id=?', [
      pending.data.transactionId,
    ]);
    stale.child.send({ type: 'release-fault' });
    await eventually(
      async () => (await fetch(`${stale.url}/metrics`)).text(),
      (text) => text.includes('wager_duplicates_total{source="reference"} 1'),
    );
    expect(
      await db.query('SELECT * FROM wager_transactions WHERE id=?', [pending.data.transactionId]),
    ).toEqual(snapshot);
    expect((await submit(refund)).data.balance!.amount).toBe('100.00');
    expect((await balance(w)).balance.amount).toBe('105.00');
    expect(await ledgerCount(w)).toBe(4);
  } finally {
    await stale.stop('SIGKILL');
  }
  await integrity();
});

test('consultas preservam resultado original e recusam cursor de outra wallet ou inválido', async () => {
  const w = await open();
  const c = command(w);
  const result = await submit(c);
  await submit(command(w, 'WIN', '5.00'));
  const queried = await http<ProcessingResult>(
    apis[1]!,
    `/wagering/transactions/${result.data.transactionId}`,
  );
  const external = await http<ProcessingResult>(
    apis[2]!,
    `/providers/${c.providerId}/wagering/transactions/${c.externalTransactionId}`,
  );
  expect(queried.status).toBe(200);
  expect(external.data).toEqual(queried.data);
  expect(queried.data.balance!.amount).toBe('90.00');
  for (const path of [
    `/wallets/${randomUUID()}`,
    `/wallets/${randomUUID()}/ledger`,
    `/wagering/transactions/${randomUUID()}`,
    `/providers/absent/wagering/transactions/${randomUUID()}`,
  ])
    expect((await http(apis[0]!, path)).status).toBe(404);
  expect((await http(apis[0]!, '/wallets/invalid-uuid')).status).toBe(400);
  const page = await http<{ nextCursor: string }>(apis[0]!, `/wallets/${w.id}/ledger?limit=1`);
  const other = await open();
  expect((await http(apis[0]!, `/wallets/${other.id}/ledger?cursor=${page.data.nextCursor}`)).status).toBe(
    400,
  );
  for (const limit of ['0', '-1', '101', '1.5', 'Infinity', 'NaN'])
    expect((await http(apis[0]!, `/wallets/${w.id}/ledger?limit=${limit}`)).status).toBe(400);
  for (const cursor of [
    'invalid',
    Buffer.from(JSON.stringify({ v: 1, walletId: w.id, upper: 1000, last: 1001 })).toString('base64url'),
  ])
    expect((await http(apis[0]!, `/wallets/${w.id}/ledger?cursor=${cursor}`)).status).toBe(400);
  await integrity();
});

test('consultas externas recuperam identificadores válidos com 128 caracteres e caracteres multibyte', async () => {
  const w = await open();
  for (const character of ['a', 'á']) {
    const c = command(w, 'WIN', '1.00');
    c.providerId = character.repeat(128);
    c.externalTransactionId = character.toUpperCase().repeat(128);
    const submitted = await submit(c);
    expect(submitted.status).toBe(200);
    const response = await http<ProcessingResult & { providerId: string; externalTransactionId: string }>(
      apis[1]!,
      `/providers/${encodeURIComponent(c.providerId)}/wagering/transactions/${encodeURIComponent(c.externalTransactionId)}`,
    );
    expect(response.status).toBe(200);
    expect(response.data.transactionId).toBe(submitted.data.transactionId);
    expect(response.data.providerId).toBe(c.providerId);
    expect(response.data.externalTransactionId).toBe(c.externalTransactionId);
  }
  await integrity();
});

test('consultas externas recusam controles, identidade vazia e excesso de tamanho com HTTP 400', async () => {
  for (const [provider, external] of [
    ['provider\u0000', 'external'],
    ['provider', 'external\u0000'],
    [' ', 'external'],
    ['provider', ' '],
    ['p'.repeat(129), 'external'],
    ['provider', 'e'.repeat(129)],
  ]) {
    const response = await http<{ code: string }>(
      apis[0]!,
      `/providers/${encodeURIComponent(provider!)}/wagering/transactions/${encodeURIComponent(external!)}`,
    );
    expect(response.status).toBe(400);
    expect(response.data.code).toBe('INVALID_REQUEST');
  }
  expect(
    (await http(apis[0]!, '/providers/missing-provider/wagering/transactions/missing-external')).status,
  ).toBe(404);
  await integrity();
});

test('reconciliação usa um snapshot consistente quando há commit entre suas duas leituras', async () => {
  const w = await open();
  const observer = await start('api', { TEST_FAULT_POINT: 'reconciliation-after-wallet' });
  try {
    const check = http<{
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
      consistent: boolean;
    }>(observer, `/wallets/${w.id}/reconciliation`, {});
    await observer.wait('barrier');
    expect((await submit(command(w, 'BET', '5.00'), 2)).status).toBe(200);
    observer.child.send({ type: 'release-fault' });
    const result = await check;
    expect(result.status).toBe(200);
    expect(result.data.storedBalance.amount).toBe('100.00');
    expect(result.data.calculatedBalance.amount).toBe('100.00');
    expect(result.data.consistent).toBe(true);
    expect((await balance(w)).balance.amount).toBe('95.00');
  } finally {
    await observer.stop('SIGKILL');
  }
  await integrity();
});

test('conexão HTTP perdida após commit é recuperada pela mesma chave em outra instância', async () => {
  const w = await open();
  const c = command(w);
  const { idempotencyKey, ...body } = c;
  const doomed = await start('api', { TEST_FAULT_POINT: 'after-commit' });
  const lost = http<ProcessingResult>(doomed, '/wagering/transactions', body, idempotencyKey).then(
    (response) => ({ response, error: undefined }),
    (error: unknown) => ({ response: undefined, error }),
  );
  await doomed.wait('barrier');
  expect((await balance(w)).balance.amount).toBe('90.00');
  await doomed.stop('SIGKILL');
  expect((await lost).error).toBeInstanceOf(Error);
  await submit(command(w, 'WIN', '5.00'));
  const replay = await submit(c, 2);
  expect(replay.status).toBe(200);
  expect(replay.data.idempotentReplay).toBe(true);
  expect(replay.data.balance!.amount).toBe('90.00');
  expect(replay.data.walletVersion).toBe(2);
  expect((await balance(w)).balance.amount).toBe('95.00');
  expect(
    await scalar('SELECT count(*)::text AS count FROM wallet_ledger WHERE transaction_id=?', [
      replay.data.transactionId,
    ]),
  ).toBe(1);
  expect(
    await scalar('SELECT count(*)::text AS count FROM outbox_messages WHERE aggregate_id=?', [
      replay.data.transactionId,
    ]),
  ).toBe(1);
  await integrity();
});

for (const initialSeed of [0x4a554e47n, 0x12345678n, 0xdeadbeefn])
  test(`120 operações com seed ${initialSeed.toString(16)} correspondem a um modelo financeiro independente`, async () => {
    // The oracle uses only integer cents and the contract rules, never application/domain arithmetic.
    const decimal = (cents: bigint) => `${cents / 100n}.${(cents % 100n).toString().padStart(2, '0')}`;
    let seed = initialSeed;
    const random = () => {
      seed = (1664525n * seed + 1013904223n) & 0xffffffffn;
      return seed;
    };
    type Reference = { kind: 'BET' | 'WIN' | 'REFUND'; externalId: string; cents: bigint };
    type Entry = {
      transaction_id: string;
      direction: 'CREDIT' | 'DEBIT';
      amount: string;
      balance_before: string;
      balance_after: string;
      wallet_version: number;
    };
    const w = await open();
    const [opening] = await db.query<{ id: string }>(
      "SELECT id FROM wager_transactions WHERE wallet_id=? AND kind='OPENING'",
      [w.id],
    );
    let expectedBalance = 10000n;
    let expectedVersion = 1;
    const entries: Entry[] = [
      {
        transaction_id: opening!.id,
        direction: 'CREDIT',
        amount: '100.00',
        balance_before: '0.00',
        balance_after: '100.00',
        wallet_version: 1,
      },
    ];
    const references: Reference[] = [];
    const reversed = new Set<string>();
    const snapshots: { command: WagerCommand; result: ProcessingResult }[] = [];

    async function exercise(c: WagerCommand, cents: bigint, reference?: Reference) {
      const before = expectedBalance;
      let failure: string | undefined;
      let signed =
        c.kind === 'LOSS'
          ? 0n
          : c.kind === 'BET' || (c.kind === 'ROLLBACK' && reference!.kind !== 'BET')
            ? -cents
            : cents;
      const reversal = c.kind === 'REFUND' || c.kind === 'ROLLBACK';
      if (reversal && cents !== reference!.cents) failure = 'REFERENCE_AMOUNT_MISMATCH';
      else if (reversal && reversed.has(`${reference!.externalId}:${c.kind}`))
        failure = 'REVERSAL_ALREADY_APPLIED';
      else if (signed < 0n && expectedBalance + signed < 0n)
        failure = c.kind === 'BET' ? 'INSUFFICIENT_FUNDS' : 'REVERSAL_INSUFFICIENT_FUNDS';
      if (!failure) {
        expectedBalance += signed;
        if (signed !== 0n) expectedVersion++;
        if (reversal) reversed.add(`${reference!.externalId}:${c.kind}`);
        if (c.kind === 'BET' || c.kind === 'WIN' || c.kind === 'REFUND')
          references.push({ kind: c.kind, externalId: c.externalTransactionId, cents });
      }
      const result = await submit(c, snapshots.length);
      expect(result.status).toBe(failure ? 422 : 200);
      expect(result.data.status).toBe(failure ? 'REJECTED' : 'PROCESSED');
      expect(result.data.failureCode).toBe(failure);
      expect(result.data.balance!.amount).toBe(decimal(expectedBalance));
      expect(result.data.walletVersion).toBe(expectedVersion);
      snapshots.push({ command: c, result: result.data });
      if (!failure && signed !== 0n)
        entries.push({
          transaction_id: result.data.transactionId,
          direction: signed > 0n ? 'CREDIT' : 'DEBIT',
          amount: decimal(cents),
          balance_before: decimal(before),
          balance_after: decimal(expectedBalance),
          wallet_version: expectedVersion,
        });
    }

    await exercise(command(w, 'BET', '10.00'), 1000n);
    const kinds = ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'] as const;
    for (let i = 0; i < 120; i++) {
      const kind = kinds[i % kinds.length]!;
      let cents = (random() % 15000n) + 1n;
      let reference: Reference | undefined;
      if (kind === 'REFUND' || kind === 'ROLLBACK' || (kind === 'WIN' && random() % 2n === 0n)) {
        const pool = kind === 'ROLLBACK' ? references : references.filter((r) => r.kind === 'BET');
        reference = pool[Number(random() % BigInt(pool.length))]!;
        if (kind !== 'WIN') cents = reference.cents + (i % 13 === 0 ? 1n : 0n);
      }
      if (kind === 'LOSS' && i % 2 === 0) cents = 0n;
      await exercise(command(w, kind, decimal(cents), reference?.externalId), cents, reference);
    }
    for (const snapshot of snapshots.filter((_, i) => i % 9 === 0))
      expect((await submit(snapshot.command, 2)).data).toEqual({
        ...snapshot.result,
        idempotentReplay: true,
      });
    const stored = await balance(w);
    expect(stored.balance.amount).toBe(decimal(expectedBalance));
    expect(stored.version).toBe(expectedVersion);
    expect(
      await db.query<Entry>(
        'SELECT transaction_id,direction,amount::text,balance_before::text,balance_after::text,wallet_version FROM wallet_ledger WHERE wallet_id=? ORDER BY wallet_version',
        [w.id],
      ),
    ).toEqual(entries);
    const processed = snapshots.filter((s) => s.result.status === 'PROCESSED').length;
    expect(processed).toBeGreaterThan(0);
    expect(processed).toBeLessThan(snapshots.length);
    expect(
      await scalar(
        "SELECT count(*)::text AS count FROM outbox_messages WHERE wallet_id=? AND event_type='WalletBalanceChanged'",
        [w.id],
      ),
    ).toBe(entries.length);
    expect(
      await scalar(
        "SELECT count(*)::text AS count FROM outbox_messages WHERE wallet_id=? AND event_type='WagerTransactionProcessed'",
        [w.id],
      ),
    ).toBe(processed + 1);
    expect(
      await scalar(
        "SELECT count(*)::text AS count FROM outbox_messages WHERE wallet_id=? AND event_type='WagerTransactionRejected'",
        [w.id],
      ),
    ).toBe(snapshots.length - processed);
    expect(
      await scalar('SELECT count(*)::text AS count FROM wager_transactions WHERE wallet_id=?', [w.id]),
    ).toBe(snapshots.length + 1);
    await integrity();
  }, 30000);

test('operações mistas concorrentes por HTTP/SQS preservam decisões, cadeia financeira e recibos de eventos', async () => {
  const cents = (value: string) => BigInt(value.replace('.', ''));
  const decimal = (value: bigint) => `${value / 100n}.${(value % 100n).toString().padStart(2, '0')}`;
  let seed = 0xc0ffee42n;
  const random = () => {
    seed = (1664525n * seed + 1013904223n) & 0xffffffffn;
    return seed;
  };
  const scenarios: { wallet: WalletView; bet: WagerCommand; win: WagerCommand; commands: WagerCommand[] }[] =
    [];
  for (let index = 0; index < 3; index++) {
    const wallet = await open('30.00');
    const bet = command(wallet, 'BET', '5.00');
    const win = command(wallet, 'WIN', '3.00');
    expect((await submit(bet)).status).toBe(200);
    expect((await submit(win)).status).toBe(200);
    const commands: WagerCommand[] = [];
    for (let operation = 0; operation < 48; operation++) {
      const amount = decimal((random() % 12000n) + 1n);
      commands.push(
        operation % 8 === 0
          ? command(wallet, 'BET', amount)
          : operation % 8 === 1
            ? command(wallet, 'WIN', decimal((random() % 2000n) + 1n))
            : operation % 8 === 2
              ? command(wallet, 'LOSS', operation % 16 === 2 ? '0.00' : amount)
              : operation % 8 === 3
                ? command(wallet, 'REFUND', '5.00', bet.externalTransactionId)
                : operation % 8 === 4
                  ? command(wallet, 'ROLLBACK', '5.00', bet.externalTransactionId)
                  : operation % 8 === 5
                    ? command(wallet, 'ROLLBACK', '3.00', win.externalTransactionId)
                    : operation % 8 === 6
                      ? command(wallet, 'REFUND', '5.01', bet.externalTransactionId)
                      : command(wallet, 'WIN', decimal((random() % 1000n) + 1n), bet.externalTransactionId),
      );
    }
    scenarios.push({ wallet, bet, win, commands });
  }
  const workers = [
    await start('consumer'),
    await start('consumer'),
    await start('publisher,events'),
    await start('publisher,events'),
  ];
  try {
    const work = scenarios.flatMap((scenario) => scenario.commands);
    for (let index = work.length - 1; index > 0; index--) {
      const target = Number(random() % BigInt(index + 1));
      [work[index], work[target]] = [work[target]!, work[index]!];
    }
    const responses = new Map<string, ProcessingResult[]>();
    let next = 0;
    await Promise.all(
      Array.from({ length: 12 }, async () => {
        while (next < work.length) {
          const index = next++;
          const c = work[index]!;
          const [first, second] = await Promise.all([
            submit(c, index),
            submit(c, index + 1),
            envelope(c),
            envelope(c),
          ]);
          expect([200, 422]).toContain(first.status);
          expect([200, 422]).toContain(second.status);
          responses.set(c.idempotencyKey, [first.data, second.data]);
        }
      }),
    );
    await inputDrained();
    for (const { wallet, bet, win, commands } of scenarios) {
      const known = new Map([bet, win, ...commands].map((c) => [c.idempotencyKey, c]));
      const rows = await db.query<{
        id: string;
        idempotency_key: string;
        kind: string;
        status: string;
        result_snapshot: ProcessingResult;
      }>('SELECT id,idempotency_key,kind,status,result_snapshot FROM wager_transactions WHERE wallet_id=?', [
        wallet.id,
      ]);
      expect(rows).toHaveLength(51);
      const byId = new Map(rows.map((row) => [row.id, row]));
      const signed = (c: WagerCommand) =>
        c.kind === 'LOSS'
          ? 0n
          : c.kind === 'BET' ||
              (c.kind === 'ROLLBACK' && c.referenceExternalTransactionId === win.externalTransactionId)
            ? -cents(c.money.amount)
            : cents(c.money.amount);
      const entries = await db.query<{
        transaction_id: string;
        direction: string;
        amount: string;
        balance_before: string;
        balance_after: string;
        wallet_version: number;
      }>(
        'SELECT transaction_id,direction,amount::text,balance_before::text,balance_after::text,wallet_version FROM wallet_ledger WHERE wallet_id=? ORDER BY wallet_version',
        [wallet.id],
      );
      // Reconstruct the committed serialization with plain integer cents, independently of domain classes.
      let expected = 0n;
      let version = 0;
      const balances = new Map<number, bigint>();
      const ledgerIds = new Set<string>();
      for (const entry of entries) {
        const row = byId.get(entry.transaction_id)!;
        const c = known.get(row.idempotency_key);
        const movement = row.kind === 'OPENING' ? 3000n : signed(c!);
        expect(row.status).toBe('PROCESSED');
        expect(cents(entry.amount)).toBe(movement < 0n ? -movement : movement);
        expect(entry.direction).toBe(movement > 0n ? 'CREDIT' : 'DEBIT');
        expect(cents(entry.balance_before)).toBe(expected);
        expected += movement;
        expect(expected >= 0n).toBe(true);
        expect(cents(entry.balance_after)).toBe(expected);
        expect(entry.wallet_version).toBe(++version);
        expect(row.result_snapshot.balance!.amount).toBe(decimal(expected));
        expect(row.result_snapshot.walletVersion).toBe(version);
        balances.set(version, expected);
        ledgerIds.add(row.id);
      }
      const reversals = new Map<string, ProcessingResult>();
      for (const row of rows.filter((r) => r.status === 'PROCESSED')) {
        const c = known.get(row.idempotency_key);
        if (c && ['REFUND', 'ROLLBACK'].includes(c.kind)) {
          const identity = `${c.kind}:${c.referenceExternalTransactionId}`;
          expect(reversals.has(identity)).toBe(false);
          reversals.set(identity, row.result_snapshot);
        }
      }
      for (const row of rows) {
        const result = row.result_snapshot;
        expect(['PROCESSED', 'REJECTED']).toContain(row.status);
        const observed = balances.get(result.walletVersion!);
        expect(observed).toBeDefined();
        expect(cents(result.balance!.amount)).toBe(observed!);
        const c = known.get(row.idempotency_key);
        if (!c) {
          expect(row.kind).toBe('OPENING');
          continue;
        }
        if (row.status === 'PROCESSED') expect(ledgerIds.has(row.id)).toBe(c.kind !== 'LOSS');
        else {
          expect(ledgerIds.has(row.id)).toBe(false);
          const failure = result.failureCode;
          expect([
            'INSUFFICIENT_FUNDS',
            'REVERSAL_INSUFFICIENT_FUNDS',
            'REVERSAL_ALREADY_APPLIED',
            'REFERENCE_AMOUNT_MISMATCH',
          ]).toContain(failure!);
          if (failure === 'INSUFFICIENT_FUNDS' || failure === 'REVERSAL_INSUFFICIENT_FUNDS')
            expect(cents(result.balance!.amount) + signed(c) < 0n).toBe(true);
          else if (failure === 'REFERENCE_AMOUNT_MISMATCH') expect(c.money.amount).toBe('5.01');
          else {
            const winner = reversals.get(`${c.kind}:${c.referenceExternalTransactionId}`);
            expect(winner).toBeDefined();
            expect(winner!.walletVersion! <= result.walletVersion!).toBe(true);
          }
        }
        for (const response of responses.get(c.idempotencyKey) ?? [])
          expect({ ...response, idempotentReplay: false }).toEqual(result);
      }
      const final = await balance(wallet);
      expect(final.balance.amount).toBe(decimal(expected));
      expect(final.version).toBe(version);
      expect(
        await scalar(
          'SELECT count(*)::text AS count FROM inbox_messages WHERE transaction_id IN (SELECT id FROM wager_transactions WHERE wallet_id=?) AND processed_at IS NOT NULL',
          [wallet.id],
        ),
      ).toBe(96);
      const processed = rows.filter((row) => row.status === 'PROCESSED').length;
      for (const [eventType, count] of [
        ['WalletBalanceChanged', entries.length],
        ['WagerTransactionProcessed', processed],
        ['WagerTransactionRejected', rows.length - processed],
      ] as const)
        expect(
          await scalar(
            'SELECT count(*)::text AS count FROM outbox_messages WHERE wallet_id=? AND event_type=?',
            [wallet.id, eventType],
          ),
        ).toBe(count);
      await eventually(
        () =>
          scalar(
            'SELECT count(*)::text AS count FROM outbox_messages o WHERE o.wallet_id=? AND (o.published_at IS NULL OR NOT EXISTS (SELECT 1 FROM integration_event_receipts r WHERE r.event_id=o.id))',
            [wallet.id],
          ),
        (count) => count === 0,
        30000,
      );
      for (const c of commands.filter((_, index) => index % 7 === 0)) {
        const original = rows.find((row) => row.idempotency_key === c.idempotencyKey)!;
        expect((await submit(c, 2)).data).toEqual({ ...original.result_snapshot, idempotentReplay: true });
      }
    }
    await integrity();
  } finally {
    await Promise.all(workers.map((worker) => worker.stop()));
  }
}, 90000);

test('identidades com pontuação, espaços, Unicode e chaves no limite preservam consulta e replay canônico', async () => {
  const wallet = await open();
  const identities = [
    'segment/with/slash',
    'percent%2F-literal',
    'query?#&=+',
    `quotes'"\\colon:`,
    ' espaço interno ',
    'é',
    'e\u0301',
    '🎮'.repeat(64),
  ];
  for (const identity of identities) {
    const c = command(wallet, 'WIN', '0001.00');
    c.providerId = identity;
    c.externalTransactionId = identity;
    c.idempotencyKey = `boundary-${randomUUID()}`.padEnd(256, 'k');
    const first = await submit(c);
    expect(first.status).toBe(200);
    const query = await http<ProcessingResult>(
      apis[1]!,
      `/providers/${encodeURIComponent(identity)}/wagering/transactions/${encodeURIComponent(identity)}`,
    );
    expect(query.status).toBe(200);
    expect(query.data.transactionId).toBe(first.data.transactionId);
    const replay = await submit(
      {
        ...c,
        money: { amount: '1.00', currency: 'BRL' },
        walletId: c.walletId.toUpperCase(),
        playerId: c.playerId.toUpperCase(),
      },
      2,
    );
    expect(replay.status).toBe(200);
    expect(replay.data).toEqual({ ...first.data, idempotentReplay: true });
    expect((await submit({ ...c, idempotencyKey: c.idempotencyKey + 'x' })).status).toBe(400);
  }
  expect((await balance(wallet)).balance.amount).toBe('108.00');
  expect(await ledgerCount(wallet)).toBe(9);
  await integrity();
});

test('messageId inválido é auditado na DLQ sem entrar no processamento financeiro', async () => {
  const w = await open();
  const c = command(w);
  const worker = await start('consumer,dlq');
  try {
    for (const messageId of ['\u0000invalid', ' \t', 'x'.repeat(129), '\ud800', '\udfff']) {
      const body = JSON.stringify({
        messageId,
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: c,
      });
      await queues.send(queues.names.input, body, w.id, randomUUID());
      await eventually(
        () => scalar('SELECT count(*)::text AS count FROM dead_letter_records WHERE body=?', [body]),
        (n) => n === 1,
      );
      const [record] = await db.query<{ reason: string; message_id: string }>(
        'SELECT reason,message_id FROM dead_letter_records WHERE body=?',
        [body],
      );
      expect(record!.reason).toBe('INVALID_ENVELOPE');
      expect(record!.message_id).toMatch(/^[0-9a-f-]{36}$/);
      expect(record!.message_id).not.toBe(messageId);
    }
    expect(
      await scalar('SELECT count(*)::text AS count FROM wager_transactions WHERE idempotency_key=?', [
        c.idempotencyKey,
      ]),
    ).toBe(0);
    expect((await balance(w)).balance.amount).toBe('100.00');
    expect(await ledgerCount(w)).toBe(1);
    await inputDrained();
  } finally {
    await worker.stop();
  }
  await integrity();
});

test('guard global consulta identidade em todas as rotas de negócio e métricas; health permanece público', async () => {
  const { NestFactory } = await import('@nestjs/core');
  const { UnauthorizedException } = await import('@nestjs/common');
  const { FastifyAdapter } = await import('@nestjs/platform-fastify');
  const apiPath = new URL('../../dist/src/http/api.js', import.meta.url).href;
  const { ApiModule, ApiErrorFilter } = (await import(apiPath)) as typeof import('../../src/http/api.js');
  const { Observability } = (await import(
    new URL('../../dist/src/infrastructure/observability.js', import.meta.url).href
  )) as typeof import('../../src/infrastructure/observability.js');
  const { Wagering } = (await import(
    new URL('../../dist/src/application/wagering.js', import.meta.url).href
  )) as typeof import('../../src/application/wagering.js');
  const { Database: GuardDatabase } = (await import(
    new URL('../../dist/src/infrastructure/database.js', import.meta.url).href
  )) as typeof import('../../src/infrastructure/database.js');
  const guardDb = await GuardDatabase.connect(appUrl.toString());
  const metrics = new Observability();
  const calls: string[] = [];
  const identity: IdentityPort = {
    async identify(req) {
      calls.push(`${req.method} ${req.url}`);
      if (req.headers.authorization !== 'Bearer test-only') throw new UnauthorizedException();
      return { subject: 'test-principal', mode: 'test' };
    },
  };
  const server = await NestFactory.create<import('@nestjs/platform-fastify').NestFastifyApplication>(
    ApiModule.configure(guardDb, queues, metrics, new Wagering(guardDb, metrics), identity),
    new FastifyAdapter(),
    { logger: false, abortOnError: false },
  );
  try {
    server.useGlobalFilters(new ApiErrorFilter());
    await server.listen(0, '127.0.0.1');
    const port = (server.getHttpServer().address() as { port: number }).port;
    const url = `http://127.0.0.1:${port}`;
    for (const path of ['/health/live', '/health/ready'])
      expect((await fetch(`${url}${path}`)).status).toBe(200);
    expect(calls).toEqual([]);
    const w = await open();
    const bet = command(w);
    const result = await submit(bet);
    const { idempotencyKey, ...win } = command(w, 'WIN', '1.00');
    const routes = [
      {
        method: 'POST',
        path: '/wallets',
        body: { playerId: randomUUID(), initialBalance: { amount: '0.00', currency: 'BRL' } },
        status: 201,
      },
      { method: 'GET', path: `/wallets/${w.id}`, status: 200 },
      { method: 'GET', path: `/wallets/${w.id}/ledger`, status: 200 },
      { method: 'GET', path: `/wagering/transactions/${result.data.transactionId}`, status: 200 },
      {
        method: 'GET',
        path: `/providers/${bet.providerId}/wagering/transactions/${bet.externalTransactionId}`,
        status: 200,
      },
      { method: 'POST', path: '/wagering/transactions', body: win, status: 200 },
      { method: 'POST', path: `/wallets/${w.id}/reconciliation`, body: {}, status: 200 },
      { method: 'GET', path: '/metrics', status: 200 },
    ];
    for (const route of routes) {
      const response = await fetch(`${url}${route.path}`, {
        method: route.method,
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
        ...('body' in route ? { body: JSON.stringify(route.body) } : {}),
      });
      expect(response.status).toBe(401);
    }
    expect(calls).toEqual(routes.map((route) => `${route.method} ${route.path}`));
    expect((await balance(w)).balance.amount).toBe('90.00');
    expect(await ledgerCount(w)).toBe(2);
    expect(
      await scalar('SELECT count(*)::text AS count FROM wager_transactions WHERE idempotency_key=?', [
        idempotencyKey,
      ]),
    ).toBe(0);
    calls.length = 0;
    for (const route of routes) {
      const response = await fetch(`${url}${route.path}`, {
        method: route.method,
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': idempotencyKey,
          Authorization: 'Bearer test-only',
        },
        ...('body' in route ? { body: JSON.stringify(route.body) } : {}),
      });
      expect(response.status).toBe(route.status);
      if (route.path === '/metrics') expect(response.headers.get('content-type')).toContain('text/plain');
    }
    expect(calls).toEqual(routes.map((route) => `${route.method} ${route.path}`));
    expect((await balance(w)).balance.amount).toBe('91.00');
    expect(await ledgerCount(w)).toBe(3);
  } finally {
    await server.close();
    await guardDb.close();
    metrics.registry.clear();
  }
  await integrity();
});
