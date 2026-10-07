import { randomUUID } from 'node:crypto';
import type { ProcessingResult } from '../src/application/contracts.js';
import type { WagerCommand } from '../src/domain/transaction.js';
export interface WalletView {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}
export async function request<T>(url: string, path: string, body?: unknown, key?: string): Promise<T> {
  const response = await fetch(`${url}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(10000),
  });
  const result: unknown = await response.json();
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${JSON.stringify(result)}`);
  return result as T;
}
export function open(url: string, amount = '100.00') {
  return request<WalletView>(url, '/wallets', {
    playerId: randomUUID(),
    initialBalance: { amount, currency: 'BRL' },
  });
}
export function command(
  wallet: WalletView,
  kind: WagerCommand['kind'],
  amount: string,
  reference?: string,
): WagerCommand {
  const external = randomUUID();
  return {
    providerId: 'demo-provider',
    externalTransactionId: external,
    idempotencyKey: `demo:${external}`,
    walletId: wallet.id,
    playerId: wallet.playerId,
    roundId: 'demo-round',
    gameId: 'demo-game',
    kind,
    money: { amount, currency: 'BRL' },
    ...(reference ? { referenceExternalTransactionId: reference } : {}),
  };
}
export function submit(url: string, c: WagerCommand) {
  const { idempotencyKey, ...body } = c;
  return request<ProcessingResult>(url, '/wagering/transactions', body, idempotencyKey);
}
export async function withInstances<T>(
  count: number,
  roles: string,
  work: (urls: string[]) => Promise<T>,
): Promise<T> {
  const configured = process.env.API_URLS ?? process.env.API_URL;
  if (configured) return work(configured.split(',').map((url) => url.replace(/\/$/, '')));
  const children: Bun.Subprocess<'ignore', 'ignore', 'inherit'>[] = [];
  const urls: string[] = [];
  try {
    for (let i = 0; i < count; i++) {
      let ready: { port: number } | undefined;
      const child = Bun.spawn([process.execPath, 'dist/src/main.js'], {
        stdin: 'ignore',
        stdout: 'ignore',
        stderr: 'inherit',
        env: { ...process.env, PORT: '0', APP_ROLES: roles, LOG_LEVEL: 'error' },
        ipc(message: unknown) {
          if (
            message &&
            typeof message === 'object' &&
            'type' in message &&
            message.type === 'ready' &&
            'port' in message
          )
            ready = { port: Number(message.port) };
        },
      });
      children.push(child);
      const deadline = Date.now() + 15000;
      while (!ready) {
        if (child.exitCode !== null || Date.now() > deadline)
          throw new Error('Application failed to start. Run migrations and queues:init first.');
        await Bun.sleep(20);
      }
      urls.push(`http://127.0.0.1:${ready.port}`);
    }
    return await work(urls);
  } finally {
    for (const child of children) if (child.exitCode === null) child.kill('SIGTERM');
    await Promise.race([Promise.allSettled(children.map((child) => child.exited)), Bun.sleep(30000)]);
    for (const child of children) if (child.exitCode === null) child.kill('SIGKILL');
  }
}
