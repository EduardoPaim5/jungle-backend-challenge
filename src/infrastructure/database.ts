import { EntitySchema, LockMode, IsolationLevel } from '@mikro-orm/core';
import { MikroORM, PostgreSqlDriver, type EntityManager } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { Migration202610070001 } from './migrations/Migration202610070001.js';
import { Migration202610070002 } from './migrations/Migration202610070002.js';
import { Money } from '../domain/money.js';
import { Wallet } from '../domain/wallet.js';

export interface WalletRecord {
  id: string;
  playerId: string;
  currency: string;
  balance: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}
export const WalletSchema = new EntitySchema<WalletRecord>({
  name: 'WalletRecord',
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid', fieldName: 'player_id' },
    currency: { type: 'string', length: 3 },
    balance: { type: 'decimal', precision: 38, scale: 2, runtimeType: 'string' },
    version: { type: 'integer' },
    createdAt: { type: 'Date', fieldName: 'created_at' },
    updatedAt: { type: 'Date', fieldName: 'updated_at' },
  },
});
export class Database {
  private constructor(public readonly orm: MikroORM) {}
  static async connect(
    url = process.env.DATABASE_URL ?? 'postgresql://jungle_app:jungle_app@localhost:55432/jungle',
  ): Promise<Database> {
    const orm = await MikroORM.init({
      driver: PostgreSqlDriver,
      clientUrl: url,
      entities: [WalletSchema],
      pool: { min: 0, max: 8 },
      debug: false,
      logger: () => {},
      extensions: [Migrator],
      migrations: {
        migrationsList: [Migration202610070001, Migration202610070002],
        transactional: true,
        allOrNothing: true,
        snapshot: false,
      },
      driverOptions: { connectionTimeoutMillis: 5000 },
    });
    return new Database(orm);
  }
  async query<T extends object = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
    em: EntityManager = this.orm.em.fork(),
  ): Promise<T[]> {
    return (await em.execute(sql, [...params])) as T[];
  }
  async transaction<T>(fn: (em: EntityManager) => Promise<T>, repeatableRead = false): Promise<T> {
    return this.orm.em.fork().transactional(
      async (em) => {
        await em.execute("SET LOCAL lock_timeout='1s'");
        await em.execute("SET LOCAL statement_timeout='5s'");
        return fn(em);
      },
      { isolationLevel: repeatableRead ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED },
    );
  }
  async lockWallet(
    id: string,
    em: EntityManager,
  ): Promise<{ wallet: Wallet; record: WalletRecord } | undefined> {
    const record = await em.findOne(WalletSchema, { id }, { lockMode: LockMode.PESSIMISTIC_WRITE });
    if (!record) return undefined;
    return {
      record,
      wallet: Wallet.rehydrate({
        ...record,
        balance: Money.rehydrate({ amount: record.balance, currency: record.currency }),
      }),
    };
  }
  async close(): Promise<void> {
    await this.orm.close(true);
  }
}
export function sqlState(error: unknown): string | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const e = error as { code?: string; cause?: unknown };
  return e.code ?? (e.cause !== error ? sqlState(e.cause) : undefined);
}
