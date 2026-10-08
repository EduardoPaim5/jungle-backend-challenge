import { Database } from '../src/infrastructure/database.js';
import { Queues } from '../src/infrastructure/sqs.js';

export interface EnvironmentIssue {
  code: 'POSTGRES_UNAVAILABLE' | 'SCHEMA_NOT_READY' | 'SQS_UNAVAILABLE' | 'QUEUES_NOT_READY';
  message: string;
}

function address(value: string, fallback: string): string {
  try {
    return new URL(value).host;
  } catch {
    return fallback;
  }
}

async function checkDatabase(): Promise<EnvironmentIssue[]> {
  let db: Database | undefined;
  try {
    try {
      db = await Database.connect();
      await db.query('SELECT 1');
    } catch {
      const target = address(process.env.DATABASE_URL ?? 'postgresql://localhost:55432', 'configurado');
      return [
        {
          code: 'POSTGRES_UNAVAILABLE',
          message: `PostgreSQL inacessível em ${target}. Verifique o container e DATABASE_URL (endereço, banco e credenciais).`,
        },
      ];
    }
    const connected = db;
    try {
      await connected.transaction(async (em) => {
        // Read using the application role; no migrations, inserts or schema changes here.
        await connected.query(
          `SELECT w.id,t.id,l.id,i.message_id,o.id,d.id,e.event_id
         FROM wallets w,wager_transactions t,wallet_ledger l,inbox_messages i,
              outbox_messages o,dead_letter_records d,integration_event_receipts e LIMIT 0`,
          [],
          em,
        );
        const [schema] = await connected.query<{ current: boolean }>(
          `SELECT count(*)=3 AS current FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
         WHERE NOT t.tgisinternal AND t.tgenabled<>'D' AND c.relnamespace='public'::regnamespace
           AND (c.relname,t.tgname) IN (('wager_transactions','transaction_reference_snapshot'),
             ('inbox_messages','inbox_identity'),('outbox_messages','outbox_identity'))`,
          [],
          em,
        );
        if (!schema?.current) throw new Error('Required schema migration is missing');
      });
      return [];
    } catch {
      return [
        {
          code: 'SCHEMA_NOT_READY',
          message:
            'Schema ausente, desatualizado ou sem acesso pelo papel da aplicação. Execute bun run db:migrate e confira DATABASE_URL/MIGRATION_DATABASE_URL.',
        },
      ];
    }
  } finally {
    await db?.close();
  }
}

async function checkQueues(): Promise<EnvironmentIssue[]> {
  let queues: Queues | undefined;
  try {
    queues = new Queues();
    await queues.ready();
    return [];
  } catch (error) {
    const name = error instanceof Error ? error.name : '';
    if (['QueueDoesNotExist', 'AWS.SimpleQueueService.NonExistentQueue'].includes(name))
      return [
        {
          code: 'QUEUES_NOT_READY',
          message:
            'Uma ou mais filas SQS estão ausentes. Execute bun run queues:init com o mesmo AWS_ENDPOINT_URL e QUEUE_PREFIX.',
        },
      ];
    return [
      {
        code: 'SQS_UNAVAILABLE',
        message: `SQS inacessível em ${address(process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566', 'endpoint configurado')}. Verifique o broker, AWS_ENDPOINT_URL e as credenciais.`,
      },
    ];
  } finally {
    queues?.close();
  }
}

export async function checkEnvironment(): Promise<EnvironmentIssue[]> {
  const [database, queues] = await Promise.all([checkDatabase(), checkQueues()]);
  return [...database, ...queues];
}

export function formatEnvironmentIssues(issues: EnvironmentIssue[]): string {
  return [
    'Ambiente incompleto; nenhuma instância da aplicação ou carga foi iniciada.',
    ...issues.map((issue) => `[${issue.code}] ${issue.message}`),
    '',
    'Para preparar o ambiente local, escolha um broker (ambos usam a porta 4566):',
    '  LocalStack: docker compose --env-file .env.local --profile reference up -d --wait postgres localstack',
    '  MiniStack:  docker compose --profile portable up -d --wait postgres ministack',
    'Depois execute:',
    '  bun run db:migrate',
    '  bun run queues:init',
    '  bun run environment:check',
    'Consulte o README para configuração ou troca de perfil.',
  ].join('\n');
}
