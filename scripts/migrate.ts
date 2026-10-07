import { Database } from '../src/infrastructure/database.js';
const db = await Database.connect(
  process.env.MIGRATION_DATABASE_URL ?? 'postgresql://jungle_owner:jungle_owner@localhost:55432/jungle',
);
try {
  if (process.argv[2] === 'down') await db.orm.migrator.down();
  else await db.orm.migrator.up();
  console.log(JSON.stringify({ migrations: process.argv[2] ?? 'up', success: true }));
} finally {
  await db.close();
}
