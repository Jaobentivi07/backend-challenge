import { MikroORM } from '@mikro-orm/postgresql';
import config from '../mikro-orm.config';

async function rollback(): Promise<void> {
  const orm = await MikroORM.init(config);
  try {
    const migrations = await orm.getMigrator().down();
    for (const migration of migrations) console.log(`Reverted ${migration.name}`);
    if (migrations.length === 0) console.log('No migration to revert');
  } finally {
    await orm.close(true);
  }
}

void rollback().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
