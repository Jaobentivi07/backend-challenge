import { MikroORM } from '@mikro-orm/postgresql';
import config from '../mikro-orm.config';

async function migrate(): Promise<void> {
  const orm = await MikroORM.init(config);
  try {
    const migrations = await orm.getMigrator().up();
    for (const migration of migrations) console.log(`Applied ${migration.name}`);
    if (migrations.length === 0) console.log('Database is already up to date');
  } finally {
    await orm.close(true);
  }
}

void migrate().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
