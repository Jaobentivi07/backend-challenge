import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';

export default defineConfig({
  baseDir: process.cwd(),
  clientUrl: process.env.DATABASE_URL ?? 'postgresql://wager:wager@localhost:5432/wagering',
  entities: [],
  discovery: { warnWhenNoEntities: false },
  extensions: [Migrator],
  preferTs: false,
  migrations: { path: 'dist/src/migrations', pathTs: 'src/migrations', snapshot: false },
  allowGlobalContext: false,
});
