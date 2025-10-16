/**
 * @since 1.0.0
 */
import * as SqlClient from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import * as Arr from "effect/Array"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { PersistenceError } from "./ClusterError.js"
import * as RunnerStorage from "./RunnerStorage.js"
import * as ShardingConfig from "./ShardingConfig.js"

const withTracerDisabled = Effect.withTracerEnabled(false)

/**
 * @since 1.0.0
 * @category Constructors
 */
export const make = Effect.fnUntraced(function*(options: {
  readonly prefix?: string | undefined
}) {
  const config = yield* ShardingConfig.ShardingConfig
  const sql = (yield* SqlClient.SqlClient).withoutTransforms()
  const prefix = options?.prefix ?? "cluster"
  const table = (name: string) => `${prefix}_${name}`

  const runnersTable = table("runners")
  const runnersTableSql = sql(runnersTable)

  // Migrate old tables if they exist
  // TODO: Remove in next major version
  const hasOldTables = yield* sql`SELECT shard_id FROM ${sql(table("shards"))} LIMIT 1`.pipe(
    Effect.isSuccess
  )
  if (hasOldTables) {
    yield* sql`DROP TABLE ${sql(table("shards"))}`.pipe(Effect.ignore)
    yield* sql`DROP TABLE ${runnersTableSql}`.pipe(Effect.ignore)
  }

  yield* sql.onDialectOrElse({
    mssql: () =>
      sql`
        IF OBJECT_ID(N'${runnersTableSql}', N'U') IS NULL
        CREATE TABLE ${runnersTableSql} (
          machine_id INT IDENTITY PRIMARY KEY,
          address VARCHAR(255) NOT NULL,
          runner TEXT NOT NULL,
          healthy BIT NOT NULL DEFAULT 1,
          last_heartbeat DATETIME NOT NULL DEFAULT GETDATE(),
          UNIQUE(address)
        )
      `,
    mysql: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${runnersTableSql} (
          machine_id INT AUTO_INCREMENT PRIMARY KEY,
          address VARCHAR(255) NOT NULL,
          runner TEXT NOT NULL,
          healthy BOOLEAN NOT NULL DEFAULT TRUE,
          last_heartbeat DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(address)
        )
      `,
    pg: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${runnersTableSql} (
          machine_id SERIAL PRIMARY KEY,
          address VARCHAR(255) NOT NULL,
          runner TEXT NOT NULL,
          healthy BOOLEAN NOT NULL DEFAULT TRUE,
          last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
          UNIQUE(address)
        )
      `,
    orElse: () =>
      // sqlite
      sql`
        CREATE TABLE IF NOT EXISTS ${runnersTableSql} (
          machine_id INTEGER PRIMARY KEY AUTOINCREMENT,
          address TEXT NOT NULL,
          runner TEXT NOT NULL,
          healthy INTEGER NOT NULL DEFAULT 1,
          last_heartbeat DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
          UNIQUE(address)
        )
      `
  })

  const locksTable = table("locks")
  const locksTableSql = sql(locksTable)

  yield* sql.onDialectOrElse({
    mssql: () =>
      sql`
        IF OBJECT_ID(N'${locksTableSql}', N'U') IS NULL
        CREATE TABLE ${locksTableSql} (
          shard_id VARCHAR(50) PRIMARY KEY,
          address VARCHAR(255) NOT NULL,
          acquired_at DATETIME NOT NULL
        )
      `,
    mysql: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${locksTableSql} (
          shard_id VARCHAR(50) PRIMARY KEY,
          address VARCHAR(255) NOT NULL,
          acquired_at DATETIME NOT NULL
        )
      `,
    pg: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${locksTableSql} (
          shard_id VARCHAR(50) PRIMARY KEY,
          address VARCHAR(255) NOT NULL,
          acquired_at TIMESTAMP NOT NULL
        )
      `,
    orElse: () =>
      // sqlite
      sql`
        CREATE TABLE IF NOT EXISTS ${locksTableSql} (
          shard_id TEXT PRIMARY KEY,
          address TEXT NOT NULL,
          acquired_at DATETIME NOT NULL
        )
      `
  })

  const sqlNowString = sql.onDialectOrElse({
    pg: () => "NOW()",
    mysql: () => "NOW()",
    mssql: () => "GETDATE()",
    orElse: () => "CURRENT_TIMESTAMP"
  })
  const sqlNow = sql.literal(sqlNowString)

  const expiresSeconds = sql.literal(Math.ceil(Duration.toSeconds(config.shardLockExpiration)).toString())
  const lockExpiresAt = sql.onDialectOrElse({
    pg: () => sql`${sqlNow} - INTERVAL '${expiresSeconds} seconds'`,
    mysql: () => sql`DATE_SUB(${sqlNow}, INTERVAL ${expiresSeconds} SECOND)`,
    mssql: () => sql`DATEADD(SECOND, -${expiresSeconds}, ${sqlNow})`,
    orElse: () => sql`datetime(${sqlNow}, '-${expiresSeconds} seconds')`
  })

  const encodeBoolean = sql.onDialectOrElse({
    mssql: () => (b: boolean) => (b ? 1 : 0),
    sqlite: () => (b: boolean) => (b ? 1 : 0),
    orElse: () => (b: boolean) => b
  })

  // Upsert runner and return machine_id
  const insertRunner = sql.onDialectOrElse({
    mssql: () => (address: string, runner: string, healthy: boolean) =>
      sql`
        MERGE ${runnersTableSql} AS target
        USING (SELECT ${address} AS address, ${runner} AS runner, ${sqlNow} AS last_heartbeat, ${
        encodeBoolean(healthy)
      } AS healthy) AS source
        ON target.address = source.address
        WHEN MATCHED THEN
          UPDATE SET runner = source.runner, last_heartbeat = source.last_heartbeat, healthy = source.healthy
        WHEN NOT MATCHED THEN
          INSERT (address, runner, last_heartbeat, healthy)
          VALUES (source.address, source.runner, source.last_heartbeat, source.healthy)
        OUTPUT INSERTED.machine_id;
      `.values,
    mysql: () => (address: string, runner: string, healthy: boolean) =>
      sql<{ machine_id: number }>`
        INSERT INTO ${runnersTableSql} (address, runner, last_heartbeat, healthy)
        VALUES (${address}, ${runner}, ${sqlNow}, ${healthy})
        ON DUPLICATE KEY UPDATE
          runner = VALUES(runner),
          last_heartbeat = VALUES(last_heartbeat),
          healthy = VALUES(healthy);
        SELECT machine_id FROM ${runnersTableSql} WHERE address = ${address};
      `.unprepared.pipe(
        Effect.map((results: any) => [[results[1][0].machine_id]])
      ),
    pg: () => (address: string, runner: string, healthy: boolean) =>
      sql`
        INSERT INTO ${runnersTableSql} (address, runner, last_heartbeat, healthy)
        VALUES (${address}, ${runner}, ${sqlNow}, ${healthy})
        ON CONFLICT (address) DO UPDATE
        SET runner = EXCLUDED.runner,
            last_heartbeat = EXCLUDED.last_heartbeat,
            healthy = EXCLUDED.healthy
        RETURNING machine_id
      `.values,
    orElse: () => (address: string, runner: string, healthy: boolean) =>
      // sqlite
      sql`
        INSERT INTO ${runnersTableSql} (address, runner, last_heartbeat, healthy)
        VALUES (${address}, ${runner}, ${sqlNow}, ${encodeBoolean(healthy)})
        ON CONFLICT(address) DO UPDATE SET
          runner = excluded.runner,
          last_heartbeat = excluded.last_heartbeat,
          healthy = excluded.healthy
        RETURNING machine_id;
      `.values
  })

  const acquireLock = sql.onDialectOrElse({
    pg: () => (address: string, values: Array<any>) =>
      sql`
        INSERT INTO ${locksTableSql} (shard_id, address, acquired_at) VALUES ${sql.csv(values)}
        ON CONFLICT (shard_id) DO UPDATE
        SET address = ${address}, acquired_at = ${sqlNow}
        WHERE ${locksTableSql}.address = ${address}
          OR ${locksTableSql}.acquired_at < ${lockExpiresAt}
      `,
    mysql: () => (_address: string, values: Array<any>) =>
      sql`
        INSERT INTO ${locksTableSql} (shard_id, address, acquired_at) VALUES ${sql.csv(values)}
        ON DUPLICATE KEY UPDATE
        address = IF(address = VALUES(address) OR acquired_at < ${lockExpiresAt}, VALUES(address), address),
        acquired_at = IF(address = VALUES(address) OR acquired_at < ${lockExpiresAt}, VALUES(acquired_at), acquired_at)
      `.unprepared,
    mssql: () => (_address: string, values: Array<any>) =>
      sql`
        MERGE ${locksTableSql} WITH (HOLDLOCK) AS target
        USING (SELECT * FROM (VALUES ${sql.csv(values)})) AS source (shard_id, address, acquired_at)
        ON target.shard_id = source.shard_id
        WHEN MATCHED AND (target.address = source.address OR DATEDIFF(SECOND, target.acquired_at, ${sqlNow}) > ${expiresSeconds}) THEN
          UPDATE SET address = source.address, acquired_at = source.acquired_at
        WHEN NOT MATCHED THEN
          INSERT (shard_id, address, acquired_at)
          VALUES (source.shard_id, source.address, source.acquired_at);
      `,
    orElse: () => (address: string, values: Array<any>) =>
      // sqlite
      sql`
        WITH source(shard_id, address, acquired_at) AS (VALUES ${sql.csv(values)})
        INSERT INTO ${locksTableSql} (shard_id, address, acquired_at)
        SELECT source.shard_id, source.address, source.acquired_at
        FROM source
        WHERE NOT EXISTS (
          SELECT 1 FROM ${locksTableSql}
          WHERE shard_id = source.shard_id
          AND address != ${address}
          AND (strftime('%s', ${sqlNow}) - strftime('%s', acquired_at)) <= ${expiresSeconds}
        )
        ON CONFLICT(shard_id) DO UPDATE
        SET address = ${address}, acquired_at = ${sqlNow}
      `
  })

  const wrapString = sql.onDialectOrElse({
    mssql: () => (s: string) => `N'${s}'`,
    orElse: () => (s: string) => `'${s}'`
  })
  const stringLiteral = (s: string) => sql.literal(wrapString(s))

  const refreshShards = sql.onDialectOrElse({
    mysql: () => (address: string) =>
      sql<Array<{ shard_id: string }>>`
        UPDATE ${locksTableSql}
        SET acquired_at = ${sqlNow}
        WHERE address = ${address};
        SELECT shard_id FROM ${locksTableSql} WHERE address = ${address}
      `.unprepared.pipe(
        Effect.map((rows) => rows[1].map((row) => [row.shard_id]))
      ),
    mssql: () => (address: string) =>
      sql`
        UPDATE ${locksTableSql}
        SET acquired_at = ${sqlNow}
        OUTPUT inserted.shard_id
        WHERE address = ${address}
      `.values,
    orElse: () => (address: string) =>
      sql`
        UPDATE ${locksTableSql}
        SET acquired_at = ${sqlNow}
        WHERE address = ${address}
        RETURNING shard_id
      `.values
  })

  return RunnerStorage.makeEncoded({
    getRunners: sql`SELECT runner, healthy FROM ${runnersTableSql} WHERE last_heartbeat > ${lockExpiresAt}`.values.pipe(
      PersistenceError.refail,
      Effect.map(Arr.map(([runner, healthy]) => [String(runner), Boolean(healthy)] as const)),
      withTracerDisabled
    ),

    register: (address, runner, healthy) =>
      insertRunner(address, runner, healthy).pipe(
        Effect.map((rows: any) => Number(rows[0][0])),
        PersistenceError.refail,
        withTracerDisabled
      ),

    unregister: (address) =>
      sql`DELETE FROM ${runnersTableSql} WHERE address = ${address} OR last_heartbeat < ${lockExpiresAt}`.pipe(
        Effect.asVoid,
        PersistenceError.refail,
        withTracerDisabled
      ),

    setRunnerHealth: (address, healthy) =>
      sql`UPDATE ${runnersTableSql} SET healthy = ${encodeBoolean(healthy)} WHERE address = ${address}`
        .pipe(
          Effect.asVoid,
          PersistenceError.refail,
          withTracerDisabled
        ),

    acquire: Effect.fnUntraced(
      function*(address, shardIds) {
        const values = shardIds.map((shardId) => sql`(${stringLiteral(shardId)}, ${stringLiteral(address)}, ${sqlNow})`)
        yield* acquireLock(address, values)
        const currentLocks = yield* sql<{ shard_id: string }>`
          SELECT shard_id FROM ${sql(locksTable)}
          WHERE address = ${address} AND acquired_at >= ${lockExpiresAt}
        `.values
        return currentLocks.map((row) => row[0] as string)
      },
      sql.withTransaction,
      PersistenceError.refail,
      withTracerDisabled
    ),

    refresh: (address) =>
      sql`UPDATE ${runnersTableSql} SET last_heartbeat = ${sqlNow} WHERE address = ${address}`.pipe(
        Effect.andThen(refreshShards(address)),
        Effect.map((rows) => rows.map((row) => row[0] as string)),
        PersistenceError.refail,
        withTracerDisabled
      ),

    release: (address, shardId) =>
      sql`DELETE FROM ${locksTableSql} WHERE address = ${address} AND shard_id = ${shardId}`.pipe(
        PersistenceError.refail,
        withTracerDisabled
      ),

    releaseAll: (address) =>
      sql`DELETE FROM ${locksTableSql} WHERE address = ${address}`.pipe(
        PersistenceError.refail,
        withTracerDisabled
      )
  })
}, withTracerDisabled)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layer: Layer.Layer<
  RunnerStorage.RunnerStorage,
  SqlError,
  SqlClient.SqlClient | ShardingConfig.ShardingConfig
> = Layer.effect(RunnerStorage.RunnerStorage)(make({}))

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWith = (options: {
  readonly prefix?: string | undefined
}): Layer.Layer<RunnerStorage.RunnerStorage, SqlError, SqlClient.SqlClient | ShardingConfig.ShardingConfig> =>
  Layer.effect(RunnerStorage.RunnerStorage)(make(options))
