import Redis from "ioredis";

let schedulerRedis: Redis | null = null;

function getSchedulerRedis(): Redis | null {
  const redisUrl = process.env.REDIS_URL || process.env.KV_URL;
  if (!redisUrl) return null;

  if (schedulerRedis) return schedulerRedis;

  schedulerRedis = new Redis(redisUrl, {
    lazyConnect: true,
    enableReadyCheck: true,
    maxRetriesPerRequest: 2,
    retryStrategy: (times: number) => Math.min(times * 100, 1500),
    tls: {
      rejectUnauthorized: false,
    },
  });

  schedulerRedis.on("error", (err) => {
    console.error("[SCHEDULER][REDIS] error:", err.message);
  });

  return schedulerRedis;
}

async function ensureConnected(client: Redis): Promise<void> {
  if (client.status === "ready") return;
  if (client.status === "connecting") return;
  try {
    await client.connect();
  } catch {
    // Ignore connect races/errors; subsequent command will reveal state.
  }
}

export async function acquireLease(key: string, owner: string, ttlMs: number): Promise<boolean> {
  const client = getSchedulerRedis();
  if (!client) return false;

  await ensureConnected(client);
  const result = await client.set(key, owner, "PX", ttlMs, "NX");
  return result === "OK";
}

export async function renewLease(key: string, owner: string, ttlMs: number): Promise<boolean> {
  const client = getSchedulerRedis();
  if (!client) return false;

  await ensureConnected(client);
  const result = await client.eval(
    `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end`,
    1,
    key,
    owner,
    String(ttlMs)
  );

  return Number(result) === 1;
}

export async function releaseLease(key: string, owner: string): Promise<void> {
  const client = getSchedulerRedis();
  if (!client) return;

  await ensureConnected(client);
  await client.eval(
    `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end`,
    1,
    key,
    owner
  );
}

export function redisAvailableForScheduler(): boolean {
  return Boolean(process.env.REDIS_URL || process.env.KV_URL);
}
