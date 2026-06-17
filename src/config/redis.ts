import Redis from "ioredis";

const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";

/**
 * Shared Redis client configured for BullMQ compatibility.
 */
export const redis = new Redis(redisUrl, {
  maxRetriesPerRequest: null,
});

/**
 * Returns the configured Redis connection URL.
 *
 * @returns Redis connection URL from environment or default.
 */
export function getRedisUrl(): string {
  return redisUrl;
}
