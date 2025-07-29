import Redis from "ioredis";

export const REDIS_URL="";
export const createRedisClient = () => {
  const client = new Redis(REDIS_URL, {
    tls: {
      rejectUnauthorized: true,
    },
    
    retryStrategy: (times) => {
      const delay = Math.min(times * 50, 2000);
      return delay;
    },
    maxRetriesPerRequest: 10,
    enableReadyCheck: true,
    reconnectOnError: (err) => {
      const targetError = "READONLY";
      if (err.message.includes(targetError)) {
        return true;
      }
      return false;
    },
  });

  client.on("error", (err) => {
    console.error("Redis Client Error:", err);
  });

  client.on("connect", () => {
    console.log("Connected to Redis");
  });

  return client;
};


