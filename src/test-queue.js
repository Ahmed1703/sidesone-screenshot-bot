require("dotenv").config();
const { Redis } = require("@upstash/redis");
const redis = Redis.fromEnv();

async function check() {
  const queue = await redis.lrange("jobs:queue", 0, -1);
  console.log("Queue contents:", queue);
}

check();