require("dotenv").config()
const { Redis } = require("@upstash/redis")

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
})

async function test() {
  const meta = await redis.get("job:job_1772318013243:meta")
  console.log(meta)
}

test()