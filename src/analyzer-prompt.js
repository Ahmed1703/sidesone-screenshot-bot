// src/analyzer-prompt.js

const { Redis } = require("@upstash/redis");
require("dotenv").config();

const redis = Redis.fromEnv();

/**
 * Returns the FULL prompt stored in Redis.
 * No hardcoded fallback.
 * Throws error if prompt is missing.
 */
async function buildAnalyzerPrompt(language = "no") {
  const lang = language === "en" ? "en" : "no";
const key = `prompt:${lang}`;

const prompt = await redis.get(key);

  if (!prompt || !String(prompt).trim()) {
    throw new Error(
      "Redis prompt is empty. Set a prompt in /admin before running analysis."
    );
  }

  return String(prompt);
}

module.exports = { buildAnalyzerPrompt };