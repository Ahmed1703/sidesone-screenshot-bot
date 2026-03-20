// src/analyzer-prompt.js

const { Redis } = require("@upstash/redis");
require("dotenv").config();

const redis = Redis.fromEnv();

function normalizePromptLanguage(language = "no") {
  const value = String(language || "").trim().toLowerCase();

  if (value === "en" || value === "english") return "en";
  if (
    value === "no" ||
    value === "nb" ||
    value === "nn" ||
    value === "norwegian" ||
    value === "bokmal" ||
    value === "norsk" ||
    value === "no-no"
  ) {
    return "no";
  }

  return "no";
}

/**
 * Returns the FULL prompt stored in Redis.
 * No hardcoded fallback.
 * Throws error if prompt is missing.
 */
async function buildAnalyzerPrompt(language = "no") {
  const lang = normalizePromptLanguage(language);
  const key = `prompt:${lang}`;

  const prompt = await redis.get(key);

  if (!prompt || !String(prompt).trim()) {
    throw new Error(
      `Redis prompt is empty for key "${key}". Set a prompt in /admin before running analysis.`
    );
  }

  return String(prompt).trim();
}

module.exports = { buildAnalyzerPrompt };