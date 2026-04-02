require("dotenv").config();

const { Redis } = require("@upstash/redis");
const { google } = require("googleapis");

const redis = Redis.fromEnv();

async function loadGoogleTokensForUser(userId) {
  const safeUserId = String(userId || "").trim();

  if (!safeUserId) {
    throw new Error("Missing userId for Google Sheets OAuth access.");
  }

  const raw = await redis.get(`google:oauth:tokens:${safeUserId}`);

  if (!raw) {
    throw new Error(
      `No Google OAuth tokens found for user ${safeUserId}.`
    );
  }

  if (typeof raw === "object") {
    return raw;
  }

  try {
    return JSON.parse(String(raw));
  } catch {
    throw new Error(
      `Stored Google OAuth tokens for user ${safeUserId} are invalid.`
    );
  }
}

async function getSheetsClientForUser(userId) {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  const redirectUri = process.env.GOOGLE_REDIRECT_URI;

  if (!clientId) throw new Error("Missing GOOGLE_CLIENT_ID in .env");
  if (!clientSecret) throw new Error("Missing GOOGLE_CLIENT_SECRET in .env");
  if (!redirectUri) throw new Error("Missing GOOGLE_REDIRECT_URI in .env");

  const tokens = await loadGoogleTokensForUser(userId);

  if (!tokens?.access_token && !tokens?.refresh_token) {
    throw new Error(`Google account is not connected for user ${userId}.`);
  }

  const oauth2Client = new google.auth.OAuth2(
    clientId,
    clientSecret,
    redirectUri
  );

  oauth2Client.setCredentials({
    access_token: tokens.access_token || undefined,
    refresh_token: tokens.refresh_token || undefined,
    scope: tokens.scope || undefined,
    token_type: tokens.token_type || undefined,
    expiry_date: tokens.expiry_date || undefined,
  });

  return google.sheets({
    version: "v4",
    auth: oauth2Client,
  });
}

module.exports = {
  getSheetsClientForUser,
  loadGoogleTokensForUser,
};