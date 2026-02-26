const fs = require("fs");
const path = require("path");
require("dotenv").config();
const { google } = require("googleapis");

const ROOT = path.join(__dirname, "..");
const INPUT_DIR = path.join(ROOT, "input");
const URLS_TXT_PATH = path.join(INPUT_DIR, "urls.txt");
const URL_MAP_PATH = path.join(INPUT_DIR, "url-map.json");

fs.mkdirSync(INPUT_DIR, { recursive: true });

function parseArg(name, fallback = null) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] ?? true;
}

async function getSheetsClient() {
  const keyFile = process.env.GOOGLE_SERVICE_ACCOUNT_KEYFILE;
  if (!keyFile) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_KEYFILE in .env");

  const auth = new google.auth.GoogleAuth({
    keyFile: path.isAbsolute(keyFile) ? keyFile : path.join(ROOT, keyFile),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

async function main() {
  const sheetId = process.env.GOOGLE_SHEET_ID;
  const tab = process.env.GOOGLE_SHEET_TAB || "Test";
  const urlCol = process.env.GOOGLE_SHEET_URL_COLUMN || "D";
  const startRow = Number(process.env.GOOGLE_SHEET_START_ROW || 2);

  const limitArg = parseArg("--limit", null);
  const limit = limitArg ? Number(limitArg) : null;

  if (!sheetId) throw new Error("Missing GOOGLE_SHEET_ID in .env");

  const sheets = await getSheetsClient();
  const range = `${tab}!${urlCol}${startRow}:${urlCol}`;

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range
  });

  const rows = res.data.values || [];
  const mapped = [];

  for (let i = 0; i < rows.length; i++) {
    const raw = rows[i]?.[0];
    const url = String(raw || "").trim();
    const rowNumber = startRow + i;

    if (!url) continue;
    mapped.push({ row: rowNumber, url });

    if (limit && mapped.length >= limit) break;
  }

  if (!mapped.length) {
    throw new Error(`No URLs found in range: ${range}`);
  }

  fs.writeFileSync(URLS_TXT_PATH, mapped.map((x) => x.url).join("\n") + "\n", "utf8");
  fs.writeFileSync(URL_MAP_PATH, JSON.stringify(mapped, null, 2), "utf8");

  console.log(`Pulled ${mapped.length} URL(s) from Google Sheet`);
  console.log(`Saved: ${URLS_TXT_PATH}`);
  console.log(`Saved map: ${URL_MAP_PATH}`);
  console.log(`Range read: ${range}`);
}

main().catch((err) => {
  console.error("pull-sheet-urls error:", err.message || err);
  process.exit(1);
});