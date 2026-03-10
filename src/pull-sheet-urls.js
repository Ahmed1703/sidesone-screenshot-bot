const fs = require("fs");
const path = require("path");
require("dotenv").config();
const { google } = require("googleapis");

const ROOT = path.join(__dirname, "..");
const INPUT_DIR = path.join(ROOT, "input");
const URLS_TXT_PATH = path.join(INPUT_DIR, "urls.txt");
const URL_MAP_PATH = path.join(INPUT_DIR, "url-map.json");

fs.mkdirSync(INPUT_DIR, { recursive: true });

async function getSheetsClient() {
  const keyFile = process.env.GOOGLE_SERVICE_ACCOUNT_KEYFILE;
  if (!keyFile) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_KEYFILE in .env");

  const auth = new google.auth.GoogleAuth({
    keyFile: path.isAbsolute(keyFile) ? keyFile : path.join(ROOT, keyFile),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

function looksLikeWebsiteUrl(value) {
  const v = String(value || "").trim().toLowerCase();
  if (!v) return false;

  // reject emails
  if (v.includes("@")) return false;

  // allow full urls
  if (/^https?:\/\//.test(v)) return true;

  // allow plain domains like example.no or www.example.no
  if (/^(www\.)?[a-z0-9.-]+\.[a-z]{2,}(\/.*)?$/.test(v)) return true;

  return false;
}

async function pullSheetUrls({ sheetId, sheetTab, urlColumn }) {
  const tab = String(sheetTab || "").trim();
  const urlCol = String(urlColumn || "").trim();
  const startRow = Number(process.env.GOOGLE_SHEET_START_ROW || 2);

  if (!sheetId) throw new Error("Missing sheetId");
  if (!tab) throw new Error("Missing sheetTab");
  if (!urlCol) throw new Error("Missing urlColumn");

  const sheets = await getSheetsClient();
  const range = `${tab}!${urlCol}${startRow}:${urlCol}`;

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range,
  });

  const rows = res.data.values || [];
  const mapped = [];
  let ignored = 0;

  for (let i = 0; i < rows.length; i++) {
    const raw = rows[i]?.[0];
    const value = String(raw || "").trim();
    const rowNumber = startRow + i;

    if (!value) continue;

    if (!looksLikeWebsiteUrl(value)) {
      ignored += 1;
      continue;
    }

    mapped.push({
      url: value,
      rowIndex: rowNumber,
    });
  }

  if (mapped.length === 0) {
    throw new Error(`No website URLs detected in selected URL column (${urlCol}).`);
  }

  console.log(`Pulled ${mapped.length} website URL(s) from Google Sheet`);
  console.log(`Ignored ${ignored} non-URL value(s)`);
  console.log(`Range read: ${range}`);

  return mapped;
}

module.exports = pullSheetUrls;