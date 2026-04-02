const fs = require("fs");
const path = require("path");
require("dotenv").config();

const { getSheetsClientForUser } = require("./google-user-sheets");

const ROOT = path.join(__dirname, "..");
const INPUT_DIR = path.join(ROOT, "input");
const URLS_TXT_PATH = path.join(INPUT_DIR, "urls.txt");
const URL_MAP_PATH = path.join(INPUT_DIR, "url-map.json");

fs.mkdirSync(INPUT_DIR, { recursive: true });

function looksLikeWebsiteUrl(value) {
  const v = String(value || "").trim().toLowerCase();
  if (!v) return false;

  if (v.includes("@")) return false;

  if (/^https?:\/\//.test(v)) return true;
  if (/^(www\.)?[a-z0-9.-]+\.[a-z]{2,}(\/.*)?$/.test(v)) return true;

  return false;
}

function normalizeColumnLetter(value) {
  const v = String(value || "").trim().toUpperCase();
  if (!v) return "";
  if (!/^[A-Z]+$/.test(v)) {
    throw new Error(`Invalid column reference: ${value}`);
  }
  return v;
}

function cleanCell(value) {
  return String(value || "").trim();
}

async function pullSheetUrls({
  userId,
  sheetId,
  sheetTab,
  urlColumn,
  mailColumn,
  firstNameColumn,
  companyNameColumn,
  industryColumn,
  locationColumn,
}) {
  const tab = String(sheetTab || "").trim();
  const urlCol = normalizeColumnLetter(urlColumn);
  const mailCol = normalizeColumnLetter(mailColumn);
  const firstNameCol = normalizeColumnLetter(firstNameColumn);
  const companyNameCol = normalizeColumnLetter(companyNameColumn);
  const industryCol = normalizeColumnLetter(industryColumn);
  const locationCol = normalizeColumnLetter(locationColumn);

  const startRow = Number(process.env.GOOGLE_SHEET_START_ROW || 2);

  if (!userId) throw new Error("Missing userId");
  if (!sheetId) throw new Error("Missing sheetId");
  if (!tab) throw new Error("Missing sheetTab");
  if (!urlCol) throw new Error("Missing urlColumn");

  const sheets = await getSheetsClientForUser(userId);

  const requestedColumns = [
    { key: "url", column: urlCol, required: true },
    { key: "recipientEmail", column: mailCol, required: false },
    { key: "firstName", column: firstNameCol, required: false },
    { key: "companyName", column: companyNameCol, required: false },
    { key: "industry", column: industryCol, required: false },
    { key: "location", column: locationCol, required: false },
  ].filter((item) => item.required || item.column);

  const ranges = requestedColumns.map(
    (item) => `${tab}!${item.column}${startRow}:${item.column}`
  );

  const res = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: sheetId,
    ranges,
  });

  const valueRanges = Array.isArray(res?.data?.valueRanges)
    ? res.data.valueRanges
    : [];

  const columnData = {};
  requestedColumns.forEach((item, index) => {
    columnData[item.key] = valueRanges[index]?.values || [];
  });

  const urlRows = columnData.url || [];
  const mapped = [];
  let ignored = 0;

  for (let i = 0; i < urlRows.length; i++) {
    const rawUrl = urlRows[i]?.[0];
    const urlValue = cleanCell(rawUrl);
    const rowNumber = startRow + i;

    if (!urlValue) continue;

    if (!looksLikeWebsiteUrl(urlValue)) {
      ignored += 1;
      continue;
    }

    mapped.push({
      url: urlValue,
      rowIndex: rowNumber,
      recipientEmail: cleanCell(columnData.recipientEmail?.[i]?.[0]),
      firstName: cleanCell(columnData.firstName?.[i]?.[0]),
      companyName: cleanCell(columnData.companyName?.[i]?.[0]),
      industry: cleanCell(columnData.industry?.[i]?.[0]),
      location: cleanCell(columnData.location?.[i]?.[0]),
    });
  }

  if (mapped.length === 0) {
    throw new Error(
      `No website URLs detected in selected URL column (${urlCol}).`
    );
  }

  console.log(`Pulled ${mapped.length} website URL(s) from Google Sheet`);
  console.log(`Ignored ${ignored} non-URL value(s)`);
  console.log(`Ranges read: ${ranges.join(", ")}`);
  console.log(`Google Sheets access via connected user: ${userId}`);

  try {
    fs.writeFileSync(
      URLS_TXT_PATH,
      mapped.map((x) => x.url).join("\n"),
      "utf8"
    );

    fs.writeFileSync(URL_MAP_PATH, JSON.stringify(mapped, null, 2), "utf8");
  } catch (_) {}

  return mapped;
}

module.exports = pullSheetUrls;