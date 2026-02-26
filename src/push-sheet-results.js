// src/push-sheet-results.js
const fs = require("fs");
const path = require("path");
require("dotenv").config();
const { google } = require("googleapis");

const ROOT = path.join(__dirname, "..");
const INPUT_DIR = path.join(ROOT, "input");
const URL_MAP_PATH = path.join(INPUT_DIR, "url-map.json");

const DEFAULT_OUT_DIR =
  process.platform === "win32"
    ? "D:/sidesone-screenshot-output"
    : "/data/sidesone-screenshot-output";

const OUT_DIR = process.env.OUTPUT_DIR || DEFAULT_OUT_DIR;
const MANIFEST_DIR = path.join(OUT_DIR, "manifests");
const ANALYSIS_RESULTS_DIR = path.join(OUT_DIR, "analysis", "results");

function safeFileName(str) {
  return String(str || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/[^\w.-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
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

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function buildScreenshotStatus(manifest) {
  if (!manifest) return "missing manifest";
  if (manifest.capture_status === "success") return "success";
  if (manifest.capture_status === "failed") {
    const err = manifest.capture_error ? `failed: ${manifest.capture_error}` : "failed";
    return err.slice(0, 200);
  }
  return String(manifest.capture_status || "unknown").slice(0, 200);
}

function buildScreenshotPath(manifest) {
  if (!manifest || manifest.capture_status !== "success") return "";
  const top = manifest.desktop_top_path || "";
  const mid = manifest.desktop_mid_path || "";
  const bot = manifest.desktop_bottom_path || "";

  // Keep it useful but not too long
  // You can change this to only top if you want
  const parts = [];
  if (top) parts.push(`top=${top}`);
  if (mid) parts.push(`mid=${mid}`);
  if (bot) parts.push(`bottom=${bot}`);
  return parts.join(" | ").slice(0, 50000);
}

function buildScreenshotTake(manifest) {
  if (!manifest) return "";
  return String(manifest.timestamp || "").slice(0, 200);
}

function buildAnalyzeStatus(manifest, analysisObj) {
  if (!manifest) return "missing manifest";
  if (manifest.capture_status !== "success") return "skipped (capture failed)";

  if (!analysisObj) return "missing analysis";
  const mode = analysisObj.mode || "unknown";
  const model = analysisObj.model || "";
  return model ? `ok (${mode}, ${model})` : `ok (${mode})`;
}

function buildComment(analysisObj) {
  if (!analysisObj) return "";
  return String(analysisObj.comment_no || analysisObj.raw_output_text || "").trim().slice(0, 50000);
}

async function main() {
  const sheetId = process.env.GOOGLE_SHEET_ID;
  const tab = process.env.GOOGLE_SHEET_TAB || "Test";

  // Column config (new names)
  const colScreenshotStatus = process.env.GOOGLE_SHEET_SCREENSHOT_STATUS_COLUMN || "K";
  const colScreenshotPath = process.env.GOOGLE_SHEET_SCREENSHOT_PATH_COLUMN || "L";
  const colScreenshotTake = process.env.GOOGLE_SHEET_SCREENSHOT_TAKE_COLUMN || "M";
  const colAnalyzeStatus =
    process.env.GOOGLE_SHEET_ANALYZE_STATUS_COLUMN ||
    process.env.GOOGLE_SHEET_STATUS_COLUMN || // backward compat
    "N";
  const colComment =
    process.env.GOOGLE_SHEET_COMMENT_COLUMN ||
    "O";

  if (!sheetId) throw new Error("Missing GOOGLE_SHEET_ID in .env");

  if (!fs.existsSync(URL_MAP_PATH)) {
    throw new Error(`Missing URL map file: ${URL_MAP_PATH}. Run pull-sheet-urls.js first.`);
  }

  const urlMap = readJson(URL_MAP_PATH);
  const sheets = await getSheetsClient();

  const updates = [];
  let foundAnalysis = 0;
  let missingAnalysis = 0;
  let missingManifest = 0;

  for (const item of urlMap) {
    const url = item.url;
    const row = item.row;

    const base = safeFileName(url);

    const manifestPath = path.join(MANIFEST_DIR, `${base}.json`);
    const analysisPath = path.join(ANALYSIS_RESULTS_DIR, `${base}.analysis.json`);

    let manifest = null;
    let analysisFile = null;
    let analysisObj = null;

    if (fs.existsSync(manifestPath)) {
      try {
        manifest = readJson(manifestPath);
      } catch {
        manifest = { capture_status: "failed", capture_error: "manifest parse error" };
      }
    } else {
      missingManifest++;
    }

    if (fs.existsSync(analysisPath)) {
      try {
        analysisFile = readJson(analysisPath);
        analysisObj = analysisFile.analysis || null;
        if (analysisObj) foundAnalysis++;
        else missingAnalysis++;
      } catch {
        analysisObj = null;
        missingAnalysis++;
      }
    } else {
      missingAnalysis++;
    }

    const screenshotStatus = buildScreenshotStatus(manifest);
    const screenshotPath = buildScreenshotPath(manifest);
    const screenshotTake = buildScreenshotTake(manifest);
    const analyzeStatus = buildAnalyzeStatus(manifest, analysisObj);
    const comment = buildComment(analysisObj);

    // Update K:O (5 columns)
    const range = `${tab}!${colScreenshotStatus}${row}:${colComment}${row}`;
    updates.push({
      range,
      values: [[screenshotStatus, screenshotPath, screenshotTake, analyzeStatus, comment]]
    });
  }

  if (!updates.length) throw new Error("No rows to update.");

  // Batch updates in chunks (safe for larger runs later)
  const chunks = chunkArray(updates, 400);

  for (const chunk of chunks) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: sheetId,
      requestBody: {
        valueInputOption: "RAW",
        data: chunk
      }
    });
  }

  console.log("Google Sheet updated.");
  console.log(`Rows processed: ${urlMap.length}`);
  console.log(`Found analysis: ${foundAnalysis}`);
  console.log(`Missing analysis: ${missingAnalysis}`);
  console.log(`Missing manifest: ${missingManifest}`);
}

main().catch((err) => {
  console.error("push-sheet-results error:", err.message || err);
  process.exit(1);
});