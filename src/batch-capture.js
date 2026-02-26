// src/batch-capture.js
const fs = require("fs");
const path = require("path");
const { captureWebsite } = require("./capture");

const ROOT = path.join(__dirname, "..");
const INPUT_FILE = path.join(ROOT, "input", "urls.txt");
const DEFAULT_OUT_DIR =
  process.platform === "win32"
    ? "D:/sidesone-screenshot-output"
    : "/data/sidesone-screenshot-output";

const OUT_DIR = process.env.OUTPUT_DIR || DEFAULT_OUT_DIR;
const LOGS_DIR = path.join(OUT_DIR, "logs");

fs.mkdirSync(LOGS_DIR, { recursive: true });

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

function readUrls(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Input file not found: ${filePath}`);
  }

  const raw = fs.readFileSync(filePath, "utf8");
  return raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));
}

function manifestExistsFor(url) {
  const fileBase = safeFileName(url);
  const manifestPath = path.join(OUT_DIR, "manifests", `${fileBase}.json`);
  return fs.existsSync(manifestPath);
}

function parseArg(name, fallback) {
  const idx = process.argv.indexOf(name);
  if (idx !== -1 && process.argv[idx + 1]) {
    return process.argv[idx + 1];
  }
  return fallback;
}

async function runOne(url, index, total) {
  const startedAt = Date.now();
  console.log(`\n[${index + 1}/${total}] Capturing: ${url}`);

  try {
    const result = await captureWebsite(url);
    const ms = Date.now() - startedAt;
    console.log(
      `[${index + 1}/${total}] ${result.capture_status.toUpperCase()} in ${ms}ms -> ${url}`
    );
    if (result.capture_status !== "success") {
      console.log(`  Error: ${result.capture_error || "Unknown error"}`);
    }
    return result;
  } catch (err) {
    const ms = Date.now() - startedAt;
    console.log(`[${index + 1}/${total}] FAILED in ${ms}ms -> ${url}`);
    console.log(`  Error: ${err?.message || String(err)}`);
    return {
      input_url: url,
      capture_status: "failed",
      capture_error: err?.message || String(err)
    };
  }
}

async function main() {
  const concurrency = Math.max(1, Number(parseArg("--concurrency", "1")) || 1);
  const skipExisting = parseArg("--skip-existing", "true") !== "false";

  const urls = readUrls(INPUT_FILE);
  if (!urls.length) {
    console.log("No URLs found in input\\urls.txt");
    return;
  }

  const filtered = skipExisting
    ? urls.filter((u) => !manifestExistsFor(u))
    : urls;

  console.log(`Total URLs in file: ${urls.length}`);
  console.log(`Skip existing manifests: ${skipExisting}`);
  console.log(`To process now: ${filtered.length}`);
  console.log(`Concurrency: ${concurrency}`);

  const logPath = path.join(
    LOGS_DIR,
    `batch_${new Date().toISOString().replace(/[:.]/g, "-")}.log`
  );

  let nextIndex = 0;
  let successCount = 0;
  let failCount = 0;

  async function worker(workerId) {
    while (true) {
      const current = nextIndex++;
      if (current >= filtered.length) return;

      const url = filtered[current];
      const result = await runOne(url, current, filtered.length);

      if (result.capture_status === "success") successCount++;
      else failCount++;

      const logLine = JSON.stringify({
        workerId,
        index: current + 1,
        total: filtered.length,
        url,
        status: result.capture_status,
        error: result.capture_error || null,
        timestamp: new Date().toISOString()
      });

      fs.appendFileSync(logPath, logLine + "\n", "utf8");
    }
  }

  const workers = Array.from({ length: concurrency }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  console.log("\n=== Batch Finished ===");
  console.log(`Success: ${successCount}`);
  console.log(`Failed:  ${failCount}`);
  console.log(`Log file: ${logPath}`);
}

main().catch((err) => {
  console.error("Fatal batch error:", err);
  process.exit(1);
});