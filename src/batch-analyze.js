// src/batch-analyze.js
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
require("dotenv").config();

// ✅ single source of truth
const OUT_DIR = process.env.OUTPUT_DIR || "D:/sidesone-screenshot-output";

const MANIFEST_DIR = path.join(OUT_DIR, "manifests");
const RESULTS_DIR = path.join(OUT_DIR, "analysis", "results");
const LOGS_DIR = path.join(OUT_DIR, "analysis", "logs");

[RESULTS_DIR, LOGS_DIR].forEach((dir) => fs.mkdirSync(dir, { recursive: true }));

function parseArg(name, fallback) {
  const idx = process.argv.indexOf(name);
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  return fallback;
}

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

function getManifestFiles() {
  if (!fs.existsSync(MANIFEST_DIR)) return [];
  return fs
    .readdirSync(MANIFEST_DIR)
    .filter((f) => f.toLowerCase().endsWith(".json"))
    .map((f) => path.join(MANIFEST_DIR, f));
}

function getExpectedAnalysisPath(manifest) {
  const base = safeFileName(manifest.input_url || manifest.final_url || "unknown");
  return path.join(RESULTS_DIR, `${base}.analysis.json`);
}

function shouldProcessManifest(manifestPath, skipExisting = true) {
  let manifest;
  try {
    manifest = readJson(manifestPath);
  } catch {
    return { ok: false, reason: "invalid_json" };
  }

  if (manifest.capture_status !== "success") {
    return { ok: false, reason: "capture_not_success" };
  }

  const expectedOut = getExpectedAnalysisPath(manifest);
  if (skipExisting && fs.existsSync(expectedOut)) {
    return { ok: false, reason: "analysis_exists" };
  }

  return { ok: true, manifest };
}

function runAnalyzeManifest(manifestPath) {
  return new Promise((resolve) => {
    const scriptPath = path.join(__dirname, "analyze-manifest.js");

    const child = spawn(process.execPath, [scriptPath, manifestPath], {
      cwd: path.join(__dirname, ".."),
      stdio: ["ignore", "pipe", "pipe"]
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    child.on("close", (code) => {
      resolve({
        code,
        stdout: stdout.trim(),
        stderr: stderr.trim()
      });
    });

    child.on("error", (err) => {
      resolve({
        code: -1,
        stdout: "",
        stderr: err?.message || String(err)
      });
    });
  });
}

async function main() {
  const concurrency = Math.max(1, Number(parseArg("--concurrency", "1")) || 1);
  const skipExisting = parseArg("--skip-existing", "true") !== "false";
  const limit = Number(parseArg("--limit", "0")) || 0;

  const allManifests = getManifestFiles();

  const queue = [];
  let skippedExists = 0;
  let skippedFailedCapture = 0;
  let skippedInvalid = 0;

  for (const mf of allManifests) {
    const check = shouldProcessManifest(mf, skipExisting);
    if (!check.ok) {
      if (check.reason === "analysis_exists") skippedExists++;
      else if (check.reason === "capture_not_success") skippedFailedCapture++;
      else skippedInvalid++;
      continue;
    }
    queue.push(mf);
  }

  const finalQueue = limit > 0 ? queue.slice(0, limit) : queue;

  console.log(`Total manifests: ${allManifests.length}`);
  console.log(`Ready to analyze: ${finalQueue.length}`);
  console.log(`Skipped (existing): ${skippedExists}`);
  console.log(`Skipped (failed capture): ${skippedFailedCapture}`);
  console.log(`Skipped (invalid json): ${skippedInvalid}`);
  console.log(`Concurrency: ${concurrency}`);

  const logPath = path.join(
    LOGS_DIR,
    `batch_analyze_${new Date().toISOString().replace(/[:.]/g, "-")}.log`
  );

  let nextIndex = 0;
  let successCount = 0;
  let failCount = 0;

  async function worker(workerId) {
    while (true) {
      const current = nextIndex++;
      if (current >= finalQueue.length) return;

      const manifestPath = finalQueue[current];
      const label = path.basename(manifestPath);

      console.log(`\n[${current + 1}/${finalQueue.length}] Worker ${workerId} -> ${label}`);

      const started = Date.now();
      const result = await runAnalyzeManifest(manifestPath);
      const ms = Date.now() - started;

      const ok = result.code === 0;
      if (ok) {
        successCount++;
        console.log(`[${current + 1}/${finalQueue.length}] OK in ${ms}ms`);
      } else {
        failCount++;
        console.log(`[${current + 1}/${finalQueue.length}] FAIL in ${ms}ms`);
      }

      const logObj = {
        workerId,
        index: current + 1,
        total: finalQueue.length,
        manifest: manifestPath,
        exit_code: result.code,
        ok,
        ms,
        stdout: result.stdout,
        stderr: result.stderr,
        timestamp: new Date().toISOString()
      };

      fs.appendFileSync(logPath, JSON.stringify(logObj) + "\n", "utf8");
    }
  }

  const workers = Array.from({ length: concurrency }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  console.log("\n=== Batch Analyze Finished ===");
  console.log(`Success: ${successCount}`);
  console.log(`Failed:  ${failCount}`);
  console.log(`Log file: ${logPath}`);
  console.log(`Results dir: ${RESULTS_DIR}`);
}

main().catch((err) => {
  console.error("Fatal batch analyze error:", err);
  process.exit(1);
});