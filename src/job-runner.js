require("dotenv").config();

const { Redis } = require("@upstash/redis");
const redis = Redis.fromEnv();

const pullSheetUrls = require("./pull-sheet-urls");
const pushSheetResult = require("./push-sheet-results");
const { deleteSheetRow } = pushSheetResult;
const { runAnalysis, runScoreOnlyAnalysis } = require("./analyze-manifest");
const { captureWebsite } = require("./capture");

/* =========================
   HELPERS
========================== */

function nowIso() {
  return new Date().toISOString();
}

/**
 * Accepts any legacy/new screenshotMode values and normalizes to:
 *   "top" | "full" | "sections"
 *
 * - "recommended" (your old 3-screenshot mode) => "sections"
 * - "full" or "fullpage" => "full"
 * - "cheap" => "top"
 */
function normalizeScreenshotMode(value) {
  const v = String(value || "").trim().toLowerCase();

  if (v === "top" || v === "hero" || v === "cheap") return "top";
  if (v === "full" || v === "fullpage" || v === "page") return "full";
  if (v === "sections" || v === "recommended" || v === "precision" || v === "3") return "sections";

  // Safe default
  return "full";
}

function normalizeLowScoreAction(value) {
  const v = String(value || "").toLowerCase();
  if (v === "skip" || v === "tag" || v === "delete") return v;
  return "skip";
}

function normalizeUnreachableAction(value) {
  const v = String(value || "").toLowerCase();
  if (v === "skip" || v === "tag" || v === "fallback") return v;
  return "skip";
}

function getAnalysisConfig(meta) {
  const a = meta?.analysis || {};

  return {
    screenshotMode: normalizeScreenshotMode(a.screenshotMode),

    concurrency: Math.max(1, Number(a.concurrency || 1)),
    maxBatchSize: Math.max(1, Number(a.maxBatchSize || 100)),

    minScore: Math.max(1, Math.min(10, Number(a.minScore ?? 7))),
    lowScoreAction: normalizeLowScoreAction(a.lowScoreAction),
    unreachableAction: normalizeUnreachableAction(a.unreachableAction),
    fallbackPrompt: String(a.fallbackPrompt || ""),
  };
}

function applyScreenshotEnv(mode) {
  process.env.SCREENSHOT_MODE = mode;
  process.env.SCREENSHOT_STRATEGY = mode;
  process.env.SIDESONE_SCREENSHOT_MODE = mode;
}

async function persistMeta(jobId, meta) {
  meta.updatedAt = nowIso();
  await redis.set(`job:${jobId}:meta`, meta);
}

async function persistProgressMeta(jobId, meta) {
  const current = await redis.get(`job:${jobId}:meta`);

  meta.updatedAt = nowIso();

  await redis.set(`job:${jobId}:meta`, {
    ...meta,
    status: current?.status || meta.status,
  });
}

async function pushRedisResult(jobId, payload) {
  await redis.rpush(`job:${jobId}:results`, JSON.stringify(payload));
}

// Serialize meta updates even if concurrency > 1
function createMetaWriteQueue(jobId, meta) {
  let chain = Promise.resolve();

  return function queueUpdate(mutator) {
    chain = chain.then(async () => {
      mutator();
      await persistProgressMeta(jobId, meta);
    });
    return chain;
  };
}

async function writeSheet(meta, row, text) {
  await pushSheetResult({
    sheetId: meta.sheetId,
    sheetTab: meta.sheetTab,
    rowIndex: row.rowIndex,
    comment: text,
    column: meta.outputColumn || "O",
  });
}

/**
 * ✅ PRESET SUPPORT:
 * Reads system:config and resolves the prompt text for the current job.
 * Priority:
 *   1) meta.presetId
 *   2) system defaultPresetId
 * If none found, returns "" and analyzer falls back to buildAnalyzerPrompt().
 */
async function resolvePresetPrompt(meta) {
  try {
    const cfg = await redis.get("system:config");
    const presets = Array.isArray(cfg?.presets) ? cfg.presets : [];
    const chosenId = meta?.presetId || cfg?.defaultPresetId || null;

    if (!chosenId) return "";

    const p = presets.find((x) => String(x?.id) === String(chosenId));
    return String(p?.prompt || "").trim();
  } catch (_) {
    return "";
  }
}

/* =========================
   PROCESS JOB
========================== */

async function processJob(jobId) {
  console.log("Processing job:", jobId);

  let meta = await redis.get(`job:${jobId}:meta`);
  console.log("JOB META RECEIVED:", meta);

  if (!meta) return;

  // Reset state
  meta.status = "running";
  meta.analyzed = 0;
  meta.failed = 0;
  meta.updatedAt = nowIso();

  await redis.del(`job:${jobId}:results`);
  await redis.set(`job:${jobId}:meta`, meta);

  const cfg = getAnalysisConfig(meta);
  const queueMetaUpdate = createMetaWriteQueue(jobId, meta);


  console.log("CONFIG USED:", cfg);

  // Apply screenshot mode for capture layer
  applyScreenshotEnv(cfg.screenshotMode);

  // ✅ Load preset prompt ONCE per job (used for all qualified sites)
  const presetPrompt = await resolvePresetPrompt(meta);

  try {
    /* =========================
       SINGLE MODE
    ========================== */
    if (meta.type === "single" && meta.siteUrl) {
      console.log("Single site:", meta.siteUrl);

      // 1) Capture
      applyScreenshotEnv(cfg.screenshotMode);
      await captureWebsite(meta.siteUrl);

      // 2) Score-only
      const scoreResult = await runScoreOnlyAnalysis(meta.siteUrl, "no", "openai");
      console.log("Score result (single):", scoreResult);

      const reasonText = String(scoreResult?.reason || "").toLowerCase();
const looksBlocked =
  reasonText.includes("access denied") ||
  reasonText.includes("forbidden") ||
  reasonText.includes("blocked") ||
  reasonText.includes("not authorized") ||
  reasonText.includes("browser error") ||
  reasonText.includes("parked") ||
  reasonText.includes("domain for sale") ||
  reasonText.includes("coming soon") ||
  reasonText.includes("under construction");

if (looksBlocked) {
  scoreResult.reachable = false;
  scoreResult.score = 0;
}

      const scoreValue = scoreResult?.score ?? null; // keep original
      const score = parseInt(scoreValue, 10) || 0;       // numeric for comparisons

      console.log("QUALIFICATION CHECK:", score, cfg.minScore);

      // Persist score on meta so UI/history can show it even if we skip
      meta.siteReachable = !!scoreResult?.reachable;
      meta.siteScore = meta.siteReachable && typeof scoreValue === "number" ? scoreValue : null;
      await persistMeta(jobId, meta);

      // Unreachable handling
      if (!scoreResult?.reachable) {
        const action = cfg.unreachableAction;

        let out = "UNREACHABLE";
        let status = "unreachable";

        if (action === "fallback") {
          out = cfg.fallbackPrompt || "It looks like your website may currently be unavailable.";
          status = "fallback";
        }

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: out,
          status,
          score: scoreValue,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.failed = 1;
        meta.status = "completed";
        await persistMeta(jobId, meta);

        console.log("Single job completed (unreachable):", jobId);
        return;
      }

      // Too-good site handling (score >= minScore => do NOT run outreach)
// Too-good site handling (score >= minScore => do NOT run outreach)
// Too-good site handling (score >= minScore => do NOT run outreach)
if (score >= cfg.minScore) {
  const action = cfg.lowScoreAction;

  let out = `This website scored ${score}/10 and was marked as GOOD_SITE because it is above your current outreach threshold. You can edit this in Qualification rules in Settings.`;
  let status = "good_site";

  if (action === "skip") {
    out = `This website scored ${score}/10 and was skipped because it is above your current outreach threshold. You can edit this in Qualification rules in Settings.`;
    status = "skipped";
  }

  if (action === "delete") {
    out = `This website scored ${score}/10 and was removed because it is above your current outreach threshold. You can edit this in Qualification rules in Settings.`;
    status = "cleared";
  }

  await pushRedisResult(jobId, {
    url: meta.siteUrl,
    comment: out,
    status,
    score: scoreValue,
    createdAt: nowIso(),
  });

  meta.total = 1;
  meta.analyzed = 1;
  meta.status = "completed";
  await persistMeta(jobId, meta);

  console.log("Single job completed (too good / high score):", jobId);
  return;
}

      // 3) Full AI analysis (only for qualified sites) ✅ uses presetPrompt
          // 3) Full AI analysis (only for qualified sites)
      console.log("Qualified single site, starting full AI analysis...");
      console.log("Using presetId:", meta.presetId);
      console.log("Resolved preset prompt length:", String(presetPrompt || "").length);

      const analysisResult = await runAnalysis(meta.siteUrl, "no", "openai", presetPrompt);

      console.log("Full analysis result received:", JSON.stringify(analysisResult?.analysis || null, null, 2));

      const comment =
        analysisResult?.analysis?.comment_no ||
        analysisResult?.analysis?.comment ||
        "No comment generated";

      console.log("Final extracted comment:", comment);

      await pushRedisResult(jobId, {
        url: meta.siteUrl,
        comment,
        status: "success",
        score: scoreValue,
        createdAt: nowIso(),
      });

      const checkResults = await redis.lrange(`job:${jobId}:results`, 0, -1);
      console.log("Redis results after push:", checkResults);

      meta.total = 1;
      meta.analyzed = 1;
      meta.status = "completed";
      await persistMeta(jobId, meta);

      console.log("Single job completed:", jobId);
      return;
    }

    /* =========================
       BATCH MODE
    ========================== */
    if (meta.type === "batch") {
      console.log("Batch mode started");
      const rowsToDelete = [];

    let allRows = [];

try {
  allRows = await pullSheetUrls({
    sheetId: meta.sheetId,
    sheetTab: meta.sheetTab,
    urlColumn: meta.urlColumn,
  });
} catch (err) {
  const message =
    err?.message || "No website URLs detected in selected URL column.";

  await pushRedisResult(jobId, {
    url: "Batch setup",
    comment: message,
    status: "failed",
    score: null,
    createdAt: nowIso(),
  });

  meta.total = 0;
  meta.analyzed = 0;
  meta.failed = 0;
  meta.status = "completed";
  meta.error = message;

  await persistMeta(jobId, meta);

  console.log("Batch setup failed:", message);
  return;
}

// Apply maxBatchSize from settings
const rows = allRows.slice(0, cfg.maxBatchSize);

meta.total = rows.length;
await persistMeta(jobId, meta);

      console.log(`Progress: ${meta.analyzed}/${meta.total}`);

      const concurrency = cfg.concurrency;

      for (let start = 0; start < rows.length; start += concurrency) {

  let freshMeta = await redis.get(`job:${jobId}:meta`);

  // STOP
  if (freshMeta?.status === "stopped") {
    console.log("Job stopped:", jobId);
    return;
  }

  // PAUSE
  while (freshMeta?.status === "paused") {
    console.log("Job paused:", jobId);

    await new Promise((r) => setTimeout(r, 2000));

    freshMeta = await redis.get(`job:${jobId}:meta`);

    if (freshMeta?.status === "stopped") {
      console.log("Job stopped while paused:", jobId);
      return;
    }
  }

        const chunk = rows.slice(start, start + concurrency);

        await Promise.all(
          chunk.map(async (row, idx) => {
            const rowNumber = start + idx + 1;

            try {
              console.log(`Processing row ${rowNumber}/${rows.length}:`, row.url);

              // 1) Capture
              applyScreenshotEnv(cfg.screenshotMode);
              await captureWebsite(row.url);

              // 2) Score-only
              // 2) Score-only
const scoreResult = await runScoreOnlyAnalysis(row.url, "no", "openai");
console.log("Score result:", row.url, scoreResult);

const reasonText = String(scoreResult?.reason || "").toLowerCase();
const looksBlocked =
  reasonText.includes("access denied") ||
  reasonText.includes("forbidden") ||
  reasonText.includes("blocked") ||
  reasonText.includes("not authorized") ||
  reasonText.includes("browser error") ||
  reasonText.includes("parked") ||
  reasonText.includes("domain for sale") ||
  reasonText.includes("coming soon") ||
  reasonText.includes("under construction");

if (looksBlocked) {
  scoreResult.reachable = false;
  scoreResult.score = 0;
}

const scoreValue = scoreResult?.score ?? null;
const score = parseInt(scoreValue, 10) || 0;

              // Unreachable
              if (!scoreResult?.reachable) {
                const action = cfg.unreachableAction;

                if (action === "skip") {
                  await pushRedisResult(jobId, {
                    url: row.url,
                    comment: "UNREACHABLE (skipped)",
                    status: "unreachable",
                    score: scoreValue,
                    createdAt: nowIso(),
                  });

                  await queueMetaUpdate(() => {
                    meta.failed += 1;
                  });
                  return;
                }

                if (action === "tag") {
                  await writeSheet(meta, row, "UNREACHABLE");

                  await pushRedisResult(jobId, {
                    url: row.url,
                    comment: "UNREACHABLE",
                    status: "unreachable",
                    score: scoreValue,
                    createdAt: nowIso(),
                  });

                  await queueMetaUpdate(() => {
                    meta.failed += 1;
                  });
                  return;
                }

                if (action === "fallback") {
                  const fallbackText =
                    cfg.fallbackPrompt ||
                    "It looks like your website may currently be unavailable.";

                  await writeSheet(meta, row, fallbackText);

                  await pushRedisResult(jobId, {
                    url: row.url,
                    comment: fallbackText,
                    status: "fallback",
                    score: scoreValue,
                    createdAt: nowIso(),
                  });

                  await queueMetaUpdate(() => {
                    meta.analyzed += 1;
                  });
                  return;
                }

                await queueMetaUpdate(() => {
                  meta.failed += 1;
                });
                return;
              }

              // Too-good site (score > minScore)
             // Too-good site (score >= minScore)
// Too-good site (score >= minScore)
// Too-good site (score >= minScore)
if (score >= cfg.minScore) {
  const action = cfg.lowScoreAction;

  if (action === "skip") {
    await writeSheet(meta, row, "");

    await pushRedisResult(jobId, {
      url: row.url,
      comment: `This website scored ${score}/10 and was skipped because it is above your outreach threshold.`,
      status: "skipped",
      score: scoreValue,
      createdAt: nowIso(),
    });

    await queueMetaUpdate(() => {
      meta.analyzed += 1;
    });
    return;
  }

  if (action === "tag") {
    const text = `This website scored ${score}/10 and was marked as GOOD_SITE based on your current settings.`;

    await writeSheet(meta, row, `GOOD_SITE (${score}/10)`);

    await pushRedisResult(jobId, {
      url: row.url,
      comment: text,
      status: "good_site",
      score: scoreValue,
      createdAt: nowIso(),
    });

    await queueMetaUpdate(() => {
      meta.analyzed += 1;
    });
    return;
  }

  if (action === "delete") {
    rowsToDelete.push(row.rowIndex);

    await pushRedisResult(jobId, {
      url: row.url,
      comment: `This website scored ${score}/10 and was removed because it is above your outreach threshold.`,
      status: "cleared",
      score: scoreValue,
      createdAt: nowIso(),
    });

    await queueMetaUpdate(() => {
      meta.analyzed += 1;
    });
    return;
  }

  await queueMetaUpdate(() => {
    meta.analyzed += 1;
  });
  return;
}
              // 3) Full AI analysis (only for qualified) ✅ uses presetPrompt
              const analysisResult = await runAnalysis(row.url, "no", "openai", presetPrompt);

              const comment =
                analysisResult?.analysis?.comment_no ||
                analysisResult?.analysis?.comment ||
                "No comment generated";

              // Write to sheet
              await writeSheet(meta, row, comment);

              // Save to Redis results
              await pushRedisResult(jobId, {
                url: row.url,
                comment,
                status: "success",
                score: scoreValue,
                createdAt: nowIso(),
              });

              await queueMetaUpdate(() => {
                meta.analyzed += 1;
              });
            } catch (err) {
              console.error("Row failed:", row.url, err);

              try {
                await writeSheet(meta, row, "FAILED");
              } catch (_) {}

              await pushRedisResult(jobId, {
                url: row.url,
                comment: "FAILED",
                status: "failed",
                score: null,
                createdAt: nowIso(),
              });

              await queueMetaUpdate(() => {
                meta.failed += 1;
              });
            }
          })
        );
      }

      if (rowsToDelete.length > 0) {
  const uniqueSortedRows = [...new Set(rowsToDelete)].sort((a, b) => b - a);

  for (const rowIndex of uniqueSortedRows) {
    try {
      await deleteSheetRow({
        sheetId: meta.sheetId,
        sheetTab: meta.sheetTab,
        rowIndex,
      });
    } catch (err) {
      console.error("Failed to delete row:", rowIndex, err);
    }
  }
}

meta.status = "completed";
await persistMeta(jobId, meta);

console.log("Batch completed:", jobId);
    }
  } catch (err) {
    console.error("Job crashed:", err);

    // If we crash in single before pushing any result, push a failure result so UI doesn't look stuck.
    try {
      if (meta?.type === "single" && meta?.siteUrl) {
        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: "FAILED",
          status: "failed",
          score: meta.siteScore ?? null,
          createdAt: nowIso(),
        });
      }
    } catch (_) {}

      if (meta?.type === "single") {
      meta.failed = 1;
      meta.total = 1;
    }

    meta.status = "completed";
    await persistMeta(jobId, meta);
  }

  console.log("JOB META:", meta);
}

/* =========================
   QUEUE LISTENER
========================== */

async function runQueue() {
  console.log("Worker listening for jobs...");

  while (true) {
    try {
      const jobId = await redis.rpop("jobs:queue");

      if (jobId) {
        await processJob(jobId);
      } else {
        await new Promise((r) => setTimeout(r, 1000));
      }
    } catch (err) {
      console.error("Queue error:", err);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

runQueue();