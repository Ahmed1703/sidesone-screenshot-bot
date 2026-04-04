require("dotenv").config();

const { Redis } = require("@upstash/redis");
const redis = Redis.fromEnv();

const pullCsvRows = require("./pull-csv-rows");
const pullSheetUrls = require("./pull-sheet-urls");
const pushSheetResult = require("./push-sheet-results");
const { deleteSheetRow } = pushSheetResult;
const { runAnalysis, runScoreOnlyAnalysis } = require("./analyze-manifest");
const { captureWebsite } = require("./capture");
const { buildAnalyzerPrompt } = require("./analyzer-prompt");
const {
  normalizeEmailAddress,
  verifyEmailWithBouncer,
  verifyEmailsWithBouncerBatch,
  getBouncerCredits,
} = require("./bouncer-verifier");

const APP_BASE_URL = String(process.env.APP_BASE_URL || "").replace(/\/+$/, "");
const INTERNAL_WORKER_SECRET = String(process.env.INTERNAL_WORKER_SECRET || "");

function normalizeUrlForCreditKey(url) {
  return String(url || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/\/+$/, "");
}

function isChargeableOutcome(status) {
  return (
    status === "success" ||
    status === "skipped" ||
    status === "good_site" ||
    status === "cleared"
  );
}

async function consumeAnalysisCredit({
  appUserId,
  jobId,
  siteUrl,
  rowNumber = null,
  status,
  score = null,
  pageType = null,
}) {
  if (!isChargeableOutcome(status)) {
    return { charged: false };
  }

  if (!appUserId) {
    throw new Error("Missing appUserId for credit deduction.");
  }

  if (!APP_BASE_URL || !INTERNAL_WORKER_SECRET) {
    throw new Error("Missing APP_BASE_URL or INTERNAL_WORKER_SECRET.");
  }

  const referenceKey =
    rowNumber === null
      ? `analysis:${jobId}:single`
      : `analysis:${jobId}:row:${rowNumber}:${normalizeUrlForCreditKey(siteUrl)}`;

  const res = await fetch(`${APP_BASE_URL}/api/internal/consume-credit`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-internal-worker-secret": INTERNAL_WORKER_SECRET,
    },
    body: JSON.stringify({
      userId: String(appUserId),
      amount: 1,
      referenceKey,
      reason: "Website analysis completed",
      metadata: {
        jobId,
        rowNumber,
        siteUrl,
        status,
        score,
        pageType,
      },
    }),
  });

  const data = await res.json().catch(() => null);

  if (!res.ok) {
    throw new Error(data?.error || "Credit deduction failed.");
  }

  return {
    charged: true,
    data,
  };
}

async function consumeVerificationCredit({
  appUserId,
  jobId,
  quantity,
  mode,
  batchId = null,
}) {
  const checkedCount = Number(quantity) || 0;

  if (checkedCount <= 0) {
    return { charged: false };
  }

  if (!appUserId) {
    throw new Error("Missing appUserId for verification credit deduction.");
  }

  if (!APP_BASE_URL || !INTERNAL_WORKER_SECRET) {
    throw new Error("Missing APP_BASE_URL or INTERNAL_WORKER_SECRET.");
  }

  const amount = checkedCount * 0.5;
  const referenceKey =
    mode === "batch"
      ? `verification:${jobId}:batch:${batchId || "default"}`
      : `verification:${jobId}:single`;

  const res = await fetch(`${APP_BASE_URL}/api/internal/consume-credit`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-internal-worker-secret": INTERNAL_WORKER_SECRET,
    },
    body: JSON.stringify({
      userId: String(appUserId),
      amount,
      referenceKey,
      reason: "Email verification completed",
      metadata: {
        jobId,
        quantity: checkedCount,
        mode,
        batchId,
        unitPrice: 0.5,
      },
    }),
  });

  const data = await res.json().catch(() => null);

  if (!res.ok) {
    throw new Error(data?.error || "Verification credit deduction failed.");
  }

  return {
    charged: true,
    data,
    amount,
  };
}

/* =========================
   TIMEOUT / RETRY CONFIG
========================== */

const STEP_TIMEOUTS = {
  pullRowsMs: Number(process.env.WORKER_PULL_ROWS_TIMEOUT_MS) || 45000,
  captureMs: Number(process.env.WORKER_CAPTURE_TIMEOUT_MS) || 45000,
  scoreMs: Number(process.env.WORKER_SCORE_TIMEOUT_MS) || 45000,
  analysisMs: Number(process.env.WORKER_ANALYSIS_TIMEOUT_MS) || 70000,
  writeSheetMs: Number(process.env.WORKER_WRITE_SHEET_TIMEOUT_MS) || 20000,
  deleteRowMs: Number(process.env.WORKER_DELETE_ROW_TIMEOUT_MS) || 20000,
};

const STEP_RETRIES = {
  pullRows: Number(process.env.WORKER_PULL_ROWS_RETRIES) || 1,
  capture: Number(process.env.WORKER_CAPTURE_RETRIES) || 2,
  score: Number(process.env.WORKER_SCORE_RETRIES) || 2,
  analysis: Number(process.env.WORKER_ANALYSIS_RETRIES) || 2,
  writeSheet: Number(process.env.WORKER_WRITE_SHEET_RETRIES) || 2,
  deleteRow: Number(process.env.WORKER_DELETE_ROW_RETRIES) || 2,
};

const STEP_RETRY_DELAY_MS =
  Number(process.env.WORKER_RETRY_DELAY_MS) || 1200;

/* =========================
   HELPERS
========================== */

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeNumericScore(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeText(value) {
  return String(value || "").trim().toLowerCase();
}

function clampInt(value, min, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.floor(n));
}

function clampScore(value, fallback = 7) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(10, Math.max(1, Math.floor(n)));
}

function normalizeScreenshotMode(value) {
  const v = String(value || "").trim().toLowerCase();

  if (v === "top" || v === "hero" || v === "cheap") return "top";
  if (v === "full" || v === "fullpage" || v === "page") return "full";
  if (
    v === "sections" ||
    v === "recommended" ||
    v === "precision" ||
    v === "3"
  ) {
    return "sections";
  }

  return "sections";
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

function normalizeLanguage(value) {
  const v = String(value || "").trim().toLowerCase();

  if (v === "en" || v === "english") return "en";
  if (
    v === "no" ||
    v === "norwegian" ||
    v === "bokmal" ||
    v === "bokmål" ||
    v === "norsk" ||
    v === "nb" ||
    v === "nn"
  ) {
    return "no";
  }

  return "no";
}

function normalizeTone(value) {
  const v = String(value || "").toLowerCase();
  if (
    v === "professional" ||
    v === "friendly" ||
    v === "direct" ||
    v === "sales" ||
    v === "soft"
  ) {
    return v;
  }
  return "professional";
}

function normalizeOutputLength(value) {
  const v = String(value || "").toLowerCase();
  if (
    v === "one_sentence" ||
    v === "two_sentences" ||
    v === "short_paragraph" ||
    v === "medium_paragraph"
  ) {
    return v;
  }
  return "short_paragraph";
}

function safeString(value, max = 5000) {
  return String(value || "").slice(0, max);
}

function safeErrorMessage(err, fallback = "Unknown error") {
  if (!err) return fallback;

  if (typeof err === "string") {
    return err.slice(0, 500);
  }

  if (err instanceof Error) {
    return String(err.message || fallback).slice(0, 500);
  }

  return String(err?.message || err || fallback).slice(0, 500);
}

function createTimeoutError(label, ms) {
  const err = new Error(`${label} timed out after ${ms}ms`);
  err.code = "STEP_TIMEOUT";
  err.stepLabel = label;
  err.timeoutMs = ms;
  return err;
}

function withTimeout(work, ms, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(createTimeoutError(label, ms));
    }, ms);

    if (typeof timer.unref === "function") {
      timer.unref();
    }

    Promise.resolve()
      .then(() => (typeof work === "function" ? work() : work))
      .then(
        (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        (err) => {
          clearTimeout(timer);
          reject(err);
        }
      );
  });
}

async function runWithRetry({
  label,
  timeoutMs,
  attempts = 1,
  retryDelayMs = STEP_RETRY_DELAY_MS,
  fn,
}) {
  let lastErr = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await withTimeout(() => fn(attempt), timeoutMs, label);
    } catch (err) {
      lastErr = err;
      console.warn(
        `[${label}] attempt ${attempt}/${attempts} failed:`,
        safeErrorMessage(err)
      );

      if (attempt < attempts) {
        await sleep(retryDelayMs * attempt);
      }
    }
  }

  throw lastErr;
}

function buildBatchResultPayload(row, payload = {}) {
  return {
    url: row?.url || "",
    recipientEmail:
      row?.recipientEmail ||
      row?.email ||
      row?.mail ||
      row?.contactEmail ||
      "",
    firstName: row?.firstName || row?.first_name || "",
    companyName: row?.companyName || row?.company_name || "",
    industry: row?.industry || "",
    location: row?.location || "",
    ...payload,
  };
}

function toBoolean(value) {
  if (typeof value === "boolean") return value;
  const normalized = String(value || "").trim().toLowerCase();
  return (
    normalized === "1" ||
    normalized === "true" ||
    normalized === "yes" ||
    normalized === "enabled"
  );
}

function getVerificationControl(meta) {
  const enabled =
    toBoolean(meta?.verifyEmails) ||
    toBoolean(meta?.verificationEnabled) ||
    toBoolean(meta?.emailVerificationEnabled) ||
    toBoolean(meta?.emailVerification?.enabled) ||
    toBoolean(meta?.verification?.enabled);

  const verificationMode = String(
    meta?.verificationMode ||
      meta?.emailVerification?.mode ||
      meta?.verification?.verificationMode ||
      meta?.verification?.mode ||
      "review"
  )
    .trim()
    .toLowerCase();

  const continueRequested =
    toBoolean(meta?.continueAfterVerification) ||
    toBoolean(meta?.verificationContinue) ||
    toBoolean(meta?.verificationReviewCompleted) ||
    toBoolean(meta?.verification?.reviewCompleted) ||
    toBoolean(meta?.verification?.approved) ||
    toBoolean(meta?.emailVerification?.reviewCompleted);

  const autoExcludeBad =
    toBoolean(meta?.autoExcludeBadEmails) ||
    toBoolean(meta?.removeBadEmails) ||
    toBoolean(meta?.deleteBadEmails) ||
    toBoolean(meta?.verification?.autoExcludeBad) ||
    toBoolean(meta?.emailVerification?.autoExcludeBad);

  return {
    enabled,
    mode: verificationMode,
    continueRequested,
    autoExcludeBad,
  };
}

function isVerificationOnlyBatch(meta, verificationControl = null) {
  const control = verificationControl || getVerificationControl(meta);

  return (
    toBoolean(meta?.verificationOnly) ||
    toBoolean(meta?.verifyOnly) ||
    toBoolean(meta?.verification?.verificationOnly) ||
    toBoolean(meta?.verification?.verifyOnly) ||
    control.mode === "only"
  );
}

function extractSingleVerificationEmail(meta) {
  return (
    meta?.email ||
    meta?.recipientEmail ||
    meta?.mail ||
    meta?.siteEmail ||
    meta?.contactEmail ||
    meta?.emailToVerify ||
    ""
  );
}

function parseExcludedRowToken(token, rowsByEmail) {
  if (token === null || token === undefined) return null;

  if (typeof token === "number" && Number.isFinite(token)) {
    return { rowIndex: token, rowNumber: token };
  }

  if (typeof token === "string") {
    const trimmed = token.trim();
    if (!trimmed) return null;

    if (/^\d+$/.test(trimmed)) {
      const value = Number(trimmed);
      return { rowIndex: value, rowNumber: value };
    }

    const emailKey = trimmed.toLowerCase();
    if (rowsByEmail.has(emailKey)) {
      return { email: emailKey };
    }

    return null;
  }

  if (typeof token === "object") {
    if (Number.isFinite(token.rowIndex)) {
      return { rowIndex: Number(token.rowIndex) };
    }

    if (Number.isFinite(token.rowNumber)) {
      return { rowNumber: Number(token.rowNumber) };
    }

    const emailValue = String(
      token.email || token.normalizedEmail || token.recipientEmail || ""
    )
      .trim()
      .toLowerCase();

    if (emailValue) {
      return { email: emailValue };
    }
  }

  return null;
}

function buildExcludedRowMatcher(meta, verificationResults) {
  const rowsByEmail = new Map();
  for (const result of verificationResults || []) {
    const emailKey = String(result?.normalizedEmail || result?.email || "")
      .trim()
      .toLowerCase();

    if (emailKey) {
      rowsByEmail.set(emailKey, result);
    }
  }

  const rawSelections = []
    .concat(meta?.excludedRows || [])
    .concat(meta?.removedRows || [])
    .concat(meta?.excludedRowNumbers || [])
    .concat(meta?.rowsToExclude || [])
    .concat(meta?.verification?.excludedRows || [])
    .concat(meta?.emailVerification?.excludedRows || []);

  const rowIndexes = new Set();
  const rowNumbers = new Set();
  const emails = new Set();

  for (const token of rawSelections) {
    const parsed = parseExcludedRowToken(token, rowsByEmail);
    if (!parsed) continue;
    if (parsed.rowIndex) rowIndexes.add(parsed.rowIndex);
    if (parsed.rowNumber) rowNumbers.add(parsed.rowNumber);
    if (parsed.email) emails.add(parsed.email);
  }

  if (getVerificationControl(meta).autoExcludeBad) {
    for (const result of verificationResults || []) {
      if (result?.status === "invalid") {
        if (Number.isFinite(result?.rowIndex)) rowIndexes.add(result.rowIndex);
        if (Number.isFinite(result?.rowNumber)) rowNumbers.add(result.rowNumber);
        const emailKey = String(result?.normalizedEmail || result?.email || "")
          .trim()
          .toLowerCase();
        if (emailKey) emails.add(emailKey);
      }
    }
  }

  return function isExcluded(row, verificationResult) {
    const emailKey = String(
      verificationResult?.normalizedEmail || row?.recipientEmail || ""
    )
      .trim()
      .toLowerCase();

    return (
      rowIndexes.has(row?.rowIndex) ||
      rowNumbers.has(verificationResult?.rowNumber) ||
      rowNumbers.has(row?.rowIndex) ||
      (emailKey && emails.has(emailKey))
    );
  };
}

function buildVerificationSummary(results, excludedCount = 0) {
  const summary = {
    totalChecked: results.length,
    validCount: 0,
    invalidCount: 0,
    riskyCount: 0,
    badCount: 0,
    excludedCount,
    unknownCount: 0,
  };

  for (const result of results) {
    switch (result?.status) {
      case "valid":
        summary.validCount += 1;
        break;
      case "invalid":
        summary.invalidCount += 1;
        summary.badCount += 1;
        break;
      case "risky":
        summary.riskyCount += 1;
        break;
      default:
        summary.unknownCount += 1;
        break;
    }
  }

  return summary;
}

function normalizeVerificationIdentityEmail(value) {
  return String(value || "").trim();
}

function getRequestedVerificationEntries(meta, rows = []) {
  const metaEntries = Array.isArray(meta?.verification?.entries)
    ? meta.verification.entries
    : Array.isArray(meta?.verificationEntries)
    ? meta.verificationEntries
    : [];

  if (metaEntries.length) {
    return metaEntries;
  }

  return rows;
}

function getVerificationRowEmail(row) {
  return String(
    row?.recipientEmail ||
      row?.email ||
      row?.mail ||
      row?.contactEmail ||
      ""
  ).trim();
}

function hasVerificationResults(results) {
  return Array.isArray(results) && results.length > 0;
}

function isVerificationReviewCompleted(meta, verificationState) {
  return (
    toBoolean(meta?.continueAfterVerification) ||
    toBoolean(meta?.verificationContinue) ||
    toBoolean(meta?.verificationReviewCompleted) ||
    toBoolean(meta?.verification?.reviewCompleted) ||
    toBoolean(meta?.verification?.approved) ||
    toBoolean(meta?.emailVerification?.reviewCompleted) ||
    toBoolean(verificationState?.reviewCompleted) ||
    toBoolean(verificationState?.approved) ||
    String(verificationState?.phase || "").trim().toLowerCase() === "completed"
  );
}

function getPersistedVerificationState(meta, priorVerificationState) {
  if (
    priorVerificationState &&
    (String(priorVerificationState?.phase || "").trim() ||
      hasVerificationResults(priorVerificationState?.results))
  ) {
    return priorVerificationState;
  }

  if (
    meta?.verification &&
    (String(meta.verification?.phase || "").trim() ||
      hasVerificationResults(meta.verification?.results))
  ) {
    return meta.verification;
  }

  return null;
}

function shouldRunFreshVerification({
  verificationControl,
  verificationState,
  requestedVerificationRows = [],
}) {
  return (
    !!verificationControl?.enabled &&
    requestedVerificationRows.length > 0 &&
    !hasVerificationResults(verificationState?.results)
  );
}

function shouldWaitForVerificationReview({
  verificationControl,
  verificationState,
  meta,
}) {
  return (
    !!verificationControl?.enabled &&
    hasVerificationResults(verificationState?.results) &&
    !isVerificationReviewCompleted(meta, verificationState)
  );
}

function shouldResumeAfterVerification({
  verificationControl,
  verificationState,
  meta,
}) {
  return (
    !!verificationControl?.enabled &&
    hasVerificationResults(verificationState?.results) &&
    isVerificationReviewCompleted(meta, verificationState)
  );
}

function buildRequestedVerificationIdentity(row, index = 0) {
  const email = normalizeVerificationIdentityEmail(
    row?.recipientEmail ||
      row?.email ||
      row?.normalizedEmail ||
      row?.mail ||
      row?.contactEmail
  );
  const normalized = normalizeEmailAddress(email);
  const rowNumber = Number.isFinite(row?.rowNumber)
    ? Number(row.rowNumber)
    : Number.isFinite(row?.verificationRowNumber)
    ? Number(row.verificationRowNumber)
    : index + 1;
  const rowIndex = Number.isFinite(row?.rowIndex)
    ? Number(row.rowIndex)
    : null;

  return {
    email,
    normalizedEmail: normalized.normalizedEmail || email,
    rowNumber,
    rowIndex,
  };
}

function buildFallbackStoredVerificationResult(identity, originalResult = {}) {
  const normalized = normalizeEmailAddress(identity?.email || "");
  const status = "unknown";

  return {
    ...originalResult,
    email: identity?.email || String(originalResult?.email || "").trim(),
    normalizedEmail:
      identity?.normalizedEmail ||
      normalized.normalizedEmail ||
      String(originalResult?.normalizedEmail || "").trim(),
    rowNumber: Number.isFinite(identity?.rowNumber) ? identity.rowNumber : null,
    rowIndex: Number.isFinite(identity?.rowIndex) ? identity.rowIndex : null,
    status,
    shouldContinue: true,
    reason:
      String(originalResult?.reason || "").trim() ||
      "Verification result was repaired from the original requested row.",
    provider:
      originalResult?.provider ||
      (originalResult?.bouncer ? "bouncer" : null),
    providerStatus:
      originalResult?.providerStatus ||
      originalResult?.bouncer?.status ||
      "repaired",
    providerCredits:
      originalResult?.providerCredits ??
      originalResult?.credits ??
      null,
    score:
      Number.isFinite(Number(originalResult?.score))
        ? Number(originalResult.score)
        : null,
    toxic: originalResult?.toxic ?? null,
    toxicity:
      Number.isFinite(Number(originalResult?.toxicity))
        ? Number(originalResult.toxicity)
        : null,
    domain:
      originalResult?.domain || normalized.domain || null,
    account:
      originalResult?.account || normalized.localPart || null,
    dns: originalResult?.dns ?? null,
    bouncer: {
      ...(originalResult?.bouncer || {}),
      status:
        originalResult?.bouncer?.status ||
        originalResult?.providerStatus ||
        "repaired",
      reason:
        originalResult?.bouncer?.reason ||
        originalResult?.reason ||
        "Verification result was repaired from the original requested row.",
      retryAfter: originalResult?.bouncer?.retryAfter || null,
    },
  };
}

function repairStoredVerificationResult(result, identity) {
  if (!identity) {
    return result;
  }

  const repaired = buildFallbackStoredVerificationResult(identity, result);
  const status = String(result?.status || repaired.status).trim().toLowerCase();

  return {
    ...repaired,
    email: identity.email || repaired.email,
    normalizedEmail: identity.normalizedEmail || repaired.normalizedEmail,
    rowNumber: repaired.rowNumber,
    rowIndex: repaired.rowIndex,
    status:
      status === "valid" ||
      status === "invalid" ||
      status === "risky" ||
      status === "unknown"
        ? status
        : "unknown",
    shouldContinue:
      typeof result?.shouldContinue === "boolean"
        ? result.shouldContinue
        : (status || "unknown") !== "invalid",
    reason: String(result?.reason || repaired.reason || "unknown"),
    provider: result?.provider || repaired.provider,
    providerStatus: result?.providerStatus || repaired.providerStatus,
    providerCredits:
      result?.providerCredits ??
      result?.credits ??
      repaired.providerCredits,
    score:
      Number.isFinite(Number(result?.score))
        ? Number(result.score)
        : repaired.score,
    toxic: result?.toxic ?? repaired.toxic,
    toxicity:
      Number.isFinite(Number(result?.toxicity))
        ? Number(result.toxicity)
        : repaired.toxicity,
    domain: result?.domain || repaired.domain,
    account: result?.account || repaired.account,
    dns: result?.dns ?? repaired.dns,
    bouncer: {
      ...(repaired.bouncer || {}),
      ...(result?.bouncer || {}),
      status:
        result?.bouncer?.status ||
        result?.providerStatus ||
        repaired.bouncer?.status ||
        null,
      reason:
        result?.bouncer?.reason ||
        result?.reason ||
        repaired.bouncer?.reason ||
        null,
      retryAfter:
        result?.bouncer?.retryAfter ||
        repaired.bouncer?.retryAfter ||
        null,
    },
  };
}

function normalizeStoredVerificationResults(results, requestedRows = []) {
  const identities = requestedRows.map((row, index) =>
    buildRequestedVerificationIdentity(row, index)
  );

  if (!identities.length) {
    return (results || []).map((result, index) =>
      repairStoredVerificationResult(
        result,
        buildRequestedVerificationIdentity(result, index)
      )
    );
  }

  const normalizedResults = Array.isArray(results) ? results : [];
  const usedIndexes = new Set();

  function claimMatch(predicate) {
    const matchIndex = normalizedResults.findIndex(
      (result, index) => !usedIndexes.has(index) && predicate(result)
    );

    if (matchIndex >= 0) {
      usedIndexes.add(matchIndex);
      return normalizedResults[matchIndex];
    }

    return null;
  }

  return identities.map((identity) => {
    const identityEmail = String(identity.email || "").trim().toLowerCase();
    const identityNormalizedEmail = String(identity.normalizedEmail || "")
      .trim()
      .toLowerCase();

    const matchedResult =
      claimMatch(
        (result) =>
          Number.isFinite(identity.rowIndex) &&
          Number(result?.rowIndex) === identity.rowIndex
      ) ||
      claimMatch(
        (result) =>
          Number.isFinite(identity.rowNumber) &&
          Number(result?.rowNumber) === identity.rowNumber
      ) ||
      claimMatch((result) => {
        const resultNormalizedEmail = String(result?.normalizedEmail || "")
          .trim()
          .toLowerCase();
        const resultEmail = String(result?.email || "")
          .trim()
          .toLowerCase();

        return (
          (identityNormalizedEmail &&
            resultNormalizedEmail &&
            resultNormalizedEmail === identityNormalizedEmail) ||
          (identityEmail && resultEmail && resultEmail === identityEmail)
        );
      });

    return repairStoredVerificationResult(matchedResult, identity);
  });
}

async function persistVerificationState(
  jobId,
  meta,
  verificationPayload,
  requestedRows = []
) {
  const normalizedResults = normalizeStoredVerificationResults(
    verificationPayload.results || [],
    requestedRows
  );

  verificationPayload = {
    ...verificationPayload,
    results: normalizedResults,
    summary:
      verificationPayload.summary ||
      buildVerificationSummary(normalizedResults),
  };

  meta.verification = {
    ...(meta.verification || {}),
    ...verificationPayload,
  };

  meta.verificationResults = normalizedResults;
  meta.verificationSummary =
    verificationPayload.summary ||
    buildVerificationSummary(normalizedResults);

  await redis.set(`job:${jobId}:verification`, verificationPayload);
  await persistMeta(jobId, meta);
}

async function runEmailVerificationStage({
  jobId,
  meta,
  rows,
  queueMetaUpdate,
}) {
  const verificationRows = getRequestedVerificationEntries(meta, rows)
    .map((row, index) => ({
      ...row,
      recipientEmail: getVerificationRowEmail(row),
      verificationRowNumber: Number.isFinite(row?.verificationRowNumber)
        ? Number(row.verificationRowNumber)
        : Number.isFinite(row?.rowNumber)
        ? Number(row.rowNumber)
        : index + 1,
    }))
    .filter((row) => !!row.recipientEmail);

  if (verificationRows.length === 0) {
    return {
      stopped: false,
      noEmails: true,
      results: [],
      summary: buildVerificationSummary([]),
      checkedAt: nowIso(),
    };
  }

  await queueMetaUpdate(() => {
    meta.verificationProgress = {
      checked: 0,
      total: verificationRows.length,
    };
  });

  console.log(`[email-verify][${jobId}] Bouncer batch verification requested.`);

  const bouncerCredits = await getBouncerCredits().catch((err) => {
    console.warn(
      `[email-verify][${jobId}] Could not fetch Bouncer credits:`,
      safeErrorMessage(err)
    );
    return null;
  });

  const batchResult = await verifyEmailsWithBouncerBatch(verificationRows, {
    logger: (message) => console.log(`[email-verify][${jobId}] ${message}`),
  });
  const normalizedVerificationResults = normalizeStoredVerificationResults(
    batchResult.results,
    verificationRows
  );

  await queueMetaUpdate(() => {
    meta.verificationProgress = {
      checked: verificationRows.length,
      total: verificationRows.length,
    };
  });

  console.log(
    `[email-verify][${jobId}] Bouncer batch results stored for ${normalizedVerificationResults.length} email(s).`
  );

  const summary = buildVerificationSummary(normalizedVerificationResults);

  return {
    stopped: false,
    noEmails: false,
    results: normalizedVerificationResults,
    summary,
    checkedAt: batchResult.checkedAt,
    provider: "bouncer",
    providerBatch: batchResult.batch,
    providerCredits: bouncerCredits,
  };
}

function normalizePageType(value) {
  const v = String(value || "").trim().toLowerCase();

  if (
    v === "real_site" ||
    v === "placeholder_page" ||
    v === "parking_page" ||
    v === "thin_page" ||
    v === "broken_page" ||
    v === "unreachable" ||
    v === "social_only" ||
    v === "platform_listing" ||
    v === "under_construction" ||
    v === "unclear"
  ) {
    return v;
  }

  return "unclear";
}

function textLooksLikeDeadSite(text) {
  const t = normalizeText(text);

  return (
    t.includes("404") ||
    t.includes("page not found") ||
    t.includes("this site can't be reached") ||
    t.includes("site can't be reached") ||
    t.includes("cannot be reached") ||
    t.includes("can't be reached") ||
    t.includes("err_name_not_resolved") ||
    t.includes("dns_probe_finished_nxdomain") ||
    t.includes("server ip address could not be found") ||
    t.includes("parked") ||
    t.includes("domain for sale") ||
    t.includes("coming soon") ||
    t.includes("under construction")
  );
}

function textLooksLikeBrokenPage(text) {
  const t = normalizeText(text);

  return (
    t.includes("403") ||
    t.includes("forbidden") ||
    t.includes("access denied") ||
    t.includes("not authorized") ||
    t.includes("temporarily unavailable") ||
    t.includes("service unavailable") ||
    t.includes("internal server error") ||
    t.includes("bad gateway") ||
    t.includes("gateway timeout") ||
    t.includes("an error occurred") ||
    t.includes("something went wrong") ||
    t.includes("technical problem") ||
    t.includes("technical problems") ||
    t.includes("maintenance") ||
    t.includes("under maintenance") ||
    t.includes("midlertidig utilgjengelig") ||
    t.includes("midlertidig feil") ||
    t.includes("error page") ||
    t.includes("feilmelding")
  );
}

function browserCaptureWorked(captureResult) {
  if (!captureResult) return false;

  if (captureResult?.reachable === false) return false;
  if (captureResult?.success === false) return false;
  if (captureResult?.ok === false) return false;
  if (captureResult?.error) return false;
  if (captureResult?.capture_error) return false;

  if (
    captureResult?.capture_status &&
    String(captureResult.capture_status).toLowerCase() !== "success"
  ) {
    return false;
  }

  if (
    Array.isArray(captureResult?.image_paths) &&
    captureResult.image_paths.length === 0
  ) {
    return false;
  }

  return true;
}

function classifySiteState(captureResult, scoreResult) {
  const browserWorked = browserCaptureWorked(captureResult);

  const reasonText = normalizeText(scoreResult?.reason);
  const titleText = normalizeText(captureResult?.homepage_title);
  const finalUrlText = normalizeText(captureResult?.final_url);
  const pageType = normalizePageType(scoreResult?.page_type);

  const texts = [reasonText, titleText, finalUrlText];

  const hardDeadPage = texts.some(textLooksLikeDeadSite);
  const brokenPage = texts.some(textLooksLikeBrokenPage);

  if (!browserWorked || hardDeadPage || pageType === "unreachable") {
    return {
      state: "unreachable",
      browserWorked,
      hardDeadPage,
      brokenPage,
      pageType,
    };
  }

  if (pageType === "broken_page" || brokenPage) {
    return {
      state: "broken_page",
      browserWorked,
      hardDeadPage,
      brokenPage,
      pageType,
    };
  }

  if (
    pageType === "placeholder_page" ||
    pageType === "parking_page" ||
    pageType === "under_construction" ||
    pageType === "thin_page"
  ) {
    return {
      state: "placeholder",
      browserWorked,
      hardDeadPage,
      brokenPage,
      pageType,
    };
  }

  return {
    state: "normal",
    browserWorked,
    hardDeadPage,
    brokenPage,
    pageType,
  };
}

function getBrokenPageComment(language) {
  return language === "en"
    ? "Your website appears to be showing an error page or a temporary technical page right now, so it was not analyzed as a normal website."
    : "Nettsiden deres ser ut til å vise en feilside eller en midlertidig teknisk side akkurat nå, så den ble ikke analysert som en vanlig nettside.";
}

function getPlaceholderPageComment(language) {
  return language === "en"
    ? "Your website currently looks more like a temporary or very thin page, so it was not analyzed as a normal website."
    : "Nettsiden deres ser mer ut som en midlertidig eller veldig tynn side akkurat nå, så den ble ikke analysert som en vanlig nettside.";
}

function getDefaultFallbackComment(language) {
  return language === "en"
    ? "Your website does not appear to be properly available right now, so there may be a technical issue at the moment."
    : "Nettsiden deres ser ikke ut til å være ordentlig tilgjengelig akkurat nå, så det kan hende det er noe teknisk feil der nå.";
}

function getTooGoodMessage(language, score, action) {
  if (language === "en") {
    if (action === "skip") {
      return `This website scored ${score}/10 and was skipped because it is above your current outreach threshold.`;
    }
    if (action === "delete") {
      return `This website scored ${score}/10 and was removed because it is above your current outreach threshold.`;
    }
    return `This website scored ${score}/10 and was marked as GOOD_SITE because it is above your current outreach threshold.`;
  }

  if (action === "skip") {
    return `Denne nettsiden fikk ${score}/10 og ble hoppet over fordi den er over outreach-grensen deres.`;
  }
  if (action === "delete") {
    return `Denne nettsiden fikk ${score}/10 og ble fjernet fordi den er over outreach-grensen deres.`;
  }
  return `Denne nettsiden fikk ${score}/10 og ble markert som GOOD_SITE fordi den er over outreach-grensen deres.`;
}

function removeRepeatedPrefix(text, prefix) {
  const value = String(text || "").trim();
  const p = String(prefix || "").trim();

  if (!value || !p) return value;

  const lowerValue = value.toLowerCase();
  const lowerPrefix = p.toLowerCase();

  if (lowerValue.startsWith(lowerPrefix)) {
    return value.slice(p.length).trim();
  }

  return value;
}

function sanitizeGeneratedMiddleSection(text, writing) {
  let value = String(text || "").replace(/\r/g, "").trim();
  if (!value) return value;

  const intro = safeString(writing?.opening, 4000).trim();
  const outro = safeString(writing?.closing, 4000).trim();
  const isEnglish = writing?.language === "en";

  value = removeRepeatedPrefix(value, intro);
  value = removeRepeatedPrefix(value, outro);

  const lines = value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  value = lines.join(" ").replace(/\s+/g, " ").trim();

  const startPatterns = isEnglish
    ? [
        /^hi[\s,!.-]*/i,
        /^hello[\s,!.-]*/i,
        /^hey[\s,!.-]*/i,
        /^i took a quick look[^.?!]*[.?!]?\s*/i,
        /^i took a look[^.?!]*[.?!]?\s*/i,
        /^i checked your website[^.?!]*[.?!]?\s*/i,
        /^i looked at your website[^.?!]*[.?!]?\s*/i,
        /^i noticed that\s*/i,
      ]
    : [
        /^hei[\s,!.-]*/i,
        /^hallo[\s,!.-]*/i,
        /^jeg tok en (rask |kjapp )?titt[^.?!]*[.?!]?\s*/i,
        /^jeg så på nettsiden[^.?!]*[.?!]?\s*/i,
        /^jeg la merke til at\s*/i,
      ];

  let changed = true;
  while (changed) {
    changed = false;
    for (const pattern of startPatterns) {
      const next = value.replace(pattern, "").trim();
      if (next !== value) {
        value = next;
        changed = true;
      }
    }
  }

  const endPatterns = isEnglish
    ? [
        /\s*happy to share[^.?!]*[.?!]?$/i,
        /\s*let me know if you want[^.?!]*[.?!]?$/i,
        /\s*if you want,? i can[^.?!]*[.?!]?$/i,
      ]
    : [
        /\s*jeg kan gjerne vise[^.?!]*[.?!]?$/i,
        /\s*hvis du vil,? kan jeg[^.?!]*[.?!]?$/i,
      ];

  for (const pattern of endPatterns) {
    value = value.replace(pattern, "").trim();
  }

  value = value.replace(/\s+/g, " ").trim();
  return value;
}

function pickBestGeneratedComment(analysisResult) {
  const candidates = [
    analysisResult?.analysis?.comment_no,
    analysisResult?.analysis?.ai_middle,
    analysisResult?.analysis?.comment,
    analysisResult?.analysis?.raw_output_text,
  ];

  for (const candidate of candidates) {
    const value = String(candidate || "").trim();
    if (value) return value;
  }

  return "";
}

function finalizeGeneratedComment(analysisResult, writing, language) {
  const rawBest = pickBestGeneratedComment(analysisResult);
  const sanitized = sanitizeGeneratedMiddleSection(rawBest, writing).trim();

  if (sanitized) return sanitized;
  if (rawBest) return rawBest.trim();

  return getDefaultFallbackComment(language);
}

function buildStoredAnalysisPayload(analysisResult) {
  const analysis = analysisResult?.analysis || {};

  return {
    mode: analysis.mode || null,
    model: analysis.model || null,
    screenshotModeUsed: analysis.screenshotModeUsed || null,
    page_type: analysis.page_type || null,
    confidence:
      typeof analysis.confidence === "number" ? analysis.confidence : null,
    should_generate_comment:
      typeof analysis.should_generate_comment === "boolean"
        ? analysis.should_generate_comment
        : null,
    score: typeof analysis.score === "number" ? analysis.score : null,
    strengths: Array.isArray(analysis.strengths) ? analysis.strengths : [],
    issues: Array.isArray(analysis.issues) ? analysis.issues : [],
    evidence: Array.isArray(analysis.evidence) ? analysis.evidence : [],
    visible_signals: analysis.visible_signals || null,
    reason_short: analysis.reason_short || "",
    raw_analysis_json: analysis.raw_analysis_json || "",
    raw_output_text: analysis.raw_output_text || "",
    ai_middle: analysis.ai_middle || "",
    comment_no: analysis.comment_no || "",
  };
}

function getDefaultSystemConfig() {
  return {
    analysis: {
      screenshotMode: "sections",
      concurrency: 3,
      maxBatchSize: 100,
      minScore: 7,
      lowScoreAction: "skip",
      unreachableAction: "skip",
      fallbackPrompt: "",
    },
    writing: {
      language: "no",
      tone: "professional",
      outputLength: "short_paragraph",
      opening: "",
      closing: "",
    },
  };
}

function normalizeSystemConfig(input) {
  const defaults = getDefaultSystemConfig();

  return {
    analysis: {
      screenshotMode: normalizeScreenshotMode(
        input?.analysis?.screenshotMode ?? defaults.analysis.screenshotMode
      ),
      concurrency: clampInt(
        input?.analysis?.concurrency,
        1,
        defaults.analysis.concurrency
      ),
      maxBatchSize: clampInt(
        input?.analysis?.maxBatchSize,
        1,
        defaults.analysis.maxBatchSize
      ),
      minScore: clampScore(
        input?.analysis?.minScore,
        defaults.analysis.minScore
      ),
      lowScoreAction: normalizeLowScoreAction(
        input?.analysis?.lowScoreAction ?? defaults.analysis.lowScoreAction
      ),
      unreachableAction: normalizeUnreachableAction(
        input?.analysis?.unreachableAction ??
          defaults.analysis.unreachableAction
      ),
      fallbackPrompt:
        normalizeUnreachableAction(
          input?.analysis?.unreachableAction ??
            defaults.analysis.unreachableAction
        ) === "fallback"
          ? safeString(input?.analysis?.fallbackPrompt, 8000)
          : "",
    },
    writing: {
      language: normalizeLanguage(
        input?.writing?.language ?? defaults.writing.language
      ),
      tone: normalizeTone(input?.writing?.tone ?? defaults.writing.tone),
      outputLength: normalizeOutputLength(
        input?.writing?.outputLength ?? defaults.writing.outputLength
      ),
      opening: safeString(input?.writing?.opening, 4000),
      closing: safeString(input?.writing?.closing, 4000),
    },
  };
}

async function loadSystemConfig() {
  try {
    const cfg = await redis.get("system:config");
    return normalizeSystemConfig(cfg || {});
  } catch (err) {
    console.warn("Failed to load system:config, using defaults.");
    return getDefaultSystemConfig();
  }
}

function getAnalysisConfig(meta, systemConfig) {
  const merged = {
    ...(systemConfig?.analysis || {}),
    ...(meta?.analysis || {}),
  };

  return {
    screenshotMode: normalizeScreenshotMode(merged.screenshotMode),
    concurrency: Math.max(1, Number(merged.concurrency || 1)),
    maxBatchSize: Math.max(1, Number(merged.maxBatchSize || 100)),
    minScore: Math.max(1, Math.min(10, Number(merged.minScore ?? 7))),
    lowScoreAction: normalizeLowScoreAction(merged.lowScoreAction),
    unreachableAction: normalizeUnreachableAction(merged.unreachableAction),
    fallbackPrompt: String(merged.fallbackPrompt || ""),
  };
}

function getWritingConfig(meta, systemConfig) {
  const merged = {
    ...(systemConfig?.writing || {}),
    ...(meta?.writing || {}),
  };

  return {
    language: normalizeLanguage(merged.language),
    tone: normalizeTone(merged.tone),
    outputLength: normalizeOutputLength(merged.outputLength),
    opening: safeString(merged.opening, 4000),
    closing: safeString(merged.closing, 4000),
  };
}

function applyScreenshotEnv(mode) {
  process.env.SCREENSHOT_MODE = mode;
  process.env.SCREENSHOT_STRATEGY = mode;
  process.env.SIDESONE_SCREENSHOT_MODE = mode;
}

function clearLiveStep(meta) {
  meta.currentStep = null;
  meta.currentStepLabel = null;
  meta.currentUrl = null;
  meta.currentRowIndex = null;
  meta.currentAttempt = null;
  meta.stepStartedAt = null;
}

function isCsvBatch(meta) {
  if (String(meta?.sourceType || "").toLowerCase() === "csv") return true;
  return !!meta?.csvRawText;
}

function isSheetBatch(meta) {
  if (String(meta?.sourceType || "").toLowerCase() === "google") return true;
  if (!meta?.sourceType && meta?.sheetId) return true;
  return !isCsvBatch(meta) && !!meta?.sheetId;
}

async function persistMeta(jobId, meta) {
  meta.updatedAt = nowIso();
  meta.lastHeartbeatAt = nowIso();
  await redis.set(`job:${jobId}:meta`, meta);
}

async function persistProgressMeta(jobId, meta) {
  const current = await redis.get(`job:${jobId}:meta`);

  meta.updatedAt = nowIso();
  meta.lastHeartbeatAt = nowIso();

  await redis.set(`job:${jobId}:meta`, {
    ...meta,
    status: current?.status || meta.status,
  });
}

async function markStep(jobId, meta, step, label, extras = {}) {
  meta.currentStep = step;
  meta.currentStepLabel = label;
  meta.currentUrl = extras.url || null;
  meta.currentRowIndex =
    Number.isFinite(extras.rowIndex) ? extras.rowIndex : null;
  meta.currentAttempt = extras.attempt || 1;
  meta.stepStartedAt = nowIso();
  meta.lastStepError = null;
  await persistProgressMeta(jobId, meta);
}

async function runStep(jobId, meta, options) {
  const {
    step,
    label,
    timeoutMs,
    attempts = 1,
    url = null,
    rowIndex = null,
    fn,
  } = options;

  let lastErr = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    await markStep(jobId, meta, step, label, {
      url,
      rowIndex,
      attempt,
    });

    try {
      const result = await withTimeout(
        () => fn(attempt),
        timeoutMs,
        `${label}${url ? ` (${url})` : ""}`
      );

      meta.lastSuccessfulStep = step;
      meta.lastStepError = null;
      await persistProgressMeta(jobId, meta);

      return result;
    } catch (err) {
      lastErr = err;
      meta.lastStepError = safeErrorMessage(err);
      await persistProgressMeta(jobId, meta);

      console.warn(
        `[${label}] attempt ${attempt}/${attempts} failed:`,
        url || "",
        safeErrorMessage(err)
      );

      if (attempt < attempts) {
        await sleep(STEP_RETRY_DELAY_MS * attempt);
      }
    }
  }

  throw lastErr;
}

async function pushRedisResult(jobId, payload) {
  await redis.rpush(`job:${jobId}:results`, JSON.stringify(payload));
}

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
  const shouldWriteBackToSheet = meta?.sourceType !== "google";

  if (shouldWriteBackToSheet) {
    try {
      await runWithRetry({
        label: `writeSheet row ${row.rowIndex}`,
        timeoutMs: STEP_TIMEOUTS.writeSheetMs,
        attempts: STEP_RETRIES.writeSheet,
        fn: () =>
          pushSheetResult({
            userId: meta.userId,
            sheetId: meta.sheetId,
            sheetTab: meta.sheetTab,
            rowIndex: row.rowIndex,
            comment: text,
            column: meta.outputColumn || "O",
          }),
      });
    } catch (err) {
      console.error("[writeSheet non-fatal]", err);
    }
  } else {
    console.log(
      "[writeSheet skipped] Google Sheets source now behaves like CSV batch."
    );
  }
}

async function maybeWriteSheet(meta, row, text) {
  if (!isSheetBatch(meta)) return;
  return writeSheet(meta, row, text);
}

async function safeDeleteSheetRow(meta, rowIndex) {
  return runWithRetry({
    label: `deleteSheetRow row ${rowIndex}`,
    timeoutMs: STEP_TIMEOUTS.deleteRowMs,
    attempts: STEP_RETRIES.deleteRow,
    fn: () =>
      deleteSheetRow({
        userId: meta.userId,
        sheetId: meta.sheetId,
        sheetTab: meta.sheetTab,
        rowIndex,
      }),
  });
}

async function waitIfPausedOrStopped(jobId) {
  const freshMeta = await redis.get(`job:${jobId}:meta`);

  if (freshMeta?.status === "stopped") {
    console.log("Job stopped:", jobId);
    return { stopped: true, paused: false, meta: freshMeta };
  }

  if (freshMeta?.status === "paused") {
    console.log("Job paused, yielding back to queue:", jobId);
    return { stopped: false, paused: true, meta: freshMeta };
  }

  return { stopped: false, paused: false, meta: freshMeta };
}

function buildPromptOverrideFromWriting(basePrompt, writing) {
  const isEnglish = writing.language === "en";

  const intro = safeString(writing.opening, 4000).trim();
  const outro = safeString(writing.closing, 4000).trim();

  const defaultIntro = isEnglish
    ? "I took a quick look through your website and noticed that"
    : "Jeg tok en rask titt på nettsiden deres og la merke til at";

  const defaultOutro = isEnglish
    ? "Happy to share a few ideas if helpful."
    : "Jeg kan gjerne vise noen enkle ideer om det er interessant.";

  const toneMap = {
    professional: isEnglish
      ? "Sound professional, calm, credible, and observant."
      : "Vær profesjonell, rolig, troverdig og observant.",
    friendly: isEnglish
      ? "Sound friendly, natural, warm, and easy to read."
      : "Vær vennlig, naturlig, varm og lett å lese.",
    direct: isEnglish
      ? "Sound direct, clear, and confident, but still polite."
      : "Vær direkte, tydelig og trygg, men fortsatt høflig.",
    sales: isEnglish
      ? "Sound commercially sharp, but never pushy, cheesy, or scripted."
      : "Vær kommersielt skarp, men aldri pushy, cheesy eller innøvd.",
    soft: isEnglish
      ? "Sound softer, gentler, and less harsh in the criticism."
      : "Vær mildere, mykere og mindre hard i kritikken.",
  };

  const lengthMap = {
    one_sentence: isEnglish
      ? "Write exactly 1 short sentence."
      : "Skriv nøyaktig 1 kort setning.",
    two_sentences: isEnglish
      ? "Write exactly 2 short sentences."
      : "Skriv nøyaktig 2 korte setninger.",
    short_paragraph: isEnglish
      ? "Write 3 to 4 short sentences."
      : "Skriv 3 til 4 korte setninger.",
    medium_paragraph: isEnglish
      ? "Write 4 to 6 short sentences."
      : "Skriv 4 til 6 korte setninger.",
  };

  return [
    String(basePrompt || "").trim(),
    "",
    isEnglish ? "JOB RULES:" : "JOBBREGLER:",
    toneMap[writing.tone] || toneMap.professional,
    lengthMap[writing.outputLength] || lengthMap.short_paragraph,
    "",
    isEnglish
      ? "Write ONLY the critique section that goes between an already-written intro and an already-written outro."
      : "Skriv KUN selve kommentardelen som skal stå mellom en allerede skrevet åpning og en allerede skrevet avslutning.",
    isEnglish
      ? "Do NOT greet. Do NOT introduce yourself. Do NOT say you looked at the site. Do NOT sign off. Do NOT add a CTA."
      : "Ikke hils. Ikke introduser deg selv. Ikke skriv at du har sett på nettsiden. Ikke signer av. Ikke legg til CTA.",
    isEnglish
      ? "Do NOT restart the email. Do NOT repeat the intro. Do NOT repeat the outro."
      : "Ikke start e-posten på nytt. Ikke gjenta åpningen. Ikke gjenta avslutningen.",
    isEnglish
      ? "Start immediately with the actual critique."
      : "Start direkte med selve kommentaren.",
    isEnglish
      ? "Your output must sound like the middle of an email, not the beginning or the ending."
      : "Outputen må høres ut som midten av en e-post, ikke starten eller slutten.",
    isEnglish
      ? "Never begin with phrases like: I took a quick look, I checked your website, I noticed that, Hi, Hello."
      : "Begynn aldri med fraser som: Jeg tok en titt, Jeg så på nettsiden, Jeg la merke til at, Hei, Hallo.",
    isEnglish
      ? "Avoid vague filler like: the site works, could be more polished, room for improvement, a bit more spacing would help, unless tied to a specific visible element."
      : "Unngå vag fylltekst som: nettsiden fungerer, kunne vært mer polert, rom for forbedring, litt mer luft ville hjulpet, med mindre det knyttes til et konkret synlig element.",
    isEnglish
      ? "Prefer concrete visible details such as contact form length, font readability, menu density, weak button visibility, heavy footer, dark background, cramped sections, isolated image/video block, dense text blocks, or awkward spacing."
      : "Foretrekk konkrete synlige detaljer som lengden på kontaktskjema, lesbarhet i font, tett meny, svak knappesynlighet, tung footer, mørk bakgrunn, trange seksjoner, isolert bilde-/videoblokk, tette tekstblokker eller svak spacing.",
    "",
    isEnglish ? "INTRO ALREADY WRITTEN:" : "ÅPNING SOM ALLEREDE ER SKREVET:",
    intro || defaultIntro,
    "",
    isEnglish ? "OUTRO ALREADY WRITTEN:" : "AVSLUTNING SOM ALLEREDE ER SKREVET:",
    outro || defaultOutro,
    "",
    isEnglish ? "Return plain text only." : "Returner kun ren tekst.",
  ]
    .filter(Boolean)
    .join("\n");
}

function getUnreachableOutcome(cfg, analysisLanguage) {
  if (cfg.unreachableAction === "fallback") {
    return {
      out: cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage),
      status: "fallback",
      sheetValue:
        cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage),
    };
  }

  if (cfg.unreachableAction === "tag") {
    return {
      out: "UNREACHABLE",
      status: "unreachable",
      sheetValue: "UNREACHABLE",
    };
  }

  return {
    out: "UNREACHABLE (skipped)",
    status: "unreachable",
    sheetValue: "",
  };
}

/* =========================
   PROCESS JOB
========================== */

async function processJob(jobId) {
  console.log("Processing job:", jobId);

  let meta = await redis.get(`job:${jobId}:meta`);
  const priorVerificationState = await redis.get(`job:${jobId}:verification`);
  console.log("JOB META RECEIVED:", meta);

  if (!meta) return;

  console.log("CSV META DEBUG:", {
    sourceType: meta?.sourceType,
    csvFileName: meta?.csvFileName,
    csvUrlColumn: meta?.csvUrlColumn,
    csvMailColumn: meta?.csvMailColumn,
    hasCsvRawText: !!meta?.csvRawText,
    fallbackUrlColumn: meta?.urlColumn,
    fallbackMailColumn: meta?.mailColumn,
  });

  const systemConfig = await loadSystemConfig();
  const cfg = getAnalysisConfig(meta, systemConfig);
  const writing = getWritingConfig(meta, systemConfig);

  delete meta.presetId;

  meta.analysis = cfg;
  meta.writing = writing;

  meta.status = "running";
  meta.analyzed = 0;
  meta.failed = 0;
  meta.updatedAt = nowIso();
  meta.startedAt = meta.startedAt || nowIso();
  meta.error = null;
  meta.lastStepError = null;
  meta.verificationProgress = {
    checked: 0,
    total:
      priorVerificationState?.summary?.totalChecked ||
      meta?.verificationSummary?.totalChecked ||
      0,
  };

  if (priorVerificationState?.results?.length) {
    meta.verification = {
      ...(meta.verification || {}),
      ...priorVerificationState,
    };
    meta.verificationResults = priorVerificationState.results;
    meta.verificationSummary =
      priorVerificationState.summary ||
      buildVerificationSummary(priorVerificationState.results || []);
  }

  clearLiveStep(meta);

  await redis.del(`job:${jobId}:results`);
  await redis.set(`job:${jobId}:meta`, meta);

  const queueMetaUpdate = createMetaWriteQueue(jobId, meta);

  console.log("ANALYSIS CONFIG USED:", cfg);
  console.log("WRITING CONFIG USED:", writing);

  applyScreenshotEnv(cfg.screenshotMode);

  const analysisLanguage = writing.language === "en" ? "en" : "no";

  let basePrompt = "";
  try {
    basePrompt = await buildAnalyzerPrompt(analysisLanguage);
  } catch (err) {
    console.warn(
      "Could not load analyzer prompt from Redis:",
      safeErrorMessage(err)
    );
    basePrompt = "";
  }

  const writingPromptOverride = buildPromptOverrideFromWriting(
    basePrompt,
    writing
  );

  try {
    /* =========================
       SINGLE MODE
    ========================== */
    if (meta.type === "single") {
      console.log("Single job:", meta.siteUrl || "[verification only]");

      let gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped || gate.paused) return;

      const verificationControl = getVerificationControl(meta);
      const singleVerificationEmail = extractSingleVerificationEmail(meta);

      if (verificationControl.enabled && singleVerificationEmail) {
        const existingResult =
          meta?.verificationResults?.[0] || priorVerificationState?.results?.[0];

        let verificationResult = existingResult
          ? normalizeStoredVerificationResults(
              [existingResult],
              [
                {
                  recipientEmail: singleVerificationEmail,
                  verificationRowNumber: 1,
                  rowIndex: 1,
                },
              ]
            )[0]
          : existingResult;

        if (!verificationResult) {
          console.log(`[email-verify][${jobId}] Bouncer single verification started.`);

          const bouncerCredits = await getBouncerCredits().catch((err) => {
            console.warn(
              `[email-verify][${jobId}] Could not fetch Bouncer credits:`,
              safeErrorMessage(err)
            );
            return null;
          });

          verificationResult = await runStep(jobId, meta, {
            step: "verify_email",
            label: "Verify single email with Bouncer",
            timeoutMs: Number(process.env.BOUNCER_REQUEST_TIMEOUT_MS) || 30000,
            attempts: 1,
            url: meta.siteUrl,
            fn: async () =>
              verifyEmailWithBouncer(singleVerificationEmail, {
                rowNumber: 1,
                rowIndex: 1,
              }),
          });

          const verificationPayload = {
            enabled: true,
            phase: "review_required",
            type: "single",
            provider: "bouncer",
            emailColumn:
              meta.emailColumn ||
              meta.mailColumn ||
              meta.csvMailColumn ||
              null,
            checkedAt: verificationResult.checkedAt,
            providerCredits: bouncerCredits,
            summary: buildVerificationSummary([verificationResult]),
            results: [verificationResult],
          };

          await persistVerificationState(
            jobId,
            meta,
            verificationPayload,
            [
              {
                recipientEmail: singleVerificationEmail,
                verificationRowNumber: 1,
                rowIndex: 1,
              },
            ]
          );
          verificationResult = normalizeStoredVerificationResults(
            [verificationResult],
            [
              {
                recipientEmail: singleVerificationEmail,
                verificationRowNumber: 1,
                rowIndex: 1,
              },
            ]
          )[0];

          if (!meta.verificationCreditsCharged) {
            const creditResult = await consumeVerificationCredit({
              appUserId: meta.appUserId,
              jobId,
              quantity: 1,
              mode: "single",
            });

            console.log(
              `[email-verify][${jobId}] Swokei verification credits consumed: ${creditResult.amount}.`
            );
            meta.verificationCreditsCharged = true;
            await persistMeta(jobId, meta);
          }
        }

        if (!verificationControl.continueRequested) {
          console.log(`[email-verify][${jobId}] Verification review required.`);
          await pushRedisResult(jobId, {
            type: "email_verification",
            status: "verification_review_required",
            verification: {
              type: "single",
              provider: "bouncer",
              email: verificationResult.email,
              result: verificationResult,
              summary: buildVerificationSummary([verificationResult]),
            },
            createdAt: nowIso(),
          });

          meta.total = 1;
          meta.status = "awaiting_verification_review";
          meta.awaitingUserAction = "email_verification_review";
          clearLiveStep(meta);
          await persistMeta(jobId, meta);
          return;
        }

        console.log(`[email-verify][${jobId}] Resume after review.`);
        await persistVerificationState(jobId, meta, {
          ...(priorVerificationState || meta.verification || {}),
          enabled: true,
          phase: "completed",
          type: "single",
          reviewCompletedAt: nowIso(),
          summary: buildVerificationSummary([verificationResult]),
          results: [verificationResult],
        }, [
          {
            recipientEmail: singleVerificationEmail,
            verificationRowNumber: 1,
            rowIndex: 1,
          },
        ]);

        if (!meta.siteUrl) {
          await pushRedisResult(jobId, {
            type: "email_verification",
            status: verificationResult.status,
            verification: {
              type: "single",
              provider: "bouncer",
              email: verificationResult.email,
              result: verificationResult,
              summary: buildVerificationSummary([verificationResult]),
            },
            createdAt: nowIso(),
          });

          meta.total = 1;
          meta.analyzed = 1;
          meta.status = "completed";
          clearLiveStep(meta);
          await persistMeta(jobId, meta);
          return;
        }
      }

      if (!meta.siteUrl) {
        throw new Error("Single analysis job is missing siteUrl.");
      }

      let captureResult = null;
      try {
        captureResult = await runStep(jobId, meta, {
          step: "capturing",
          label: "Capture website",
          timeoutMs: STEP_TIMEOUTS.captureMs,
          attempts: STEP_RETRIES.capture,
          url: meta.siteUrl,
          fn: async () => {
            applyScreenshotEnv(cfg.screenshotMode);
            return captureWebsite(meta.siteUrl);
          },
        });
      } catch (err) {
        const errorMessage = safeErrorMessage(err);
        console.error("Single capture failed:", meta.siteUrl, errorMessage);

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: "FAILED_CAPTURE",
          status: "failed",
          score: null,
          page_type: "unclear",
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.failed = 1;
        meta.error = errorMessage;
        meta.status = "completed";
        clearLiveStep(meta);
        await persistMeta(jobId, meta);
        return;
      }

      console.log("Capture result (single):", meta.siteUrl, captureResult);

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped || gate.paused) return;

      let scoreResult = null;
      try {
        scoreResult = await runStep(jobId, meta, {
          step: "scoring",
          label: "Score website",
          timeoutMs: STEP_TIMEOUTS.scoreMs,
          attempts: STEP_RETRIES.score,
          url: meta.siteUrl,
          fn: async () =>
            runScoreOnlyAnalysis(meta.siteUrl, analysisLanguage, "openai"),
        });
      } catch (err) {
        const errorMessage = safeErrorMessage(err);
        console.error("Single score failed:", meta.siteUrl, errorMessage);

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: "FAILED_SCORE_STEP",
          status: "failed",
          score: null,
          page_type: "unclear",
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.failed = 1;
        meta.error = errorMessage;
        meta.status = "completed";
        clearLiveStep(meta);
        await persistMeta(jobId, meta);
        return;
      }

      console.log("Score result (single):", scoreResult);

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped || gate.paused) return;

      const siteState = classifySiteState(captureResult, scoreResult);
      const scoreValue = normalizeNumericScore(scoreResult?.score);

      console.log("Site state debug (single):", {
        url: meta.siteUrl,
        reason: scoreResult?.reason,
        aiReachable: scoreResult?.reachable,
        pageType: scoreResult?.page_type,
        state: siteState.state,
        scoreValue,
        captureResult,
      });

      meta.siteReachable = siteState.state !== "unreachable";
      meta.siteScore = siteState.state === "normal" ? scoreValue : null;
      meta.sitePageType = scoreResult?.page_type || siteState.pageType || null;
      await persistMeta(jobId, meta);

      if (
        siteState.state === "unreachable" ||
        siteState.state === "placeholder" ||
        siteState.state === "broken_page"
      ) {
        let out = "";
        let status = siteState.state;
        const pageType =
          scoreResult?.page_type || siteState.pageType || "unclear";

        if (siteState.state === "broken_page") {
          out = getBrokenPageComment(analysisLanguage);
          status = "broken_page";
        } else if (siteState.state === "placeholder") {
          out = getPlaceholderPageComment(analysisLanguage);
          status = "placeholder";
        } else {
          const unreachableOutcome = getUnreachableOutcome(
            cfg,
            analysisLanguage
          );
          out = unreachableOutcome.out;
          status = unreachableOutcome.status;
        }

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: out,
          status,
          score: scoreValue,
          page_type: pageType,
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.analyzed = 1;
        meta.failed = 0;
        meta.status = "completed";
        clearLiveStep(meta);
        await persistMeta(jobId, meta);

        console.log(`Single job completed (${status}):`, jobId);
        return;
      }

      if (scoreValue === null) {
        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: "FAILED_SCORE_PARSE",
          status: "failed",
          score: null,
          page_type: scoreResult?.page_type || "unclear",
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.failed = 1;
        meta.status = "completed";
        clearLiveStep(meta);
        await persistMeta(jobId, meta);

        console.log("Single job completed (score parse failed):", jobId);
        return;
      }

      const score = scoreValue;
      console.log("QUALIFICATION CHECK:", score, cfg.minScore);

      if (score >= cfg.minScore) {
        const action = cfg.lowScoreAction;
        const out = getTooGoodMessage(analysisLanguage, score, action);
        const status =
          action === "skip"
            ? "skipped"
            : action === "delete"
            ? "cleared"
            : "good_site";

        await consumeAnalysisCredit({
          appUserId: meta.appUserId,
          jobId,
          siteUrl: meta.siteUrl,
          rowNumber: null,
          status,
          score: scoreValue,
          pageType: scoreResult?.page_type || "real_site",
        });

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: out,
          status,
          score: scoreValue,
          page_type: scoreResult?.page_type || "real_site",
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.analyzed = 1;
        meta.status = "completed";
        clearLiveStep(meta);
        await persistMeta(jobId, meta);

        console.log("Single job completed (too good / high score):", jobId);
        return;
      }

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped || gate.paused) return;

      console.log("Qualified single site, starting full AI analysis...");

      let analysisResult = null;
      try {
        analysisResult = await runStep(jobId, meta, {
          step: "full_analysis",
          label: "Full analysis",
          timeoutMs: STEP_TIMEOUTS.analysisMs,
          attempts: STEP_RETRIES.analysis,
          url: meta.siteUrl,
          fn: async () =>
            runAnalysis(meta.siteUrl, analysisLanguage, "openai", basePrompt),
        });
      } catch (err) {
        const errorMessage = safeErrorMessage(err);
        console.error("Single full analysis failed:", meta.siteUrl, errorMessage);

        const fallbackComment =
          cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage);

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: fallbackComment,
          status: "fallback",
          score: scoreValue,
          page_type: scoreResult?.page_type || "real_site",
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.analyzed = 1;
        meta.error = errorMessage;
        meta.status = "completed";
        clearLiveStep(meta);
        await persistMeta(jobId, meta);
        return;
      }

      console.log(
        "Full analysis result received:",
        JSON.stringify(analysisResult?.analysis || null, null, 2)
      );

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped || gate.paused) return;

      const finalPageType =
        analysisResult?.analysis?.page_type ||
        scoreResult?.page_type ||
        "real_site";

      const shouldGenerateComment =
        analysisResult?.analysis?.should_generate_comment !== false &&
        finalPageType === "real_site";

      const analysisPayload = buildStoredAnalysisPayload(analysisResult);

      let comment = "";
      let finalStatus = "success";
      let finalScore = scoreValue;

      if (!shouldGenerateComment) {
        comment =
          cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage);
        finalStatus = "fallback";
        finalScore = 0;
      } else {
        comment = finalizeGeneratedComment(
          analysisResult,
          writing,
          analysisLanguage
        );
      }

      meta.singleAnalysisPayload = analysisPayload;
      await persistMeta(jobId, meta);

      console.log("Final extracted comment:", comment);

      await consumeAnalysisCredit({
        appUserId: meta.appUserId,
        jobId,
        siteUrl: meta.siteUrl,
        rowNumber: null,
        status: finalStatus,
        score: finalScore,
        pageType: finalPageType,
      });

      await pushRedisResult(jobId, {
        url: meta.siteUrl,
        comment,
        status: finalStatus,
        score: finalScore,
        page_type: finalPageType,
        analysis_payload: analysisPayload,
        createdAt: nowIso(),
      });

      const checkResults = await redis.lrange(`job:${jobId}:results`, 0, -1);
      console.log("Redis results after push:", checkResults);

      meta.total = 1;
      meta.analyzed = 1;
      meta.status = "completed";
      clearLiveStep(meta);
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
      const batchIsCsv = isCsvBatch(meta);
      const shouldDeleteRowsFromSheet =
        isSheetBatch(meta) &&
        String(meta?.sourceType || "").toLowerCase() !== "google";

      let allRows = [];

      try {
        allRows = await runStep(jobId, meta, {
          step: "pull_rows",
          label: batchIsCsv ? "Pull CSV rows" : "Pull sheet URLs",
          timeoutMs: STEP_TIMEOUTS.pullRowsMs,
          attempts: STEP_RETRIES.pullRows,
          fn: async () => {
            if (batchIsCsv) {
              const csvRawText = meta.csvRawText || "";
              const csvUrlColumn = meta.csvUrlColumn || "";
              const csvMailColumn = meta.csvMailColumn || "";

              console.log("CSV PULL DEBUG:", {
                hasCsvRawText: !!csvRawText,
                csvUrlColumn,
                csvMailColumn,
                firstNameColumn: meta.firstNameColumn || "",
                companyNameColumn: meta.companyNameColumn || "",
                industryColumn: meta.industryColumn || "",
                locationColumn: meta.locationColumn || "",
              });

              return pullCsvRows({
                rawText: csvRawText,
                csvRawText,
                urlColumn: csvUrlColumn,
                mailColumn: csvMailColumn,
                csvUrlColumn,
                csvMailColumn,
                firstNameColumn: meta.firstNameColumn || "",
                companyNameColumn: meta.companyNameColumn || "",
                industryColumn: meta.industryColumn || "",
                locationColumn: meta.locationColumn || "",
              });
            }

            return pullSheetUrls({
              userId: meta.userId,
              sheetId: meta.sheetId,
              sheetTab: meta.sheetTab,
              urlColumn: meta.urlColumn,
              mailColumn: meta.mailColumn,
              firstNameColumn: meta.firstNameColumn,
              companyNameColumn: meta.companyNameColumn,
              industryColumn: meta.industryColumn,
              locationColumn: meta.locationColumn,
            });
          },
        });
      } catch (err) {
        const message =
          safeErrorMessage(err) ||
          (batchIsCsv
            ? "No website URLs detected in selected CSV URL column."
            : "No website URLs detected in selected URL column.");

        await pushRedisResult(jobId, {
          url: "Batch setup",
          comment: message,
          status: "failed",
          score: null,
          page_type: "unclear",
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.total = 0;
        meta.analyzed = 0;
        meta.failed = 0;
        meta.status = "completed";
        meta.error = message;
        clearLiveStep(meta);

        await persistMeta(jobId, meta);

        console.log("Batch setup failed:", message);
        return;
      }

      let rows = allRows.slice(0, cfg.maxBatchSize);
      const requestedVerificationRows = getRequestedVerificationEntries(meta, rows)
        .map((row, index) => ({
          ...row,
          recipientEmail: getVerificationRowEmail(row),
          verificationRowNumber: Number.isFinite(row?.verificationRowNumber)
            ? Number(row.verificationRowNumber)
            : Number.isFinite(row?.rowNumber)
            ? Number(row.rowNumber)
            : index + 1,
        }))
        .filter((row) => !!row.recipientEmail);

      const verificationControl = getVerificationControl(meta);
      const verificationOnlyBatch = isVerificationOnlyBatch(
        meta,
        verificationControl
      );
      const persistedVerificationState = getPersistedVerificationState(
        meta,
        priorVerificationState
      );
      let existingVerificationResults = hasVerificationResults(
        persistedVerificationState?.results
      )
        ? normalizeStoredVerificationResults(
            persistedVerificationState.results,
            requestedVerificationRows
          )
        : [];

      if (verificationControl.enabled) {
        if (
          shouldRunFreshVerification({
            verificationControl,
            verificationState: persistedVerificationState,
            requestedVerificationRows,
          })
        ) {
          console.log(`[email-verify][${jobId}] Fresh verification run starting.`);
          const verificationRun = await runEmailVerificationStage({
            jobId,
            meta,
            rows,
            queueMetaUpdate,
          });

          if (verificationRun.stopped) return;

          if (verificationRun.noEmails) {
            await persistVerificationState(
              jobId,
              meta,
              {
                enabled: true,
                phase: "completed",
                type: "batch",
                provider: "bouncer",
                checkedAt: verificationRun.checkedAt,
                summary: verificationRun.summary,
                excludedRowCount: 0,
                results: [],
              },
              []
            );
            existingVerificationResults = [];
          } else {
            const verificationPayload = {
              enabled: true,
              phase: "review_required",
              type: "batch",
              provider: "bouncer",
              emailColumn:
                meta.emailColumn ||
                meta.mailColumn ||
                meta.csvMailColumn ||
                null,
              checkedAt: verificationRun.checkedAt,
              providerBatch: verificationRun.providerBatch || null,
              providerCredits: verificationRun.providerCredits || null,
              summary: verificationRun.summary,
              excludedRowCount: 0,
              results: verificationRun.results,
            };

            await persistVerificationState(
              jobId,
              meta,
              verificationPayload,
              requestedVerificationRows
            );
            existingVerificationResults = normalizeStoredVerificationResults(
              verificationRun.results,
              requestedVerificationRows
            );

            if (!meta.verificationCreditsCharged) {
              const creditResult = await consumeVerificationCredit({
                appUserId: meta.appUserId,
                jobId,
                quantity: existingVerificationResults.length,
                mode: "batch",
                batchId: verificationRun.providerBatch?.batchId || null,
              });

              console.log(
                `[email-verify][${jobId}] Swokei verification credits consumed: ${creditResult.amount}.`
              );
              meta.verificationCreditsCharged = true;
              await persistMeta(jobId, meta);
            }

            console.log(`[email-verify][${jobId}] Waiting for verification review.`);
            await pushRedisResult(jobId, {
              type: "email_verification",
              status: "verification_review_required",
              verification: {
                type: "batch",
                provider: "bouncer",
                emailColumn: verificationPayload.emailColumn,
                summary: buildVerificationSummary(existingVerificationResults, 0),
                results: existingVerificationResults,
              },
              createdAt: nowIso(),
            });

            meta.total = rows.length;
            meta.status = "awaiting_verification_review";
            meta.awaitingUserAction = "email_verification_review";
            clearLiveStep(meta);
            await persistMeta(jobId, meta);
            return;
          }
        }

        if (
          shouldWaitForVerificationReview({
            verificationControl,
            verificationState: persistedVerificationState,
            meta,
          })
        ) {
          console.log(`[email-verify][${jobId}] Waiting for verification review.`);
          await pushRedisResult(jobId, {
            type: "email_verification",
            status: "verification_review_required",
            verification: {
              type: "batch",
              provider: "bouncer",
              emailColumn:
                meta.emailColumn ||
                meta.mailColumn ||
                meta.csvMailColumn ||
                null,
              summary:
                persistedVerificationState?.summary ||
                buildVerificationSummary(existingVerificationResults, 0),
              results: existingVerificationResults,
            },
            createdAt: nowIso(),
          });

          meta.total = rows.length;
          meta.status = "awaiting_verification_review";
          meta.awaitingUserAction = "email_verification_review";
          clearLiveStep(meta);
          await persistMeta(jobId, meta);
          return;
        }

        if (
          shouldResumeAfterVerification({
            verificationControl,
            verificationState: persistedVerificationState,
            meta,
          })
        ) {
          console.log(`[email-verify][${jobId}] Resume after review.`);

          if (verificationOnlyBatch) {
            const excludedCount = Math.max(
              0,
              Number(persistedVerificationState?.excludedRowCount) || 0
            );
            const verificationPayload = {
              ...(persistedVerificationState || meta.verification || {}),
              enabled: true,
              phase: "completed",
              type: "batch",
              provider: "bouncer",
              reviewCompletedAt: nowIso(),
              excludedRowCount: Math.max(0, excludedCount),
              summary: buildVerificationSummary(
                existingVerificationResults,
                Math.max(0, excludedCount)
              ),
              results: existingVerificationResults,
            };

            await persistVerificationState(
              jobId,
              meta,
              verificationPayload,
              requestedVerificationRows
            );

            await pushRedisResult(jobId, {
              type: "email_verification",
              status: "completed",
              verification: {
                type: "batch",
                provider: "bouncer",
                emailColumn:
                  meta.emailColumn ||
                  meta.mailColumn ||
                  meta.csvMailColumn ||
                  null,
                summary: buildVerificationSummary(
                  existingVerificationResults,
                  Math.max(0, excludedCount)
                ),
                results: existingVerificationResults,
              },
              createdAt: nowIso(),
            });

            console.log(`[email-verify][${jobId}] Verification-only batch completed.`);
            meta.total = existingVerificationResults.length;
            meta.analyzed = existingVerificationResults.length;
            meta.failed = 0;
            meta.status = "completed";
            clearLiveStep(meta);
            await persistMeta(jobId, meta);
            return;
          }
        }

        const isExcluded = buildExcludedRowMatcher(meta, existingVerificationResults);
        const verificationByRowIndex = new Map(
          existingVerificationResults.map((result) => [result.rowIndex, result])
        );

        rows = rows.filter((row) => !isExcluded(row, verificationByRowIndex.get(row.rowIndex)));

        const excludedCount = Math.max(
          0,
          existingVerificationResults.length - rows.length
        );
        const verificationPayload = {
          ...(persistedVerificationState || meta.verification || {}),
          enabled: true,
          phase: "completed",
          type: "batch",
          provider: "bouncer",
          reviewCompletedAt: nowIso(),
          excludedRowCount: Math.max(0, excludedCount),
          summary: buildVerificationSummary(
            existingVerificationResults,
            Math.max(0, excludedCount)
          ),
          results: existingVerificationResults,
        };

        await persistVerificationState(
          jobId,
          meta,
          verificationPayload,
          requestedVerificationRows
        );
      }

      meta.total = rows.length;
      await persistMeta(jobId, meta);

      console.log(`Progress: ${meta.analyzed}/${meta.total}`);

      if (rows.length === 0) {
        await pushRedisResult(jobId, {
          type: "analysis",
          url: "Batch verification",
          comment: "No rows left to analyze after email verification exclusions.",
          status: "completed",
          score: null,
          page_type: "unclear",
          analysis_payload: null,
          createdAt: nowIso(),
        });

        meta.status = "completed";
        clearLiveStep(meta);
        await persistMeta(jobId, meta);
        return;
      }

      const concurrency = cfg.concurrency;

      for (let start = 0; start < rows.length; start += concurrency) {
        const gate = await waitIfPausedOrStopped(jobId);
        if (gate.stopped || gate.paused) return;

        const chunk = rows.slice(start, start + concurrency);

        await Promise.all(
          chunk.map(async (row, idx) => {
            const rowNumber = start + idx + 1;

            try {
              let rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped || rowGate.paused) return;

              console.log(`Processing row ${rowNumber}/${rows.length}:`, row.url);

              let captureResult = null;
              try {
                captureResult = await runStep(jobId, meta, {
                  step: "capturing",
                  label: "Capture website",
                  timeoutMs: STEP_TIMEOUTS.captureMs,
                  attempts: STEP_RETRIES.capture,
                  url: row.url,
                  rowIndex: row.rowIndex,
                  fn: async () => {
                    applyScreenshotEnv(cfg.screenshotMode);
                    return captureWebsite(row.url);
                  },
                });
              } catch (err) {
                const errorMessage = safeErrorMessage(err);
                console.error("Row capture failed:", row.url, errorMessage);

                const unreachableOutcome = getUnreachableOutcome(
                  cfg,
                  analysisLanguage
                );

                try {
                  await maybeWriteSheet(meta, row, unreachableOutcome.sheetValue);
                } catch (sheetErr) {
                  console.error(
                    "Failed to write unreachable capture outcome:",
                    row.url,
                    safeErrorMessage(sheetErr)
                  );
                }

                await pushRedisResult(
                  jobId,
                  buildBatchResultPayload(row, {
                    comment: unreachableOutcome.out,
                    status: unreachableOutcome.status,
                    score: null,
                    page_type: "unclear",
                    analysis_payload: null,
                    createdAt: nowIso(),
                  })
                );

                await queueMetaUpdate(() => {
                  meta.analyzed += 1;
                });
                return;
              }

              console.log("Capture result:", row.url, captureResult);

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped || rowGate.paused) return;

              let scoreResult = null;
              try {
                scoreResult = await runStep(jobId, meta, {
                  step: "scoring",
                  label: "Score website",
                  timeoutMs: STEP_TIMEOUTS.scoreMs,
                  attempts: STEP_RETRIES.score,
                  url: row.url,
                  rowIndex: row.rowIndex,
                  fn: async () =>
                    runScoreOnlyAnalysis(row.url, analysisLanguage, "openai"),
                });
              } catch (err) {
                const errorMessage = safeErrorMessage(err);
                console.error("Row score failed:", row.url, errorMessage);

                try {
                  await maybeWriteSheet(meta, row, "FAILED_SCORE_STEP");
                } catch (sheetErr) {
                  console.error(
                    "Failed to write score failure:",
                    row.url,
                    safeErrorMessage(sheetErr)
                  );
                }

                await pushRedisResult(
                  jobId,
                  buildBatchResultPayload(row, {
                    comment: "FAILED_SCORE_STEP",
                    status: "failed",
                    score: null,
                    page_type: "unclear",
                    analysis_payload: null,
                    createdAt: nowIso(),
                  })
                );

                await queueMetaUpdate(() => {
                  meta.failed += 1;
                });
                return;
              }

              console.log("Score result:", row.url, scoreResult);

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped || rowGate.paused) return;

              const siteState = classifySiteState(captureResult, scoreResult);
              const scoreValue = normalizeNumericScore(scoreResult?.score);

              console.log("Site state debug:", {
                url: row.url,
                reason: scoreResult?.reason,
                aiReachable: scoreResult?.reachable,
                pageType: scoreResult?.page_type,
                state: siteState.state,
                scoreValue,
                captureResult,
              });

              if (
                siteState.state === "unreachable" ||
                siteState.state === "placeholder" ||
                siteState.state === "broken_page"
              ) {
                let out = "";
                let status = siteState.state;
                const pageType =
                  scoreResult?.page_type || siteState.pageType || "unclear";
                let sheetValue = "";

                if (siteState.state === "broken_page") {
                  out = getBrokenPageComment(analysisLanguage);
                  status = "broken_page";
                  sheetValue = out;
                } else if (siteState.state === "placeholder") {
                  out = getPlaceholderPageComment(analysisLanguage);
                  status = "placeholder";
                  sheetValue = out;
                } else {
                  const unreachableOutcome = getUnreachableOutcome(
                    cfg,
                    analysisLanguage
                  );
                  out = unreachableOutcome.out;
                  status = unreachableOutcome.status;
                  sheetValue = unreachableOutcome.sheetValue;
                }

                await maybeWriteSheet(meta, row, sheetValue);

                await pushRedisResult(
                  jobId,
                  buildBatchResultPayload(row, {
                    comment: out,
                    status,
                    score: scoreValue,
                    page_type: pageType,
                    analysis_payload: null,
                    createdAt: nowIso(),
                  })
                );

                await queueMetaUpdate(() => {
                  meta.analyzed += 1;
                });
                return;
              }

              if (scoreValue === null) {
                await maybeWriteSheet(meta, row, "FAILED_SCORE_PARSE");

                await pushRedisResult(
                  jobId,
                  buildBatchResultPayload(row, {
                    comment: "FAILED_SCORE_PARSE",
                    status: "failed",
                    score: null,
                    page_type: scoreResult?.page_type || "unclear",
                    analysis_payload: null,
                    createdAt: nowIso(),
                  })
                );

                await queueMetaUpdate(() => {
                  meta.failed += 1;
                });
                return;
              }

              const score = scoreValue;

              if (score >= cfg.minScore) {
                const action = cfg.lowScoreAction;
                const text = getTooGoodMessage(analysisLanguage, score, action);

                if (action === "skip") {
                  await consumeAnalysisCredit({
                    appUserId: meta.appUserId,
                    jobId,
                    siteUrl: row.url,
                    rowNumber,
                    status: "skipped",
                    score: scoreValue,
                    pageType: scoreResult?.page_type || "real_site",
                  });

                  await maybeWriteSheet(meta, row, "");

                  await pushRedisResult(
                    jobId,
                    buildBatchResultPayload(row, {
                      comment: text,
                      status: "skipped",
                      score: scoreValue,
                      page_type: scoreResult?.page_type || "real_site",
                      analysis_payload: null,
                      createdAt: nowIso(),
                    })
                  );

                  await queueMetaUpdate(() => {
                    meta.analyzed += 1;
                  });
                  return;
                }

                if (action === "tag") {
                  await consumeAnalysisCredit({
                    appUserId: meta.appUserId,
                    jobId,
                    siteUrl: row.url,
                    rowNumber,
                    status: "good_site",
                    score: scoreValue,
                    pageType: scoreResult?.page_type || "real_site",
                  });

                  await maybeWriteSheet(meta, row, `GOOD_SITE (${score}/10)`);

                  await pushRedisResult(
                    jobId,
                    buildBatchResultPayload(row, {
                      comment: text,
                      status: "good_site",
                      score: scoreValue,
                      page_type: scoreResult?.page_type || "real_site",
                      analysis_payload: null,
                      createdAt: nowIso(),
                    })
                  );

                  await queueMetaUpdate(() => {
                    meta.analyzed += 1;
                  });
                  return;
                }

                if (action === "delete") {
                  await consumeAnalysisCredit({
                    appUserId: meta.appUserId,
                    jobId,
                    siteUrl: row.url,
                    rowNumber,
                    status: "cleared",
                    score: scoreValue,
                    pageType: scoreResult?.page_type || "real_site",
                  });

                  if (shouldDeleteRowsFromSheet) {
                    rowsToDelete.push(row.rowIndex);
                  }

                  await pushRedisResult(
                    jobId,
                    buildBatchResultPayload(row, {
                      comment: text,
                      status: "cleared",
                      score: scoreValue,
                      page_type: scoreResult?.page_type || "real_site",
                      analysis_payload: null,
                      createdAt: nowIso(),
                    })
                  );

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

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped || rowGate.paused) return;

              let analysisResult = null;
              try {
                analysisResult = await runStep(jobId, meta, {
                  step: "full_analysis",
                  label: "Full analysis",
                  timeoutMs: STEP_TIMEOUTS.analysisMs,
                  attempts: STEP_RETRIES.analysis,
                  url: row.url,
                  rowIndex: row.rowIndex,
                  fn: async () =>
                    runAnalysis(
                      row.url,
                      analysisLanguage,
                      "openai",
                      writingPromptOverride
                    ),
                });
              } catch (err) {
                const errorMessage = safeErrorMessage(err);
                console.error("Row full analysis failed:", row.url, errorMessage);

                try {
                  await maybeWriteSheet(meta, row, "FAILED_ANALYSIS_STEP");
                } catch (sheetErr) {
                  console.error(
                    "Failed to write analysis failure:",
                    row.url,
                    safeErrorMessage(sheetErr)
                  );
                }

                await pushRedisResult(
                  jobId,
                  buildBatchResultPayload(row, {
                    comment: "FAILED_ANALYSIS_STEP",
                    status: "failed",
                    score: scoreValue,
                    page_type: scoreResult?.page_type || "real_site",
                    analysis_payload: null,
                    createdAt: nowIso(),
                  })
                );

                await queueMetaUpdate(() => {
                  meta.failed += 1;
                });
                return;
              }

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped || rowGate.paused) return;

              const finalPageType =
                analysisResult?.analysis?.page_type ||
                scoreResult?.page_type ||
                "real_site";

              const shouldGenerateComment =
                analysisResult?.analysis?.should_generate_comment !== false &&
                finalPageType === "real_site";

              const analysisPayload = buildStoredAnalysisPayload(analysisResult);

              let comment = "";
              let finalStatus = "success";
              let finalScore = scoreValue;

              if (!shouldGenerateComment) {
                comment =
                  cfg.fallbackPrompt ||
                  getDefaultFallbackComment(analysisLanguage);
                finalStatus = "fallback";
                finalScore = 0;
              } else {
                comment = finalizeGeneratedComment(
                  analysisResult,
                  writing,
                  analysisLanguage
                );
              }

              await consumeAnalysisCredit({
                appUserId: meta.appUserId,
                jobId,
                siteUrl: row.url,
                rowNumber,
                status: finalStatus,
                score: finalScore,
                pageType: finalPageType,
              });

              await maybeWriteSheet(meta, row, comment);

              await pushRedisResult(
                jobId,
                buildBatchResultPayload(row, {
                  comment,
                  status: finalStatus,
                  score: finalScore,
                  page_type: finalPageType,
                  analysis_payload: analysisPayload,
                  createdAt: nowIso(),
                })
              );

              await queueMetaUpdate(() => {
                meta.analyzed += 1;
              });
            } catch (err) {
              console.error("Row failed:", row.url, err);

              try {
                await maybeWriteSheet(meta, row, "FAILED");
              } catch (_) {}

              await pushRedisResult(
                jobId,
                buildBatchResultPayload(row, {
                  comment: "FAILED",
                  status: "failed",
                  score: null,
                  page_type: "unclear",
                  analysis_payload: null,
                  createdAt: nowIso(),
                })
              );

              await queueMetaUpdate(() => {
                meta.failed += 1;
              });
            }
          })
        );
      }

      if (shouldDeleteRowsFromSheet && rowsToDelete.length > 0) {
        const uniqueSortedRows = [...new Set(rowsToDelete)].sort((a, b) => b - a);

        for (const rowIndex of uniqueSortedRows) {
          try {
            await runStep(jobId, meta, {
              step: "delete_row",
              label: "Delete sheet row",
              timeoutMs: STEP_TIMEOUTS.deleteRowMs,
              attempts: STEP_RETRIES.deleteRow,
              rowIndex,
              fn: async () => safeDeleteSheetRow(meta, rowIndex),
            });
          } catch (err) {
            console.error("Failed to delete row:", rowIndex, err);
          }
        }
      }

      meta.status = "completed";
      clearLiveStep(meta);
      await persistMeta(jobId, meta);

      console.log("Batch completed:", jobId);
    }
  } catch (err) {
    console.error("Job crashed:", err);

    try {
      if (meta?.type === "single" && meta?.siteUrl) {
        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: "FAILED",
          status: "failed",
          score: meta.siteScore ?? null,
          page_type: meta.sitePageType || "unclear",
          analysis_payload: null,
          createdAt: nowIso(),
        });
      }
    } catch (_) {}

    if (meta?.type === "single") {
      meta.failed = 1;
      meta.total = 1;
    }

    meta.status = "completed";
    meta.error = safeErrorMessage(err);
    clearLiveStep(meta);
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
        await sleep(1000);
      }
    } catch (err) {
      console.error("Queue error:", err);
      await sleep(2000);
    }
  }
}

process.on("unhandledRejection", (err) => {
  console.error("Unhandled rejection:", err);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err);
});

runQueue();
