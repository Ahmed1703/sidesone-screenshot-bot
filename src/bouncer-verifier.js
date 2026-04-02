require("dotenv").config();

const BOUNCER_API_BASE_URL = String(
  process.env.BOUNCER_API_BASE_URL || "https://api.usebouncer.com"
).replace(/\/+$/, "");
const BOUNCER_API_KEY = String(process.env.BOUNCER_API_KEY || "").trim();
const BOUNCER_REQUEST_TIMEOUT_MS =
  Number(process.env.BOUNCER_REQUEST_TIMEOUT_MS) || 30000;
const BOUNCER_REALTIME_TIMEOUT_SECONDS = Math.max(
  1,
  Math.min(30, Number(process.env.BOUNCER_REALTIME_TIMEOUT_SECONDS) || 15)
);
const BOUNCER_BATCH_POLL_INTERVAL_MS =
  Number(process.env.BOUNCER_BATCH_POLL_INTERVAL_MS) || 10000;
const BOUNCER_BATCH_MAX_WAIT_MS =
  Number(process.env.BOUNCER_BATCH_MAX_WAIT_MS) || 300000;

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeEmailAddress(input) {
  const raw = String(input || "").trim().replace(/^<|>$/g, "");
  const atIndex = raw.lastIndexOf("@");

  if (!raw || atIndex <= 0 || atIndex === raw.length - 1) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: raw,
      reason: "Malformed email address.",
    };
  }

  const localPart = raw.slice(0, atIndex).trim();
  const domain = raw.slice(atIndex + 1).trim().toLowerCase();

  if (!localPart || !domain) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: raw,
      reason: "Malformed email address.",
    };
  }

  if (!/^[^\s@]+$/.test(localPart)) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: `${localPart}@${domain}`,
      reason: "Local part contains unsupported characters or whitespace.",
    };
  }

  if (
    !/^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/i.test(
      domain
    )
  ) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: `${localPart}@${domain}`,
      reason: "Domain part is malformed.",
    };
  }

  return {
    ok: true,
    email: raw,
    localPart,
    domain,
    normalizedEmail: `${localPart}@${domain}`,
  };
}

async function fetchBouncerJson(pathname, options = {}) {
  if (!BOUNCER_API_KEY) {
    throw new Error("Missing BOUNCER_API_KEY.");
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), BOUNCER_REQUEST_TIMEOUT_MS);
  if (typeof timer.unref === "function") timer.unref();

  try {
    const res = await fetch(`${BOUNCER_API_BASE_URL}${pathname}`, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        "x-api-key": BOUNCER_API_KEY,
        ...(options.headers || {}),
      },
      signal: controller.signal,
    });

    const data = await res.json().catch(() => null);

    if (!res.ok) {
      const message =
        data?.message ||
        data?.error ||
        `Bouncer API request failed with status ${res.status}.`;
      const err = new Error(message);
      err.statusCode = res.status;
      err.payload = data;
      throw err;
    }

    return data;
  } finally {
    clearTimeout(timer);
  }
}

function mapBouncerStatus(status) {
  switch (String(status || "").toLowerCase()) {
    case "deliverable":
      return "valid";
    case "undeliverable":
      return "invalid";
    case "risky":
      return "risky";
    default:
      return "unknown";
  }
}

function statusConfidence(status, score) {
  const numericScore = Number(score);
  if (Number.isFinite(numericScore)) {
    return Math.max(0, Math.min(1, numericScore / 100));
  }

  switch (status) {
    case "valid":
      return 0.9;
    case "invalid":
      return 0.95;
    case "risky":
      return 0.55;
    default:
      return 0.2;
  }
}

function shouldContinueForStatus(status) {
  return status !== "invalid";
}

function mapBouncerResult(raw, extras = {}) {
  const normalized = normalizeEmailAddress(raw?.email || extras.email || "");
  const status = mapBouncerStatus(raw?.status);

  return {
    email: String(raw?.email || extras.email || "").trim(),
    normalizedEmail: normalized.normalizedEmail,
    status,
    confidence: statusConfidence(status, raw?.score),
    reason: String(raw?.reason || "unknown"),
    provider: raw?.provider || null,
    score: Number.isFinite(Number(raw?.score)) ? Number(raw.score) : null,
    toxic: raw?.toxic ?? null,
    toxicity: Number.isFinite(Number(raw?.toxicity))
      ? Number(raw.toxicity)
      : null,
    domain: raw?.domain || null,
    account: raw?.account || null,
    dns: raw?.dns || null,
    bouncer: {
      status: raw?.status || null,
      reason: raw?.reason || null,
      retryAfter: raw?.retryAfter || null,
    },
    checkedAt: extras.checkedAt || nowIso(),
    rowNumber: Number.isFinite(extras.rowNumber) ? extras.rowNumber : null,
    rowIndex: Number.isFinite(extras.rowIndex) ? extras.rowIndex : null,
    shouldContinue: shouldContinueForStatus(status),
  };
}

function buildInvalidLocalResult(email, extras = {}) {
  const normalized = normalizeEmailAddress(email);
  return {
    email: String(email || "").trim(),
    normalizedEmail: normalized.normalizedEmail,
    status: "invalid",
    confidence: 1,
    reason: normalized.reason || "invalid_email",
    provider: null,
    score: 0,
    toxic: null,
    toxicity: null,
    domain: null,
    account: null,
    dns: null,
    bouncer: {
      status: "undeliverable",
      reason: normalized.reason || "invalid_email",
      retryAfter: null,
    },
    checkedAt: extras.checkedAt || nowIso(),
    rowNumber: Number.isFinite(extras.rowNumber) ? extras.rowNumber : null,
    rowIndex: Number.isFinite(extras.rowIndex) ? extras.rowIndex : null,
    shouldContinue: false,
  };
}

async function getBouncerCredits() {
  return fetchBouncerJson("/v1.1/credits", {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  });
}

async function verifyEmailWithBouncer(email, extras = {}) {
  const normalized = normalizeEmailAddress(email);

  if (!normalized.ok) {
    return buildInvalidLocalResult(email, extras);
  }

  const params = new URLSearchParams({
    email: normalized.normalizedEmail,
    timeout: String(BOUNCER_REALTIME_TIMEOUT_SECONDS),
  });

  const data = await fetchBouncerJson(`/v1.1/email/verify?${params}`, {
    method: "GET",
  });

  return mapBouncerResult(data, {
    ...extras,
    email: normalized.normalizedEmail,
    checkedAt: nowIso(),
  });
}

async function createBouncerBatch(emails) {
  return fetchBouncerJson("/v1.1/email/verify/batch", {
    method: "POST",
    body: JSON.stringify(emails.map((email) => ({ email }))),
  });
}

async function getBouncerBatchStatus(batchId) {
  return fetchBouncerJson(
    `/v1.1/email/verify/batch/${encodeURIComponent(batchId)}?with-stats=true`,
    {
      method: "GET",
    }
  );
}

async function getBouncerBatchResults(batchId) {
  return fetchBouncerJson(
    `/v1.1/email/verify/batch/${encodeURIComponent(batchId)}/download?download=all`,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    }
  );
}

async function verifyEmailsWithBouncerBatch(rows, { logger } = {}) {
  const checkedAt = nowIso();
  const validRows = [];
  const localInvalidResults = [];

  for (const row of rows) {
    const normalized = normalizeEmailAddress(row.recipientEmail);
    if (!normalized.ok) {
      localInvalidResults.push(
        buildInvalidLocalResult(row.recipientEmail, {
          rowNumber: row.verificationRowNumber,
          rowIndex: row.rowIndex,
          checkedAt,
        })
      );
      continue;
    }

    validRows.push({
      ...row,
      normalizedEmail: normalized.normalizedEmail,
    });
  }

  if (validRows.length === 0) {
    return {
      mode: "batch",
      checkedAt,
      batch: null,
      credits: null,
      results: localInvalidResults,
    };
  }

  logger?.(`Bouncer batch create started for ${validRows.length} email(s).`);
  const creation = await createBouncerBatch(
    validRows.map((row) => row.normalizedEmail)
  );
  logger?.(`Bouncer batch created: ${creation.batchId}.`);

  const startedAt = Date.now();
  let status = null;

  while (Date.now() - startedAt < BOUNCER_BATCH_MAX_WAIT_MS) {
    status = await getBouncerBatchStatus(creation.batchId);
    logger?.(
      `Bouncer batch status polled: ${creation.batchId} -> ${status.status}.`
    );

    if (String(status?.status || "").toLowerCase() === "completed") {
      break;
    }

    await sleep(BOUNCER_BATCH_POLL_INTERVAL_MS);
  }

  if (String(status?.status || "").toLowerCase() !== "completed") {
    throw new Error(
      `Bouncer batch ${creation.batchId} did not complete within ${BOUNCER_BATCH_MAX_WAIT_MS}ms.`
    );
  }

  const downloaded = await getBouncerBatchResults(creation.batchId);
  const resultByEmail = new Map();

  for (const item of downloaded || []) {
    const normalizedEmail = normalizeEmailAddress(item?.email).normalizedEmail;
    if (normalizedEmail) {
      resultByEmail.set(normalizedEmail.toLowerCase(), item);
    }
  }

  const mappedResults = [];
  for (const row of validRows) {
    const raw = resultByEmail.get(row.normalizedEmail.toLowerCase());

    if (!raw) {
      mappedResults.push({
        email: row.recipientEmail,
        normalizedEmail: row.normalizedEmail,
        status: "unknown",
        confidence: 0.2,
        reason: "unknown",
        provider: null,
        score: null,
        toxic: null,
        toxicity: null,
        domain: null,
        account: null,
        dns: null,
        bouncer: {
          status: "unknown",
          reason: "missing_batch_result",
          retryAfter: null,
        },
        checkedAt,
        rowNumber: row.verificationRowNumber,
        rowIndex: row.rowIndex,
        shouldContinue: true,
      });
      continue;
    }

    mappedResults.push(
      mapBouncerResult(raw, {
        rowNumber: row.verificationRowNumber,
        rowIndex: row.rowIndex,
        checkedAt,
      })
    );
  }

  return {
    mode: "batch",
    checkedAt,
    batch: {
      batchId: creation.batchId,
      created: creation.created || checkedAt,
      quantity: creation.quantity || validRows.length,
      duplicates: creation.duplicates || 0,
      status: status.status || creation.status || "completed",
      processed: status.processed || validRows.length,
      credits: status.credits ?? null,
      stats: status.stats || null,
    },
    credits: status.credits ?? null,
    results: mappedResults.concat(localInvalidResults),
  };
}

module.exports = {
  normalizeEmailAddress,
  verifyEmailWithBouncer,
  verifyEmailsWithBouncerBatch,
  getBouncerCredits,
};
