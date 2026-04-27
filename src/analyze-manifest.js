const fs = require("fs");
const path = require("path");
require("dotenv").config();
const OpenAI = require("openai");

// ✅ Use env var so this works on VPS/Linux too
const OUT_DIR = process.env.OUTPUT_DIR || "D:\\sidesone-screenshot-output";
const MANIFEST_DIR = path.join(OUT_DIR, "manifests");
const ANALYSIS_RESULTS_DIR = path.join(OUT_DIR, "analysis", "results");
const ANALYSIS_LOGS_DIR = path.join(OUT_DIR, "analysis", "logs");

// Ensure dirs exist
[ANALYSIS_RESULTS_DIR, ANALYSIS_LOGS_DIR].forEach((dir) => {
  fs.mkdirSync(dir, { recursive: true });
});

/* ======================
   TIMEOUTS
====================== */

const OPENAI_SCORE_TIMEOUT_MS =
  Number(process.env.OPENAI_SCORE_TIMEOUT_MS) || 25000;

const OPENAI_CLASSIFY_TIMEOUT_MS =
  Number(process.env.OPENAI_CLASSIFY_TIMEOUT_MS) || 35000;

const OPENAI_WRITE_TIMEOUT_MS =
  Number(process.env.OPENAI_WRITE_TIMEOUT_MS) || 30000;

/* ======================
   FILE HELPERS
====================== */

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
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

function getManifestPath(input) {
  if (String(input || "").toLowerCase().endsWith(".json")) {
    return path.isAbsolute(input) ? input : path.join(process.cwd(), input);
  }
  return path.join(MANIFEST_DIR, `${safeFileName(input)}.json`);
}

function imageToDataUrl(imagePath) {
  const ext = path.extname(imagePath).toLowerCase();
  const mime =
    ext === ".jpg" || ext === ".jpeg"
      ? "image/jpeg"
      : ext === ".png"
      ? "image/png"
      : ext === ".webp"
      ? "image/webp"
      : "application/octet-stream";

  const base64 = fs.readFileSync(imagePath).toString("base64");
  return `data:${mime};base64,${base64}`;
}

/* ======================
   GENERIC HELPERS
====================== */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(promise, ms, label = "operation") {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      const err = new Error(`${label} timed out after ${ms}ms`);
      setTimeout(() => reject(err), ms);
    }),
  ]);
}

function safeErrorMessage(err, fallback = "Unknown error") {
  if (!err) return fallback;
  if (typeof err === "string") return err;
  if (err instanceof Error) return err.message || fallback;
  return String(err?.message || err || fallback);
}

function normalizeComment(text) {
  return String(text || "").replace(/\r/g, "").trim();
}

function cleanGeneratedText(input) {
  let text = String(input || "").replace(/\r/g, "").trim();

  text = text.replace(/```json/gi, "```");
  text = text.replace(/```[\s\S]*?```/g, (match) => {
    return match.replace(/```json/gi, "").replace(/```/g, "").trim();
  });

  // Strip forbidden punctuation that the model occasionally slips through
  // despite the prompt rules (mostly em/en-dashes in English output).
  // Replace dashes with a period+space so the sentence still reads cleanly,
  // and normalize curly quotes to straight quotes.
  text = text.replace(/\s*[—–]\s*/g, ". ");
  text = text.replace(/[“”]/g, '"');
  text = text.replace(/[‘’]/g, "'");

  // Collapse any double-period left over from "X — Y" → "X. . Y"-style edges.
  text = text.replace(/\.\s*\./g, ".");

  text = text.replace(/^["'“”‘’]+|["'“”‘’]+$/g, "").trim();
  text = text.replace(/\n{3,}/g, "\n\n").trim();
  text = text.replace(/[ \t]+/g, " ").trim();

  return text;
}

function normalizeText(value) {
  return String(value || "").trim().toLowerCase();
}

function safeString(value, max = 5000) {
  return String(value || "").slice(0, max);
}

function normalizeNumericScore(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function extractTextFromResponse(response) {
  if (
    response &&
    typeof response.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  try {
    const chunks = [];
    const output = Array.isArray(response?.output) ? response.output : [];

    for (const item of output) {
      const content = Array.isArray(item?.content) ? item.content : [];
      for (const c of content) {
        if (typeof c?.text === "string" && c.text.trim()) {
          chunks.push(c.text.trim());
        }
      }
    }

    return chunks.join("\n").trim();
  } catch (_) {
    return "";
  }
}

function extractFirstJsonObject(text) {
  const value = String(text || "").trim();
  if (!value) return "";

  const cleaned = value
    .replace(/```json/gi, "```")
    .replace(/```/g, "")
    .trim();

  const start = cleaned.indexOf("{");
  if (start === -1) return "";

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let i = start; i < cleaned.length; i += 1) {
    const ch = cleaned[i];

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }

    if (ch === "{") depth += 1;
    if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return cleaned.slice(start, i + 1);
      }
    }
  }

  return "";
}

function safeJsonParse(input) {
  try {
    return JSON.parse(String(input || "").trim());
  } catch {
    return null;
  }
}

function safeJsonParseFromText(input) {
  const direct = safeJsonParse(input);
  if (direct) return direct;

  const extracted = extractFirstJsonObject(input);
  if (!extracted) return null;

  return safeJsonParse(extracted);
}

function splitSentences(text) {
  return String(text || "")
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function ensureSentence(text) {
  const value = String(text || "").trim();
  if (!value) return "";
  if (/[.!?…]$/.test(value)) return value;
  return `${value}.`;
}

function lowerFirst(text) {
  const value = String(text || "").trim();
  if (!value) return "";
  return value.charAt(0).toLowerCase() + value.slice(1);
}

function upperFirst(text) {
  const value = String(text || "").trim();
  if (!value) return "";
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function uniqueCaseInsensitive(items = []) {
  const seen = new Set();
  const out = [];

  for (const item of items) {
    const value = String(item || "").trim();
    const key = value.toLowerCase();
    if (!value || seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }

  return out;
}

/* ======================
   LANGUAGE HELPERS
====================== */

function detectLikelyLanguage(text) {
  const value = String(text || "").toLowerCase();

  const norwegianSignals = [
    " og ",
    " det ",
    " ikke ",
    "nettsiden",
    "kunne",
    "også",
    "føles",
    "inntrykk",
    "struktur",
    "kontakt",
    "lese",
    "siden",
    "tydelig",
    "seksjon",
    "overskrift",
    "bakgrunn",
    "knapp",
    "skjema",
    "luft",
  ];

  const englishSignals = [
    " the ",
    " and ",
    " could ",
    " feels ",
    " overall ",
    " site ",
    " contact ",
    " section ",
    " clear ",
    " layout ",
    " readability ",
    " headline ",
    " background ",
    " button ",
    " form ",
    " spacing ",
  ];

  const noScore = norwegianSignals.reduce(
    (sum, token) => sum + (value.includes(token) ? 1 : 0),
    0
  );

  const enScore = englishSignals.reduce(
    (sum, token) => sum + (value.includes(token) ? 1 : 0),
    0
  );

  if (noScore >= 2 && noScore > enScore) return "norwegian";
  if (enScore >= 2 && enScore > noScore) return "english";
  return "unknown";
}

function isWrongLanguage(text, languageArg = "no") {
  const detected = detectLikelyLanguage(text);
  const wantsEnglish = String(languageArg || "no").toLowerCase() === "en";

  if (detected === "unknown") return false;
  if (wantsEnglish) return detected !== "english";
  return detected !== "norwegian";
}

function getLanguageSafeText(text, languageArg = "no") {
  const value = String(text || "").trim();
  if (!value) return "";
  if (isWrongLanguage(value, languageArg)) return "";
  return value;
}

/* ======================
   PAGE TYPE HELPERS
====================== */

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
    t.includes("under construction") ||
    t.includes("her kommer") ||
    t.includes("kommer snart") ||
    t.includes("under utvikling") ||
    t.includes("under bygging") ||
    t.includes("nettside kommer") ||
    t.includes("website coming") ||
    t.includes("site coming") ||
    t.includes("launching soon") ||
    t.includes("we're building") ||
    t.includes("we are building") ||
    t.includes("stay tuned") ||
    t.includes("watch this space") ||
    t.includes("vi bygger") ||
    t.includes("siden er under") ||
    t.includes("nettsiden er under") ||
    /^.{0,5}www\.\S+\.\S{2,4}.{0,5}$/.test(t)
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

function guessFallbackPageState(manifest) {
  const title = normalizeText(manifest?.homepage_title);
  const finalUrl = normalizeText(manifest?.final_url || manifest?.input_url);
  const combined = `${title} ${finalUrl}`.trim();

  if (manifest?.capture_status !== "success") {
    return {
      reachable: false,
      page_type: "unreachable",
      reason: "Capture failed/unreachable",
    };
  }

  if (textLooksLikeDeadSite(combined)) {
    return {
      reachable: false,
      page_type: "unreachable",
      reason: "Page looks unreachable or parked",
    };
  }

  if (textLooksLikeBrokenPage(combined)) {
    return {
      reachable: false,
      page_type: "broken_page",
      reason: "Page looks like an error page",
    };
  }

  return {
    reachable: true,
    page_type: "real_site",
    reason: "Capture succeeded",
  };
}

/* ======================
   FALLBACK BUILDERS
====================== */

function buildInputBundle(manifest) {
  const paths = [
    { role: "top", path: manifest.desktop_top_path },
    { role: "mid", path: manifest.desktop_mid_path },
    { role: "bottom", path: manifest.desktop_bottom_path },
  ].filter((x) => x.path && fs.existsSync(x.path));

  return {
    paths,
    pageHeight: manifest.page_height ?? null,
    viewportHeight: manifest.viewport_desktop?.height ?? null,
    homepageTitle: manifest.homepage_title || "",
    finalUrl: manifest.final_url || manifest.input_url || "",
  };
}

function getCurrentScreenshotMode() {
  return String(
    process.env.SCREENSHOT_MODE ||
      process.env.SCREENSHOT_STRATEGY ||
      process.env.SIDESONE_SCREENSHOT_MODE ||
      "sections"
  )
    .trim()
    .toLowerCase();
}

function getMobileImagePath(manifest) {
  const candidate =
    manifest?.mobile_top_path ||
    (Array.isArray(manifest?.mobile_image_paths) && manifest.mobile_image_paths[0]) ||
    null;
  if (candidate && fs.existsSync(candidate)) return candidate;
  return null;
}

/**
 * Formats the manifest.seo block into a compact text summary the analyzer can
 * scan to write SEO findings. Returns "" if no SEO data is available.
 *
 * Includes light hints (length thresholds, mobile-friendly check) so the model
 * has signal-density without us hard-coding any judgment about what's "wrong".
 */
function formatSeoForPrompt(seo) {
  if (!seo || typeof seo !== "object") return "";

  const lines = [];
  const pushIf = (cond, text) => { if (cond) lines.push(text); };

  const titleLen = Number(seo.title_length || 0);
  pushIf(true, `Title: ${JSON.stringify(seo.title || "")} (length ${titleLen}; healthy is 30-60)`);

  const descLen = Number(seo.meta_description_length || 0);
  pushIf(true, `Meta description: ${descLen ? JSON.stringify((seo.meta_description || "").slice(0, 200)) : "MISSING"} (length ${descLen}; healthy is 70-160)`);

  pushIf(seo.canonical, `Canonical: ${seo.canonical}`);
  pushIf(seo.language, `Lang attr: ${seo.language}`);

  const vp = String(seo.viewport_meta || "");
  const mobileFriendly = /width\s*=\s*device-width/i.test(vp);
  pushIf(true, `Viewport meta: ${vp || "MISSING"} (mobile-friendly meta: ${mobileFriendly ? "yes" : "NO — site forces desktop width on phones"})`);

  const h1Count = seo.h1?.count ?? 0;
  const h1Texts = Array.isArray(seo.h1?.texts) ? seo.h1.texts.join(" | ") : "";
  pushIf(true, `H1: count=${h1Count}${h1Texts ? `, text=${JSON.stringify(h1Texts).slice(0, 200)}` : ""} (healthy is exactly 1)`);
  pushIf(true, `H2: count=${seo.h2?.count ?? 0}`);

  const imgs = seo.images || {};
  const altPct = imgs.alt_coverage_pct;
  pushIf(true, `Images: ${imgs.total ?? 0} total, ${imgs.with_alt ?? 0} with alt text${altPct == null ? "" : ` (${altPct}% coverage)`}`);

  pushIf(true, `OG tags present: title=${!!seo.og?.title}, description=${!!seo.og?.description}, image=${!!seo.og?.image}`);
  pushIf(true, `JSON-LD structured data: ${seo.has_jsonld ? "yes" : "no"}`);
  pushIf(true, `Favicon: ${seo.has_favicon ? "yes" : "no"}`);
  pushIf(seo.text_encoding_issue, `Text encoding issue detected: title or meta description contains UTF-8-as-Latin-1 mojibake (e.g. "Når" appearing as "NÃ¥r"). This is a real site bug visible to Google and visitors.`);

  return lines.join("\n");
}

function getImagePathsForMode(manifest) {
  const screenshotMode = getCurrentScreenshotMode();

  if (screenshotMode === "top") {
    if (!manifest.desktop_top_path || !fs.existsSync(manifest.desktop_top_path)) {
      throw new Error("Top screenshot missing");
    }
    return [manifest.desktop_top_path];
  }

  if (screenshotMode === "full") {
    if (manifest.desktop_full_path && fs.existsSync(manifest.desktop_full_path)) {
      return [manifest.desktop_full_path];
    }

    const fallback = [
      manifest.desktop_top_path,
      manifest.desktop_mid_path,
      manifest.desktop_bottom_path,
    ].filter((p) => p && fs.existsSync(p));

    if (fallback.length >= 3) return fallback.slice(0, 3);
    if (fallback.length >= 1) return [fallback[0]];

    throw new Error("No screenshots found for full-mode fallback");
  }

  const top = manifest.desktop_top_path;
  const mid = manifest.desktop_mid_path;
  const bot = manifest.desktop_bottom_path;

  const okTop = top && fs.existsSync(top);
  const okMid = mid && fs.existsSync(mid);
  const okBot = bot && fs.existsSync(bot);

  if (okTop && okMid && okBot) return [top, mid, bot];
  if (okTop) return [top];

  throw new Error("Top screenshot missing");
}

function getDefaultFallbackComment(language) {
  return language === "en"
    ? "Your website does not appear to be properly available right now, so there may be a technical issue at the moment."
    : "Nettsiden deres ser ikke ut til å være ordentlig tilgjengelig akkurat nå, så det kan hende det er noe teknisk feil der nå.";
}

function buildFallbackStructuredAnalysis(reason, languageArg = "no") {
  const isEnglish = String(languageArg || "no").toLowerCase() === "en";

  return {
    page_type: "unclear",
    confidence: 0.2,
    should_generate_comment: false,
    score: 0,
    strengths: [],
    issues: [
      isEnglish
        ? "The page could not be classified safely."
        : "Siden kunne ikke klassifiseres trygt.",
    ],
    evidence: [
      String(
        reason ||
          (isEnglish
            ? "Could not classify page safely."
            : "Kunne ikke klassifisere siden trygt.")
      ),
    ],
    reason_short: String(
      reason ||
        (isEnglish
          ? "Could not classify page safely."
          : "Kunne ikke klassifisere siden trygt.")
    ),
    visible_signals: {
      has_nav: false,
      has_headline: false,
      has_cta: false,
      has_contact_info: false,
      has_multiple_sections: false,
      mostly_blank: false,
      error_like: false,
      placeholder_like: false,
    },
    findings: { visual: [], mobile: [], seo: [] },
  };
}

function normalizeFindings(raw) {
  const empty = { visual: [], mobile: [], seo: [] };
  if (!raw || typeof raw !== "object") return empty;

  const cleanCategory = (arr, max) =>
    Array.isArray(arr)
      ? arr
          .map((f) => ({
            issue: String(f?.issue || "").trim(),
            consequence: String(f?.consequence || "").trim(),
            severity: ["low", "medium", "high"].includes(f?.severity)
              ? f.severity
              : "medium",
          }))
          .filter((f) => f.issue && f.consequence)
          .slice(0, max)
      : [];

  return {
    visual: cleanCategory(raw.visual, 4),
    mobile: cleanCategory(raw.mobile, 3),
    seo: cleanCategory(raw.seo, 3),
  };
}

function normalizeStructuredAnalysis(data, languageArg = "no") {
  const fallback = buildFallbackStructuredAnalysis(
    "Could not normalize structured analysis",
    languageArg
  );

  const normalized = {
    page_type: normalizePageType(data?.page_type || fallback.page_type),
    confidence: Number.isFinite(Number(data?.confidence))
      ? Math.max(0, Math.min(1, Number(data.confidence)))
      : fallback.confidence,
    should_generate_comment: Boolean(data?.should_generate_comment),
    score: Number.isFinite(Number(data?.score))
      ? Math.max(0, Math.min(10, Math.round(Number(data.score))))
      : fallback.score,
    strengths: Array.isArray(data?.strengths)
      ? data.strengths
          .map((x) => String(x || "").trim())
          .filter(Boolean)
          .slice(0, 3)
      : [],
    issues: Array.isArray(data?.issues)
      ? data.issues
          .map((x) => String(x || "").trim())
          .filter(Boolean)
          .slice(0, 6)
      : [],
    evidence: Array.isArray(data?.evidence)
      ? data.evidence
          .map((x) => String(x || "").trim())
          .filter(Boolean)
          .slice(0, 8)
      : [],
    reason_short: String(data?.reason_short || fallback.reason_short).trim(),
    visible_signals: {
      has_nav: Boolean(data?.visible_signals?.has_nav),
      has_headline: Boolean(data?.visible_signals?.has_headline),
      has_cta: Boolean(data?.visible_signals?.has_cta),
      has_contact_info: Boolean(data?.visible_signals?.has_contact_info),
      has_multiple_sections: Boolean(data?.visible_signals?.has_multiple_sections),
      mostly_blank: Boolean(data?.visible_signals?.mostly_blank),
      error_like: Boolean(data?.visible_signals?.error_like),
      placeholder_like: Boolean(data?.visible_signals?.placeholder_like),
    },
    findings: normalizeFindings(data?.findings),
  };

  if (!normalized.issues.length) {
    normalized.issues = fallback.issues;
  }

  if (!normalized.evidence.length) {
    normalized.evidence = [normalized.reason_short || fallback.reason_short];
  }

  if (normalized.score <= 4) {
    normalized.strengths = [];
  }

  return normalized;
}

function buildScoreFallbackFromManifest(manifest, reasonOverride = "") {
  const state = guessFallbackPageState(manifest);

  if (!state.reachable) {
    return {
      reachable: false,
      score: 0,
      severity: "low",
      reason: reasonOverride || state.reason,
      page_type: state.page_type,
    };
  }

  return {
    reachable: true,
    score: 5,
    severity: "medium",
    reason: reasonOverride || "AI score fallback used",
    page_type: "real_site",
  };
}

function buildAnalysisFallbackFromManifest(manifest, languageArg = "no", reason = "") {
  const isEnglish = String(languageArg || "no").toLowerCase() === "en";
  const state = guessFallbackPageState(manifest);
  const pageHeight = Number(manifest?.page_height || 0);
  const viewportHeight = Number(manifest?.viewport_desktop?.height || 900);
  const hasMultipleSections = pageHeight > viewportHeight * 1.8;

  if (!state.reachable) {
    const structured = normalizeStructuredAnalysis(
      {
        page_type: state.page_type,
        confidence: 0.65,
        should_generate_comment: false,
        score: 0,
        strengths: [],
        issues: [
          isEnglish
            ? "The page does not look like a normal reachable business website right now."
            : "Siden ser ikke ut som en vanlig tilgjengelig bedriftsnettside akkurat nå.",
        ],
        evidence: [reason || state.reason],
        reason_short: reason || state.reason,
        visible_signals: {
          has_nav: false,
          has_headline: false,
          has_cta: false,
          has_contact_info: false,
          has_multiple_sections: false,
          mostly_blank: false,
          error_like: true,
          placeholder_like: true,
        },
        findings: { visual: [], mobile: [], seo: [] },
      },
      languageArg
    );

    const comment = getDefaultFallbackComment(isEnglish ? "en" : "no");

    return {
      mode: "fallback",
      model: process.env.OPENAI_MODEL || "gpt-4.1-mini",
      screenshotModeUsed: getCurrentScreenshotMode(),
      page_type: structured.page_type,
      confidence: structured.confidence,
      should_generate_comment: structured.should_generate_comment,
      score: structured.score,
      strengths: structured.strengths,
      issues: structured.issues,
      evidence: structured.evidence,
      visible_signals: structured.visible_signals,
      reason_short: structured.reason_short,
      comment_no: comment,
      ai_middle: comment,
      raw_output_text: comment,
      raw_analysis_json: "",
    };
  }

  const structured = normalizeStructuredAnalysis(
    {
      page_type: "real_site",
      confidence: 0.48,
      should_generate_comment: true,
      score: 5,
      strengths: hasMultipleSections
        ? [
            isEnglish
              ? "The site has enough visible structure to feel like a real business page."
              : "Siden har nok synlig struktur til å føles som en ekte bedriftsnettside.",
          ]
        : [],
      issues: [
        isEnglish
          ? "The page feels more basic and less polished visually than it could."
          : "Siden føles mer enkel og mindre gjennomført visuelt enn den kunne vært.",
        isEnglish
          ? "The layout and spacing do not feel as clear or controlled as they could."
          : "Oppsettet og luftingen føles ikke like tydelig eller kontrollert som de kunne vært.",
      ],
      evidence: [
        isEnglish
          ? "The screenshots show a normal website, but the overall visual finish looks fairly modest."
          : "Skjermbildene viser en vanlig nettside, men det visuelle helhetsinntrykket virker ganske beskjedent.",
        reason || (isEnglish ? "AI full-analysis fallback used." : "Fallback for full analyse ble brukt."),
      ],
      reason_short:
        reason ||
        (isEnglish
          ? "Used fallback analysis because the AI full analysis did not finish cleanly."
          : "Brukte fallback-analyse fordi AI-fullanalysen ikke fullførte rent."),
      visible_signals: {
        has_nav: true,
        has_headline: true,
        has_cta: false,
        has_contact_info: false,
        has_multiple_sections: hasMultipleSections,
        mostly_blank: false,
        error_like: false,
        placeholder_like: false,
      },
      findings: { visual: [], mobile: [], seo: [] },
    },
    languageArg
  );

  const comment = buildDeterministicStructuredComment(structured, languageArg);

  return {
    mode: "fallback",
    model: process.env.OPENAI_MODEL || "gpt-4.1-mini",
    screenshotModeUsed: getCurrentScreenshotMode(),
    page_type: structured.page_type,
    confidence: structured.confidence,
    should_generate_comment: structured.should_generate_comment,
    score: structured.score,
    strengths: structured.strengths,
    issues: structured.issues,
    evidence: structured.evidence,
    visible_signals: structured.visible_signals,
    reason_short: structured.reason_short,
    comment_no: comment,
    ai_middle: comment,
    raw_output_text: comment,
    raw_analysis_json: "",
  };
}

/* ======================
   COMMENT HELPERS
====================== */

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

function classifyIssueFamily(text) {
  const t = String(text || "").trim().toLowerCase();

  if (
    /gammeldags|outdated|basic|enkelt|polish|polert|gjennomført|modern|moderne|dated|visuelt svak/.test(
      t
    )
  ) {
    return "overall_style";
  }

  if (
    /tett|dense|lite luft|hard to read|tung å lese|small text|liten tekst|brødtekst|tekstblokker|lesbar/.test(
      t
    )
  ) {
    return "text_density";
  }

  if (/button|buttons|knapp|knapper|cta/.test(t)) {
    return "cta";
  }

  if (/footer|bunntekst/.test(t)) {
    return "footer";
  }

  if (/image|images|photo|bilder|bilde|skewed|skrå/.test(t)) {
    return "image";
  }

  if (/navigation|menu|meny|top line|topplinje|lenker|link row/.test(t)) {
    return "navigation";
  }

  if (/form|skjema|contact form|kontaktfelt/.test(t)) {
    return "form";
  }

  if (/spacing|space|luft|seksjon|section|alignment|plassering|hero/.test(t)) {
    return "spacing_layout";
  }

  if (/font|script font|skrift/.test(t)) {
    return "font";
  }

  return t;
}

function dedupeIssuesByFamily(issues = []) {
  const seen = new Set();
  const out = [];

  for (const issue of issues) {
    const value = String(issue || "").trim();
    if (!value) continue;

    const family = classifyIssueFamily(value);
    if (seen.has(family)) continue;

    seen.add(family);
    out.push(value);
  }

  return out;
}

function compressStructuredForWriting(structured) {
  const score = Number(structured?.score || 0);

  const strengths = uniqueCaseInsensitive(structured?.strengths || []);
  const issues = dedupeIssuesByFamily(uniqueCaseInsensitive(structured?.issues || []));
  const evidence = uniqueCaseInsensitive(structured?.evidence || []);

  let maxStrengths = 1;
  let maxIssues = 4;
  let maxEvidence = 6;

  if (score <= 4) {
    maxStrengths = 0;
    maxIssues = 4;
    maxEvidence = 6;
  } else if (score === 5) {
    maxStrengths = 1;
    maxIssues = 4;
    maxEvidence = 6;
  } else if (score >= 8) {
    maxStrengths = 1;
    maxIssues = 2;
    maxEvidence = 4;
  }

  return {
    ...structured,
    strengths: strengths.slice(0, maxStrengths),
    issues: issues.slice(0, maxIssues),
    evidence: evidence.slice(0, maxEvidence),
    findings: structured?.findings || { visual: [], mobile: [], seo: [] },
  };
}

function buildLanguageAwareFallbackComment(structured, languageArg = "no") {
  const isEnglish = String(languageArg || "no").toLowerCase() === "en";

  const pageType = String(structured?.page_type || "unclear");
  const issue1 = getLanguageSafeText(structured?.issues?.[0] || "", languageArg);
  const issue2 = getLanguageSafeText(
    structured?.issues?.[1] || structured?.evidence?.[0] || "",
    languageArg
  );
  const reason = getLanguageSafeText(
    structured?.reason_short || structured?.evidence?.[0] || "",
    languageArg
  );

  if (
    pageType !== "real_site" ||
    !structured?.should_generate_comment ||
    Number(structured?.confidence || 0) < 0.55
  ) {
    if (isEnglish) {
      return (
        "This does not look like a normal finished business website in the screenshots, " +
        "so it would be misleading to write a standard website critique here."
      );
    }

    return (
      "Dette ser ikke ut som en vanlig ferdig bedriftsnettside i skjermbildene, " +
      "så det blir misvisende å skrive en vanlig nettsidekommentar her."
    );
  }

  if (isEnglish) {
    const first =
      issue1 || "The page structure and clarity are weaker than they should be";
    const second =
      issue2 ||
      reason ||
      "That makes the first impression feel weaker than it should";

    return `${ensureSentence(upperFirst(first))} ${ensureSentence(
      upperFirst(second)
    )}`;
  }

  const first =
    issue1 || "Strukturen og tydeligheten på siden er svakere enn den burde være";
  const second =
    issue2 ||
    reason ||
    "Det gjør at førsteinntrykket føles svakere enn det burde";

  return `${ensureSentence(upperFirst(first))} ${ensureSentence(
    upperFirst(second)
  )}`;
}

function buildDeterministicStructuredComment(structured, languageArg = "no") {
  const isEnglish = String(languageArg || "no").toLowerCase() === "en";

  if (
    String(structured?.page_type || "") !== "real_site" ||
    !structured?.should_generate_comment ||
    Number(structured?.confidence || 0) < 0.55
  ) {
    return buildLanguageAwareFallbackComment(structured, languageArg);
  }

  const score = Number(structured?.score || 0);
  const strengths = uniqueCaseInsensitive(
    (structured?.strengths || []).map((x) => getLanguageSafeText(x, languageArg))
  ).filter(Boolean);

  const issues = uniqueCaseInsensitive(
    (structured?.issues || []).map((x) => getLanguageSafeText(x, languageArg))
  ).filter(Boolean);

  const parts = [];

  if (isEnglish) {
    if (strengths[0] && score >= 5) {
      parts.push(ensureSentence(upperFirst(strengths[0])));
    }

    if (issues[0]) {
      parts.push(
        ensureSentence(
          strengths[0] && score >= 5
            ? `That said, ${lowerFirst(issues[0])}`
            : upperFirst(issues[0])
        )
      );
    }

    if (issues[1]) {
      parts.push(ensureSentence(upperFirst(issues[1])));
    }

    if (issues[2] && score <= 6) {
      parts.push(ensureSentence(upperFirst(issues[2])));
    }
  } else {
    if (strengths[0] && score >= 5) {
      parts.push(ensureSentence(upperFirst(strengths[0])));
    }

    if (issues[0]) {
      parts.push(
        ensureSentence(
          strengths[0] && score >= 5
            ? `Det er bra, men ${lowerFirst(issues[0])}`
            : upperFirst(issues[0])
        )
      );
    }

    if (issues[1]) {
      parts.push(ensureSentence(upperFirst(issues[1])));
    }

    if (issues[2] && score <= 6) {
      parts.push(ensureSentence(upperFirst(issues[2])));
    }
  }

  const fallback = buildLanguageAwareFallbackComment(structured, languageArg);
  return cleanGeneratedText(parts.join(" ")) || fallback;
}

function stripLowScoreFlattery(text, structured, languageArg = "no") {
  const score = Number(structured?.score || 0);
  if (score > 4) return text;

  const sentences = splitSentences(text);
  if (!sentences.length) return text;

  const first = String(sentences[0] || "").trim().toLowerCase();
  const lowScorePositivePatterns = [
    "navigasjonen er tydelig",
    "menyen er tydelig",
    "fargebruken",
    "fargeskjemaet",
    "oversiktlig",
    "ryddig",
    "fin sammenheng",
    "god grunnstruktur",
    "godt utgangspunkt",
    "clear navigation",
    "clear menu",
    "consistent color",
    "good structure",
    "clean layout",
    "easy to use",
  ];

  const startsTooPositive = lowScorePositivePatterns.some((p) =>
    first.includes(p)
  );

  if (!startsTooPositive) return text;

  const rest = sentences.slice(1).join(" ").trim();
  if (rest) return rest;

  return buildLanguageAwareFallbackComment(structured, languageArg);
}

function hasConcreteUiLanguage(text) {
  const t = String(text || "").toLowerCase();

  return [
    /menu/,
    /navigation/,
    /top line/,
    /topplinje/,
    /lenker/,
    /hero/,
    /button|buttons|knapp|knapper|cta/,
    /form|skjema/,
    /footer|bunntekst/,
    /icon|icons|ikon/,
    /text block|text blocks|tekstblokk|tekstblokker/,
    /font|script font|skrift/,
    /background|bakgrunn/,
    /spacing|luft|seksjon|section/,
    /image|images|bilde|bilder/,
    /headline|overskrift/,
    /kontakt/,
  ].some((pattern) => pattern.test(t));
}

function isTooGenericComment(text) {
  const t = String(text || "").toLowerCase();

  return [
    "room for improvement",
    "could be more polished",
    "feel smoother",
    "stronger visually",
    "clear room to improve clarity",
    "more refined",
    "better presentation",
    "mer gjennomført",
    "mer polert",
    "tydeligere og mer gjennomført",
    "the site works, but there is still clear room to improve clarity",
    "med noen få visuelle justeringer",
    "the whole page could feel",
  ].some((phrase) => t.includes(phrase));
}

function shouldRetrySpecificRewrite(text, structured) {
  const cleaned = cleanGeneratedText(text);
  if (!cleaned) return true;

  const issueCount = Array.isArray(structured?.issues) ? structured.issues.length : 0;

  if (issueCount >= 2 && !hasConcreteUiLanguage(cleaned)) return true;
  if (isTooGenericComment(cleaned)) return true;

  return false;
}

/* ======================
   OPENAI CALL WRAPPERS
====================== */

async function createOpenAIResponse(client, payload, timeoutMs, label) {
  const startedAt = Date.now();

  try {
    const res = await withTimeout(
      client.responses.create(payload),
      timeoutMs,
      label
    );

    return res;
  } catch (err) {
    const message = safeErrorMessage(err, `${label} failed`);
    const elapsed = Date.now() - startedAt;
    const enriched = new Error(`${label} failed after ${elapsed}ms: ${message}`);
    enriched.original = err;
    throw enriched;
  }
}

/* ======================
   MOCK analyzer
====================== */

async function analyzeWithMock(manifest, bundle) {
  const isShort =
    typeof bundle.pageHeight === "number" &&
    typeof bundle.viewportHeight === "number" &&
    bundle.pageHeight <= bundle.viewportHeight + 50;

  const comment = isShort
    ? "Forsiden virker ganske enkel og litt tynn, så førsteinntrykket blir svakere enn det kunne vært."
    : "Nettsiden fungerer, men uttrykket virker ganske enkelt og lite gjennomført visuelt.";

  return {
    mode: "mock",
    screenshotModeUsed: getCurrentScreenshotMode(),
    page_type: "real_site",
    confidence: 0.6,
    should_generate_comment: true,
    score: isShort ? 4 : 5,
    strengths: isShort ? [] : ["Siden har nok innhold til å kunne vurderes visuelt."],
    issues: isShort
      ? ["Forsiden virker ganske kort og litt tynn visuelt."]
      : ["Oppsettet virker ganske enkelt og kunne vært tydeligere visuelt."],
    evidence: isShort
      ? ["Topputsnittet virker kort og inneholder lite variasjon."]
      : ["Det er nok innhold til å se struktur, men helheten virker enkel."],
    visible_signals: {
      has_nav: true,
      has_headline: true,
      has_cta: false,
      has_contact_info: false,
      has_multiple_sections: !isShort,
      mostly_blank: false,
      error_like: false,
      placeholder_like: false,
    },
    reason_short: isShort
      ? "Forsiden virker kort og visuelt tynn."
      : "Siden er brukbar, men visuelt ganske enkel.",
    comment_no: comment,
    ai_middle: comment,
    raw_output_text: "",
    raw_analysis_json: "",
  };
}

const PAGE_ANALYSIS_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    page_type: {
      type: "string",
      enum: [
        "real_site",
        "placeholder_page",
        "parking_page",
        "thin_page",
        "broken_page",
        "unreachable",
        "social_only",
        "platform_listing",
        "under_construction",
        "unclear",
      ],
    },
    confidence: { type: "number", minimum: 0, maximum: 1 },
    should_generate_comment: { type: "boolean" },
    score: { type: "integer", minimum: 0, maximum: 10 },
    strengths: {
      type: "array",
      items: { type: "string" },
      maxItems: 3,
    },
    issues: {
      type: "array",
      items: { type: "string" },
      maxItems: 6,
    },
    evidence: {
      type: "array",
      items: { type: "string" },
      maxItems: 8,
    },
    reason_short: { type: "string" },
    visible_signals: {
      type: "object",
      additionalProperties: false,
      properties: {
        has_nav: { type: "boolean" },
        has_headline: { type: "boolean" },
        has_cta: { type: "boolean" },
        has_contact_info: { type: "boolean" },
        has_multiple_sections: { type: "boolean" },
        mostly_blank: { type: "boolean" },
        error_like: { type: "boolean" },
        placeholder_like: { type: "boolean" },
      },
      required: [
        "has_nav",
        "has_headline",
        "has_cta",
        "has_contact_info",
        "has_multiple_sections",
        "mostly_blank",
        "error_like",
        "placeholder_like",
      ],
    },
    findings: {
      type: "object",
      additionalProperties: false,
      properties: {
        visual: {
          type: "array",
          maxItems: 4,
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              issue: { type: "string" },
              consequence: { type: "string" },
              severity: { type: "string", enum: ["low", "medium", "high"] },
            },
            required: ["issue", "consequence", "severity"],
          },
        },
        mobile: {
          type: "array",
          maxItems: 3,
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              issue: { type: "string" },
              consequence: { type: "string" },
              severity: { type: "string", enum: ["low", "medium", "high"] },
            },
            required: ["issue", "consequence", "severity"],
          },
        },
        seo: {
          type: "array",
          maxItems: 3,
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              issue: { type: "string" },
              consequence: { type: "string" },
              severity: { type: "string", enum: ["low", "medium", "high"] },
            },
            required: ["issue", "consequence", "severity"],
          },
        },
      },
      required: ["visual", "mobile", "seo"],
    },
  },
  required: [
    "page_type",
    "confidence",
    "should_generate_comment",
    "score",
    "strengths",
    "issues",
    "evidence",
    "reason_short",
    "visible_signals",
    "findings",
  ],
};

/* ======================
   AI WRITING PASS
====================== */

async function runWritingPass({
  client,
  model,
  outputLanguageName,
  prompt,
  writingStructured,
}) {
  const commentResponse = await createOpenAIResponse(
    client,
    {
      model,
      temperature: 0.7,
      instructions:
        `You are writing a complete outreach email. ` +
        `LANGUAGE RULE — THIS IS ABSOLUTE: Every single word in the email must be in ${outputLanguageName}. ` +
        `The greeting must be in ${outputLanguageName}. The closing must be in ${outputLanguageName}. ` +
        `Do NOT mix languages. Do NOT use English words if the language is not English. ` +
        `For example, if writing in Norwegian: "Hei" not "Hi", "Hei der" not "Hei there", "nettsiden" not "website". ` +
        `Follow the WRITING RULES exactly — they specify the recipient name, sender name, tone, structure, finding selection, and closing goal. ` +
        `If the rules provide a SENDER section with a name and company, you MUST introduce yourself using that name and company in the opening. This is not optional. ` +
        `If the rules provide a RECIPIENT section with a first name, you MUST use that name in the greeting. ` +
        `Use the WEBSITE ANALYSIS (especially the findings.visual / findings.mobile / findings.seo arrays) as the factual basis for the critique. Do not invent observations. Skip categories where findings are empty — never announce them, never give filler praise. ` +
        `Sound like a real human, not a template. Vary your phrasing. ` +
        `End the body on the closing sentence with NO sign-off — no name, no "Mvh", no "Best regards", nothing. A signature is appended automatically downstream. ` +
        `Return plain text only. No HTML. No markdown. No subject line.`,
      input: [
        {
          role: "user",
          content: [
            {
              type: "input_text",
              text:
                `WRITING RULES:\n${prompt}\n\n` +
                `LANGUAGE: ${outputLanguageName}\n\n` +
                `WEBSITE ANALYSIS (use these facts for the critique section):\n${JSON.stringify(
                  writingStructured,
                  null,
                  2
                )}\n` +
                `
Write the complete email now. Follow the WRITING RULES exactly.

CHECKLIST — verify before returning:
- Is the greeting in ${outputLanguageName}? (e.g. "Hei [name]" for Norwegian, not "Hi [name]")
- Did you use the recipient's first name in the greeting (if provided in RECIPIENT section)?
- Did you introduce yourself with sender name and company (if provided in SENDER section)?
- Is every word in ${outputLanguageName}? No English words mixed in?
- Did you draw findings from the WEBSITE ANALYSIS (findings.visual / mobile / seo) as instructed in the WRITING RULES, weaving them into one flowing message instead of listing them?
- Did you skip categories where findings are empty without announcing them?
- Does the closing match the CLOSING GOAL from the rules?
- The body MUST end on the closing sentence with NO sign-off, NO name at the bottom, NO "Mvh"/"Hilsen"/"Best regards"/"Thanks". A signature is appended automatically after the body.

Write the email now. Every word in ${outputLanguageName}.`,
            },
          ],
        },
      ],
      max_output_tokens: 600,
    },
    OPENAI_WRITE_TIMEOUT_MS,
    "OpenAI full email"
  );

  return cleanGeneratedText(normalizeComment(extractTextFromResponse(commentResponse)));
}

/* ======================
   REAL AI analyzer
====================== */

async function analyzeWithAI(
  manifest,
  bundle,
  languageArg,
  engineArg,
  promptOverride = ""
) {
  const engine = String(
    engineArg || process.env.ANALYZER_MODE || "openai"
  ).toLowerCase();

  if (engine === "mock") return analyzeWithMock(manifest, bundle);
  if (engine !== "openai") {
    throw new Error(`Unsupported ANALYZER_MODE: ${engine}`);
  }

  const apiKey = process.env.OPENAI_API_KEY;
  // Two distinct models: a cheap/fast one for the structured classify pass,
  // and a stronger one for the user-facing email writing pass. Voice quality
  // (avoiding corporate filler, foreign-language leaks, etc.) depends heavily
  // on the writing model.
  const classifyModel =
    process.env.OPENAI_MODEL_SCORE ||
    process.env.OPENAI_MODEL ||
    "gpt-4.1-mini";
  const writingModel =
    process.env.OPENAI_MODEL_COMMENT ||
    process.env.OPENAI_MODEL ||
    classifyModel;
  if (!apiKey) throw new Error("OPENAI_API_KEY is missing in .env");

  const screenshotMode = getCurrentScreenshotMode();
  const imagePaths = getImagePathsForMode(manifest);

  const langCode = String(languageArg || "no").toLowerCase();
  const outputLanguageNames = {
    en: "English", no: "Norwegian Bokmål", sv: "Swedish", da: "Danish",
    de: "German", fr: "French", es: "Spanish", nl: "Dutch",
    fi: "Finnish", pt: "Portuguese", it: "Italian", pl: "Polish",
  };
  const isEnglish = langCode === "en";
  const outputLanguageName = outputLanguageNames[langCode] || "Norwegian Bokmål";

  const prompt =
    String(promptOverride || "").trim() ||
    (isEnglish
      ? "Write only the requested outreach middle section based only on what is clearly visible. Do not greet. Do not sign off. Keep it concrete, human, and specific."
      : "Skriv kun den ønskede midtdelen til outreach basert bare på det som er tydelig synlig. Ikke hils. Ikke signer av. Hold det konkret, menneskelig og spesifikt.");

  const client = new OpenAI({ apiKey });

  const sharedContext =
    `CONTEXT (from capture manifest):\n` +
    `- URL: ${
      manifest.final_url || manifest.attempted_url || manifest.input_url || "unknown"
    }\n` +
    `- Title: ${manifest.homepage_title || "unknown"}\n` +
    `- Page height: ${manifest.page_height || "unknown"}\n` +
    `- Viewport: ${
      manifest.viewport_desktop?.width || 1440
    }x${manifest.viewport_desktop?.height || 900}\n` +
    `- Image order: ${imagePaths.length === 3 ? "TOP, MID, BOTTOM" : "TOP"}\n`;

  const mobileImagePath = getMobileImagePath(manifest);
  const seoSummary = formatSeoForPrompt(manifest.seo);

  // Build the user content as a labeled sequence so the model knows which
  // image is desktop vs mobile, and reads SEO context as text.
  const userContent = [
    { type: "input_text", text: `${sharedContext}\nDESKTOP SCREENSHOT${imagePaths.length > 1 ? "S (in order: top, mid, bottom)" : ""}:` },
    ...imagePaths.map((imgPath) => ({
      type: "input_image",
      image_url: imageToDataUrl(imgPath),
    })),
  ];

  if (mobileImagePath) {
    userContent.push({
      type: "input_text",
      text: `\nMOBILE HERO (iPhone emulation, viewport 390x664, real mobile UA — this is what a phone visitor actually sees above the fold):`,
    });
    userContent.push({
      type: "input_image",
      image_url: imageToDataUrl(mobileImagePath),
    });
  }

  if (seoSummary) {
    userContent.push({
      type: "input_text",
      text:
        `\nSEO SIGNALS (scraped from the live page DOM — these are facts, not guesses):\n${seoSummary}`,
    });
  }

  userContent.push({
    type: "input_text",
    text:
      `\nTask:\n` +
      `1. Classify the page strictly (page_type, score, etc).\n` +
      `2. Fill the findings object with categorized observations:\n` +
      `   - findings.visual: design/layout/clarity issues you can see in the DESKTOP screenshots.\n` +
      `   - findings.mobile: issues specific to the MOBILE hero shot OR derived from SEO viewport meta. Examples: hero text too small on phone, CTA hidden below fold, broken nav, viewport meta forces desktop width on phones.\n` +
      `   - findings.seo: issues derived ONLY from the SEO SIGNALS block above. Examples: title is just the domain, meta description missing/too long, no H1 or many H1s, low alt-text coverage on images, broken charset (mojibake), no og:image for social sharing.\n` +
      `3. Each finding must have:\n` +
      `   - issue: WHAT is wrong, in plain everyday language. One short sentence.\n` +
      `   - consequence: the human cost — what it means for a real visitor or for the business. NOT "reduces conversion rate". Instead: "people leave before they understand what you do", "phones don't ring", "you're invisible when someone Googles your service".\n` +
      `   - severity: low/medium/high based on how badly it hurts the visitor experience or discoverability.\n` +
      `4. CRITICAL — leave a category as an empty array [] if there is genuinely nothing wrong there. Do NOT pad. A site with a great SEO setup and a clean mobile experience should have findings.seo: [] and findings.mobile: []. Only include real, verifiable problems.\n` +
      `5. Do NOT include praise in findings — those are issues only. Use strengths[] for positives.\n` +
      `6. Do NOT speculate about anything not visible in the screenshots or SEO data. If you cannot prove it, do not include it.\n` +
      `7. The legacy strengths/issues/evidence fields stay in use — fill them as before. The new findings object is in addition.\n` +
      `8. Do not write outreach text yet. Return classification data only.\n`,
  });

  let structured;
  let rawStructuredText = "";

  try {
    const classifyResponse = await createOpenAIResponse(
      client,
      {
        model: classifyModel,
        temperature: 0.2,
        text: {
          format: {
            type: "json_schema",
            name: "website_page_analysis",
            strict: true,
            schema: PAGE_ANALYSIS_SCHEMA,
          },
        },
        instructions:
          "You are a strict but realistic website screenshot classifier. " +
          `All free-text fields in the JSON must be written in ${outputLanguageName}. ` +
          `Never mix languages in strengths, issues, evidence, or reason_short. ` +
          "Classify what the page actually is before any outreach writing. " +
          "Never invent strengths. Never give fake praise. " +
          "Score visual design quality, not business legitimacy. " +
          "Do not force low scores onto decent normal business websites. " +
          "Do not reward a site just because it has many sections or contact details. " +
          "Focus on modern vs outdated look, spacing, density, clarity, button visibility, image placement, and overall first impression. " +
          "Use simple everyday language in issues and evidence. " +
          "Do not treat Norwegian language on a .no site as a weakness. " +
          "Only include the strongest clearly visible issues. " +
          "CRITICAL DISTINCTION — real vs placeholder. An ugly, plain, outdated, or bare-looking site is NOT a placeholder. If the page contains real content (multiple links pointing to real destinations, product or service names, company information, contact details, news items, article lists, any actual information a visitor could read or act on) then it IS a real_site, even if it looks like it was built in 1995, has no images, no styling, no clear sections, no modern navigation, and no footer. Set page_type=real_site, should_generate_comment=true, and give it a LOW score. An ugly real site is the whole point of the product — it is the best kind of lead, not a reject. " +
          "A page is only a placeholder_page / parking_page / under_construction if you can see literal placeholder signals: the words 'Coming soon' / 'Kommer snart' / 'Her kommer' / 'Under construction' / 'Site under development' / 'Parked domain' / 'Domain for sale', OR the page shows effectively nothing (just a logo, just a domain name, just 1-3 words, completely blank). If there is a wall of plain-text links or several lines of actual readable content, it is a real_site. When in doubt between 'ugly real site' and 'placeholder', choose real_site. " +
          "should_generate_comment=false is reserved for truly unscoreable pages: broken, parked, blank, literal 'coming soon'. Never set it to false just because the design is bad or the page looks dated. " +
          "FINDINGS — when filling findings.visual / findings.mobile / findings.seo, write the issue and consequence in everyday language a small business owner would understand. Each consequence must name a real human cost: people leaving, not trusting the site, not calling, not finding the business in search. Never use jargon like 'bounce rate' or 'conversion' or 'CTR'. Keep severity honest — most things are 'medium'; reserve 'high' for issues that genuinely lose customers (broken mobile layout, invisible CTA, page invisible in search). Empty arrays are correct and expected when a category is genuinely fine.",
        input: [
          {
            role: "user",
            content: userContent,
          },
        ],
        max_output_tokens: 1400,
      },
      OPENAI_CLASSIFY_TIMEOUT_MS,
      "OpenAI classify"
    );

    rawStructuredText = normalizeComment(extractTextFromResponse(classifyResponse));

    const parsedStructured = safeJsonParseFromText(rawStructuredText);

    structured = normalizeStructuredAnalysis(
      parsedStructured ||
        buildFallbackStructuredAnalysis(
          "Could not parse structured analysis JSON",
          languageArg
        ),
      languageArg
    );
  } catch (err) {
    const fallback = buildAnalysisFallbackFromManifest(
      manifest,
      languageArg,
      safeErrorMessage(err, "Structured analysis failed")
    );

    return {
      ...fallback,
      raw_analysis_json: rawStructuredText || "",
    };
  }

  const writingStructured = compressStructuredForWriting(structured);

  if (
    structured.page_type !== "real_site" ||
    !structured.should_generate_comment ||
    structured.confidence < 0.55
  ) {
    const fallbackComment = buildLanguageAwareFallbackComment(
      structured,
      languageArg
    );

    return {
      mode: "openai",
      model: writingModel,
      screenshotModeUsed: screenshotMode,
      page_type: structured.page_type,
      confidence: structured.confidence,
      should_generate_comment: structured.should_generate_comment,
      score: structured.score,
      strengths: structured.strengths,
      issues: structured.issues,
      evidence: structured.evidence,
      visible_signals: structured.visible_signals,
      reason_short: structured.reason_short,
      comment_no: fallbackComment,
      ai_middle: fallbackComment,
      raw_output_text: fallbackComment,
      raw_analysis_json: rawStructuredText,
    };
  }

  let rawText = "";

  try {
    rawText = await runWritingPass({
      client,
      model: writingModel,
      outputLanguageName,
      prompt,
      writingStructured,
    });

    rawText = cleanGeneratedText(rawText);
  } catch (_) {
    rawText = "";
  }

  if (!rawText) {
    rawText = buildDeterministicStructuredComment(writingStructured, languageArg);
  }

  const cleaned =
    cleanGeneratedText(rawText) ||
    buildLanguageAwareFallbackComment(writingStructured, languageArg);

  return {
    mode: "openai",
    model: writingModel,
    screenshotModeUsed: screenshotMode,
    page_type: structured.page_type,
    confidence: structured.confidence,
    should_generate_comment: structured.should_generate_comment,
    score: structured.score,
    strengths: structured.strengths,
    issues: structured.issues,
    evidence: structured.evidence,
    visible_signals: structured.visible_signals,
    reason_short: structured.reason_short,
    comment_no: cleaned,
    ai_middle: cleaned,
    raw_output_text: rawText,
    raw_analysis_json: rawStructuredText,
  };
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

/* ======================
   STAGE 2: FULL ANALYSIS
====================== */

async function runAnalysis(
  input,
  languageArg = "no",
  engineArg = "openai",
  promptOverride = ""
) {
  const manifestPath = getManifestPath(input);
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Manifest not found: ${manifestPath}`);
  }

  const manifest = readJson(manifestPath);
  if (manifest.capture_status !== "success") {
    throw new Error(`Capture not successful for manifest: ${manifestPath}`);
  }

  const bundle = buildInputBundle(manifest);

  let analysis;
  try {
    analysis = await analyzeWithAI(
      manifest,
      bundle,
      languageArg,
      engineArg,
      promptOverride
    );
  } catch (err) {
    analysis = buildAnalysisFallbackFromManifest(
      manifest,
      languageArg,
      safeErrorMessage(err, "runAnalysis failed")
    );
  }

  const result = {
    source_manifest: manifestPath,
    input_url: manifest.input_url,
    final_url: manifest.final_url,
    homepage_title: manifest.homepage_title,
    capture_timestamp: manifest.timestamp,
    analyzed_at: new Date().toISOString(),
    screenshots: {
      top: manifest.desktop_top_path || null,
      mid: manifest.desktop_mid_path || null,
      bottom: manifest.desktop_bottom_path || null,
      full: manifest.desktop_full_path || null,
    },
    analysis,
  };

  const base = safeFileName(
    manifest.input_url || manifest.final_url || "unknown"
  );
  const outPath = path.join(ANALYSIS_RESULTS_DIR, `${base}.analysis.json`);
  writeJson(outPath, result);

  return result;
}

/* ======================
   STAGE 1: SCORE-ONLY
====================== */

async function runScoreOnlyAnalysis(
  input,
  languageArg = "no",
  engineArg = "openai"
) {
  const manifestPath = getManifestPath(input);

  if (!fs.existsSync(manifestPath)) {
    return {
      reachable: false,
      score: 0,
      severity: "low",
      reason: "Manifest not found",
      page_type: "unreachable",
    };
  }

  const manifest = readJson(manifestPath);

  if (manifest.capture_status !== "success") {
    return {
      reachable: false,
      score: 0,
      severity: "low",
      reason: "Capture failed/unreachable",
      page_type: "unreachable",
    };
  }

  let imagePaths = [];
  try {
    imagePaths = getImagePathsForMode(manifest);
  } catch (_) {
    return buildScoreFallbackFromManifest(manifest, "Screenshot selection failed");
  }

  const engine = String(
    engineArg || process.env.ANALYZER_MODE || "openai"
  ).toLowerCase();

  if (engine === "mock") {
    return {
      reachable: true,
      score: 5,
      severity: "medium",
      reason: "Mock score",
      page_type: "real_site",
    };
  }

  const apiKey = process.env.OPENAI_API_KEY;
  const model =
    process.env.OPENAI_MODEL_SCORE ||
    process.env.OPENAI_MODEL ||
    "gpt-4.1-mini";
  if (!apiKey) {
    return buildScoreFallbackFromManifest(manifest, "OPENAI_API_KEY missing");
  }

  const client = new OpenAI({ apiKey });

  try {
    const response = await createOpenAIResponse(
      client,
      {
        model,
        instructions:
          "Return ONLY valid JSON with keys: reachable (boolean), score (0-10 integer), severity (low|medium|high), reason (short string), page_type (string). No markdown. No code fences.",
        input: [
          {
            role: "user",
            content: [
              {
                type: "input_text",
                text:
                  "Quickly qualify this website based only on the visible screenshots.\n\n" +
                  "IMPORTANT:\n" +
                  "- Score ONLY visual design quality.\n" +
                  "- Be realistic and moderately strict.\n" +
                  "- If a site looks usable, structured, and fairly clean, it often belongs around 5-6.\n" +
                  "- Do NOT reward the site just because it has lots of content, testimonials, menus, or contact info.\n" +
                  "- Focus on visual polish, spacing, text density, clarity, button visibility, image placement, and overall first impression.\n" +
                  "- Do NOT treat Norwegian language on a .no / Norwegian local business site as a weakness.\n" +
                  "- If it looks like a 404 page, browser error, forbidden page, parked domain, domain for sale page, blank page, coming soon page, or maintenance page, set reachable=false, score=0, and page_type to unreachable, broken_page, parking_page, placeholder_page, or under_construction.\n" +
                  "- CRITICAL: Pages that show ONLY a domain name (e.g. 'www.example.no'), 'Her kommer' / 'Coming soon' / 'Under construction' / 'Kommer snart' with no real content are NOT real websites. Set reachable=false, score=0, page_type=placeholder_page.\n" +
                  "- An ugly, plain, outdated, or bare-looking site is NOT a placeholder. If the page contains real content (multiple links to real destinations, product/service names, company or contact information, news items, article lists, any readable information) then it IS a real_site, even with no modern styling, no images, no clear sections, no visible footer, or a 1990s look. Set page_type=real_site and give it a LOW score. An ugly real site is a valid lead, not a reject.\n" +
                  "- A placeholder only qualifies when you see literal placeholder signals (the words 'Coming soon', 'Under construction', 'Parked', 'Domain for sale') OR the page is effectively blank (just a logo, just a domain, only 1-3 words). A wall of plain-text links is a real_site.\n" +
                  "- When in doubt between 'ugly real site' and 'placeholder', choose real_site.\n" +
                  "Return ONLY JSON.",
              },
              ...imagePaths.map((imgPath) => ({
                type: "input_image",
                image_url: imageToDataUrl(imgPath),
              })),
            ],
          },
        ],
        max_output_tokens: 220,
      },
      OPENAI_SCORE_TIMEOUT_MS,
      "OpenAI score-only"
    );

    const raw = normalizeComment(extractTextFromResponse(response));
    const parsed = safeJsonParseFromText(raw);

    if (!parsed) {
      return buildScoreFallbackFromManifest(manifest, "Could not parse score JSON");
    }

    return {
      reachable: Boolean(parsed.reachable),
      score: Math.max(0, Math.min(10, Number(parsed.score) || 0)),
      severity: String(parsed.severity || "medium"),
      reason: String(parsed.reason || ""),
      page_type: String(
        parsed.page_type || (parsed.reachable ? "real_site" : "unreachable")
      ),
    };
  } catch (err) {
    return buildScoreFallbackFromManifest(
      manifest,
      safeErrorMessage(err, "Score-only analysis failed")
    );
  }
}

// ======================
// EXPORTS
// ======================
module.exports = {
  runAnalysis,
  runScoreOnlyAnalysis,
  buildStoredAnalysisPayload,
};

// Keep CLI support
if (require.main === module) {
  const input = process.argv[2];
  const languageArg = process.argv[3] || "no";
  const engineArg = process.argv[4] || "openai";

  runAnalysis(input, languageArg, engineArg)
    .then((res) => {
      console.log("Analysis saved.");
      console.log(JSON.stringify(res.analysis, null, 2));
    })
    .catch((err) => {
      console.error("Analyzer error:", err);
      process.exit(1);
    });
}