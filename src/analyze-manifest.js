// src/analyze-manifest.js
const fs = require("fs");
const path = require("path");
require("dotenv").config();
const OpenAI = require("openai");

const { buildAnalyzerPrompt } = require("./analyzer-prompt");

// ✅ Use env var so this works on VPS/Linux too
const OUT_DIR = process.env.OUTPUT_DIR || "D:\\sidesone-screenshot-output";
const MANIFEST_DIR = path.join(OUT_DIR, "manifests");
const ANALYSIS_RESULTS_DIR = path.join(OUT_DIR, "analysis", "results");
const ANALYSIS_LOGS_DIR = path.join(OUT_DIR, "analysis", "logs");

// Ensure dirs exist
[ANALYSIS_RESULTS_DIR, ANALYSIS_LOGS_DIR].forEach((dir) => {
  fs.mkdirSync(dir, { recursive: true });
});

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
  // allow either: "mpower.one" OR full path to manifest json
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

function normalizeComment(text) {
  return String(text || "").replace(/\r/g, "").trim();
}

function extractTextFromResponse(response) {
  if (response && typeof response.output_text === "string" && response.output_text.trim()) {
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

// ✅ Guarantees the model never includes your email intro / greeting
function sanitizeOutreachComment(input) {
  let t = String(input || "").replace(/\r/g, "").trim();

  // Remove code fences / quotes
  t = t.replace(/```[\s\S]*?```/g, "").trim();
  t = t.replace(/^["'“”‘’\s]+/, "").trim();

  // Remove greeting
  t = t.replace(/^hei[,!.\s]+/i, "").trim();

  // Remove repeated email intro variants at the start
  t = t.replace(
    /^(jeg\s+tokk?\s+en\s+titt\s+gjennom\s+nettsiden\s+deres\s+og\s+la\s+merke\s+til\s+at[\s,:.-]*)/i,
    ""
  ).trim();

  t = t.replace(
    /^(jeg\s+tokk?\s+en\s+titt\s+gjennom\s+nettsiden\s+deres[\s,:.-]*)/i,
    ""
  ).trim();

  t = t.replace(
    /^(jeg\s+la\s+merke\s+til\s+at[\s,:.-]*)/i,
    ""
  ).trim();

  // Force lowercase start
  t = t.replace(/^([A-ZÆØÅ])/, (m) => m.toLowerCase());

  // Collapse weird whitespace
  t = t.replace(/\s+/g, " ").trim();

  return t;
}


function splitSentences(text) {
  return String(text || "")
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function startsWithForbiddenIntro(text) {
  const t = String(text || "").trim().toLowerCase();

  return (
    /^hei\b/.test(t) ||
    /^jeg\s+tokk?\s+en\s+titt\s+gjennom\s+nettsiden\s+deres\b/.test(t) ||
    /^jeg\s+la\s+merke\s+til\s+at\b/.test(t)
  );
}

function ensureSentenceEnding(text) {
  const t = String(text || "").trim().replace(/[.!?]+$/g, "");
  return t ? `${t}.` : "";
}

function buildSafeFallbackComment() {
  return [
    "nettsiden deres har et greit utgangspunkt visuelt.",
    "Det er bra, men oppsettet og flyten kunne vært tydeligere.",
    "Det gjør at førsteinntrykket kan føles litt mindre gjennomført enn det kunne vært.",
    "Vi ser flere konkrete ting som kunne blitt forbedret her, blant annet.",
  ].join(" ");
}

function repairOutreachComment(input) {
  let cleaned = sanitizeOutreachComment(input);
  let sentences = splitSentences(cleaned);

  if (sentences.length < 2) {
    return buildSafeFallbackComment();
  }

  if (sentences.length === 2) {
    sentences = [
      sentences[0],
      sentences[1],
      "Det gjør at siden kan føles litt mindre tydelig visuelt",
      "Vi ser flere konkrete ting som kunne blitt forbedret her, blant annet",
    ];
  } else if (sentences.length === 3) {
    sentences = [
      sentences[0],
      sentences[1],
      sentences[2],
      "Vi ser flere konkrete ting som kunne blitt forbedret her, blant annet",
    ];
  } else if (sentences.length > 4) {
    sentences = [
      sentences[0],
      sentences[1],
      sentences.slice(2, -1).join(" "),
      sentences[sentences.length - 1],
    ];
  }

  if (!sentences[0] || /^det er bra,\s*men/i.test(sentences[0])) {
    sentences[0] = "nettsiden deres har et greit utgangspunkt visuelt";
  }

  let second = String(sentences[1] || "").trim();
  second = second.replace(/^det er bra,\s*men\s*/i, "").trim();

  if (!second) {
    second = "oppsettet og flyten kunne vært tydeligere";
  }

  second = second.charAt(0).toLowerCase() + second.slice(1);
  sentences[1] = `Det er bra, men ${second}`;

  if (!sentences[2]) {
    sentences[2] =
      "Det gjør at siden kan føles litt mindre gjennomført ved førsteinntrykk";
  }

  let fourth = String(sentences[3] || "").trim().replace(/[.!?]+$/g, "");
  fourth = fourth.replace(/,\s*blant annet$/i, "").trim();
  fourth = fourth.replace(/\bblant annet\b$/i, "").trim();

  if (!fourth) {
    fourth = "Vi ser flere konkrete ting som kunne blitt forbedret her";
  }

  sentences[0] = ensureSentenceEnding(sentences[0]);
  sentences[1] = ensureSentenceEnding(sentences[1]);
  sentences[2] = ensureSentenceEnding(sentences[2]);
  sentences[3] = `${fourth}, blant annet.`;

  const finalText = sanitizeOutreachComment(sentences.join(" "));
  const validation = validateOutreachComment(finalText);

  return validation.ok ? finalText : buildSafeFallbackComment();
}

function validateOutreachComment(text) {
  const cleaned = String(text || "").trim();
  const sentences = splitSentences(cleaned);

  if (!cleaned) return { ok: false, reason: "Empty output" };
  if (startsWithForbiddenIntro(cleaned)) {
    return { ok: false, reason: "Repeated email intro" };
  }
  if (sentences.length !== 4) {
    return { ok: false, reason: "Not exactly 4 sentences" };
  }
  if (!sentences[1]?.toLowerCase().startsWith("det er bra, men")) {
    return { ok: false, reason: "Sentence 2 format invalid" };
  }
  if (!/blant annet\.$/i.test(sentences[3] || "")) {
    return { ok: false, reason: "Sentence 4 ending invalid" };
  }

  return { ok: true, reason: "" };
}

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

// ----------------------
// MOCK analyzer (optional fallback)
// ----------------------
async function analyzeWithMock(manifest, bundle) {
  const isShort =
    typeof bundle.pageHeight === "number" &&
    typeof bundle.viewportHeight === "number" &&
    bundle.pageHeight <= bundle.viewportHeight + 50;

  return {
    status: "ok",
    primary_category: isShort ? "hero" : "spacing",
    severity: isShort ? "low" : "medium",
    page_length_hint: isShort ? "short" : "normal",
    observations: isShort
      ? [
          {
            category: "hero",
            severity: "low",
            comment_no:
              "Forsiden virker ganske kort, så top, mid og bunn blir nesten samme utsnitt i analysen.",
          },
        ]
      : [
          {
            category: "spacing",
            severity: "medium",
            comment_no:
              "Siden har nok høyde til å vurdere struktur, men spacing og seksjonsrytme bør vurderes ut fra top, mid og bunn samlet.",
          },
        ],
    best_outreach_line_no: isShort
      ? "Nettsiden virker ganske kort i oppsettet, så førsteinntrykket blir fort over."
      : "Nettsiden har innhold, men oppsett og spacing mellom seksjoner kan bli tydeligere visuelt.",
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

// ----------------------
// REAL AI analyzer (OpenAI Responses API)
// ----------------------
// ✅ Added promptOverride so presets can control the prompt
async function analyzeWithAI(manifest, bundle, languageArg, engineArg, promptOverride = "") {
  const engine = String(engineArg || process.env.ANALYZER_MODE || "openai").toLowerCase();

  if (engine === "mock") return analyzeWithMock(manifest, bundle);
  if (engine !== "openai") throw new Error(`Unsupported ANALYZER_MODE: ${engine}`);

  const apiKey = process.env.OPENAI_API_KEY;
  const model = process.env.OPENAI_MODEL || "gpt-4.1-mini";
  if (!apiKey) throw new Error("OPENAI_API_KEY is missing in .env");

  // ✅ Screenshot mode comes from env set by job-runner
   const screenshotMode = getCurrentScreenshotMode();
   const imagePaths = getImagePathsForMode(manifest);

  // ✅ Prompt selection:
  // - if promptOverride provided (preset prompt), use it
  // - else fallback to default analyzer-prompt
  const prompt = String(promptOverride || "").trim();

if (!prompt) {
  throw new Error("No preset prompt resolved for this job.");
}

  const client = new OpenAI({ apiKey });

  const response = await client.responses.create({
    model,
    instructions:
      "You are analyzing website screenshots for a Norwegian cold outreach comment. Follow the user's provided rules exactly and return only the requested plain text output.",
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text:
              `${prompt}\n\n` +
              `KONTEKST (fra capture-manifest):\n` +
              `- URL: ${manifest.final_url || manifest.attempted_url || manifest.input_url || "ukjent"}\n` +
              `- Tittel: ${manifest.homepage_title || "ukjent"}\n` +
              `- Sidehøyde: ${manifest.page_height || "ukjent"}\n` +
              `- Viewport: ${manifest.viewport_desktop?.width || 1440}x${manifest.viewport_desktop?.height || 900}\n\n` +
              `Bilderekkefølge er: ${imagePaths.length === 3 ? "TOP, MID, BOTTOM" : "TOP"}.`,
          },
          ...imagePaths.map((imgPath) => ({
            type: "input_image",
            image_url: imageToDataUrl(imgPath),
          })),
        ],
      },
    ],
    max_output_tokens: 300,
  });

   let rawText = normalizeComment(extractTextFromResponse(response));
if (!rawText) throw new Error("Model returned empty text output");

let cleaned = repairOutreachComment(rawText);
let validation = validateOutreachComment(cleaned);

// Retry once if it broke the format
if (!validation.ok) {
  console.log("First outreach output invalid, retrying once:", validation.reason);

  const retryResponse = await client.responses.create({
    model,
    instructions:
      "Return only the final website comment in plain text. Do not greet. Do not repeat the email intro. The comment is inserted after the sentence 'Hei, jeg tokk en titt gjennom nettsiden deres og la merke til at'. Start directly with the actual comment. Exactly 4 sentences. Sentence 2 must start with 'Det er bra, men'. Sentence 4 must end with 'blant annet.'.",
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text:
              `${prompt}\n\n` +
              `Your previous output was invalid because: ${validation.reason}.\n` +
              `Fix it now.\n\n` +
              `KONTEKST (fra capture-manifest):\n` +
              `- URL: ${manifest.final_url || manifest.attempted_url || manifest.input_url || "ukjent"}\n` +
              `- Tittel: ${manifest.homepage_title || "ukjent"}\n` +
              `- Sidehøyde: ${manifest.page_height || "ukjent"}\n` +
              `- Viewport: ${manifest.viewport_desktop?.width || 1440}x${manifest.viewport_desktop?.height || 900}\n\n` +
              `Bilderekkefølge er: ${imagePaths.length === 3 ? "TOP, MID, BOTTOM" : "TOP"}.`,
          },
          ...imagePaths.map((imgPath) => ({
            type: "input_image",
            image_url: imageToDataUrl(imgPath),
          })),
        ],
      },
    ],
    max_output_tokens: 300,
  });

  rawText = normalizeComment(extractTextFromResponse(retryResponse));
  if (!rawText) {
    rawText = buildSafeFallbackComment();
  }

  cleaned = repairOutreachComment(rawText);
  validation = validateOutreachComment(cleaned);

  if (!validation.ok) {
    console.warn(
      "Outreach output still invalid after retry, using safe fallback:",
      validation.reason
    );
    cleaned = buildSafeFallbackComment();
  }
}

  return {
    mode: "openai",
    model,
    screenshotModeUsed: screenshotMode,
    comment_no: cleaned,
    raw_output_text: rawText,
  };
}

/* ======================
   STAGE 2: FULL ANALYSIS
====================== */
// ✅ Added promptOverride arg so job-runner can inject preset prompt
async function runAnalysis(input, languageArg = "no", engineArg = "openai", promptOverride = "") {
  const manifestPath = getManifestPath(input);
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Manifest not found: ${manifestPath}`);
  }

  const manifest = readJson(manifestPath);
  if (manifest.capture_status !== "success") {
    throw new Error(`Capture not successful for manifest: ${manifestPath}`);
  }

  const bundle = buildInputBundle(manifest);
  const analysis = await analyzeWithAI(manifest, bundle, languageArg, engineArg, promptOverride);

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

  const base = safeFileName(manifest.input_url || manifest.final_url || "unknown");
  const outPath = path.join(ANALYSIS_RESULTS_DIR, `${base}.analysis.json`);
  writeJson(outPath, result);

  return result;
}

/* ======================
   STAGE 1: SCORE-ONLY
====================== */
async function runScoreOnlyAnalysis(input, languageArg = "no", engineArg = "openai") {
  const manifestPath = getManifestPath(input);

  if (!fs.existsSync(manifestPath)) {
    return { reachable: false, score: 0, severity: "low", reason: "Manifest not found" };
  }

  const manifest = readJson(manifestPath);

  if (manifest.capture_status !== "success") {
    return { reachable: false, score: 0, severity: "low", reason: "Capture failed/unreachable" };
  }

  let imagePaths = [];
  try {
    imagePaths = getImagePathsForMode(manifest);
  } catch (err) {
    return { reachable: false, score: 0, severity: "low", reason: "Screenshot selection failed" };
  }

  const engine = String(engineArg || process.env.ANALYZER_MODE || "openai").toLowerCase();
  if (engine === "mock") {
    return { reachable: true, score: 5, severity: "medium", reason: "Mock score" };
  }

  const apiKey = process.env.OPENAI_API_KEY;
  const model = process.env.OPENAI_MODEL || "gpt-4.1-mini";
  if (!apiKey) throw new Error("OPENAI_API_KEY is missing in .env");

  const client = new OpenAI({ apiKey });

  const response = await client.responses.create({
    model,
    instructions:
      "Return ONLY valid JSON with keys: reachable (boolean), score (1-10 integer), severity (low|medium|high), reason (short string). No markdown, no code fences.",
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text:
              "Quickly qualify this website based only on the visible screenshots.\n" +
              "Use a strict, stable scale:\n" +
              "1-2 = broken, error-like, very outdated or messy\n" +
              "3-4 = weak visual quality\n" +
              "5-6 = acceptable but mixed\n" +
              "7-8 = good and solid\n" +
              "9-10 = very polished and excellent\n" +
              "If it looks like 404, browser error, access denied, forbidden, blocked, not authorized, domain for sale, parked domain, blank page, coming soon, under construction, or otherwise not like a real usable website: set reachable=false and score=0.\n" +
              "Return ONLY JSON.",
          },
          ...imagePaths.map((imgPath) => ({
            type: "input_image",
            image_url: imageToDataUrl(imgPath),
          })),
        ],
      },
    ],
    max_output_tokens: 180,
  });

  const raw = normalizeComment(extractTextFromResponse(response));
  const cleaned = raw.replace(/```json/gi, "```").replace(/```/g, "").trim();

  try {
    const parsed = JSON.parse(cleaned);
    return {
      reachable: Boolean(parsed.reachable),
      score: Math.max(0, Math.min(10, Number(parsed.score) || 0)),
      severity: String(parsed.severity || "medium"),
      reason: String(parsed.reason || ""),
    };
  } catch {
    return { reachable: true, score: 5, severity: "medium", reason: "Could not parse score JSON" };
  }
}

// ======================
// EXPORTS
// ======================
module.exports = { runAnalysis, runScoreOnlyAnalysis };

// Keep CLI support (still works, presets override is used by worker not CLI)
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