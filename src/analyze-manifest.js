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
  if (input.toLowerCase().endsWith(".json")) {
    return path.isAbsolute(input) ? input : path.join(process.cwd(), input);
  }
  return path.join(MANIFEST_DIR, `${safeFileName(input)}.json`);
}

function imageToDataUrl(imagePath) {
  const ext = path.extname(imagePath).toLowerCase();
  const mime =
    ext === ".jpg" || ext === ".jpeg" ? "image/jpeg" :
    ext === ".png" ? "image/png" :
    ext === ".webp" ? "image/webp" :
    "application/octet-stream";

  const base64 = fs.readFileSync(imagePath).toString("base64");
  return `data:${mime};base64,${base64}`;
}

function normalizeComment(text) {
  return String(text || "")
    .replace(/\r/g, "")
    .trim();
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
function sanitizeOutreachComment(s) {
  let t = String(s || "").trim();

  // Remove accidental greeting / intro line
  t = t.replace(/^hei[,!.\s]+/i, "");

  // Sometimes it repeats the email intro sentence
  t = t.replace(/^jeg\s+tok\s+en\s+titt[^.]*\.\s*/i, "");

  // Force first char lowercase (your rule)
  t = t.replace(/^\s*([A-ZÆØÅ])/, (m) => m.toLowerCase());

  // Remove leading quotes/spaces
  t = t.replace(/^["'\s]+/, "");

  return t.trim();
}

function buildInputBundle(manifest) {
  const paths = [
    { role: "top", path: manifest.desktop_top_path },
    { role: "mid", path: manifest.desktop_mid_path },
    { role: "bottom", path: manifest.desktop_bottom_path }
  ].filter((x) => x.path && fs.existsSync(x.path));

  return {
    paths,
    pageHeight: manifest.page_height ?? null,
    viewportHeight: manifest.viewport_desktop?.height ?? null,
    homepageTitle: manifest.homepage_title || "",
    finalUrl: manifest.final_url || manifest.input_url || ""
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
              "Forsiden virker ganske kort, så top, mid og bunn blir nesten samme utsnitt i analysen."
          }
        ]
      : [
          {
            category: "spacing",
            severity: "medium",
            comment_no:
              "Siden har nok høyde til å vurdere struktur, men spacing og seksjonsrytme bør vurderes ut fra top, mid og bunn samlet."
          }
        ],
    best_outreach_line_no: isShort
      ? "Nettsiden virker ganske kort i oppsettet, så førsteinntrykket blir fort over."
      : "Nettsiden har innhold, men oppsett og spacing mellom seksjoner kan bli tydeligere visuelt."
  };
}

// ----------------------
// REAL AI analyzer (OpenAI Responses API)
// ----------------------
async function analyzeWithAI(manifest, bundle) {
  const mode = (process.env.ANALYZER_MODE || "mock").toLowerCase();

  if (mode === "mock") {
    return analyzeWithMock(manifest, bundle);
  }

  if (mode !== "openai") {
    throw new Error(`Unsupported ANALYZER_MODE: ${mode}`);
  }

  const apiKey = process.env.OPENAI_API_KEY;
  const model = process.env.OPENAI_MODEL || "gpt-4.1-mini";

  if (!apiKey) throw new Error("OPENAI_API_KEY is missing in .env");

  const topPath = manifest.desktop_top_path;
  const midPath = manifest.desktop_mid_path;
  const bottomPath = manifest.desktop_bottom_path;

  if (!topPath || !fs.existsSync(topPath)) throw new Error(`Top screenshot missing: ${topPath}`);
  if (!midPath || !fs.existsSync(midPath)) throw new Error(`Mid screenshot missing: ${midPath}`);
  if (!bottomPath || !fs.existsSync(bottomPath)) throw new Error(`Bottom screenshot missing: ${bottomPath}`);

  const prompt = buildAnalyzerPrompt({
    homepageTitle: bundle.homepageTitle,
    finalUrl: bundle.finalUrl,
    pageHeight: bundle.pageHeight,
    viewportHeight: bundle.viewportHeight
  });

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
              `Bilderekkefølge er: TOP, MID, BOTTOM.`
          },
          { type: "input_image", image_url: imageToDataUrl(topPath) },
          { type: "input_image", image_url: imageToDataUrl(midPath) },
          { type: "input_image", image_url: imageToDataUrl(bottomPath) }
        ]
      }
    ],
    max_output_tokens: 300
  });

  const rawText = normalizeComment(extractTextFromResponse(response));
  if (!rawText) throw new Error("Model returned empty text output");

  const cleaned = sanitizeOutreachComment(rawText);

  return {
    mode: "openai",
    model,
    comment_no: cleaned,
    raw_output_text: rawText
  };
}

async function main() {
  const input = process.argv[2];
  if (!input) {
    console.error("Usage: node src/analyze-manifest.js <domain-or-manifest.json>");
    process.exit(1);
  }

  const manifestPath = getManifestPath(input);
  if (!fs.existsSync(manifestPath)) {
    console.error(`Manifest not found: ${manifestPath}`);
    process.exit(1);
  }

  const manifest = readJson(manifestPath);

  if (manifest.capture_status !== "success") {
    console.error(`Capture not successful for manifest: ${manifestPath}`);
    process.exit(1);
  }

  const bundle = buildInputBundle(manifest);
  const analysis = await analyzeWithAI(manifest, bundle);

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
      bottom: manifest.desktop_bottom_path || null
    },
    analysis
  };

  const base = safeFileName(manifest.input_url || manifest.final_url || "unknown");
  const outPath = path.join(ANALYSIS_RESULTS_DIR, `${base}.analysis.json`);
  writeJson(outPath, result);

  console.log(`Analysis saved: ${outPath}`);
  console.log(JSON.stringify(result.analysis, null, 2));
}

main().catch((err) => {
  console.error("Analyzer error:", err);
  process.exit(1);
});