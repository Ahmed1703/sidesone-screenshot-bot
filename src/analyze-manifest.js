// src/analyze-manifest.js
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

function normalizeComment(text) {
  return String(text || "").replace(/\r/g, "").trim();
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

function cleanGeneratedText(input) {
  let text = String(input || "").replace(/\r/g, "").trim();

  text = text.replace(/```[\s\S]*?```/g, "").trim();
  text = text.replace(/^["'“”‘’]+|["'“”‘’]+$/g, "").trim();
  text = text.replace(/\n{3,}/g, "\n\n").trim();
  text = text.replace(/[ \t]+/g, " ").trim();

  return text;
}

function safeJsonParse(input) {
  try {
    return JSON.parse(String(input || "").trim());
  } catch {
    return null;
  }
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
    mode: "mock",
    screenshotModeUsed: getCurrentScreenshotMode(),
    page_type: "real_site",
    confidence: 0.6,
    should_generate_comment: true,
    score: isShort ? 5 : 6,
    strengths: isShort
      ? ["Siden virker enkel og oversiktlig i toppseksjonen."]
      : ["Siden har nok innhold til å kunne vurderes visuelt."],
    issues: isShort
      ? ["Forsiden virker ganske kort og litt tynn visuelt."]
      : ["Oppsett og spacing kunne vært tydeligere visuelt."],
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
      ? "The homepage is short and simple."
      : "The page is usable but visually basic.",
    comment_no: isShort
      ? "Forsiden virker ganske enkel og litt kort, så førsteinntrykket blir fort svakere enn det kunne vært."
      : "Nettsiden har innhold nok til å fungere, men oppsett og spacing kunne vært tydeligere visuelt.",
    raw_output_text: "",
    raw_analysis_json: "",
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
  ],
};

function buildFallbackStructuredAnalysis(reason) {
  return {
    page_type: "unclear",
    confidence: 0.2,
    should_generate_comment: false,
    score: 0,
    strengths: [],
    issues: ["The page could not be classified safely."],
    evidence: [String(reason || "Could not classify page safely.")],
    reason_short: String(reason || "Could not classify page safely."),
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
  };
}

function normalizeStructuredAnalysis(data) {
  const fallback = buildFallbackStructuredAnalysis(
    "Could not normalize structured analysis"
  );

  const normalized = {
    page_type: String(data?.page_type || fallback.page_type),
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
  };

  if (!normalized.issues.length) {
    normalized.issues = fallback.issues;
  }
  if (!normalized.evidence.length) {
    normalized.evidence = [normalized.reason_short || fallback.reason_short];
  }

  return normalized;
}

function buildLanguageAwareFallbackComment(structured, languageArg = "no") {
  const isEnglish = String(languageArg || "no").toLowerCase() === "en";

  const pageType = String(structured?.page_type || "unclear");
  const issue1 = String(structured?.issues?.[0] || "").trim();
  const issue2 = String(
    structured?.issues?.[1] || structured?.evidence?.[0] || ""
  ).trim();
  const reason = String(
    structured?.reason_short || structured?.evidence?.[0] || ""
  ).trim();

  if (
    pageType !== "real_site" ||
    !structured?.should_generate_comment ||
    Number(structured?.confidence || 0) < 0.55
  ) {
    if (isEnglish) {
      return (
        "this does not look like a normal finished business website in the screenshots, " +
        "so it would be misleading to write a standard website critique here."
      );
    }

    return (
      "dette ser ikke ut som en vanlig ferdig bedriftsnettside i skjermbildene, " +
      "så det blir misvisende å skrive en vanlig nettsidekommentar her."
    );
  }

  if (isEnglish) {
    const first =
      issue1 || "the page structure and clarity are weaker than they should be";
    const second =
      issue2 ||
      reason ||
      "that makes the first impression feel weaker than it should";

    return `the main issue is that ${first.replace(/^[A-Z]/, (m) =>
      m.toLowerCase()
    )}. ${second.replace(/^[a-z]/, (m) => m.toUpperCase())}.`;
  }

  const first =
    issue1 || "strukturen og tydeligheten på siden er svakere enn den burde være";
  const second =
    issue2 ||
    reason ||
    "det gjør at førsteinntrykket føles svakere enn det burde";

  return `hovedproblemet er at ${first.replace(/^[A-ZÆØÅ]/, (m) =>
    m.toLowerCase()
  )}. ${second.replace(/^[a-zæøå]/, (m) => m.toUpperCase())}.`;
}

// ----------------------
// REAL AI analyzer (OpenAI Responses API)
// ----------------------
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
  const model = process.env.OPENAI_MODEL || "gpt-4.1-mini";
  if (!apiKey) throw new Error("OPENAI_API_KEY is missing in .env");

  const screenshotMode = getCurrentScreenshotMode();
  const imagePaths = getImagePathsForMode(manifest);

  const isEnglish = String(languageArg || "no").toLowerCase() === "en";
  const outputLanguageName = isEnglish ? "English" : "Norwegian Bokmål";

  const prompt =
    String(promptOverride || "").trim() ||
    (isEnglish
      ? "Write a short, natural website outreach comment based only on what is clearly visible. Do not greet. Do not say that you looked at the site. Keep it concrete, human, and specific."
      : "Skriv en kort og naturlig nettsidekommentar for outreach basert kun på det som er tydelig synlig. Ikke hils. Ikke skriv at du har sett på nettsiden. Hold det konkret, menneskelig og spesifikt.");

  const client = new OpenAI({ apiKey });

  const sharedContext =
    `CONTEXT (from capture manifest):\n` +
    `- URL: ${manifest.final_url || manifest.attempted_url || manifest.input_url || "unknown"}\n` +
    `- Title: ${manifest.homepage_title || "unknown"}\n` +
    `- Page height: ${manifest.page_height || "unknown"}\n` +
    `- Viewport: ${manifest.viewport_desktop?.width || 1440}x${manifest.viewport_desktop?.height || 900}\n` +
    `- Image order: ${imagePaths.length === 3 ? "TOP, MID, BOTTOM" : "TOP"}\n`;

  const imageContent = imagePaths.map((imgPath) => ({
    type: "input_image",
    image_url: imageToDataUrl(imgPath),
  }));

  // -----------------------
  // STAGE 1: STRICT CLASSIFICATION
  // -----------------------
  const classifyResponse = await client.responses.create({
    model,
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
      "You are a strict website screenshot classifier. " +
      "Classify what the page actually is before any outreach writing. " +
      "Never invent strengths. Never give fake praise. " +
      "Score visual design quality, not business legitimacy or completeness. " +
      "A site can be real, complete, and usable while still having weak or outdated visual design. " +
      "Do not reward a site with a high score just because it has many sections, testimonials, contact details, or a navigation menu. " +
      "Focus strengths and issues on what is visually apparent: layout balance, spacing, hierarchy, typography, density, section clarity, CTA clarity, consistency, polish, and whether the design feels modern or dated. " +
      "Do not mostly summarize what content exists on the page. " +
      "Do not treat local language as a weakness if the site clearly targets a local market, especially a .no site aimed at Norwegian users. " +
      "If the screenshots look blank, broken, parked, placeholder-like, under construction, or too thin to judge safely, set should_generate_comment=false.",
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text:
              `${sharedContext}\n` +
              `Task:\n` +
              `1. Classify the page strictly.\n` +
              `2. Decide whether it is appropriate to generate a normal website comment.\n` +
              `3. Include strengths only if they are truly visible and visually meaningful.\n` +
              `4. Evidence must be concrete observations from the screenshots, not generic advice.\n` +
              `5. Separate website completeness from visual design quality.\n\n` +
              `Important:\n` +
              `- Use "real_site" only when this clearly looks like an actual business website with enough content to judge.\n` +
              `- If the page looks blank, temporary, parked, broken, placeholder-like, or very thin, set should_generate_comment=false.\n` +
              `- Score means visual design quality only.\n` +
              `- A real business website can still score low if it looks outdated, basic, heavy, cramped, messy, or visually weak.\n` +
              `- Do not use “the site is in Norwegian” as an issue for a Norwegian local business site.\n` +
              `- Prefer issues about outdated styling, heavy sections, weak spacing, weak hierarchy, dense text, uneven balance, basic typography, or low visual polish.\n` +
              `- Do not write outreach text yet. Return classification data only.\n`,
          },
          ...imageContent,
        ],
      },
    ],
    max_output_tokens: 900,
  });

  const rawStructuredText = normalizeComment(
    extractTextFromResponse(classifyResponse)
  );
  const parsedStructured = safeJsonParse(rawStructuredText);
  const structured = normalizeStructuredAnalysis(
    parsedStructured ||
      buildFallbackStructuredAnalysis(
        "Could not parse structured analysis JSON"
      )
  );

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
      model,
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
      raw_output_text: fallbackComment,
      raw_analysis_json: rawStructuredText,
    };
  }

  // -----------------------
  // STAGE 2: FINAL WRITING
  // -----------------------
  const commentResponse = await client.responses.create({
    model,
    temperature: 0.3,
    instructions:
      `You are writing the final visible outreach text in ${outputLanguageName}. ` +
      `Follow the provided style rules exactly. ` +
      `Use only the structured analysis provided. ` +
      `Do not invent positives. ` +
      `If there is no clear positive, begin with a neutral factual observation instead. ` +
      `Write like a short visual audit, not a summary of page contents. ` +
      `At most one short positive observation is allowed, then move quickly to the main weakness. ` +
      `Focus more on what feels outdated, heavy, basic, cramped, visually weak, or less polished. ` +
      `Do not mostly describe products, features, sections, or company claims. ` +
      `Do not criticize the site for being in Norwegian if it clearly targets a Norwegian market. ` +
      `Do not greet. ` +
      `Do not repeat any email intro unless the style rules explicitly require it. ` +
      `Do not mention screenshots, AI, tools, browsing, or technical limitations. ` +
      `Return plain text only.`,
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text:
              `WRITING STYLE RULES:\n${prompt}\n\n` +
              `LANGUAGE REQUIRED:\n${outputLanguageName}\n\n` +
              `STRUCTURED FACTS THAT MUST BE FOLLOWED:\n${JSON.stringify(
                structured,
                null,
                2
              )}\n\n` +
              `Write the final visible outreach text now. ` +
              `It must sound concrete, believable, and based on what is actually visible. ` +
              `It should feel like a short critique of visual quality and clarity, not a summary of what the company offers.`,
          },
        ],
      },
    ],
    max_output_tokens: 260,
  });

  let rawText = normalizeComment(extractTextFromResponse(commentResponse));
  rawText = cleanGeneratedText(rawText);

  if (!rawText) {
    rawText = buildLanguageAwareFallbackComment(structured, languageArg);
  }

  const cleaned =
    cleanGeneratedText(rawText) ||
    buildLanguageAwareFallbackComment(structured, languageArg);

  return {
    mode: "openai",
    model,
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
    raw_output_text: rawText,
    raw_analysis_json: rawStructuredText,
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
  const analysis = await analyzeWithAI(
    manifest,
    bundle,
    languageArg,
    engineArg,
    promptOverride
  );

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
  } catch (err) {
    return {
      reachable: false,
      score: 0,
      severity: "low",
      reason: "Screenshot selection failed",
      page_type: "unreachable",
    };
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
  const model = process.env.OPENAI_MODEL || "gpt-4.1-mini";
  if (!apiKey) throw new Error("OPENAI_API_KEY is missing in .env");

  const client = new OpenAI({ apiKey });

  const response = await client.responses.create({
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
              "- Do NOT reward the site just because it is real, complete, trustworthy, or has a lot of information.\n" +
              "- A real business website can still have weak visual design.\n" +
              "- Do NOT use these as reasons for a high score by themselves: menu exists, testimonials exist, contact info exists, many sections exist, business looks legitimate.\n" +
              "- Focus on how modern or outdated it looks, visual polish, spacing, typography, layout balance, hierarchy, consistency, density, clarity of sections, and overall first impression.\n" +
              "- Do NOT treat Norwegian language on a .no / Norwegian local business site as a design weakness.\n\n" +
              "Use this strict scale:\n" +
              "0 = not a real usable website / unreachable / broken / parked / placeholder\n" +
              "1-2 = broken-looking, extremely outdated, or very poor visual quality\n" +
              "3-4 = weak / clearly outdated visual design\n" +
              "5-6 = acceptable but basic, mixed, or only somewhat polished\n" +
              "7-8 = strong and polished visual design\n" +
              "9-10 = excellent, modern, highly polished visual design\n\n" +
              "If it looks like a 404 page, browser error, forbidden page, parked domain, domain for sale page, blank page, coming soon page, maintenance page, or otherwise not like a real usable website, set reachable=false, score=0, and page_type to one of: unreachable, broken_page, parking_page, placeholder_page, under_construction.\n\n" +
              "If it looks like a real website, set page_type=real_site.\n" +
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
      page_type: String(parsed.page_type || (parsed.reachable ? "real_site" : "unreachable")),
    };
  } catch {
    return {
      reachable: true,
      score: 5,
      severity: "medium",
      reason: "Could not parse score JSON",
      page_type: "real_site",
    };
  }
}

// ======================
// EXPORTS
// ======================
module.exports = { runAnalysis, runScoreOnlyAnalysis };

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