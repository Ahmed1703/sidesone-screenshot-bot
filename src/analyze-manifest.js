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

function splitSentences(text) {
  return String(text || "")
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

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
  };
}

function normalizeStructuredAnalysis(data, languageArg = "no") {
  const fallback = buildFallbackStructuredAnalysis(
    "Could not normalize structured analysis",
    languageArg
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

  if (normalized.score <= 4) {
    normalized.strengths = [];
  }

  return normalized;
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

async function runWritingPass({
  client,
  model,
  outputLanguageName,
  prompt,
  writingStructured,
  strictSpecificity = false,
}) {
  const strictBlock = strictSpecificity
    ? `

STRICT SPECIFICITY MODE:
- Mention 2 to 4 concrete visible observations if the structured facts support them.
- Preserve specific page areas from the structured facts: top line navigation, menu, icon row, buttons, contact form, footer, text blocks, section spacing, images, script font, dark background, or similar.
- Do not collapse concrete observations into vague wording like "could be more polished" or "room for improvement".
- Do not write a bland summary.`
    : "";

  const commentResponse = await client.responses.create({
    model,
    temperature: strictSpecificity ? 0.3 : 0.45,
    instructions:
      `You are writing the final requested outreach output in ${outputLanguageName}. ` +
      `Every sentence must be written only in ${outputLanguageName}. ` +
      `Never mix languages. ` +
      `If the structured facts contain text in another language, translate them before writing. ` +
      `Do not copy English phrases into Norwegian output. ` +
      `Do not copy Norwegian phrases into English output. ` +
      `Follow the provided style rules exactly. ` +
      `Use only the structured analysis and evidence provided. ` +
      `Do not invent positives. ` +
      `If there is no clear positive, begin with a neutral factual observation instead. ` +
      `Write like a short visual audit, not a summary of what the company offers. ` +
      `Use simple everyday language, not designer or consultant wording. ` +
      `Avoid generic filler like "could be more polished", "room for improvement", "feel smoother", or "stronger visually" unless you immediately tie it to a concrete visible reason. ` +
      `Prefer specific page areas and elements when supported by the evidence: top line navigation, menu, icon row, buttons, form, footer, text blocks, images, section spacing, contact area, dark background, script font, and similar visible details. ` +
      `A strong comment should usually contain one brief fair opening, then the clearest concrete weakness, then one or two smaller supporting visible details if available. ` +
      `Do not flatten everything into one vague statement. ` +
      `Do not compress concrete observations into abstract summary wording. ` +
      `Do not mostly summarize page content. ` +
      `Do not mention screenshots, AI, tools, browsing, or technical limitations. ` +
      `Do not criticize the site for being in Norwegian if it clearly targets a Norwegian market. ` +
      `When the rules say this text is inserted between an already-written intro and outro, write ONLY the middle critique section. ` +
      `Do not greet. Do not introduce yourself. Do not say you looked at the website. Do not add a CTA. Do not add a closing line. ` +
      `Start directly with the critique itself. ` +
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
                writingStructured,
                null,
                2
              )}\n` +
              strictBlock +
              `

Write the requested output now.
Follow the WRITING STYLE RULES exactly.
The output must be ONLY the middle critique section of an email when the rules say it is inserted between intro and outro.
Do not restart the email.
Do not greet.
Do not introduce yourself.
Do not say you looked at the website.
Do not add a closing line or CTA.
Start directly with the critique itself.
It must sound concrete, believable, and based on what is actually visible.
Use the exact structured evidence whenever possible.
If the evidence contains several specific visible weaknesses, you may include up to three of them if the chosen writing style allows it.
Prefer concrete details over vague wording.
It should feel like a short critique of visual quality and clarity, not a summary of what the company offers.
Write the full output only in ${outputLanguageName}.
Never mix languages.`,
          },
        ],
      },
    ],
    max_output_tokens: 340,
  });

  return cleanGeneratedText(normalizeComment(extractTextFromResponse(commentResponse)));
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
      "You are a strict but realistic website screenshot classifier. " +
      `All free-text fields in the JSON must be written in ${outputLanguageName}. ` +
      `That includes strengths, issues, evidence, and reason_short. ` +
      `Never mix languages in those fields. ` +
      `If ${outputLanguageName} is Norwegian Bokmål, do not write English in any free-text field. ` +
      `If ${outputLanguageName} is English, do not write Norwegian in any free-text field. ` +
      "Classify what the page actually is before any outreach writing. " +
      "Never invent strengths. Never give fake praise. " +
      "Score visual design quality, not business legitimacy or completeness. " +
      "Do not force negativity onto decent sites just to sound critical. " +
      "If a site looks normal, usable, and fairly clean, reflect that in the score. " +
      "Do not reward a site with a high score just because it has many sections, testimonials, contact details, or a navigation menu. " +
      "Focus on what is visually apparent: outdated look, cramped layout, dense text, weak spacing, weak button visibility, awkward image placement, heavy sections, low polish, and whether the site feels old or modern. " +
      "Use simple everyday language in issues and evidence. Avoid design-school wording like typography, hierarchy, visual refinement, layout balance, or contrast unless absolutely necessary. " +
      "Do not mostly summarize what content exists on the page. " +
      "Do not treat local language as a weakness if the site clearly targets a local market, especially a .no site aimed at Norwegian users. " +
      "Do not default to generic issues like small button, dense footer, low contrast, weak spacing, or heavy text unless they are clearly visible in the screenshots. " +
      "Only include 2 to 5 issues that are actually the strongest visible weaknesses, and make them concrete rather than generic. " +
      "Every issue should refer to a visible area or element whenever possible, such as top line navigation, menu, icon row, buttons, contact form, footer, script font, text blocks, image placement, or section spacing. " +
      "Evidence should sound like direct visual observation, not design advice. " +
      "For scores 0-4, strengths should usually be empty unless something genuinely stands out. " +
      "For score 5, at most one small positive is allowed if it is clearly visible. " +
      "For score 6, allow a mixed view: decent but not polished. " +
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
              `5. Separate website completeness from visual design quality.\n` +
              `6. Do not repeat generic critique patterns unless they are clearly supported by the screenshots.\n\n` +
              `Important:\n` +
              `- All free-text values in the JSON must be written in ${outputLanguageName}.\n` +
              `- Never mix English and Norwegian in strengths, issues, evidence, or reason_short.\n` +
              `- Use "real_site" only when this clearly looks like an actual business website with enough content to judge.\n` +
              `- If the page looks blank, temporary, parked, broken, placeholder-like, or very thin, set should_generate_comment=false.\n` +
              `- Score means visual design quality only.\n` +
              `- A real business website can still score low if it looks outdated, basic, heavy, cramped, messy, or visually weak.\n` +
              `- A normal usable SMB site often belongs around 5-6, not automatically 3-4.\n` +
              `- Do not use “the site is in Norwegian” as an issue for a Norwegian local business site.\n` +
              `- Prefer simple issue wording like gammeldags, enkelt, tett, tung å lese, lite luft, svak knapp, bildet sitter ikke helt, mindre ryddig, mindre gjennomført.\n` +
              `- Do not write outreach text yet. Return classification data only.\n` +
              `- When possible, evidence should name the visible area directly, such as top line navigation, icon row, buttons, footer, contact form, text blocks, background, script font, image placement, or section spacing.\n` +
              `- At least one issue should name a concrete visible element or area when confidence is high enough.\n`,
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
        "Could not parse structured analysis JSON",
        languageArg
      ),
    languageArg
  );

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
      ai_middle: fallbackComment,
      raw_output_text: fallbackComment,
      raw_analysis_json: rawStructuredText,
    };
  }

  // -----------------------
  // STAGE 2: FINAL WRITING
  // -----------------------
  let rawText = await runWritingPass({
    client,
    model,
    outputLanguageName,
    prompt,
    writingStructured,
    strictSpecificity: false,
  });

  rawText = cleanGeneratedText(rawText);

  if (
    !rawText ||
    shouldRetrySpecificRewrite(rawText, writingStructured) ||
    isWrongLanguage(rawText, languageArg)
  ) {
    rawText = await runWritingPass({
      client,
      model,
      outputLanguageName,
      prompt,
      writingStructured,
      strictSpecificity: true,
    });

    rawText = cleanGeneratedText(rawText);
  }

  if (
    !rawText ||
    shouldRetrySpecificRewrite(rawText, writingStructured) ||
    isWrongLanguage(rawText, languageArg)
  ) {
    rawText = buildDeterministicStructuredComment(writingStructured, languageArg);
  }

  rawText = stripLowScoreFlattery(rawText, writingStructured, languageArg);

  if (isWrongLanguage(rawText, languageArg)) {
    rawText = buildDeterministicStructuredComment(writingStructured, languageArg);
  }

  const cleaned =
    cleanGeneratedText(rawText) ||
    buildLanguageAwareFallbackComment(writingStructured, languageArg);

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
    ai_middle: cleaned,
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
              "- Be realistic and moderately strict, but do not force weak scores on decent normal business websites.\n" +
              "- If a site looks usable, structured, and fairly clean, it often belongs around 5-6 even if it is not modern.\n" +
              "- Use lower scores only when the site clearly looks rough, messy, broken-looking, very outdated, or very weak visually.\n" +
              "- Do NOT reward the site just because it is real, complete, trustworthy, or has a lot of information.\n" +
              "- Do NOT use these as reasons for a high score by themselves: menu exists, testimonials exist, contact info exists, many sections exist, business looks legitimate.\n" +
              "- Focus on how modern or outdated it looks, visual polish, spacing, text density, clarity, balance, button visibility, image placement, and overall first impression.\n" +
              "- Do NOT treat Norwegian language on a .no / Norwegian local business site as a weakness.\n\n" +
              "Use this scale:\n" +
              "0 = not a real usable website / unreachable / broken / parked / placeholder\n" +
              "1-2 = extremely poor, broken-looking, or almost unusable visually\n" +
              "3-4 = clearly weak, rough, or obviously outdated visual quality\n" +
              "5 = usable but basic, dated, or somewhat weak visually\n" +
              "6 = decent and fairly solid, but not polished or modern\n" +
              "7-8 = strong, clean, and clearly above average visual quality\n" +
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
      page_type: String(
        parsed.page_type || (parsed.reachable ? "real_site" : "unreachable")
      ),
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