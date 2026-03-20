require("dotenv").config();

const { Redis } = require("@upstash/redis");
const redis = Redis.fromEnv();

const pullSheetUrls = require("./pull-sheet-urls");
const pushSheetResult = require("./push-sheet-results");
const { deleteSheetRow } = pushSheetResult;
const { runAnalysis, runScoreOnlyAnalysis } = require("./analyze-manifest");
const { captureWebsite } = require("./capture");
const { buildAnalyzerPrompt } = require("./analyzer-prompt");

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
  if (v === "no" || v === "norwegian" || v === "bokmal" || v === "bokmål") {
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

function getDefaultSystemConfig() {
  return {
    analysis: {
      screenshotMode: "sections",
      concurrency: 1,
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
    userId: meta.userId,
    sheetId: meta.sheetId,
    sheetTab: meta.sheetTab,
    rowIndex: row.rowIndex,
    comment: text,
    column: meta.outputColumn || "O",
  });
}

async function waitIfPausedOrStopped(jobId) {
  while (true) {
    const freshMeta = await redis.get(`job:${jobId}:meta`);

    if (freshMeta?.status === "stopped") {
      console.log("Job stopped:", jobId);
      return { stopped: true, meta: freshMeta };
    }

    if (freshMeta?.status === "paused") {
      console.log("Job paused:", jobId);
      await sleep(1500);
      continue;
    }

    return { stopped: false, meta: freshMeta };
  }
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

/* =========================
   PROCESS JOB
========================== */

async function processJob(jobId) {
  console.log("Processing job:", jobId);

  let meta = await redis.get(`job:${jobId}:meta`);
  console.log("JOB META RECEIVED:", meta);

  if (!meta) return;

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
  meta.error = null;

  await redis.del(`job:${jobId}:results`);
  await redis.set(`job:${jobId}:meta`, meta);

  const queueMetaUpdate = createMetaWriteQueue(jobId, meta);

  console.log("ANALYSIS CONFIG USED:", cfg);
  console.log("WRITING CONFIG USED:", writing);

  applyScreenshotEnv(cfg.screenshotMode);

  const analysisLanguage = writing.language === "en" ? "en" : "no";
  const basePrompt = await buildAnalyzerPrompt(analysisLanguage);
  const promptOverride = buildPromptOverrideFromWriting(basePrompt, writing);

  try {
    /* =========================
       SINGLE MODE
    ========================== */
    if (meta.type === "single" && meta.siteUrl) {
      console.log("Single site:", meta.siteUrl);

      let gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped) return;

      applyScreenshotEnv(cfg.screenshotMode);
      const captureResult = await captureWebsite(meta.siteUrl);
      console.log("Capture result (single):", meta.siteUrl, captureResult);

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped) return;

      const scoreResult = await runScoreOnlyAnalysis(
        meta.siteUrl,
        analysisLanguage,
        "openai"
      );
      console.log("Score result (single):", scoreResult);

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped) return;

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
          if (cfg.unreachableAction === "fallback") {
            out = cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage);
            status = "fallback";
          } else if (cfg.unreachableAction === "tag") {
            out = "UNREACHABLE";
            status = "unreachable";
          } else {
            out = "UNREACHABLE (skipped)";
            status = "unreachable";
          }
        }

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: out,
          status,
          score: scoreValue,
          page_type: pageType,
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.analyzed = 1;
        meta.failed = 0;
        meta.status = "completed";
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
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.failed = 1;
        meta.status = "completed";
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

        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: out,
          status,
          score: scoreValue,
          page_type: scoreResult?.page_type || "real_site",
          createdAt: nowIso(),
        });

        meta.total = 1;
        meta.analyzed = 1;
        meta.status = "completed";
        await persistMeta(jobId, meta);

        console.log("Single job completed (too good / high score):", jobId);
        return;
      }

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped) return;

      console.log("Qualified single site, starting full AI analysis...");

      const analysisResult = await runAnalysis(
        meta.siteUrl,
        analysisLanguage,
        "openai",
        promptOverride
      );

      console.log(
        "Full analysis result received:",
        JSON.stringify(analysisResult?.analysis || null, null, 2)
      );

      gate = await waitIfPausedOrStopped(jobId);
      if (gate.stopped) return;

      const finalPageType =
        analysisResult?.analysis?.page_type ||
        scoreResult?.page_type ||
        "real_site";

      const shouldGenerateComment =
        analysisResult?.analysis?.should_generate_comment !== false &&
        finalPageType === "real_site";

      let comment = "";
      let finalStatus = "success";
      let finalScore = scoreValue;

      if (!shouldGenerateComment) {
        comment = cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage);
        finalStatus = "fallback";
        finalScore = 0;
      } else {
        comment = finalizeGeneratedComment(
          analysisResult,
          writing,
          analysisLanguage
        );
      }

      console.log("Final extracted comment:", comment);

      await pushRedisResult(jobId, {
        url: meta.siteUrl,
        comment,
        status: finalStatus,
        score: finalScore,
        page_type: finalPageType,
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
          userId: meta.userId,
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
          page_type: "unclear",
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

      const rows = allRows.slice(0, cfg.maxBatchSize);

      meta.total = rows.length;
      await persistMeta(jobId, meta);

      console.log(`Progress: ${meta.analyzed}/${meta.total}`);

      const concurrency = cfg.concurrency;

      for (let start = 0; start < rows.length; start += concurrency) {
        const gate = await waitIfPausedOrStopped(jobId);
        if (gate.stopped) return;

        const chunk = rows.slice(start, start + concurrency);

        await Promise.all(
          chunk.map(async (row, idx) => {
            const rowNumber = start + idx + 1;

            try {
              let rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped) return;

              console.log(`Processing row ${rowNumber}/${rows.length}:`, row.url);

              applyScreenshotEnv(cfg.screenshotMode);
              const captureResult = await captureWebsite(row.url);
              console.log("Capture result:", row.url, captureResult);

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped) return;

              const scoreResult = await runScoreOnlyAnalysis(
                row.url,
                analysisLanguage,
                "openai"
              );
              console.log("Score result:", row.url, scoreResult);

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped) return;

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
                  if (cfg.unreachableAction === "fallback") {
                    out = cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage);
                    status = "fallback";
                    sheetValue = out;
                  } else if (cfg.unreachableAction === "tag") {
                    out = "UNREACHABLE";
                    status = "unreachable";
                    sheetValue = "UNREACHABLE";
                  } else {
                    out = "UNREACHABLE (skipped)";
                    status = "unreachable";
                    sheetValue = "";
                  }
                }

                await writeSheet(meta, row, sheetValue);

                await pushRedisResult(jobId, {
                  url: row.url,
                  comment: out,
                  status,
                  score: scoreValue,
                  page_type: pageType,
                  createdAt: nowIso(),
                });

                await queueMetaUpdate(() => {
                  meta.analyzed += 1;
                });
                return;
              }

              if (scoreValue === null) {
                await writeSheet(meta, row, "FAILED_SCORE_PARSE");

                await pushRedisResult(jobId, {
                  url: row.url,
                  comment: "FAILED_SCORE_PARSE",
                  status: "failed",
                  score: null,
                  page_type: scoreResult?.page_type || "unclear",
                  createdAt: nowIso(),
                });

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
                  await writeSheet(meta, row, "");

                  await pushRedisResult(jobId, {
                    url: row.url,
                    comment: text,
                    status: "skipped",
                    score: scoreValue,
                    page_type: scoreResult?.page_type || "real_site",
                    createdAt: nowIso(),
                  });

                  await queueMetaUpdate(() => {
                    meta.analyzed += 1;
                  });
                  return;
                }

                if (action === "tag") {
                  await writeSheet(meta, row, `GOOD_SITE (${score}/10)`);

                  await pushRedisResult(jobId, {
                    url: row.url,
                    comment: text,
                    status: "good_site",
                    score: scoreValue,
                    page_type: scoreResult?.page_type || "real_site",
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
                    comment: text,
                    status: "cleared",
                    score: scoreValue,
                    page_type: scoreResult?.page_type || "real_site",
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

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped) return;

              const analysisResult = await runAnalysis(
                row.url,
                analysisLanguage,
                "openai",
                promptOverride
              );

              rowGate = await waitIfPausedOrStopped(jobId);
              if (rowGate.stopped) return;

              const finalPageType =
                analysisResult?.analysis?.page_type ||
                scoreResult?.page_type ||
                "real_site";

              const shouldGenerateComment =
                analysisResult?.analysis?.should_generate_comment !== false &&
                finalPageType === "real_site";

              let comment = "";
              let finalStatus = "success";
              let finalScore = scoreValue;

              if (!shouldGenerateComment) {
                comment = cfg.fallbackPrompt || getDefaultFallbackComment(analysisLanguage);
                finalStatus = "fallback";
                finalScore = 0;
              } else {
                comment = finalizeGeneratedComment(
                  analysisResult,
                  writing,
                  analysisLanguage
                );
              }

              await writeSheet(meta, row, comment);

              await pushRedisResult(jobId, {
                url: row.url,
                comment,
                status: finalStatus,
                score: finalScore,
                page_type: finalPageType,
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
                page_type: "unclear",
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
              userId: meta.userId,
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

    try {
      if (meta?.type === "single" && meta?.siteUrl) {
        await pushRedisResult(jobId, {
          url: meta.siteUrl,
          comment: "FAILED",
          status: "failed",
          score: meta.siteScore ?? null,
          page_type: meta.sitePageType || "unclear",
          createdAt: nowIso(),
        });
      }
    } catch (_) {}

    if (meta?.type === "single") {
      meta.failed = 1;
      meta.total = 1;
    }

    meta.status = "completed";
    meta.error = err?.message || "Unknown worker error";
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

runQueue();