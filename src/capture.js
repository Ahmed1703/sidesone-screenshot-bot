// src/capture.js
require("dotenv").config();

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

// Output stays on D:
const DEFAULT_OUT_DIR =
  process.platform === "win32"
    ? "D:/sidesone-screenshot-output"
    : "/data/sidesone-screenshot-output";

const OUT_DIR = process.env.OUTPUT_DIR || DEFAULT_OUT_DIR;

const DESKTOP_DIR = path.join(OUT_DIR, "screenshots", "desktop");
const MANIFEST_DIR = path.join(OUT_DIR, "manifests");
const LOGS_DIR = path.join(OUT_DIR, "logs");

[DESKTOP_DIR, MANIFEST_DIR, LOGS_DIR].forEach((dir) => {
  fs.mkdirSync(dir, { recursive: true });
});

/* =========================
   TIMEOUTS / LIMITS
========================== */

const DEFAULT_VIEWPORT = { width: 1440, height: 900 };

const CAPTURE_TOTAL_TIMEOUT_MS =
  Number(process.env.CAPTURE_TOTAL_TIMEOUT_MS) || 45000;

const PLAYWRIGHT_LAUNCH_TIMEOUT_MS =
  Number(process.env.CAPTURE_LAUNCH_TIMEOUT_MS) || 15000;

const NAV_TIMEOUT_MS =
  Number(process.env.CAPTURE_NAV_TIMEOUT_MS) || 18000;

const ACTION_TIMEOUT_MS =
  Number(process.env.CAPTURE_ACTION_TIMEOUT_MS) || 7000;

const SCREENSHOT_TIMEOUT_MS =
  Number(process.env.CAPTURE_SCREENSHOT_TIMEOUT_MS) || 12000;

const FULLPAGE_SCREENSHOT_TIMEOUT_MS =
  Number(process.env.CAPTURE_FULLPAGE_SCREENSHOT_TIMEOUT_MS) || 18000;

const POPUP_CLICK_TIMEOUT_MS =
  Number(process.env.CAPTURE_POPUP_CLICK_TIMEOUT_MS) || 1000;

const POPUP_VISIBLE_TIMEOUT_MS =
  Number(process.env.CAPTURE_POPUP_VISIBLE_TIMEOUT_MS) || 600;

const SCROLL_MAX_HEIGHT =
  Number(process.env.CAPTURE_SCROLL_MAX_HEIGHT) || 7000;

/* =========================
   HELPERS
========================== */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function safeErrorMessage(err, fallback = "Unknown capture error") {
  if (!err) return fallback;
  if (typeof err === "string") return err;
  if (err instanceof Error) return err.message || fallback;
  return String(err?.message || err || fallback);
}

/**
 * Normalizes any legacy/new values to:
 *   "top" | "full" | "sections"
 */
function normalizeMode(value) {
  const v = String(value || "").trim().toLowerCase();

  if (v === "top" || v === "hero" || v === "cheap") return "top";
  if (v === "full" || v === "fullpage" || v === "page") return "full";
  if (v === "sections" || v === "recommended" || v === "precision" || v === "3")
    return "sections";

  return "sections";
}

function getModeFromEnv() {
  return (
    process.env.SCREENSHOT_MODE ||
    process.env.SCREENSHOT_STRATEGY ||
    process.env.SIDESONE_SCREENSHOT_MODE ||
    ""
  );
}

async function safeClose(target, label = "resource") {
  if (!target || typeof target.close !== "function") return;

  try {
    await Promise.race([
      target.close().catch(() => {}),
      sleep(2500),
    ]);
  } catch (_) {
    console.warn(`safeClose failed for ${label}`);
  }
}

async function installLightweightBlocking(page) {
  const blockedTypes = new Set(["media", "eventsource", "websocket"]);

  const blockedUrlParts = [
    "googletagmanager.com",
    "google-analytics.com",
    "doubleclick.net",
    "hotjar.com",
    "clarity.ms",
    "facebook.net",
    "connect.facebook.net",
    "fullstory.com",
    "intercom.io",
    "intercomcdn.com",
    "segment.com",
    "cdn.segment.com",
    "analytics",
    "gtm.js",
    "fbevents.js",
  ];

  await page.route("**/*", async (route) => {
    try {
      const req = route.request();
      const url = req.url().toLowerCase();
      const type = req.resourceType();

      if (blockedTypes.has(type)) {
        await route.abort();
        return;
      }

      if (type === "script" && blockedUrlParts.some((part) => url.includes(part))) {
        await route.abort();
        return;
      }

      await route.continue();
    } catch (_) {
      try {
        await route.continue();
      } catch (_) {}
    }
  });
}

async function dismissCommonPopups(page) {
  const selectors = [
    'button:has-text("Godta")',
    'button:has-text("Aksepter")',
    'button:has-text("Tillat alle")',
    'button:has-text("Accept")',
    'button:has-text("Allow all")',
    'button:has-text("I agree")',
    'button:has-text("OK")',
    'button:has-text("Close")',
    '[id*="cookie"] button',
    '[class*="cookie"] button',
    '[aria-label*="cookie" i] button',
  ];

  for (const selector of selectors) {
    try {
      const el = page.locator(selector).first();

      if (await el.isVisible({ timeout: POPUP_VISIBLE_TIMEOUT_MS })) {
        await el.click({ timeout: POPUP_CLICK_TIMEOUT_MS });
        await page.waitForTimeout(450);
        return true;
      }
    } catch (_) {}
  }

  return false;
}

async function autoScroll(page) {
  try {
    await Promise.race([
      page.evaluate(async (maxScrollHeight) => {
        await new Promise((resolve) => {
          const html = document.documentElement;
          const body = document.body;

          const fullHeight = Math.max(
            html?.scrollHeight || 0,
            body?.scrollHeight || 0,
            html?.offsetHeight || 0,
            body?.offsetHeight || 0
          );

          const maxScroll = Math.min(fullHeight, maxScrollHeight);
          const step = 550;
          let total = 0;

          const timer = setInterval(() => {
            window.scrollBy(0, step);
            total += step;

            if (total >= maxScroll) {
              clearInterval(timer);
              window.scrollTo(0, 0);
              setTimeout(resolve, 350);
            }
          }, 110);
        });
      }, SCROLL_MAX_HEIGHT),
      sleep(9000),
    ]);
  } catch (_) {}
}

async function shortSettle(page) {
  try {
    await page.waitForTimeout(900);
    await Promise.race([
      page.waitForLoadState("load", { timeout: 2500 }).catch(() => {}),
      sleep(2600),
    ]);
  } catch (_) {}
}

async function tryGoto(page, rawUrl) {
  const cleaned = String(rawUrl || "").trim();
  const candidates =
    cleaned.startsWith("http://") || cleaned.startsWith("https://")
      ? [cleaned]
      : [`https://${cleaned}`, `http://${cleaned}`];

  let lastError = null;

  for (const url of candidates) {
    try {
      const response = await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: NAV_TIMEOUT_MS,
      });

      return { ok: true, response, attemptedUrl: url };
    } catch (err) {
      lastError = err;
    }
  }

  return { ok: false, error: lastError };
}

async function getPageMetrics(page) {
  try {
    return await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;

      const pageHeight = Math.max(
        body ? body.scrollHeight : 0,
        body ? body.offsetHeight : 0,
        html ? html.clientHeight : 0,
        html ? html.scrollHeight : 0,
        html ? html.offsetHeight : 0
      );

      return { pageHeight: Number(pageHeight) || 0 };
    });
  } catch (_) {
    return { pageHeight: 0 };
  }
}

async function scrollTo(page, y, waitMs = 650) {
  try {
    await page.evaluate((yy) => window.scrollTo(0, yy), y);
    await page.waitForTimeout(waitMs);
  } catch (_) {}
}

async function captureTop(page, outDir, fileBase, result) {
  await scrollTo(page, 0, 450);

  const topPath = path.join(outDir, `${fileBase}_top.jpg`);
  await page.screenshot({
    path: topPath,
    type: "jpeg",
    quality: 75,
    timeout: SCREENSHOT_TIMEOUT_MS,
  });

  result.desktop_top_path = topPath;
  result.image_paths = [topPath];
}

async function captureFull(page, outDir, fileBase, result) {
  await scrollTo(page, 0, 450);

  const fullPath = path.join(outDir, `${fileBase}_full.jpg`);
  await page.screenshot({
    path: fullPath,
    type: "jpeg",
    quality: 75,
    fullPage: true,
    timeout: FULLPAGE_SCREENSHOT_TIMEOUT_MS,
  });

  result.desktop_full_path = fullPath;
  result.image_paths = [fullPath];
}

async function captureSections(page, outDir, fileBase, result) {
  const metrics = await getPageMetrics(page);
  result.page_height = metrics.pageHeight;

  const viewport = page.viewportSize() || DEFAULT_VIEWPORT;
  const viewportHeight = viewport.height;

  const topY = 0;
  const maxScrollableY = Math.max(0, metrics.pageHeight - viewportHeight);
  const midY = Math.max(0, Math.floor(maxScrollableY / 2));
  const bottomY = maxScrollableY;

  result.top_scroll_y = topY;
  result.mid_scroll_y = midY;
  result.bottom_scroll_y = bottomY;

  // TOP
  await scrollTo(page, topY, 450);
  const topPath = path.join(outDir, `${fileBase}_top.jpg`);
  await page.screenshot({
    path: topPath,
    type: "jpeg",
    quality: 75,
    timeout: SCREENSHOT_TIMEOUT_MS,
  });
  result.desktop_top_path = topPath;

  // MID
  await scrollTo(page, midY, 650);
  const midPath = path.join(outDir, `${fileBase}_mid.jpg`);
  await page.screenshot({
    path: midPath,
    type: "jpeg",
    quality: 75,
    timeout: SCREENSHOT_TIMEOUT_MS,
  });
  result.desktop_mid_path = midPath;

  // BOTTOM
  await scrollTo(page, bottomY, 650);
  const bottomPath = path.join(outDir, `${fileBase}_bottom.jpg`);
  await page.screenshot({
    path: bottomPath,
    type: "jpeg",
    quality: 75,
    timeout: SCREENSHOT_TIMEOUT_MS,
  });
  result.desktop_bottom_path = bottomPath;

  result.image_paths = [topPath, midPath, bottomPath];
}

/**
 * captureWebsite(rawUrl, {
 *   screenshotMode?: "top"|"full"|"sections"|"recommended"|...legacy,
 *   outDir?: string,
 *   disableAutoScroll?: boolean,
 * })
 */
async function captureWebsite(rawUrl, opts = {}) {
  const requestedMode =
    opts.screenshotMode !== undefined ? opts.screenshotMode : getModeFromEnv();
  const screenshotMode = normalizeMode(requestedMode);

  const outDir = opts.outDir || DESKTOP_DIR;
  const fileBase = safeFileName(rawUrl);

  const result = {
    input_url: rawUrl,
    attempted_url: null,
    final_url: null,
    homepage_title: null,
    http_status: null,

    screenshot_mode_requested: String(requestedMode || ""),
    screenshot_mode_used: screenshotMode,

    capture_status: "failed",
    capture_error: null,
    capture_warning: null,

    reachable: false,
    success: false,
    ok: false,
    error: null,

    desktop_top_path: null,
    desktop_mid_path: null,
    desktop_bottom_path: null,
    desktop_full_path: null,

    image_paths: [],

    viewport_desktop: { ...DEFAULT_VIEWPORT },
    page_height: null,
    top_scroll_y: 0,
    mid_scroll_y: null,
    bottom_scroll_y: null,

    timestamp: new Date().toISOString(),
  };

  let browser = null;
  let context = null;
  let page = null;
  let timedOut = false;

  const watchdog = setTimeout(async () => {
    timedOut = true;

    const timeoutMessage = `Capture timed out after ${CAPTURE_TOTAL_TIMEOUT_MS}ms`;

    result.capture_status = "failed";
    result.capture_error = timeoutMessage;
    result.error = timeoutMessage;
    result.success = false;
    result.ok = false;

    await safeClose(page, "page");
    await safeClose(context, "context");
    await safeClose(browser, "browser");
  }, CAPTURE_TOTAL_TIMEOUT_MS);

  if (typeof watchdog.unref === "function") {
    watchdog.unref();
  }

  try {
    fs.mkdirSync(outDir, { recursive: true });

    browser = await chromium.launch({
      headless: true,
      timeout: PLAYWRIGHT_LAUNCH_TIMEOUT_MS,
      args: [
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-background-networking",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
      ],
    });

    context = await browser.newContext({
      viewport: DEFAULT_VIEWPORT,
      ignoreHTTPSErrors: true,
      javaScriptEnabled: true,
      bypassCSP: false,
    });

    page = await context.newPage();
    page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
    page.setDefaultTimeout(ACTION_TIMEOUT_MS);

    await installLightweightBlocking(page);

    page.on("crash", () => {
      result.capture_warning = "Page crashed during capture.";
    });

    const nav = await tryGoto(page, rawUrl);

    if (!nav.ok) {
      throw nav.error || new Error("Navigation failed.");
    }

    result.attempted_url = nav.attemptedUrl;
    result.http_status =
      typeof nav.response?.status === "function" ? nav.response.status() : null;
    result.reachable = true;

    await shortSettle(page);
    await dismissCommonPopups(page);

    const disableAutoScroll =
      typeof opts.disableAutoScroll === "boolean"
        ? opts.disableAutoScroll
        : screenshotMode === "top";

    if (!disableAutoScroll) {
      await autoScroll(page);
      await page.waitForTimeout(600);
    }

    result.final_url = page.url();
    result.homepage_title = await page.title().catch(() => null);

    if (screenshotMode === "top") {
      await captureTop(page, outDir, fileBase, result);
    } else if (screenshotMode === "full") {
      try {
        await captureFull(page, outDir, fileBase, result);
      } catch (e) {
        result.capture_warning = `fullPage failed, fell back to sections: ${safeErrorMessage(
          e
        )}`;
        result.screenshot_mode_used = "sections";
        await captureSections(page, outDir, fileBase, result);
      }
    } else {
      await captureSections(page, outDir, fileBase, result);
    }

    result.capture_status = "success";
    result.success = true;
    result.ok = true;
    result.error = null;
  } catch (err) {
    const message = timedOut
      ? result.capture_error || `Capture timed out after ${CAPTURE_TOTAL_TIMEOUT_MS}ms`
      : safeErrorMessage(err);

    result.capture_status = "failed";
    result.capture_error = message;
    result.error = message;
    result.success = false;
    result.ok = false;
  } finally {
    clearTimeout(watchdog);

    await safeClose(page, "page");
    await safeClose(context, "context");
    await safeClose(browser, "browser");

    const manifestPath = path.join(MANIFEST_DIR, `${fileBase}.json`);

    try {
      fs.writeFileSync(manifestPath, JSON.stringify(result, null, 2), "utf8");
    } catch (err) {
      console.error("Failed to write manifest:", safeErrorMessage(err));
    }
  }

  return result;
}

// CLI usage:
// node src/capture.js example.com [top|full|sections]
if (require.main === module) {
  const url = process.argv[2];
  const mode = process.argv[3];

  if (!url) {
    console.error("Usage: node src/capture.js <url> [top|full|sections]");
    process.exit(1);
  }

  captureWebsite(url, { screenshotMode: mode })
    .then((res) => console.log(JSON.stringify(res, null, 2)))
    .catch((err) => {
      console.error("Fatal error:", err);
      process.exit(1);
    });
}

module.exports = { captureWebsite, normalizeMode };