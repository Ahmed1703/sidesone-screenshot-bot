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

const DEFAULT_VIEWPORT = { width: 1440, height: 900 };

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

  // Safe default (matches your new system normalization)
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

async function dismissCommonPopups(page) {
  const selectors = [
    'button:has-text("Godta")',
    'button:has-text("Aksepter")',
    'button:has-text("Tillat alle")',
    'button:has-text("Accept")',
    'button:has-text("Allow all")',
    'button:has-text("OK")',
    '[id*="cookie"] button',
    '[class*="cookie"] button',
    '[aria-label*="cookie" i] button',
  ];

  for (const selector of selectors) {
    try {
      const el = page.locator(selector).first();
      if (await el.isVisible({ timeout: 700 })) {
        await el.click({ timeout: 1200 });
        await page.waitForTimeout(600);
        return true;
      }
    } catch (_) {}
  }
  return false;
}

async function autoScroll(page) {
  try {
    await page.evaluate(async () => {
      await new Promise((resolve) => {
        let total = 0;
        const step = 500;
        const maxScroll = Math.min(
          Math.max(
            document.documentElement?.scrollHeight || 0,
            document.body?.scrollHeight || 0
          ),
          7000
        );

        const timer = setInterval(() => {
          window.scrollBy(0, step);
          total += step;

          if (total >= maxScroll) {
            clearInterval(timer);
            window.scrollTo(0, 0);
            setTimeout(resolve, 500);
          }
        }, 120);
      });
    });
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
        timeout: 25000,
      });
      return { ok: true, response, attemptedUrl: url };
    } catch (err) {
      lastError = err;
    }
  }

  return { ok: false, error: lastError };
}

async function getPageMetrics(page) {
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
}

async function scrollTo(page, y, waitMs = 650) {
  await page.evaluate((yy) => window.scrollTo(0, yy), y);
  await page.waitForTimeout(waitMs);
}

async function captureTop(page, outDir, fileBase, result) {
  await scrollTo(page, 0, 500);

  const topPath = path.join(outDir, `${fileBase}_top.jpg`);
  await page.screenshot({ path: topPath, type: "jpeg", quality: 75 });

  result.desktop_top_path = topPath;
  result.image_paths = [topPath];
}

async function captureFull(page, outDir, fileBase, result) {
  await scrollTo(page, 0, 500);

  const fullPath = path.join(outDir, `${fileBase}_full.jpg`);
  await page.screenshot({
    path: fullPath,
    type: "jpeg",
    quality: 75,
    fullPage: true,
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
  await scrollTo(page, topY, 500);
  const topPath = path.join(outDir, `${fileBase}_top.jpg`);
  await page.screenshot({ path: topPath, type: "jpeg", quality: 75 });
  result.desktop_top_path = topPath;

  // MID
  await scrollTo(page, midY, 700);
  const midPath = path.join(outDir, `${fileBase}_mid.jpg`);
  await page.screenshot({ path: midPath, type: "jpeg", quality: 75 });
  result.desktop_mid_path = midPath;

  // BOTTOM
  await scrollTo(page, bottomY, 700);
  const bottomPath = path.join(outDir, `${fileBase}_bottom.jpg`);
  await page.screenshot({ path: bottomPath, type: "jpeg", quality: 75 });
  result.desktop_bottom_path = bottomPath;

  result.image_paths = [topPath, midPath, bottomPath];
}

/**
 * captureWebsite(rawUrl, {
 *   screenshotMode?: "top"|"full"|"sections"|"recommended"|...legacy,
 *   outDir?: string,
 *   disableAutoScroll?: boolean,
 * })
 *
 * - If screenshotMode not provided, reads from env:
 *     SCREENSHOT_MODE / SCREENSHOT_STRATEGY / SIDESONE_SCREENSHOT_MODE
 */
async function captureWebsite(rawUrl, opts = {}) {
  const requestedMode =
    opts.screenshotMode !== undefined ? opts.screenshotMode : getModeFromEnv();
  const screenshotMode = normalizeMode(requestedMode);

  const outDir = opts.outDir || DESKTOP_DIR;

  // IMPORTANT: keep filename stable based on URL ONLY
  // so analyze-manifest can find the right manifest using just the url.
  const fileBase = safeFileName(rawUrl);

  const result = {
    input_url: rawUrl,
    attempted_url: null,
    final_url: null,
    homepage_title: null,

    screenshot_mode_requested: String(requestedMode || ""),
    screenshot_mode_used: screenshotMode,

    capture_status: "failed",
    capture_error: null,
    capture_warning: null,

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

  const browser = await chromium.launch({ headless: true });

  try {
    fs.mkdirSync(outDir, { recursive: true });

    const context = await browser.newContext({ viewport: DEFAULT_VIEWPORT });
    const page = await context.newPage();

    const nav = await tryGoto(page, rawUrl);
    if (!nav.ok) throw nav.error;

    result.attempted_url = nav.attemptedUrl;

    // settle
    await page.waitForTimeout(1200);

    // cookies
    await dismissCommonPopups(page);

    // Only autoscroll when it helps (full/sections). Top is cheap mode.
    const disableAutoScroll =
      typeof opts.disableAutoScroll === "boolean"
        ? opts.disableAutoScroll
        : screenshotMode === "top";

    if (!disableAutoScroll) {
      await autoScroll(page);
      await page.waitForTimeout(900);
    }

    result.final_url = page.url();
    result.homepage_title = await page.title();

    if (screenshotMode === "top") {
      await captureTop(page, outDir, fileBase, result);
    } else if (screenshotMode === "full") {
      try {
        await captureFull(page, outDir, fileBase, result);
      } catch (e) {
        // rare: fullPage fails on extremely long pages / memory issues
        result.capture_warning = `fullPage failed, fell back to sections: ${
          e && e.message ? e.message : String(e)
        }`;
        result.screenshot_mode_used = "sections";
        await captureSections(page, outDir, fileBase, result);
      }
    } else {
      await captureSections(page, outDir, fileBase, result);
    }

    await context.close();
    result.capture_status = "success";
  } catch (err) {
    result.capture_error = err && err.message ? err.message : String(err);
  } finally {
    await browser.close();
  }

  const manifestPath = path.join(MANIFEST_DIR, `${fileBase}.json`);
  fs.writeFileSync(manifestPath, JSON.stringify(result, null, 2), "utf8");

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