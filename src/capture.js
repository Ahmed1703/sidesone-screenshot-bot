const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const ROOT = path.join(__dirname, "..");

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
    '[aria-label*="cookie" i] button'
  ];

  for (const selector of selectors) {
    try {
      const el = page.locator(selector).first();
      if (await el.isVisible({ timeout: 700 })) {
        await el.click({ timeout: 1200 });
        await page.waitForTimeout(600);
        return true;
      }
    } catch (_) {
      // try next selector
    }
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
          7000 // enough to trigger lazy loading without being too slow
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
  } catch (_) {
    // ignore scroll errors
  }
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
        timeout: 15000
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

    return {
      pageHeight: Number(pageHeight) || 0
    };
  });
}

async function captureWebsite(rawUrl, customId = "") {
  const browser = await chromium.launch({ headless: true });

  const fileBase = safeFileName(customId ? `${customId}_${rawUrl}` : rawUrl);

  const result = {
    input_url: rawUrl,
    attempted_url: null,
    final_url: null,
    homepage_title: null,

    capture_status: "failed",
    capture_error: null,

    desktop_top_path: null,
    desktop_mid_path: null,
    desktop_bottom_path: null,

    viewport_desktop: { width: 1440, height: 900 },
    page_height: null,
    top_scroll_y: 0,
    mid_scroll_y: null,
    bottom_scroll_y: null,

    timestamp: new Date().toISOString()
  };

  try {
    const desktopContext = await browser.newContext({
      viewport: { width: 1440, height: 900 }
    });

    const page = await desktopContext.newPage();

    const nav = await tryGoto(page, rawUrl);
    if (!nav.ok) throw nav.error;

    result.attempted_url = nav.attemptedUrl;

    // Let page settle
    await page.waitForTimeout(1800);

    // Try closing cookie banners
    await dismissCommonPopups(page);

    // Trigger lazy-loaded content/images
    await autoScroll(page);

    // Settle again
    await page.waitForTimeout(1000);

    result.final_url = page.url();
    result.homepage_title = await page.title();

    const metrics = await getPageMetrics(page);
    result.page_height = metrics.pageHeight;

    const viewport = page.viewportSize() || { width: 1440, height: 900 };
    const viewportHeight = viewport.height;

    // Scroll positions
    const topY = 0;
    const maxScrollableY = Math.max(0, metrics.pageHeight - viewportHeight);
    const midY = Math.max(0, Math.floor(maxScrollableY / 2));
    const bottomY = maxScrollableY;

    result.mid_scroll_y = midY;
    result.bottom_scroll_y = bottomY;

    // ---------- 1) TOP ----------
    await page.evaluate((y) => window.scrollTo(0, y), topY);
    await page.waitForTimeout(500);

    const topPath = path.join(DESKTOP_DIR, `${fileBase}_top.jpg`);
    await page.screenshot({
      path: topPath,
      type: "jpeg",
      quality: 75
    });
    result.desktop_top_path = topPath;

    // ---------- 2) MID ----------
    await page.evaluate((y) => window.scrollTo(0, y), midY);
    await page.waitForTimeout(700);

    const midPath = path.join(DESKTOP_DIR, `${fileBase}_mid.jpg`);
    await page.screenshot({
      path: midPath,
      type: "jpeg",
      quality: 75
    });
    result.desktop_mid_path = midPath;

    // ---------- 3) BOTTOM ----------
    await page.evaluate((y) => window.scrollTo(0, y), bottomY);
    await page.waitForTimeout(700);

    const bottomPath = path.join(DESKTOP_DIR, `${fileBase}_bottom.jpg`);
    await page.screenshot({
      path: bottomPath,
      type: "jpeg",
      quality: 75
    });
    result.desktop_bottom_path = bottomPath;

    await desktopContext.close();

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
// node src/capture.js example.com
if (require.main === module) {
  const url = process.argv[2];

  if (!url) {
    console.error("Usage: node src/capture.js <url>");
    process.exit(1);
  }

  captureWebsite(url)
    .then((res) => {
      console.log(JSON.stringify(res, null, 2));
    })
    .catch((err) => {
      console.error("Fatal error:", err);
      process.exit(1);
    });
}

module.exports = { captureWebsite };