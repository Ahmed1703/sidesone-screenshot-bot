const dns = require("node:dns").promises;
const net = require("node:net");
const os = require("node:os");

const SMTP_CONNECT_TIMEOUT_MS =
  Number(process.env.EMAIL_VERIFY_CONNECT_TIMEOUT_MS) || 12000;
const SMTP_RESPONSE_TIMEOUT_MS =
  Number(process.env.EMAIL_VERIFY_RESPONSE_TIMEOUT_MS) || 12000;
const SMTP_SESSION_TIMEOUT_MS =
  Number(process.env.EMAIL_VERIFY_SESSION_TIMEOUT_MS) || 25000;
const SMTP_PORT = Number(process.env.EMAIL_VERIFY_SMTP_PORT) || 25;
const DOMAIN_THROTTLE_MS =
  Number(process.env.EMAIL_VERIFY_DOMAIN_THROTTLE_MS) || 1200;
const MAX_MX_HOSTS = Math.max(
  1,
  Number(process.env.EMAIL_VERIFY_MAX_MX_HOSTS) || 3
);
const CATCH_ALL_PROBES = Math.max(
  1,
  Math.min(2, Number(process.env.EMAIL_VERIFY_CATCH_ALL_PROBES) || 1)
);

const domainLocks = new Map();
const domainLastProbeAt = new Map();

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
  const domainPart = raw.slice(atIndex + 1).trim().toLowerCase();

  if (!localPart || !domainPart) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: raw,
      reason: "Malformed email address.",
    };
  }

  if (localPart.length > 64 || domainPart.length > 253) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: `${localPart}@${domainPart}`,
      reason: "Email address is outside normal length limits.",
    };
  }

  if (!/^[^\s@]+$/.test(localPart)) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: `${localPart}@${domainPart}`,
      reason: "Local part contains unsupported characters or whitespace.",
    };
  }

  if (!/^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/i.test(domainPart)) {
    return {
      ok: false,
      email: raw,
      normalizedEmail: `${localPart}@${domainPart}`,
      reason: "Domain part is malformed.",
    };
  }

  return {
    ok: true,
    email: raw,
    localPart,
    domain: domainPart,
    normalizedEmail: `${localPart}@${domainPart}`,
  };
}

function getVerifierIdentity() {
  const configuredEhlo = String(process.env.EMAIL_VERIFY_EHLO_NAME || "")
    .trim()
    .toLowerCase();
  const configuredMailFrom = String(process.env.EMAIL_VERIFY_MAIL_FROM || "")
    .trim();

  const fallbackHost = String(os.hostname() || "worker.local")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9.-]/g, "")
    .replace(/^\.+|\.+$/g, "");

  const ehloName =
    configuredEhlo && configuredEhlo.includes(".")
      ? configuredEhlo
      : fallbackHost && fallbackHost.includes(".")
      ? fallbackHost
      : "worker.local";

  const mailFrom =
    configuredMailFrom || `validator@${ehloName.replace(/^\.+|\.+$/g, "")}`;

  return { ehloName, mailFrom };
}

async function resolveMailHosts(domain) {
  try {
    const mxRecords = await dns.resolveMx(domain);
    const hosts = mxRecords
      .filter((record) => record && record.exchange)
      .sort((a, b) => a.priority - b.priority)
      .slice(0, MAX_MX_HOSTS)
      .map((record) => ({
        host: String(record.exchange || "").trim().replace(/\.$/, ""),
        priority: Number(record.priority) || 0,
        source: "mx",
      }))
      .filter((record) => record.host);

    if (hosts.length > 0) {
      return {
        ok: true,
        type: "mx",
        hosts,
      };
    }
  } catch (err) {
    if (err?.code !== "ENODATA" && err?.code !== "ENOTFOUND" && err?.code !== "ENOTIMP") {
      return {
        ok: false,
        type: "dns_error",
        reason: `DNS MX lookup failed: ${err?.code || err?.message || "unknown error"}`,
      };
    }
  }

  try {
    const [ipv4, ipv6] = await Promise.allSettled([
      dns.resolve4(domain),
      dns.resolve6(domain),
    ]);

    const hasAddress =
      (ipv4.status === "fulfilled" && ipv4.value.length > 0) ||
      (ipv6.status === "fulfilled" && ipv6.value.length > 0);

    if (hasAddress) {
      return {
        ok: true,
        type: "implicit_a",
        hosts: [
          {
            host: domain,
            priority: 0,
            source: "implicit_a",
          },
        ],
      };
    }

    return {
      ok: false,
      type: "no_mail_domain",
      reason: "Domain has no MX records and no fallback A/AAAA records for mail delivery.",
    };
  } catch (err) {
    return {
      ok: false,
      type: "dns_error",
      reason: `DNS fallback lookup failed: ${err?.code || err?.message || "unknown error"}`,
    };
  }
}

class SmtpClient {
  constructor({ host, port, connectTimeoutMs, responseTimeoutMs, sessionTimeoutMs }) {
    this.host = host;
    this.port = port;
    this.connectTimeoutMs = connectTimeoutMs;
    this.responseTimeoutMs = responseTimeoutMs;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.socket = null;
    this.buffer = "";
    this.pendingResponse = null;
    this.sessionTimer = null;
    this.closed = false;
  }

  async connect() {
    if (this.socket) return;

    this.socket = net.createConnection({
      host: this.host,
      port: this.port,
    });

    this.socket.setEncoding("utf8");
    this.socket.setNoDelay(true);

    this.sessionTimer = setTimeout(() => {
      this.destroy(new Error("SMTP session timed out."));
    }, this.sessionTimeoutMs);

    if (typeof this.sessionTimer.unref === "function") {
      this.sessionTimer.unref();
    }

    this.socket.on("data", (chunk) => this.onData(chunk));
    this.socket.on("error", (err) => this.rejectPending(err));
    this.socket.on("close", () => {
      this.closed = true;
      this.rejectPending(new Error("SMTP connection closed before verification completed."));
    });

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("Timed out while connecting to SMTP host."));
        this.destroy();
      }, this.connectTimeoutMs);

      if (typeof timer.unref === "function") {
        timer.unref();
      }

      this.socket.once("connect", () => {
        clearTimeout(timer);
        resolve();
      });

      this.socket.once("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });

    return this.readResponse();
  }

  onData(chunk) {
    this.buffer += chunk;

    while (this.buffer.includes("\n")) {
      const index = this.buffer.indexOf("\n");
      const rawLine = this.buffer.slice(0, index + 1);
      this.buffer = this.buffer.slice(index + 1);

      const line = rawLine.replace(/\r?\n$/, "");
      if (line) {
        this.acceptLine(line);
      }
    }
  }

  acceptLine(line) {
    if (!this.pendingResponse) {
      return;
    }

    const match = /^(\d{3})([ -])(.*)$/.exec(line);

    if (!match) {
      this.pendingResponse.lines.push(line);
      return;
    }

    const [, code, separator, text] = match;
    this.pendingResponse.lines.push(line);
    this.pendingResponse.code = Number(code);
    this.pendingResponse.text = this.pendingResponse.text
      ? `${this.pendingResponse.text}\n${text}`
      : text;

    if (separator === " ") {
      const pending = this.pendingResponse;
      clearTimeout(pending.timer);
      this.pendingResponse = null;
      pending.resolve({
        code: pending.code,
        text: pending.text || "",
        lines: pending.lines,
      });
    }
  }

  rejectPending(err) {
    if (!this.pendingResponse) {
      return;
    }

    const pending = this.pendingResponse;
    clearTimeout(pending.timer);
    this.pendingResponse = null;
    pending.reject(err instanceof Error ? err : new Error(String(err)));
  }

  readResponse(timeoutMs = this.responseTimeoutMs) {
    if (this.pendingResponse) {
      return Promise.reject(
        new Error("SMTP client attempted to read multiple responses at once.")
      );
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingResponse = null;
        reject(new Error("SMTP server timed out while waiting for a response."));
      }, timeoutMs);

      if (typeof timer.unref === "function") {
        timer.unref();
      }

      this.pendingResponse = {
        lines: [],
        code: null,
        text: "",
        timer,
        resolve,
        reject,
      };
    });
  }

  async command(line, timeoutMs = this.responseTimeoutMs) {
    if (!this.socket || this.closed) {
      throw new Error("SMTP client is not connected.");
    }

    this.socket.write(`${line}\r\n`);
    return this.readResponse(timeoutMs);
  }

  async quit() {
    try {
      if (!this.closed && this.socket) {
        await this.command("QUIT", Math.min(5000, this.responseTimeoutMs));
      }
    } catch (_) {
      // Ignore quit errors.
    } finally {
      this.destroy();
    }
  }

  destroy(err) {
    this.rejectPending(err || new Error("SMTP session ended."));

    if (this.sessionTimer) {
      clearTimeout(this.sessionTimer);
      this.sessionTimer = null;
    }

    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }

    this.closed = true;
  }
}

function classifyRcptResponse(response, normalizedEmail) {
  const code = Number(response?.code) || 0;
  const text = String(response?.text || "").toLowerCase();

  if (code === 250 || code === 251 || code === 252) {
    return {
      accepted: true,
      status: "valid",
      reason: `Mailbox accepted RCPT TO on the destination mail server (${code}).`,
    };
  }

  if ([450, 451, 452, 421].includes(code)) {
    return {
      accepted: false,
      status: "temporary_failure",
      reason: `Mail server returned a temporary SMTP response (${code}).`,
    };
  }

  if ([550, 551, 552, 553].includes(code)) {
    if (
      text.includes("user unknown") ||
      text.includes("no such user") ||
      text.includes("unknown user") ||
      text.includes("unknown recipient") ||
      text.includes("recipient rejected") ||
      text.includes("mailbox unavailable") ||
      text.includes("invalid mailbox") ||
      text.includes("not found") ||
      text.includes("does not exist") ||
      text.includes("unrouteable address") ||
      text.includes("bad destination mailbox") ||
      text.includes("5.1.1") ||
      text.includes("5.1.0")
    ) {
      return {
        accepted: false,
        status: "invalid",
        reason: `Mailbox ${normalizedEmail} was rejected by the destination mail server (${code}).`,
      };
    }

    if (
      text.includes("spam") ||
      text.includes("policy") ||
      text.includes("rate limit") ||
      text.includes("greylist") ||
      text.includes("temporarily") ||
      text.includes("try again")
    ) {
      return {
        accepted: false,
        status: "temporary_failure",
        reason: `Mail server rejected verification for policy or rate reasons (${code}).`,
      };
    }

    return {
      accepted: false,
      status: "unknown",
      reason: `Mail server rejected RCPT TO with an ambiguous permanent response (${code}).`,
    };
  }

  if (code >= 500 && code < 600) {
    return {
      accepted: false,
      status: "unknown",
      reason: `Mail server returned an unclassified SMTP rejection (${code}).`,
    };
  }

  return {
    accepted: false,
    status: "unknown",
    reason: code
      ? `Mail server returned an unclassified SMTP response (${code}).`
      : "Mail server did not complete SMTP verification cleanly.",
  };
}

function randomLocalPart() {
  return `swokei-check-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

async function probeMailboxOnHost({ host, normalizedEmail, domain, logger }) {
  const identity = getVerifierIdentity();
  const client = new SmtpClient({
    host,
    port: SMTP_PORT,
    connectTimeoutMs: SMTP_CONNECT_TIMEOUT_MS,
    responseTimeoutMs: SMTP_RESPONSE_TIMEOUT_MS,
    sessionTimeoutMs: SMTP_SESSION_TIMEOUT_MS,
  });

  try {
    const greeting = await client.connect();
    if (greeting.code !== 220) {
      return {
        status: "unknown",
        reason: `SMTP greeting was ${greeting.code || "missing"} instead of 220.`,
        mxHost: host,
      };
    }

    let ehloResponse = await client.command(`EHLO ${identity.ehloName}`);
    if (ehloResponse.code >= 500) {
      ehloResponse = await client.command(`HELO ${identity.ehloName}`);
    }

    if (ehloResponse.code < 200 || ehloResponse.code >= 400) {
      return {
        status: ehloResponse.code >= 400 && ehloResponse.code < 500
          ? "temporary_failure"
          : "unknown",
        reason: `SMTP handshake was not accepted by ${host} (${ehloResponse.code}).`,
        mxHost: host,
      };
    }

    const mailFromResponse = await client.command(
      `MAIL FROM:<${identity.mailFrom}>`
    );

    if (mailFromResponse.code >= 400 && mailFromResponse.code < 500) {
      return {
        status: "temporary_failure",
        reason: `MAIL FROM was temporarily rejected by ${host} (${mailFromResponse.code}).`,
        mxHost: host,
      };
    }

    if (mailFromResponse.code >= 500) {
      return {
        status: "unknown",
        reason: `MAIL FROM was rejected by ${host} (${mailFromResponse.code}).`,
        mxHost: host,
      };
    }

    const rcptResponse = await client.command(`RCPT TO:<${normalizedEmail}>`);
    const primary = classifyRcptResponse(rcptResponse, normalizedEmail);

    if (!primary.accepted) {
      return {
        status: primary.status,
        reason: primary.reason,
        mxHost: host,
      };
    }

    let catchAllDetected = false;

    for (let index = 0; index < CATCH_ALL_PROBES; index += 1) {
      const fakeEmail = `${randomLocalPart()}@${domain}`;
      const fakeRcptResponse = await client.command(`RCPT TO:<${fakeEmail}>`);
      const fakeResult = classifyRcptResponse(fakeRcptResponse, fakeEmail);

      if (fakeResult.accepted) {
        catchAllDetected = true;
        logger?.(
          `Catch-all accepted on ${host} for ${domain} using ${fakeEmail}.`
        );
        break;
      }

      if (fakeResult.status === "temporary_failure") {
        return {
          status: "unknown",
          reason:
            "Mailbox was accepted, but catch-all testing received a temporary response.",
          mxHost: host,
          catchAll: null,
        };
      }
    }

    return {
      status: catchAllDetected ? "catch_all" : "valid",
      reason: catchAllDetected
        ? "Mailbox was accepted, but randomized fake recipients were also accepted on the same domain."
        : primary.reason,
      mxHost: host,
      catchAll: catchAllDetected,
    };
  } finally {
    await client.quit();
  }
}

async function withDomainThrottle(domain, fn) {
  const previous = domainLocks.get(domain) || Promise.resolve();

  const next = previous
    .catch(() => {})
    .then(async () => {
      const now = Date.now();
      const last = domainLastProbeAt.get(domain) || 0;
      const waitMs = Math.max(0, DOMAIN_THROTTLE_MS - (now - last));

      if (waitMs > 0) {
        await sleep(waitMs);
      }

      domainLastProbeAt.set(domain, Date.now());
      return fn();
    });

  domainLocks.set(domain, next.finally(() => {
    if (domainLocks.get(domain) === next) {
      domainLocks.delete(domain);
    }
  }));

  return next;
}

function statusConfidence(status) {
  switch (status) {
    case "valid":
      return 0.9;
    case "invalid":
      return 0.95;
    case "catch_all":
      return 0.55;
    case "no_mail_domain":
      return 0.98;
    case "temporary_failure":
      return 0.35;
    default:
      return 0.2;
  }
}

function shouldContinueForStatus(status) {
  return status !== "invalid" && status !== "no_mail_domain";
}

function isTransientStatus(status) {
  return status === "temporary_failure" || status === "unknown";
}

async function verifyEmailAddress({
  redis,
  email,
  rowNumber = null,
  rowIndex = null,
  logger = null,
  cachePrefix = "emailverify:v1",
  cacheTtlSeconds = Number(process.env.EMAIL_VERIFY_CACHE_TTL_SECONDS) || 86400,
  transientCacheTtlSeconds =
    Number(process.env.EMAIL_VERIFY_TRANSIENT_CACHE_TTL_SECONDS) || 1800,
}) {
  const normalized = normalizeEmailAddress(email);
  const checkedAt = nowIso();

  if (!normalized.ok) {
    return {
      email: String(email || "").trim(),
      normalizedEmail: normalized.normalizedEmail,
      status: "invalid",
      confidence: statusConfidence("invalid"),
      reason: normalized.reason,
      mxHost: null,
      catchAll: false,
      checkedAt,
      rowNumber,
      rowIndex,
      shouldContinue: false,
      cached: false,
    };
  }

  const cacheKey = `${cachePrefix}:${normalized.normalizedEmail}`;

  if (redis) {
    try {
      const cached = await redis.get(cacheKey);
      if (cached && typeof cached === "object" && cached.status) {
        return {
          ...cached,
          email: String(email || "").trim(),
          rowNumber,
          rowIndex,
          cached: true,
        };
      }
    } catch (err) {
      logger?.(`Email verification cache read failed: ${err?.message || err}`);
    }
  }

  const domainResolution = await resolveMailHosts(normalized.domain);

  if (!domainResolution.ok) {
    const status =
      domainResolution.type === "no_mail_domain" ? "no_mail_domain" : "temporary_failure";
    const result = {
      email: normalized.email,
      normalizedEmail: normalized.normalizedEmail,
      status,
      confidence: statusConfidence(status),
      reason: domainResolution.reason,
      mxHost: null,
      catchAll: false,
      checkedAt,
      rowNumber,
      rowIndex,
      shouldContinue: shouldContinueForStatus(status),
      cached: false,
    };

    if (redis) {
      try {
        await redis.set(cacheKey, result, {
          ex: isTransientStatus(status)
            ? transientCacheTtlSeconds
            : cacheTtlSeconds,
        });
      } catch (_) {}
    }

    return result;
  }

  let finalResult = null;
  let lastError = null;

  for (const candidate of domainResolution.hosts) {
    try {
      finalResult = await withDomainThrottle(normalized.domain, () =>
        probeMailboxOnHost({
          host: candidate.host,
          normalizedEmail: normalized.normalizedEmail,
          domain: normalized.domain,
          logger,
        })
      );

      if (finalResult.status !== "temporary_failure" || domainResolution.hosts.length === 1) {
        break;
      }
    } catch (err) {
      lastError = err;
      logger?.(
        `SMTP probe failed for ${normalized.normalizedEmail} via ${candidate.host}: ${err?.message || err}`
      );
      finalResult = {
        status: "temporary_failure",
        reason: /timed out/i.test(String(err?.message || ""))
          ? "The mail server timed out during SMTP verification."
          : "SMTP verification could not complete on the destination mail server.",
        mxHost: candidate.host,
        catchAll: null,
      };
    }
  }

  const status = finalResult?.status || "unknown";
  const result = {
    email: normalized.email,
    normalizedEmail: normalized.normalizedEmail,
    status,
    confidence: statusConfidence(status),
    reason:
      finalResult?.reason ||
      (lastError
        ? `SMTP verification failed: ${lastError.message}`
        : "The mail server did not complete verification."),
    mxHost: finalResult?.mxHost || domainResolution.hosts[0]?.host || null,
    catchAll:
      typeof finalResult?.catchAll === "boolean"
        ? finalResult.catchAll
        : status === "catch_all",
    checkedAt,
    rowNumber,
    rowIndex,
    shouldContinue: shouldContinueForStatus(status),
    cached: false,
    mailRouting: {
      type: domainResolution.type,
      hosts: domainResolution.hosts.map((item) => item.host),
    },
  };

  if (redis) {
    try {
      await redis.set(cacheKey, result, {
        ex: isTransientStatus(status)
          ? transientCacheTtlSeconds
          : cacheTtlSeconds,
      });
    } catch (err) {
      logger?.(`Email verification cache write failed: ${err?.message || err}`);
    }
  }

  return result;
}

module.exports = {
  normalizeEmailAddress,
  verifyEmailAddress,
};
