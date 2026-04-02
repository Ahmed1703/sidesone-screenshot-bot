function looksLikeWebsiteUrl(value) {
  const v = String(value || "").trim().toLowerCase();
  if (!v) return false;
  if (v.includes("@")) return false;
  if (/^https?:\/\//.test(v)) return true;
  if (/^(www\.)?[a-z0-9.-]+\.[a-z]{2,}(\/.*)?$/.test(v)) return true;
  return false;
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      values.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current.trim());
  return values;
}

function normalizeHeader(value) {
  return String(value || "")
    .replace(/^"|"$/g, "")
    .trim()
    .toLowerCase();
}

function getCell(cells, index) {
  if (index < 0) return "";
  return String(cells[index] || "")
    .replace(/^"|"$/g, "")
    .trim();
}

function pullCsvRows(meta) {
  const rawText = String(meta.csvRawText || "").replace(/\r/g, "");
  if (!rawText.trim()) {
    throw new Error("Missing csvRawText in batch job meta.");
  }

  const lines = rawText
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length < 2) {
    throw new Error("CSV file has no data rows.");
  }

  const headers = parseCsvLine(lines[0]).map((h) =>
    String(h || "").replace(/^"|"$/g, "").trim()
  );

  const findIndex = (headerName) => {
    const target = normalizeHeader(headerName);
    if (!target) return -1;
    return headers.findIndex((header) => normalizeHeader(header) === target);
  };

  const urlIndex = findIndex(meta.csvUrlColumn);
  const emailIndex = findIndex(meta.csvMailColumn);
  const firstNameIndex = findIndex(
    meta.firstNameColumn || meta.optionalMappings?.first_name
  );
  const companyNameIndex = findIndex(
    meta.companyNameColumn || meta.optionalMappings?.company_name
  );
  const industryIndex = findIndex(
    meta.industryColumn || meta.optionalMappings?.industry
  );
  const locationIndex = findIndex(
    meta.locationColumn || meta.optionalMappings?.location
  );

  if (urlIndex === -1) {
    throw new Error(`CSV URL column not found: ${meta.csvUrlColumn}`);
  }

  if (emailIndex === -1) {
    throw new Error(`CSV email column not found: ${meta.csvMailColumn}`);
  }

  const rows = [];
  let ignored = 0;

  for (let i = 1; i < lines.length; i += 1) {
    const cells = parseCsvLine(lines[i]);

    const url = getCell(cells, urlIndex);
    if (!url || !looksLikeWebsiteUrl(url)) {
      ignored += 1;
      continue;
    }

    rows.push({
      url,
      rowIndex: i + 1,
      recipientEmail: getCell(cells, emailIndex),
      firstName: getCell(cells, firstNameIndex),
      companyName: getCell(cells, companyNameIndex),
      industry: getCell(cells, industryIndex),
      location: getCell(cells, locationIndex),
    });
  }

  if (!rows.length) {
    throw new Error("No website URLs detected in selected CSV URL column.");
  }

  console.log(`Pulled ${rows.length} website URL(s) from CSV`);
  console.log(`Ignored ${ignored} non-URL value(s)`);

  return rows;
}

module.exports = pullCsvRows;