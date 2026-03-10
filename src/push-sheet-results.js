const path = require("path");
require("dotenv").config();
const { google } = require("googleapis");

const ROOT = path.join(__dirname, "..");

async function getSheetsClient() {
  const keyFile = process.env.GOOGLE_SERVICE_ACCOUNT_KEYFILE;
  if (!keyFile) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_KEYFILE in .env");

  const auth = new google.auth.GoogleAuth({
    keyFile: path.isAbsolute(keyFile) ? keyFile : path.join(ROOT, keyFile),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

async function getSheetIdByName(sheets, spreadsheetId, sheetTab) {
  const spreadsheet = await sheets.spreadsheets.get({
    spreadsheetId,
  });

  const sheet = spreadsheet.data.sheets.find(
    (s) => s.properties.title === sheetTab
  );

  if (!sheet) {
    throw new Error(`Sheet tab "${sheetTab}" not found`);
  }

  return sheet.properties.sheetId;
}

async function pushSheetResult({
  sheetId,
  sheetTab,
  rowIndex,
  comment,
  column,
}) {
  if (!sheetId) throw new Error("Missing sheetId");
  if (!rowIndex) throw new Error("Missing rowIndex");

  const tab = sheetTab || "Sheet1";
  const commentCol = column || "O";

  const sheets = await getSheetsClient();

  const numericSheetId = await getSheetIdByName(sheets, sheetId, tab);
  const columnIndex = commentCol.charCodeAt(0) - 65;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: sheetId,
    requestBody: {
      requests: [
        {
          repeatCell: {
            range: {
              sheetId: numericSheetId,
              startColumnIndex: columnIndex,
              endColumnIndex: columnIndex + 1,
            },
            cell: {
              userEnteredFormat: {
                wrapStrategy: "CLIP",
              },
            },
            fields: "userEnteredFormat.wrapStrategy",
          },
        },
      ],
    },
  });

  const range = `${tab}!${commentCol}${rowIndex}`;

  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range,
    valueInputOption: "RAW",
    requestBody: {
      values: [[comment]],
    },
  });
}

async function deleteSheetRow({
  sheetId,
  sheetTab,
  rowIndex,
}) {
  if (!sheetId) throw new Error("Missing sheetId");
  if (!rowIndex) throw new Error("Missing rowIndex");

  const tab = sheetTab || "Sheet1";
  const sheets = await getSheetsClient();
  const numericSheetId = await getSheetIdByName(sheets, sheetId, tab);

  const zeroBasedRow = Number(rowIndex) - 1;
  if (!Number.isInteger(zeroBasedRow) || zeroBasedRow < 0) {
    throw new Error(`Invalid rowIndex: ${rowIndex}`);
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: sheetId,
    requestBody: {
      requests: [
        {
          deleteDimension: {
            range: {
              sheetId: numericSheetId,
              dimension: "ROWS",
              startIndex: zeroBasedRow,
              endIndex: zeroBasedRow + 1,
            },
          },
        },
      ],
    },
  });
}

module.exports = pushSheetResult;
module.exports.deleteSheetRow = deleteSheetRow;