const path = require("path");
require("dotenv").config();
const { getSheetsClientForUser } = require("./google-user-sheets");

const ROOT = path.join(__dirname, "..");

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

async function ensureOutputHeader({
  sheets,
  sheetId,
  sheetTab,
  column,
  headerText = "Website comment",
}) {
  const headerRange = `${sheetTab}!${column}1`;

  const existing = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: headerRange,
  });

  const currentHeader = String(existing.data.values?.[0]?.[0] || "").trim();

  if (!currentHeader) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: headerRange,
      valueInputOption: "RAW",
      requestBody: {
        values: [[headerText]],
      },
    });
  }
}

async function pushSheetResult({
  userId,
  sheetId,
  sheetTab,
  rowIndex,
  comment,
  column,
}) {
  if (!userId) throw new Error("Missing userId");
  if (!sheetId) throw new Error("Missing sheetId");
  if (!rowIndex) throw new Error("Missing rowIndex");

  const tab = sheetTab || "Sheet1";
  const commentCol = column || "O";

  const sheets = await getSheetsClientForUser(userId);

  const numericSheetId = await getSheetIdByName(sheets, sheetId, tab);
  const columnIndex = commentCol.charCodeAt(0) - 65;

  await ensureOutputHeader({
    sheets,
    sheetId,
    sheetTab: tab,
    column: commentCol,
    headerText: "Website comment",
  });

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

async function deleteSheetRow({ userId, sheetId, sheetTab, rowIndex }) {
  if (!userId) throw new Error("Missing userId");
  if (!sheetId) throw new Error("Missing sheetId");
  if (!rowIndex) throw new Error("Missing rowIndex");

  const tab = sheetTab || "Sheet1";
  const sheets = await getSheetsClientForUser(userId);
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