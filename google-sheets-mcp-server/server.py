"""
Google Sheets MCP Server - Thin wrappers around Google Sheets and Drive APIs.

OAuth Setup:
1. Go to https://console.cloud.google.com/apis/credentials
2. Create OAuth 2.0 Client ID (Desktop app type)
3. Download JSON and save as 'client_secrets.json' in this directory
4. Run the server - it will prompt you to authorize on first use
"""

import csv
import json
import logging
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load .env from the server directory
SERVER_DIR = Path(__file__).parent
load_dotenv(SERVER_DIR / ".env")

# Setup logging to file (stdout reserved for MCP, stderr suppressed by Claude Code)
LOG_FILE = SERVER_DIR / "google-sheets.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)
logger = logging.getLogger(__name__)

mcp = FastMCP("google-sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# OAuth file paths
TOKEN_FILE = SERVER_DIR / "token.json"
CLIENT_SECRETS_FILE = SERVER_DIR / "client_secrets.json"


def get_credentials():
    """Get Google API credentials using OAuth2 flow."""
    creds = None

    # Check for existing token
    if TOKEN_FILE.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
            logger.info("Loaded credentials from token.json")
        except Exception as e:
            logger.error(f"Error loading token.json: {e}")

    # If no valid credentials, run OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                logger.info("Refreshed expired credentials")
            except Exception as e:
                logger.error(f"Error refreshing credentials: {e}")
                creds = None

        if not creds:
            if not CLIENT_SECRETS_FILE.exists():
                raise FileNotFoundError(
                    f"OAuth client secrets not found at {CLIENT_SECRETS_FILE}. "
                    "Please download from Google Cloud Console and save as 'client_secrets.json'"
                )

            logger.info("Starting OAuth flow...")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRETS_FILE), SCOPES
            )
            creds = flow.run_local_server(port=0)
            logger.info("OAuth flow completed successfully")

        # Save credentials for future use
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
            logger.info(f"Saved credentials to {TOKEN_FILE}")

    return creds


def get_sheets_service():
    """Get Google Sheets API service."""
    credentials = get_credentials()
    return build("sheets", "v4", credentials=credentials)


def get_drive_service():
    """Get Google Drive API service."""
    credentials = get_credentials()
    return build("drive", "v3", credentials=credentials)


@mcp.tool()
async def spreadsheets_get(
    spreadsheet_id: str,
) -> dict:
    """
    Get spreadsheet metadata.

    Args:
        spreadsheet_id
    """
    logger.info(f"spreadsheets_get request: spreadsheet_id={spreadsheet_id}")

    try:
        sheets_service = get_sheets_service()

        result = (
            sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        )

        logger.info(f"spreadsheets_get response: {json.dumps(result)}")
        return result

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}


async def spreadsheets_values_get(
    spreadsheet_id: str,
    range: str,
    major_dimension: str = "ROWS",
    value_render_option: str = "FORMATTED_VALUE",
) -> dict:
    """
    Args:
        spreadsheet_id
        range: A1 notation (eg, "Sheet1!A1:D10", "Sheet1", "A1:D10")
        major_dimension: ROWS or COLUMNS
        value_render_option: FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
    """
    logger.info(
        f"spreadsheets_values_get request: spreadsheet_id={spreadsheet_id}, range={range}"
    )

    try:
        sheets_service = get_sheets_service()

        result = (
            sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=range,
                majorDimension=major_dimension,
                valueRenderOption=value_render_option,
                dateTimeRenderOption="FORMATTED_STRING",
            )
            .execute()
        )

        logger.info(f"spreadsheets_values_get response: {json.dumps(result)}")
        return result

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}


@mcp.tool()
async def spreadsheets_batch_update(
    spreadsheet_id: str,
    requests: list[dict],
) -> dict:
    """
    Add/delete/update sheets
    Merge/unmerge cells
    Insert/delete rows/columns
    Etc.

    Args:
        spreadsheet_id
        requests: List of request objects. Common types:
            - addSheet: {"addSheet": {"properties": {"title": "NewSheet"}}}
            - deleteSheet: {"deleteSheet": {"sheetId": 123}}
            - updateSheetProperties: {"updateSheetProperties": {"properties": {"title": "New"}, "fields": "title"}}
            - mergeCells: {"mergeCells": {"range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 2}, "mergeType": "MERGE_ALL"}}
            - unmergeCells: {"unmergeCells": {"range": {...}}}
            - insertDimension: {"insertDimension": {"range": {"sheetId": 0, "dimension": "ROWS", "startIndex": 0, "endIndex": 1}}}
            - deleteDimension: {"deleteDimension": {"range": {"sheetId": 0, "dimension": "ROWS", "startIndex": 0, "endIndex": 1}}}
            - autoResizeDimensions: {"autoResizeDimensions": {"dimensions": {"sheetId": 0, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 5}}}
            - Others: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request
    """
    logger.info(
        f"spreadsheets_batch_update request: spreadsheet_id={spreadsheet_id}, requests={requests}"
    )

    try:
        sheets_service = get_sheets_service()

        body = {"requests": requests}

        result = (
            sheets_service.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body,
            )
            .execute()
        )

        logger.info(f"spreadsheets_batch_update response: {json.dumps(result)}")
        return result

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}


async def spreadsheets_values_update(
    spreadsheet_id: str,
    range: str,
    values: list[list],
    value_input_option: str = "USER_ENTERED",
    major_dimension: str = "ROWS",
) -> dict:
    """
    Args:
        spreadsheet_id
        range: A1 notation (eg, "Sheet1!A1:D10")
        values: 2D array of values (list of rows or columns depending on major_dimension)
        value_input_option: USER_ENTERED (parsed like UI) or RAW (stored as-is)
        major_dimension: ROWS or COLUMNS (how values array is interpreted)
    """
    logger.info(
        f"spreadsheets_values_update request: spreadsheet_id={spreadsheet_id}, range={range}, values={values}"
    )

    try:
        sheets_service = get_sheets_service()

        body = {
            "values": values,
            "majorDimension": major_dimension,
        }

        result = (
            sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )

        logger.info(f"spreadsheets_values_update response: {json.dumps(result)}")
        return result

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}


async def spreadsheets_values_append(
    spreadsheet_id: str,
    range: str,
    values: list[list],
    value_input_option: str = "USER_ENTERED",
    major_dimension: str = "ROWS",
    insert_data_option: str = "INSERT_ROWS",
) -> dict:
    """
    Args:
        spreadsheet_id
        range: A1 notation of table to append to (eg, "Sheet1" or "Sheet1!A:D")
        values: 2D array of values (list of rows or columns depending on major_dimension)
        value_input_option: USER_ENTERED or RAW
        major_dimension: ROWS or COLUMNS (how values array is interpreted)
        insert_data_option: INSERT_ROWS (insert new) or OVERWRITE (overwrite existing)
    """
    logger.info(
        f"spreadsheets_values_append request: spreadsheet_id={spreadsheet_id}, range={range}, values={values}"
    )

    try:
        sheets_service = get_sheets_service()

        body = {
            "values": values,
            "majorDimension": major_dimension,
        }

        result = (
            sheets_service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=range,
                valueInputOption=value_input_option,
                insertDataOption=insert_data_option,
                body=body,
            )
            .execute()
        )

        logger.info(f"spreadsheets_values_append response: {json.dumps(result)}")
        return result

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}


async def spreadsheets_values_clear(
    spreadsheet_id: str,
    range: str,
) -> dict:
    """
    Args:
        spreadsheet_id
        range: A1 notation range to clear (eg, "Sheet1!A2:D100")
    """
    logger.info(
        f"spreadsheets_values_clear request: spreadsheet_id={spreadsheet_id}, range={range}"
    )

    try:
        sheets_service = get_sheets_service()

        result = (
            sheets_service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=spreadsheet_id,
                range=range,
                body={},
            )
            .execute()
        )

        logger.info(f"spreadsheets_values_clear response: {json.dumps(result)}")
        return result

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}


async def spreadsheets_search(
    spreadsheet_id: str,
    search_term: str,
    sheet_name: str = "Sheet1",
    column: Optional[str] = None,
    case_sensitive: bool = False,
) -> dict:
    """
    Search for rows containing a term. Returns matching rows with row numbers.

    Args:
        spreadsheet_id
        search_term
        sheet_name
        column: Column letter to limit search (eg, "A", "B"). None searches all columns.
        case_sensitive: Whether search is case-sensitive (default: False)
    """
    logger.info(
        f"spreadsheets_search request: spreadsheet_id={spreadsheet_id}, search_term={search_term}, sheet_name={sheet_name}, column={column}"
    )

    try:
        sheets_service = get_sheets_service()

        # Read all data from the sheet
        result = (
            sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                majorDimension="ROWS",
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )

        values = result.get("values", [])
        matches = []

        search = search_term if case_sensitive else search_term.lower()

        # Convert column letter to index
        col_index = None
        if column is not None:
            col_index = ord(column.upper()) - ord("A")

        for i, row in enumerate(values):
            if col_index is not None:
                # Search only in specified column
                if col_index < len(row):
                    cell_value = str(row[col_index])
                    if not case_sensitive:
                        cell_value = cell_value.lower()
                    if search in cell_value:
                        matches.append({"row_number": i + 1, "data": row})
            else:
                # Search all columns
                row_text = " ".join(str(cell) for cell in row)
                if not case_sensitive:
                    row_text = row_text.lower()
                if search in row_text:
                    matches.append({"row_number": i + 1, "data": row})

        response = {"matches": matches, "total_matches": len(matches)}
        logger.info(f"spreadsheets_search response: {json.dumps(response)}")
        return response

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}


@mcp.tool()
async def upload_csv(
    csv_file_path: str,
    spreadsheet_id: str,
    sheet_name: str = "Sheet1",
    value_input_option: str = "USER_ENTERED",
) -> dict:
    """
    Upload a CSV file to a Google Sheet, overwriting the entire sheet.

    Args:
        csv_file_path: Path to the CSV file to upload
        spreadsheet_id: Target spreadsheet ID
        sheet_name: Name of the sheet to overwrite (default: "Sheet1")
        value_input_option: USER_ENTERED (parsed like UI) or RAW (stored as-is)
    """
    logger.info(
        f"upload_csv request: csv_file_path={csv_file_path}, spreadsheet_id={spreadsheet_id}, sheet_name={sheet_name}"
    )

    try:
        # Read and parse CSV file
        if not os.path.exists(csv_file_path):
            error_msg = f"CSV file not found: {csv_file_path}"
            logger.error(error_msg)
            return {"error": error_msg}

        with open(csv_file_path, "r", encoding="utf-8") as f:
            csv_reader = csv.reader(f)
            values = list(csv_reader)

        if not values:
            error_msg = "CSV file is empty"
            logger.error(error_msg)
            return {"error": error_msg}

        logger.info(f"Parsed {len(values)} rows from CSV")

        sheets_service = get_sheets_service()

        # Clear the entire sheet first
        clear_result = (
            sheets_service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                body={},
            )
            .execute()
        )
        logger.info(f"Cleared sheet: {sheet_name}")

        # Upload CSV data starting at A1
        body = {
            "values": values,
            "majorDimension": "ROWS",
        }

        result = (
            sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )

        logger.info(f"upload_csv response: {json.dumps(result)}")
        return result

    except HttpError as e:
        error_msg = f"Google API error: {e.reason}"
        logger.error(error_msg)
        return {"error": error_msg}
    except Exception as e:
        error_msg = f"Error uploading CSV: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}


if __name__ == "__main__":
    mcp.run()
