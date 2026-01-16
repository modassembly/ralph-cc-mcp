from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SERVER_DIR = Path(__file__).parent

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]
TOKEN_FILE = SERVER_DIR / "token.json"
CLIENT_SECRETS_FILE = SERVER_DIR / "client_secrets.json"


def main():
    """Generate OAuth2 token for Google Sheets API."""

    # Check if client_secrets.json exists
    if not CLIENT_SECRETS_FILE.exists():
        print(f"Error: {CLIENT_SECRETS_FILE} not found!")
        print("\nPlease follow these steps:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select an existing one")
        print("3. Enable the following APIs:")
        print("   - Google Sheets API")
        print("   - Google Drive API")
        print("4. Go to 'Credentials' -> 'Create Credentials' -> 'OAuth 2.0 Client ID'")
        print("5. Choose 'Desktop app' as the application type")
        print("6. Download the credentials JSON file")
        print(f"7. Save it as {CLIENT_SECRETS_FILE}")
        return

    creds = None

    # Check if token already exists
    if TOKEN_FILE.exists():
        print(f"Found existing token at {TOKEN_FILE}")
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    # If credentials are invalid or don't exist, run OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing expired token...")
            creds.refresh(Request())
        else:
            print("Starting OAuth flow...")
            print("A browser window will open for authentication.")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRETS_FILE), SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save the credentials
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
        print(f"\n✓ Token saved to {TOKEN_FILE}")
    else:
        print("✓ Token is valid and ready to use")

    print("\nYou can now run your Google Sheets MCP server!")


if __name__ == "__main__":
    main()
