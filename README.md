# Ralph + Claude Code + MCP + Apollo + Google Sheets

## Setup

**Install python & uv**

**Install dependencies:**
```bash
uv sync --all-packages
```

## Configuration

### Apollo Server
Create `apollo-mcp-server/.env`:
```
APOLLO_API_KEY=your_api_key_here
```

### Google Sheets Server

**Set up OAuth credentials:**
1. Create a new OAuth 2.0 Desktop Client in https://console.cloud.google.com/
2. Download and save it as `google-sheets-mcp-server/client_secrets.json`

**Generate token:**
```bash
cd google-sheets-mcp-server
uv run generate_token.py
```

This will open your browser for OAuth consent and generate `token.json`.

## Usage

**Run Claude Code:**
```bash
claude
```

Prompt it to follow `specs/companies/prompt.md` or `specs/people/prompt.md`.