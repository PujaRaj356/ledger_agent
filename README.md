# Acme Appliances Ledger Agent

FastAPI service for the Ledger Agent assignment. It accepts `POST /answer` with `{"question":"..."}` and returns `{"answer":"..."}`.

## Configure

Set the environment variable `LEDGER_DISCOVERY_URL` to the complete personal discovery URL provided by the assignment (the URL ending in `questionId=q-ledger-agent-server&path=/`). Do not commit that signed URL to a public repository.

## Run locally

```bash
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:LEDGER_DISCOVERY_URL = 'PASTE_YOUR_FULL_PERSONAL_DISCOVERY_URL_HERE'
uvicorn main:app --reload
```

On Windows, use the full original URL as the environment variable value. Keep the URL inside single quotes if it contains `&` characters.

## Test

Open `http://127.0.0.1:8000/health`, then send:

```bash
curl -X POST http://127.0.0.1:8000/answer -H "Content-Type: application/json" -d "{\"question\":\"What was the total revenue in USD from the North region in March 2026?\"}"
```

## Render

Connect this repository to Render as a Web Service. Build command: `pip install -r requirements.txt`. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`. Set `LEDGER_DISCOVERY_URL` in Environment. Health check path: `/health`.

The app fetches the discovery document, currency rates, and NDJSON export at startup, then deduplicates orders by ID using the latest `updated_at`. Revenue includes only rows whose current status is `paid` and converts amounts to USD with the supplied `usd_per_unit` rates. Order dates are interpreted in `Asia/Kolkata`.

This service uses deterministic parsing for common revenue, refund, product, customer, region, and date-period questions; it does not call a hosted LLM API.
