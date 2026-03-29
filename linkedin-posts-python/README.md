# LinkedIn Posts Python

Local Python app for collecting LinkedIn post results across all U.S. states and reviewing them in a simple web UI.

## Current scope

- create saved searches like `python developer`
- expand searches into all-state variants such as `python developer "TX"` and `python developer Texas`
- collect LinkedIn post results using Playwright
- store results in SQLite
- review grouped results by state in a FastAPI + Jinja web app

This MVP does not implement auto-apply yet.

## Stack

- FastAPI
- Jinja2
- SQLite
- Playwright for Python

## Setup

1. Create and activate a virtual environment if you want one.
2. Install dependencies:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

3. Copy the environment template:

```bash
cp .env.example .env
```

4. Bootstrap the LinkedIn session with manual login:

```bash
python scripts/bootstrap_linkedin_session.py
```

5. Start the app:

```bash
uvicorn app.main:app --reload
```

6. Open `http://127.0.0.1:8000`

## Session-based login flow

This project uses a safer option for LinkedIn authentication:

- the script opens a real Chromium window
- you log in manually one time
- the app saves LinkedIn session state to `data/linkedin_storage_state.json`
- later searches reuse that session state instead of storing your raw password in the app
- normal search runs use headless Chromium in the background by default

If LinkedIn expires the session or shows a checkpoint, just rerun the bootstrap script.

## Notes

- LinkedIn selectors and filters can change over time.
- LinkedIn may still challenge the account with captcha, email verification, or checkpoints.
- The scraper is intentionally structured so selectors and extraction logic are easy to adjust.
