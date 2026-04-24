from __future__ import annotations

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from playwright.sync_api import sync_playwright

from app.config import get_settings


MAX_WAIT_SECONDS = 600
POLL_INTERVAL_SECONDS = 3


def main() -> None:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    storage_path = settings.linkedin_storage_state_path

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False, slow_mo=50)
        context = browser.new_context(viewport={"width": 1440, "height": 960})
        page = context.new_page()
        page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")

        print("=" * 70, flush=True)
        print("Chromium is open. Log in to LinkedIn manually.", flush=True)
        print("Make sure you reach the feed (https://www.linkedin.com/feed/).", flush=True)
        print(f"Session will auto-save to: {storage_path}", flush=True)
        print(f"Auto-detect timeout: {MAX_WAIT_SECONDS}s", flush=True)
        print("=" * 70, flush=True)

        deadline = time.time() + MAX_WAIT_SECONDS
        saved = False
        while time.time() < deadline:
            try:
                cookies = context.cookies()
            except Exception:
                cookies = []
            has_li_at = any(
                c.get("name") == "li_at" and "linkedin.com" in c.get("domain", "")
                for c in cookies
            )
            current_url = ""
            try:
                current_url = page.url
            except Exception:
                pass

            on_authed_page = (
                "linkedin.com" in current_url
                and "/login" not in current_url
                and "/checkpoint" not in current_url
                and "/uas/" not in current_url
            )

            if has_li_at and on_authed_page:
                # Give the page a beat to settle so all cookies (incl. JSESSIONID) land
                time.sleep(2)
                context.storage_state(path=str(storage_path))
                print(f"\nSESSION SAVED to {storage_path}", flush=True)
                print(f"Final URL: {current_url}", flush=True)
                saved = True
                break

            time.sleep(POLL_INTERVAL_SECONDS)

        if not saved:
            print(
                f"\nTIMEOUT after {MAX_WAIT_SECONDS}s — login was not detected. "
                "If you did log in, your URL may have stayed on a checkpoint page. "
                "Re-run this script and make sure you land on the feed.",
                flush=True,
            )

        try:
            browser.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
