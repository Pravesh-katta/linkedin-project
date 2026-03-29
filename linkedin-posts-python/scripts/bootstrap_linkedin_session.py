from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings
from app.services.linkedin_scraper import LinkedInScraper


def main() -> None:
    settings = get_settings()
    scraper = LinkedInScraper(settings)
    saved_path = scraper.bootstrap_session()
    print(f"LinkedIn session saved to: {saved_path}")


if __name__ == "__main__":
    main()
