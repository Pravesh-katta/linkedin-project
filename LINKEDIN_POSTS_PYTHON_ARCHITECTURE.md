# LinkedIn Posts Python-Only Architecture

## Goal

Build a standalone Python app that:

- searches LinkedIn content for a keyword such as `java developer`
- switches to `Posts`
- sets `Sort by = Latest`
- sets `Date posted = Past 24 hours`
- runs the search across all U.S. states
- stores matched posts in a local database
- shows search results and detail pages in a simple web UI

This document is architecture only. It does not add implementation yet.

## Scope

Version 1:

- discovery and collection of LinkedIn posts
- state-aware matching across all states
- search results page
- post detail page
- local database storage

Version 2:

- email extraction from posts where a contact email is explicitly present
- draft outreach generation
- manual approval queue before sending
- send/audit/suppression tracking

## Important Constraint

This design assumes browser automation, not an official public LinkedIn post-search API.

That means:

- selectors and filters may change over time
- login/session handling may break
- state matching is heuristic because regular LinkedIn posts do not expose a reliable structured state filter

## Recommended Project Folder

Create this as a standalone app in the current folder:

```text
Linkedin project/
└── linkedin-posts-python/
    ├── app/
    │   ├── main.py
    │   ├── config.py
    │   ├── db.py
    │   ├── models.py
    │   ├── state_catalog.py
    │   ├── scoring.py
    │   ├── scheduler.py
    │   ├── services/
    │   │   ├── linkedin_scraper.py
    │   │   ├── search_runner.py
    │   │   ├── contact_extractor.py
    │   │   └── outreach_service.py
    │   ├── templates/
    │   │   ├── index.html
    │   │   ├── search_detail.html
    │   │   └── post_detail.html
    │   └── static/
    │       └── styles.css
    ├── data/
    │   └── app.db
    ├── requirements.txt
    ├── .env.example
    └── README.md
```

## Python Stack

Use:

- `FastAPI` for the web app and JSON API
- `Jinja2` for server-rendered HTML pages
- `SQLite` for the first local database
- `Playwright for Python` for LinkedIn browser automation
- a simple background loop or `APScheduler` for scheduled runs

## Exact LinkedIn Workflow To Automate

For each saved keyword search, the app should mimic this flow:

1. open LinkedIn
2. search for a keyword like `java developer`
3. switch to the `Posts` tab
4. set `Sort by` to `Latest`
5. set `Date posted` to `Past 24 hours`
6. repeat the search using state-aware query variants
7. collect the resulting post cards
8. store normalized results locally

## All-States Search Model

One user search should expand into many state-level search attempts.

Example:

- logical search: `java developer`
- expanded attempts:
  - `java developer "TX"`
  - `java developer Texas`
  - `java developer "CA"`
  - `java developer California`
  - `java developer "FL"`
  - `java developer Florida`
  - and so on for every enabled state

This means the app does not rely on a true LinkedIn location filter for posts. Instead, it uses:

- repeated state-aware searches
- state scoring
- deduplication across runs

## High-Level Components

### 1. Web Layer

`FastAPI` handles:

- create saved searches
- list saved searches
- run a search now
- view grouped results by state
- open a single post detail page
- review email candidates in phase 2

### 2. Scraper Layer

`linkedin_scraper.py` handles:

- starting the browser
- restoring LinkedIn session state
- opening search pages
- selecting `Posts`
- applying `Latest`
- applying `Past 24 hours`
- scrolling and extracting result cards

### 3. Search Runner

`search_runner.py` handles:

- expanding one logical search into all-state attempts
- calling the scraper
- normalizing raw extracted data
- deduplicating posts
- calculating keyword/state/time confidence
- storing results

### 4. State Matching

`state_catalog.py` and `scoring.py` handle:

- all 50 U.S. states and optional `DC`
- abbreviation to full-name matching
- full-name to abbreviation matching
- post-to-state confidence scoring

### 5. Storage Layer

`SQLite` stores:

- saved searches
- search runs
- normalized posts
- search results
- extracted contacts
- outreach drafts and send logs in phase 2

### 6. Scheduler

Runs searches every fixed interval:

- local MVP: background loop inside FastAPI startup
- later: separate worker if needed

## User Flow

### Search Flow

1. user creates a saved search such as `java developer`
2. app schedules or runs the search immediately
3. app fans out the search across all states
4. scraper collects LinkedIn post results
5. runner scores, filters, and deduplicates them
6. app shows results grouped by state
7. user opens a detail page for one post

### Outreach Flow Later

1. app extracts explicit contact emails from matched posts
2. app creates draft outreach messages
3. user reviews and approves drafts
4. app sends approved emails
5. app records status, opt-outs, and failures

## Data Model

### `searches`

- `id`
- `keywords`
- `state_scope`
- `enabled_states_json`
- `window_hours`
- `max_results_per_state`
- `schedule_minutes`
- `is_active`
- `created_at`
- `updated_at`
- `last_run_at`

### `search_runs`

- `id`
- `search_id`
- `state_code`
- `status`
- `started_at`
- `finished_at`
- `found_count`
- `error_message`

### `posts`

- `id`
- `external_id`
- `permalink`
- `author_name`
- `author_profile_url`
- `content_text`
- `relative_time_text`
- `published_hint_at`
- `matched_state`
- `scraped_at`
- `raw_payload_json`

### `search_results`

- `search_id`
- `post_id`
- `matched_at`
- `matched_state`
- `keyword_score`
- `state_score`
- `time_score`
- `total_score`

### `contacts`

- `id`
- `post_id`
- `email`
- `source_type`
- `is_valid_format`
- `created_at`

### `outreach_drafts`

- `id`
- `post_id`
- `contact_id`
- `subject`
- `body`
- `status`
- `approved_at`
- `sent_at`

## UI Pages

### Index Page

Shows:

- create search form
- list of saved searches
- last run status
- run-now button
- counts by state

### Search Detail Page

Shows:

- query details
- run history
- grouped posts by state
- confidence score
- link to original LinkedIn post

### Post Detail Page

Shows:

- full normalized content
- matched state
- extracted contact email if present
- draft outreach section in phase 2

## API Endpoints

- `GET /health`
- `GET /`
- `POST /searches`
- `GET /searches`
- `GET /searches/{search_id}`
- `POST /searches/{search_id}/run`
- `GET /searches/{search_id}/results`
- `GET /posts/{post_id}`

Phase 2:

- `POST /posts/{post_id}/extract-contact`
- `POST /drafts/{post_id}`
- `POST /drafts/{draft_id}/approve`
- `POST /drafts/{draft_id}/send`

## Operational Notes

Recommended environment variables:

- `LINKEDIN_EMAIL`
- `LINKEDIN_PASSWORD`
- `LINKEDIN_HEADLESS`
- `APP_HOST`
- `APP_PORT`
- `DATABASE_PATH`
- `SESSION_STATE_PATH`

Common failure cases:

- LinkedIn login challenge
- expired session
- changed filters/selectors
- slow loading or partial results
- duplicate posts across state queries

## MVP Build Order

1. create the Python project skeleton
2. add SQLite schema and CRUD helpers
3. add all-state catalog and scoring helpers
4. add FastAPI pages and routes
5. add Playwright LinkedIn search flow
6. add post normalization and dedupe
7. add grouped search results UI
8. add contact extraction
9. add manual-approval outreach flow

## Recommendation

Start with:

- `FastAPI + SQLite + Playwright`
- all-state search from day one
- grouped search results by state
- manual review before any outbound email automation

Keep this as a standalone Python app in `Linkedin project/` and do not mix it with the Rust services.
