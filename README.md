# FlexJob-Automation

Local automation workspace for a FlexJobs job-search and application assistant.

## Current Scope

The first implementation milestone includes:

- parsing the client CV into structured JSON
- initializing the local SQLite database
- preparing search targets from the client profile
- setting up the FlexJobs browser automation scaffold

## Local Files

- `clientcv.pdf`: client CV source document
- `flexjobs-client-requirements.md`: summarized client requirements
- `flexjobs-implementation-plan.md`: phased implementation plan

## Setup

Create a local virtual environment and install requirements:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## Local Config

Create a `.env` file based on `.env.example`.

Important:

- keep `.env` local only
- never commit credentials
- keep browser session data local
- optional candidate defaults such as work authorization, relocation, LinkedIn URL, address, salary expectations, and start date can be set in `.env` to improve autofill coverage

## Commands

Bootstrap the first milestone:

```bash
python3 -m app.main bootstrap
```

Extract the client profile only:

```bash
python3 -m app.main extract-profile
```

Initialize the database only:

```bash
python3 -m app.main init-db
```

Show recommended search titles:

```bash
python3 -m app.main show-search-titles
```

Open a persistent FlexJobs browser session:

```bash
python3 -m app.main open-flexjobs
```

Collect jobs using the saved browser profile:

```bash
python3 -m app.main collect-jobs --limit 10
```

If FlexJobs blocks automated login, run `open-flexjobs` first and complete the login manually in the persistent profile.

If FlexJobs blocks the automated search request with an access-denied page, use the manual capture flow instead:

```bash
python3 -m app.main collect-manual-page --title "Senior Auditor" --limit 10
```

That command attaches to a normal Google Chrome window with a separate local profile, lets you search manually in the browser, and then parses the results page after you press Enter in the terminal.

Score the collected jobs:

```bash
python3 -m app.main score-jobs
```

Enrich saved jobs with full detail text and apply-link data:

```bash
python3 -m app.main enrich-jobs --limit 10
```

That command attaches to the same normal Chrome profile used for manual capture, lets you log in if needed, then opens saved FlexJobs job pages and stores richer text in SQLite for better later scoring.

Show a filtered shortlist of enriched jobs and optionally export it to markdown:

```bash
python3 -m app.main shortlist-jobs --limit 15 --min-score 75 --out data/shortlist.md
```

You can also narrow the shortlist with a text filter such as `--query auditor`.

Prepare saved application packets from enriched jobs:

```bash
python3 -m app.main prepare-application --limit 5 --min-score 80 --query auditor
```

That command writes JSON and markdown packets under `data/applications/` and stores the prepared payload in the SQLite `applications` table for later review or automation.

Build a strict remote-U.S. daily application queue and optionally prepare missing packets:

```bash
python3 -m app.main daily-queue --limit 50 --prepare-missing
```

That command ranks the current database for remote U.S. jobs only, prioritizes easier apply flows, writes reports to `data/daily-queue.md` and `data/daily-queue.json`, and can auto-create missing application packets for queued jobs.

List prepared application rows:

```bash
python3 -m app.main list-applications --limit 20
```

Open prepared applications in the normal Chrome profile for manual review:

```bash
python3 -m app.main review-applications --limit 3 --status prepared
```

Autofill common application fields from the saved packet without submitting:

```bash
python3 -m app.main autofill-applications --application-id 1
```

Use `--dry-run` first if you want to inspect the planned field mapping before opening a live application page.

The autofill layer now supports:

- text inputs and textareas
- resume uploads
- common select menus
- common yes/no radio and checkbox questions
- optional address, LinkedIn, work authorization, sponsorship, relocation, salary, and start-date defaults from `.env`

After reviewing or submitting, update the tracked application status:

```bash
python3 -m app.main set-application-status --application-id 1 --status reviewed
python3 -m app.main set-application-status --application-id 1 --status applied --notes "Submitted on company site"
```

List the highest-ranked saved jobs:

```bash
python3 -m app.main list-jobs --limit 20
```

Generate a simple local web page for browsing all saved jobs:

```bash
python3 -m app.main jobs-page
```

That writes `data/jobs.html`, which you can open in a browser and filter by keyword, score, remote-only, and apply-link availability.

## Output Files

Generated local outputs are written to `data/`:

- `data/candidate_profile.json`
- `data/search_titles.json`
- `data/jobs.db`
- `data/applications/`
- `data/logs/`
- `data/screenshots/`
- `data/browser/`
- `data/jobs.html`
# Job-Automation-AI
