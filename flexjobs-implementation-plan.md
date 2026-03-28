# FlexJobs Agent Implementation Plan

## Goal

Build a job-search and application assistant for the client that:

- finds matching jobs on FlexJobs
- scores and prioritizes them using the client's CV
- follows redirects to company career sites
- fills common application fields automatically
- pauses for review on risky or unknown steps

## What You Already Have

- FlexJobs account access from the client
- Client CV at `/Users/khalilur/Documents/NicolasMoreno/clientcv.pdf`
- Initial requirements file at `/Users/khalilur/Documents/NicolasMoreno/flexjobs-client-requirements.md`

## Recommended Build Strategy

Do not start with full nonstop auto-apply.

Start with a semi-automated system:

- automate job discovery
- automate job matching
- automate form filling where fields are known
- require human review before final submission

This is the safest and most reliable first version because every company site is different and many sites block aggressive automation.

## Candidate Profile To Target

Based on the CV, the best-fit roles are:

- Senior Auditor
- Internal Auditor
- Regulatory Reporting Auditor
- Credit Risk Analyst
- Risk Management Analyst
- SOX Audit or Compliance roles

Strong matching keywords from the CV:

- internal audit
- risk management
- regulatory reporting
- SOX
- credit risk
- controls testing
- audit workpapers
- compliance
- bilingual Spanish/English

## Suggested Tech Stack

Use Python for the first version.

- `playwright` for browser automation
- `sqlite` for job and application tracking
- `pydantic` for structured candidate/job data
- `pdfplumber` or `pdftotext` for CV extraction
- OpenAI API for job matching, answer drafting, and cover letter generation

## Phase Plan

### Phase 1. Setup And Safety

- Create a new project folder for the automation code
- Store credentials in environment variables, not in code
- Keep `.env` out of git
- Use one dedicated browser profile for this automation
- Add logs and screenshots for every important step

Deliverable:

- local project scaffold
- env-based login flow
- browser launches successfully

### Phase 2. Convert CV Into Structured Data

- Extract text from the client CV
- Convert the CV into a structured JSON profile
- Save reusable application answers
- Prepare a short professional summary for job matching

Data to structure:

- full name
- email
- phone
- location
- target roles
- years of experience
- skills
- education
- work history
- certifications

Deliverable:

- `candidate_profile.json`
- reusable answer bank for applications

### Phase 3. FlexJobs Search Automation

- Log into FlexJobs
- Open search pages for target job titles
- apply filters such as remote, category, salary, and location if relevant
- collect job title, company, location, salary, link, and posting date
- save all results into SQLite

Deliverable:

- jobs collector script
- local database of candidate jobs

### Phase 4. Job Scoring And Prioritization

- Compare each job against the client profile
- rank jobs by fit score
- mark duplicates and already-seen jobs
- reject poor-fit jobs automatically

Scoring rules should consider:

- role similarity
- industry fit
- experience match
- required skills
- seniority level
- remote eligibility

Deliverable:

- scored job list
- shortlist of high-fit jobs

### Phase 5. External Site Navigation

- open the application target from FlexJobs
- detect whether it stays on FlexJobs or redirects to the employer site
- identify common form fields
- map stored candidate data into those fields
- upload the client CV where supported

Pause and require human review when:

- a captcha appears
- the site asks custom essay questions
- work authorization is unclear
- salary expectation is required
- the form flow is unusual

Deliverable:

- assisted application runner
- screenshots and logs for each attempt

### Phase 6. Human Review Workflow

- show the user the prepared application before submission
- allow approve, skip, or edit
- store final result in the database

Application statuses:

- discovered
- shortlisted
- draft prepared
- review needed
- submitted
- failed
- skipped

Deliverable:

- reliable review-first workflow

### Phase 7. Reporting And Daily Runs

- run the search multiple times per day
- avoid applying twice to the same job
- generate a summary of new jobs, drafts, submitted applications, and failures

Deliverable:

- daily automation routine
- application activity report

## Folder Structure Suggestion

```text
flexjobs-agent/
  .env
  README.md
  requirements.txt
  app/
    main.py
    config.py
    models.py
    db.py
    candidate_profile.py
    job_search.py
    job_score.py
    apply.py
    review.py
    utils.py
  data/
    candidate_profile.json
    jobs.db
    screenshots/
    logs/
```

## What Still Needs To Be Collected From The Client

- target job titles in priority order
- preferred location or remote-only preference
- salary expectation
- work authorization status
- preferred industries
- short answers for common application questions
- whether final submission should always require approval

## Practical Build Order

Build in this order:

1. CV extraction and candidate profile JSON
2. FlexJobs login and search collector
3. SQLite storage and deduplication
4. Job scoring
5. External site autofill
6. Review and submit flow
7. Scheduler and reporting

## What I Would Do First

First milestone:

- parse the CV
- define target roles from the CV
- set up env-based login
- collect jobs from FlexJobs into SQLite

Second milestone:

- rank the jobs
- prepare reusable application answers
- build a review-first application assistant

## Important Notes

- Do not hardcode the client's password in source files
- Do not commit `.env`, cookies, or browser session files
- Expect many employer sites to need special-case handling
- Full autonomous applying is possible only in parts; review checkpoints will make the first version much more stable
