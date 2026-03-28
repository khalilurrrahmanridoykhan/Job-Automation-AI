from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .utils import ensure_parent_dir, normalize_whitespace


_REMOTE_HINTS = (
    "100% remote",
    "remote",
    "us national",
    "work from anywhere",
)
_HYBRID_HINTS = ("hybrid",)
_EXPERT_APPLY_URL = "https://www.flexjobs.com/expertapply/applications"


def _text_snippet(value: str | None, limit: int = 220) -> str | None:
    if not value:
        return None

    cleaned = normalize_whitespace(value)
    if not cleaned or cleaned.lower().startswith("skip to content"):
        return None
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _remote_bucket(location: str | None) -> str:
    normalized = (location or "").strip().lower()
    if not normalized:
        return "unknown"
    if any(hint in normalized for hint in _REMOTE_HINTS):
        return "remote"
    if any(hint in normalized for hint in _HYBRID_HINTS):
        return "hybrid"
    return "onsite"


def _is_expertapply(raw_payload: str | None, detail_raw_payload: str | None) -> bool:
    combined = " ".join(value for value in (raw_payload or "", detail_raw_payload or "") if value).lower()
    return "expertapply" in combined or "eligibleforexpertapply" in combined


def jobs_page_rows_as_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        application_url = row["application_url"]
        has_apply_url = bool(
            application_url
            and application_url.strip()
            and application_url.strip() != _EXPERT_APPLY_URL
        )
        is_expertapply = _is_expertapply(row["raw_payload"], row["detail_raw_payload"])
        summary = _text_snippet(row["detail_text"]) or _text_snippet(row["company_overview_text"])
        result.append(
            {
                "id": int(row["id"]),
                "title": row["title"],
                "company": row["company"] or "Unknown company",
                "location": row["location"] or "Location not listed",
                "salaryText": row["salary_text"] or "",
                "postedAt": row["posted_at"] or "",
                "score": float(row["fit_score"]) if row["fit_score"] is not None else None,
                "fitReason": row["fit_reason"] or "",
                "status": row["status"] or "",
                "applicationStatus": row["application_status"] or "",
                "jobUrl": row["job_url"],
                "applicationUrl": application_url or "",
                "hasApplyUrl": has_apply_url,
                "isExpertApply": is_expertapply,
                "hasDetails": summary is not None,
                "summary": summary or "",
                "remoteBucket": _remote_bucket(row["location"]),
                "discoveredAt": row["discovered_at"] or "",
            }
        )
    return result


def render_jobs_page(rows: list[sqlite3.Row]) -> str:
    jobs = jobs_page_rows_as_dicts(rows)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    jobs_json = json.dumps(jobs, ensure_ascii=True).replace("</", "<\\/")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FlexJobs Overview</title>
  <style>
    :root {{
      --bg: #f6f1e8;
      --panel: rgba(255, 252, 247, 0.9);
      --panel-strong: #fffdf8;
      --ink: #1d2433;
      --muted: #5b6475;
      --line: rgba(29, 36, 51, 0.12);
      --accent: #0f766e;
      --accent-soft: rgba(15, 118, 110, 0.12);
      --gold: #c9871a;
      --gold-soft: rgba(201, 135, 26, 0.14);
      --rose: #b4534f;
      --rose-soft: rgba(180, 83, 79, 0.12);
      --shadow: 0 22px 54px rgba(40, 37, 31, 0.10);
      --radius: 20px;
      --radius-sm: 14px;
      --serif: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      --sans: "Avenir Next", "Segoe UI", Helvetica, sans-serif;
    }}

    * {{ box-sizing: border-box; }}

    body {{
      margin: 0;
      font-family: var(--sans);
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.12), transparent 28%),
        radial-gradient(circle at top right, rgba(201, 135, 26, 0.14), transparent 24%),
        linear-gradient(180deg, #fcfaf6 0%, var(--bg) 100%);
    }}

    a {{ color: inherit; }}

    .page {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 64px;
    }}

    .hero {{
      background: linear-gradient(135deg, rgba(255,255,255,0.88), rgba(255,250,240,0.88));
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 28px;
      margin-bottom: 24px;
    }}

    .eyebrow {{
      margin: 0 0 8px;
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}

    h1 {{
      margin: 0;
      font-family: var(--serif);
      font-size: clamp(2rem, 5vw, 3.5rem);
      line-height: 1;
    }}

    .hero p {{
      margin: 14px 0 0;
      max-width: 760px;
      color: var(--muted);
      font-size: 1rem;
      line-height: 1.6;
    }}

    .stats {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-top: 22px;
    }}

    .stat {{
      background: var(--panel-strong);
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      padding: 16px;
    }}

    .stat-label {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .stat-value {{
      display: block;
      margin-top: 8px;
      font-size: 1.9rem;
      font-weight: 800;
      line-height: 1;
    }}

    .controls {{
      display: grid;
      grid-template-columns: minmax(0, 1.7fr) repeat(3, minmax(0, 0.75fr));
      gap: 12px;
      margin-bottom: 18px;
    }}

    .control,
    .toggle {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      padding: 14px 16px;
      box-shadow: 0 10px 26px rgba(40, 37, 31, 0.05);
    }}

    .control-label {{
      display: block;
      margin-bottom: 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    .control input,
    .control select {{
      width: 100%;
      border: 0;
      background: transparent;
      color: var(--ink);
      font-size: 15px;
      outline: none;
      font-family: inherit;
    }}

    .toggle {{
      display: flex;
      align-items: center;
      gap: 10px;
      cursor: pointer;
      user-select: none;
    }}

    .toggle input {{
      width: 18px;
      height: 18px;
      accent-color: var(--accent);
    }}

    .result-bar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin: 16px 0;
      color: var(--muted);
      font-size: 0.95rem;
    }}

    .jobs-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }}

    .job-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 20px;
      box-shadow: 0 14px 36px rgba(40, 37, 31, 0.07);
    }}

    .job-card-top {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
    }}

    .job-id {{
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    .job-title {{
      margin: 0;
      font-size: 1.25rem;
      line-height: 1.25;
      font-weight: 800;
    }}

    .job-company {{
      margin: 10px 0 0;
      color: var(--muted);
      font-size: 1rem;
    }}

    .score-pill {{
      min-width: 68px;
      padding: 12px 10px;
      border-radius: 999px;
      text-align: center;
      font-weight: 800;
      font-size: 0.95rem;
      background: var(--gold-soft);
      color: #764b00;
    }}

    .score-pill.high {{
      background: var(--accent-soft);
      color: #085d56;
    }}

    .score-pill.low {{
      background: var(--rose-soft);
      color: #8b2d2a;
    }}

    .meta {{
      margin: 14px 0 0;
      color: var(--ink);
      font-size: 0.96rem;
      line-height: 1.5;
    }}

    .tags {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }}

    .tag {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.04em;
      background: rgba(29, 36, 51, 0.06);
      color: var(--ink);
    }}

    .tag.remote {{
      background: var(--accent-soft);
      color: #085d56;
    }}

    .tag.hybrid {{
      background: var(--gold-soft);
      color: #7b4e00;
    }}

    .tag.apply {{
      background: rgba(45, 98, 171, 0.12);
      color: #274b83;
    }}

    .tag.status {{
      background: rgba(112, 71, 168, 0.12);
      color: #5d3691;
    }}

    .fit-reason,
    .summary {{
      margin: 14px 0 0;
      color: var(--muted);
      line-height: 1.6;
    }}

    .fit-reason strong,
    .summary strong {{
      color: var(--ink);
    }}

    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }}

    .button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      padding: 0 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fff;
      text-decoration: none;
      font-weight: 700;
    }}

    .button.primary {{
      background: var(--ink);
      color: #fff;
      border-color: var(--ink);
    }}

    .empty {{
      background: var(--panel);
      border: 1px dashed var(--line);
      border-radius: var(--radius);
      padding: 32px 24px;
      color: var(--muted);
      text-align: center;
    }}

    @media (max-width: 980px) {{
      .stats,
      .controls,
      .jobs-grid {{
        grid-template-columns: 1fr;
      }}
    }}

    @media (max-width: 640px) {{
      .page {{
        padding: 20px 14px 40px;
      }}

      .hero,
      .job-card {{
        padding: 18px;
      }}

      .job-card-top {{
        flex-direction: column;
      }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <p class="eyebrow">FlexJobs Snapshot</p>
      <h1>All saved jobs in one simple page.</h1>
      <p>
        Generated on {generated_at}. Use the search and filters below to narrow the list by keyword,
        score, remote status, or whether the job already has a direct apply link.
      </p>
      <div class="stats" id="stats"></div>
    </section>

    <section class="controls">
      <label class="control">
        <span class="control-label">Search</span>
        <input id="searchInput" type="text" placeholder="Title, company, location, or fit reason">
      </label>

      <label class="control">
        <span class="control-label">Minimum Score</span>
        <select id="minScore">
          <option value="">All scores</option>
          <option value="90">90 and up</option>
          <option value="80">80 and up</option>
          <option value="70">70 and up</option>
          <option value="60">60 and up</option>
        </select>
      </label>

      <label class="control">
        <span class="control-label">Sort By</span>
        <select id="sortBy">
          <option value="score">Best score</option>
          <option value="recent">Most recent</option>
          <option value="company">Company A-Z</option>
          <option value="title">Title A-Z</option>
        </select>
      </label>

      <div>
        <label class="toggle">
          <input id="remoteOnly" type="checkbox">
          <span>Remote only</span>
        </label>
        <label class="toggle" style="margin-top: 12px;">
          <input id="applyOnly" type="checkbox">
          <span>Has apply link</span>
        </label>
      </div>
    </section>

    <section class="result-bar">
      <div id="resultCount"></div>
      <div>Open the links directly from each job card.</div>
    </section>

    <section class="jobs-grid" id="jobsGrid"></section>
    <section class="empty" id="emptyState" hidden>No jobs match the current filters.</section>
  </main>

  <script id="jobs-data" type="application/json">{jobs_json}</script>
  <script>
    const jobs = JSON.parse(document.getElementById("jobs-data").textContent);
    const elements = {{
      stats: document.getElementById("stats"),
      jobsGrid: document.getElementById("jobsGrid"),
      emptyState: document.getElementById("emptyState"),
      resultCount: document.getElementById("resultCount"),
      searchInput: document.getElementById("searchInput"),
      minScore: document.getElementById("minScore"),
      sortBy: document.getElementById("sortBy"),
      remoteOnly: document.getElementById("remoteOnly"),
      applyOnly: document.getElementById("applyOnly"),
    }};

    function scoreLabel(score) {{
      if (score === null || score === undefined) return "n/a";
      return Math.round(score).toString();
    }}

    function scoreClass(score) {{
      if (score === null || score === undefined) return "";
      if (score >= 80) return "high";
      if (score < 60) return "low";
      return "";
    }}

    function jobMatches(job) {{
      const query = elements.searchInput.value.trim().toLowerCase();
      const minScore = elements.minScore.value ? Number(elements.minScore.value) : null;
      const haystack = [
        job.title,
        job.company,
        job.location,
        job.fitReason,
        job.summary,
        job.applicationStatus,
      ].join(" ").toLowerCase();

      if (query && !haystack.includes(query)) return false;
      if (minScore !== null && (job.score === null || job.score < minScore)) return false;
      if (elements.remoteOnly.checked && job.remoteBucket !== "remote") return false;
      if (elements.applyOnly.checked && !job.hasApplyUrl) return false;
      return true;
    }}

    function sortJobs(list) {{
      const mode = elements.sortBy.value;
      const sorted = [...list];
      sorted.sort((a, b) => {{
        if (mode === "recent") return String(b.discoveredAt).localeCompare(String(a.discoveredAt));
        if (mode === "company") return a.company.localeCompare(b.company);
        if (mode === "title") return a.title.localeCompare(b.title);
        const aScore = a.score === null ? -1 : a.score;
        const bScore = b.score === null ? -1 : b.score;
        if (bScore !== aScore) return bScore - aScore;
        return String(b.discoveredAt).localeCompare(String(a.discoveredAt));
      }});
      return sorted;
    }}

    function makeStat(label, value) {{
      const wrap = document.createElement("div");
      wrap.className = "stat";

      const labelNode = document.createElement("span");
      labelNode.className = "stat-label";
      labelNode.textContent = label;
      wrap.appendChild(labelNode);

      const valueNode = document.createElement("span");
      valueNode.className = "stat-value";
      valueNode.textContent = value;
      wrap.appendChild(valueNode);
      return wrap;
    }}

    function renderStats(filteredJobs) {{
      const remoteJobs = filteredJobs.filter(job => job.remoteBucket === "remote").length;
      const applyJobs = filteredJobs.filter(job => job.hasApplyUrl).length;
      const strongJobs = filteredJobs.filter(job => job.score !== null && job.score >= 80).length;

      elements.stats.replaceChildren(
        makeStat("Visible Jobs", String(filteredJobs.length)),
        makeStat("Remote", String(remoteJobs)),
        makeStat("Ready To Apply", String(applyJobs)),
        makeStat("Score 80+", String(strongJobs)),
      );
    }}

    function makeTag(text, extraClass = "") {{
      const tag = document.createElement("span");
      tag.className = `tag ${{extraClass}}`.trim();
      tag.textContent = text;
      return tag;
    }}

    function makeButton(text, href, primary = false) {{
      const link = document.createElement("a");
      link.className = primary ? "button primary" : "button";
      link.textContent = text;
      link.href = href;
      link.target = "_blank";
      link.rel = "noreferrer";
      return link;
    }}

    function renderCard(job) {{
      const card = document.createElement("article");
      card.className = "job-card";

      const top = document.createElement("div");
      top.className = "job-card-top";

      const titleWrap = document.createElement("div");
      const idNode = document.createElement("p");
      idNode.className = "job-id";
      idNode.textContent = `Job #${{job.id}}`;
      titleWrap.appendChild(idNode);

      const title = document.createElement("h2");
      title.className = "job-title";
      title.textContent = job.title;
      titleWrap.appendChild(title);

      const company = document.createElement("p");
      company.className = "job-company";
      company.textContent = job.company;
      titleWrap.appendChild(company);

      const score = document.createElement("div");
      score.className = `score-pill ${{scoreClass(job.score)}}`.trim();
      score.textContent = `Score ${{scoreLabel(job.score)}}`;

      top.appendChild(titleWrap);
      top.appendChild(score);
      card.appendChild(top);

      const meta = document.createElement("p");
      meta.className = "meta";
      const metaParts = [job.location];
      if (job.salaryText) metaParts.push(job.salaryText);
      if (job.postedAt) metaParts.push(`Posted ${{job.postedAt}}`);
      meta.textContent = metaParts.join("  •  ");
      card.appendChild(meta);

      const tags = document.createElement("div");
      tags.className = "tags";
      tags.appendChild(makeTag(job.remoteBucket, job.remoteBucket));
      if (job.isExpertApply) tags.appendChild(makeTag("expertapply", "apply"));
      if (job.hasApplyUrl) tags.appendChild(makeTag("apply link", "apply"));
      if (job.hasDetails) tags.appendChild(makeTag("details saved"));
      if (job.applicationStatus) tags.appendChild(makeTag(job.applicationStatus, "status"));
      card.appendChild(tags);

      if (job.fitReason) {{
        const reason = document.createElement("p");
        reason.className = "fit-reason";
        const label = document.createElement("strong");
        label.textContent = "Why it fits: ";
        reason.appendChild(label);
        reason.append(job.fitReason);
        card.appendChild(reason);
      }}

      if (job.summary) {{
        const summary = document.createElement("p");
        summary.className = "summary";
        const label = document.createElement("strong");
        label.textContent = "Summary: ";
        summary.appendChild(label);
        summary.append(job.summary);
        card.appendChild(summary);
      }}

      const actions = document.createElement("div");
      actions.className = "actions";
      if (job.hasApplyUrl) actions.appendChild(makeButton("Apply", job.applicationUrl, true));
      actions.appendChild(makeButton("Open FlexJobs", job.jobUrl));
      card.appendChild(actions);

      return card;
    }}

    function render() {{
      const visibleJobs = sortJobs(jobs.filter(jobMatches));
      renderStats(visibleJobs);
      elements.resultCount.textContent = `${{visibleJobs.length}} of ${{jobs.length}} jobs shown`;
      elements.jobsGrid.replaceChildren(...visibleJobs.map(renderCard));
      elements.emptyState.hidden = visibleJobs.length !== 0;
      elements.jobsGrid.hidden = visibleJobs.length === 0;
    }}

    elements.searchInput.addEventListener("input", render);
    elements.minScore.addEventListener("change", render);
    elements.sortBy.addEventListener("change", render);
    elements.remoteOnly.addEventListener("change", render);
    elements.applyOnly.addEventListener("change", render);

    render();
  </script>
</body>
</html>
"""


def write_jobs_page(rows: list[sqlite3.Row], output_path: Path) -> Path:
    ensure_parent_dir(output_path)
    output_path.write_text(render_jobs_page(rows), encoding="utf-8")
    return output_path
