from __future__ import annotations

from datetime import datetime, timedelta
import json
import time
from dataclasses import dataclass
from sqlite3 import Row
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse
import urllib.error
import urllib.request

from .application_autofill import _field_key
from .config import Settings
from .db import update_application_status, upsert_prepared_application
from .job_search import _connect_to_manual_chrome, _safe_goto, load_candidate_profile
from .utils import normalize_whitespace, playwright_environment_hint


TRACKER_URL = "https://www.flexjobs.com/expertapply/applications"
SUMMARY_URL = "https://api.flexjobs.com/ea/api/v1/expert-apply/profiles/{profile_id}/jobs/applications/summary"
CREATE_URL = "https://api.flexjobs.com/ea/api/v1/expert-apply/profiles/{profile_id}/jobs/autofill"
REVIEW_URL = "https://www.flexjobs.com/expertapply/application/{application_id}/audit"
READY_STATUSES = {"ReadyForQuickApply", "ReadyForReview"}
IN_FLIGHT_STATUSES = {"Preparing", "Processing"}
TERMINAL_STATUSES = {"Success", "Fail", "CancelledByUser"}
DOCUMENT_FILTER_RESPONSE = (
    "id,legacyDocId,documentTypeCD,templateID,partyID,userId,skinCD,name,publicName,"
    "docStatusTypeCD,userStageID,dateCreated,dateModified,migrationDate,portalID,"
    "preferences,country,productCD,version,sections"
)


@dataclass(slots=True)
class NativeExpertApplyResult:
    job_id: int
    title: str
    company: str | None
    external_job_id: str
    tracker_application_id: str | None
    initial_status: str | None
    action: str
    final_status: str | None
    review_required_answers: int | None
    submitted: bool
    error: str | None = None


def _parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _row_value(row: Row, key: str) -> Any:
    return row[key] if key in row.keys() else None


def _normalize_id(value: Any) -> str:
    text = normalize_whitespace(value or "")
    if len(text) >= 2 and text.startswith('"') and text.endswith('"'):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text.strip('"')
        return normalize_whitespace(parsed or "")
    return text


def _cookie_header(context) -> str:
    cookies = context.cookies(["https://www.flexjobs.com", "https://api.flexjobs.com"])
    return "; ".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies)


def _http_json(
    url: str,
    *,
    cookie_header: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> Any:
    body = None
    headers = {
        "Cookie": cookie_header,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:  # pragma: no cover - network dependent
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"{method} {url} failed: HTTP {exc.code} {detail[:300]}") from exc
    except urllib.error.URLError as exc:  # pragma: no cover - network dependent
        raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc

    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return loaded


def _resolve_user_and_profile_id(page, cookie_header: str) -> tuple[str, str]:
    state = page.evaluate(
        """
        () => {
          const normalizeId = (value) => {
            if (!value || typeof value !== "string") {
              return "";
            }
            try {
              const parsed = JSON.parse(value);
              if (typeof parsed === "string") {
                return parsed.trim();
              }
            } catch (_error) {}
            return value.trim();
          };
          let userId = "";
          let profileId = "";
          try {
            const raw = localStorage.getItem("UserStatus");
            if (raw) {
              const parsed = JSON.parse(raw);
              userId = normalizeId(parsed?.User?.UserId || parsed?.user?.UserId || "");
            }
          } catch (_error) {}
          if (!userId) {
            try {
              userId = normalizeId(localStorage.getItem("ajs_user_id") || "");
            } catch (_error) {}
          }
          if (userId) {
            try {
              profileId = normalizeId(localStorage.getItem(`${userId}_EAPL_documentId`) || "");
            } catch (_error) {}
          }
          return { userId, profileId };
        }
        """
    )
    user_id = _normalize_id(state.get("userId") or "")
    profile_id = _normalize_id(state.get("profileId") or "")
    if not user_id:
        raise RuntimeError("Could not resolve the logged-in FlexJobs user id from the current browser session.")

    if profile_id:
        return user_id, profile_id

    documents_url = (
        "https://www.flexjobs.com/eb/api/v1/documents/getall?"
        + urlencode(
            {
                "userId": user_id,
                "portalCD": "FXJ",
                "filterResponse": DOCUMENT_FILTER_RESPONSE,
            }
        )
    )
    documents = _http_json(documents_url, cookie_header=cookie_header)
    items = documents if isinstance(documents, list) else documents.get("documents") or documents.get("results") or []
    if not isinstance(items, list):
        items = []

    for item in items:
        if isinstance(item, dict) and item.get("documentTypeCD") == "EAPL" and item.get("id"):
            profile_id = _normalize_id(item["id"])
            break

    if not profile_id:
        raise RuntimeError("Could not resolve the FlexJobs ExpertApply profile id (EAPL document id).")
    return user_id, profile_id


def _summary_query_url(profile_id: str, *, statuses: list[str] | None = None, ready_only: bool = False) -> str:
    params: list[tuple[str, str]] = []
    if ready_only:
        params.append(("jobApplicationCreateType", "UserFilled"))
    for status in statuses or []:
        params.append(("statuses", status))
    params.extend(
        [
            ("page", "1"),
            ("size", "100"),
            ("sortBy", "CreatedAt"),
            ("isDescendingSort", "true"),
        ]
    )
    return SUMMARY_URL.format(profile_id=profile_id) + "?" + urlencode(params, doseq=True)


def _fetch_summary(
    profile_id: str,
    *,
    cookie_header: str,
    statuses: list[str] | None = None,
    ready_only: bool = False,
) -> list[dict[str, Any]]:
    payload = _http_json(
        _summary_query_url(profile_id, statuses=statuses, ready_only=ready_only),
        cookie_header=cookie_header,
    )
    results = payload.get("results")
    return [item for item in results if isinstance(item, dict)] if isinstance(results, list) else []


def _summary_by_external_job_id(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for item in results:
        job_detail = item.get("jobDetail") or {}
        job_id = normalize_whitespace(job_detail.get("id") or item.get("jobId") or "")
        if job_id and job_id not in mapped:
            mapped[job_id] = item
    return mapped


def _extract_selected_job_payload(row: Row) -> dict[str, str]:
    parsed = urlparse(row["job_url"] or "")
    query = parse_qs(parsed.query)
    job_id = normalize_whitespace(row["external_id"] or query.get("id", [""])[0])
    match_id = normalize_whitespace(query.get("matchId", [""])[0])
    match_score = normalize_whitespace(query.get("score", [""])[0])

    if not (job_id and match_id and match_score):
        raw_payload = _parse_json(row["raw_payload"])
        job_id = job_id or normalize_whitespace(raw_payload.get("external_id") or "")
        match_id = match_id or normalize_whitespace(raw_payload.get("match_id") or "")
        match_score = match_score or normalize_whitespace(raw_payload.get("match_score") or "")

    if not job_id:
        raise RuntimeError(f"Job {row['id']} is missing the FlexJobs job identifier.")
    if not match_id:
        raise RuntimeError(f"Job {row['id']} is missing the FlexJobs match id.")
    if not match_score:
        raise RuntimeError(f"Job {row['id']} is missing the FlexJobs match score.")

    return {
        "jobId": job_id,
        "matchId": match_id,
        "matchScore": match_score,
    }


def _wait_for_tracker_item(
    profile_id: str,
    external_job_id: str,
    *,
    cookie_header: str,
    timeout_seconds: float = 25.0,
) -> dict[str, Any] | None:
    deadline = time.time() + timeout_seconds
    last_match: dict[str, Any] | None = None
    while time.time() < deadline:
        results = _fetch_summary(profile_id, cookie_header=cookie_header)
        matched = _summary_by_external_job_id(results).get(external_job_id)
        if matched is not None:
            last_match = matched
            if matched.get("jobApplicationStatus") not in {"Preparing", "", None}:
                return matched
        time.sleep(1.5)
    return last_match


def _visible_button(page, *, exact_name: str):
    locator = page.get_by_role("button", name=exact_name, exact=True)
    for index in range(locator.count()):
        candidate = locator.nth(index)
        try:
            if candidate.is_visible() and candidate.is_enabled():
                return candidate
        except Exception:
            continue
    return None


def _default_start_date_text() -> str:
    return (datetime.now() + timedelta(days=14)).strftime("%m/%d/%Y")


def _review_answer_values(settings: Settings) -> dict[str, Any]:
    profile = load_candidate_profile(settings.candidate_profile_path)
    experience = profile.get("experience") if isinstance(profile, dict) else None
    current_company = ""
    if isinstance(experience, list):
        for item in experience:
            if isinstance(item, dict) and normalize_whitespace(item.get("employer") or ""):
                current_company = normalize_whitespace(item.get("employer") or "")
                break
    return {
        "middle_name": normalize_whitespace(settings.candidate_middle_name or ""),
        "phone_type": normalize_whitespace(settings.candidate_phone_type or "Mobile"),
        "salary_expectations": normalize_whitespace(settings.candidate_salary_expectations or ""),
        "start_date": normalize_whitespace(settings.candidate_start_date or "") or _default_start_date_text(),
        "current_company": current_company,
        "county": normalize_whitespace(settings.candidate_county or ""),
        "over_18": True,
        "work_authorization": settings.candidate_work_authorized_us,
        "require_sponsorship": settings.candidate_require_sponsorship,
        "willing_to_relocate": settings.candidate_willing_to_relocate,
        "veteran_status": "I DO NOT WISH TO SELF-IDENTIFY",
    }


def _normalize_choice(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    return normalize_whitespace(value or "").lower()


def _is_placeholder_choice(value: Any) -> bool:
    return _normalize_choice(value) in {"", "select", "select one", "please select"}


def _discover_review_fields(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        () => {
          const text = (node) => (node && (node.innerText || node.textContent || '') || '').replace(/\\s+/g, ' ').trim();
          const visible = (node) => {
            if (!node) {
              return false;
            }
            const style = window.getComputedStyle(node);
            const rect = node.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && !node.disabled && (rect.width > 0 || rect.height > 0);
          };
          const forms = Array.from(document.querySelectorAll('form'));
          const targetForm = forms.sort((left, right) => text(right).length - text(left).length)[0];
          if (!targetForm) {
            return [];
          }
          const items = [];
          const seen = new Set();
          const push = (item) => {
            const key = [
              item.tag || '',
              item.id || '',
              item.name || '',
              item.label_text || '',
              item.current_value || '',
            ].join('|');
            if (!seen.has(key)) {
              seen.add(key);
              items.push(item);
            }
          };
          Array.from(targetForm.querySelectorAll('input, textarea, select')).forEach((el) => {
            let labels = [];
            if (el.id) {
              labels = labels.concat(
                Array.from(document.querySelectorAll(`label[for="${el.id.replace(/"/g, '\\"')}"]`)).map((node) => node.textContent || '')
              );
            }
            const wrapped = el.closest('label');
            if (wrapped) {
              labels.push(wrapped.textContent || '');
            }
            const labelledBy = el.getAttribute('aria-labelledby');
            if (labelledBy) {
              labels = labels.concat(
                labelledBy
                  .split(/\\s+/)
                  .map((id) => document.getElementById(id))
                  .filter(Boolean)
                  .map((node) => node.textContent || '')
              );
            }
            push({
              visible: visible(el),
              tag: el.tagName.toLowerCase(),
              type: (el.getAttribute('type') || '').toLowerCase(),
              id: el.getAttribute('id') || '',
              name: el.getAttribute('name') || '',
              placeholder: el.getAttribute('placeholder') || '',
              aria_label: el.getAttribute('aria-label') || '',
              autocomplete: el.getAttribute('autocomplete') || '',
              label_text: labels.join(' '),
              section_text: (() => {
                const container = el.closest('fieldset, section, li, tr, div');
                return container ? (container.textContent || '').slice(0, 400) : '';
              })(),
              current_value: ('value' in el ? el.value : '') || '',
              options: el.tagName.toLowerCase() === 'select'
                ? Array.from(el.options || []).map((option) => ({
                    value: option.value || '',
                    text: option.textContent || '',
                  }))
                : [],
            });
          });

          Array.from(targetForm.querySelectorAll('label')).forEach((label) => {
            const labelText = label.getAttribute('aria-label') || text(label);
            if (!labelText) {
              return;
            }
            const container = label.parentElement;
            if (!container || !visible(container)) {
              return;
            }
            const associatedControl = (
              (label.htmlFor && document.getElementById(label.htmlFor))
              || label.querySelector('input, textarea, select')
              || container.querySelector('input, textarea, select')
            );
            if (associatedControl && visible(associatedControl)) {
              return;
            }
            const siblings = Array.from(container.children).filter((node) => node !== label);
            const host = siblings.find((node) => visible(node)) || label.nextElementSibling;
            if (!host || !visible(host)) {
              return;
            }
            const clickable = (
              [host, ...Array.from(host.querySelectorAll('button, [role="button"], div, span'))]
                .find((node) => visible(node) && !node.querySelector('input, textarea, select'))
            );
            if (!clickable) {
              return;
            }
            const currentValue = text(clickable) || clickable.getAttribute('title') || '';
            const sectionText = text(container).slice(0, 400);
            if (!currentValue && !sectionText.toLowerCase().includes('select')) {
              return;
            }
            push({
              visible: true,
              tag: 'custom_select',
              type: '',
              id: '',
              name: '',
              placeholder: '',
              aria_label: labelText,
              autocomplete: '',
              label_text: labelText,
              section_text: sectionText,
              current_value: currentValue,
              options: [],
            });
          });

          return items.filter((item) => item.visible);
        }
        """
    )


def _review_field_key(field: dict[str, Any]) -> str | None:
    payload = {
        "tag": field.get("tag") or "",
        "type": field.get("type") or "",
        "value": field.get("current_value") or "",
        "name": field.get("name") or "",
        "id": field.get("id") or "",
        "placeholder": field.get("placeholder") or "",
        "aria_label": field.get("aria_label") or "",
        "autocomplete": field.get("autocomplete") or "",
        "label_text": field.get("label_text") or "",
        "section_text": field.get("section_text") or "",
    }
    return _field_key(payload)


def _fill_review_text_field(page, field_id: str, value: str) -> bool:
    locator = page.locator(f"#{field_id}")
    if locator.count() == 0:
        return False
    try:
        locator.first.scroll_into_view_if_needed(timeout=1000)
    except Exception:
        pass
    try:
        locator.first.fill(value, timeout=2000)
    except Exception:
        pass
    try:
        changed = bool(
            page.evaluate(
                """
                ([fieldId, value]) => {
                  const el = document.getElementById(fieldId);
                  if (!el) return false;
                  const proto = el.tagName.toLowerCase() === 'textarea'
                    ? HTMLTextAreaElement.prototype
                    : HTMLInputElement.prototype;
                  const desc = Object.getOwnPropertyDescriptor(proto, 'value');
                  if (desc && desc.set) {
                    desc.set.call(el, value);
                  } else {
                    el.value = value;
                  }
                  el.dispatchEvent(new Event('input', { bubbles: true }));
                  el.dispatchEvent(new Event('change', { bubbles: true }));
                  el.dispatchEvent(new Event('blur', { bubbles: true }));
                  return true;
                }
                """,
                [field_id, value],
            )
        )
    except Exception:
        changed = False
    if changed:
        page.wait_for_timeout(500)
    return changed


def _click_visible_list_item(page, choice_text: str) -> bool:
    try:
        return bool(
            page.evaluate(
                """
                (choiceText) => {
                  const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                  const target = normalize(choiceText);
                  const score = (text) => {
                    if (!text) return 0;
                    if (text === target) return 6;
                    if (text.startsWith(target + ' ')) return 5;
                    if (text.includes(target)) return 4;
                    if (target.startsWith(text + ' ')) return 3;
                    if (target.includes(text)) return 2;
                    if ((target === 'yes' || target === 'no') && text.startsWith(target)) return 1;
                    return 0;
                  };
                  const hits = Array.from(document.querySelectorAll('li, [role="option"]'))
                    .filter((node) => {
                      const style = window.getComputedStyle(node);
                      const rect = node.getBoundingClientRect();
                      return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                    })
                    .map((node) => ({
                      node,
                      text: normalize(node.innerText || node.textContent || ''),
                      score: score(normalize(node.innerText || node.textContent || '')),
                    }))
                    .filter((entry) => entry.score > 0)
                    .sort((left, right) => right.score - left.score || left.node.getBoundingClientRect().top - right.node.getBoundingClientRect().top);
                  const hit = hits[0]?.node;
                  if (!hit) {
                    return false;
                  }
                  hit.click();
                  return true;
                }
                """,
                choice_text,
            )
        )
    except Exception:
        return False


def _confirm_review_select(page, field_id: str, choice_text: str) -> bool:
    locator = page.locator(f"#{field_id}")
    if locator.count() == 0:
        return False
    try:
        locator.first.scroll_into_view_if_needed(timeout=1000)
    except Exception:
        pass
    try:
        locator.first.click(force=True, timeout=2000)
    except Exception:
        pass
    page.wait_for_timeout(250)
    if _click_visible_list_item(page, choice_text):
        page.wait_for_timeout(500)
        return True
    try:
        if locator.first.select_option(label=choice_text, timeout=1500):
            page.wait_for_timeout(500)
            return True
    except Exception:
        pass
    try:
        if locator.first.select_option(value=choice_text, timeout=1500):
            page.wait_for_timeout(500)
            return True
    except Exception:
        pass
    try:
        return bool(
            page.evaluate(
                """
                ([fieldId, choiceText]) => {
                  const el = document.getElementById(fieldId);
                  if (!el) return false;
                  el.dispatchEvent(new Event('input', { bubbles: true }));
                  el.dispatchEvent(new Event('change', { bubbles: true }));
                  el.dispatchEvent(new Event('blur', { bubbles: true }));
                  return ((el.value || '').trim().toLowerCase() === (choiceText || '').trim().toLowerCase());
                }
                """,
                [field_id, choice_text],
            )
        )
    except Exception:
        return False


def _confirm_review_custom_select(page, label_text: str, choice_text: str) -> bool:
    try:
        prepared = bool(
            page.evaluate(
                """
                (targetLabel) => {
                  const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                  const matches = (value) => {
                    const normalized = normalize(value);
                    const target = normalize(targetLabel);
                    return normalized === target || normalized.includes(target) || target.includes(normalized);
                  };
                  const visible = (node) => {
                    if (!node) return false;
                    const style = window.getComputedStyle(node);
                    const rect = node.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                  };
                  const labels = Array.from(document.querySelectorAll('form label'));
                  const label = labels.find((node) => matches(node.getAttribute('aria-label') || node.innerText || node.textContent || ''));
                  if (!label) {
                    return false;
                  }
                  const container = label.parentElement;
                  if (!container) {
                    return false;
                  }
                  const siblings = Array.from(container.children).filter((node) => node !== label && visible(node));
                  const host = siblings[0] || label.nextElementSibling;
                  if (!host || !visible(host)) {
                    return false;
                  }
                  const clickable = [host, ...Array.from(host.querySelectorAll('button, [role="button"], div, span'))]
                    .find((node) => visible(node) && !node.querySelector('input, textarea, select'));
                  if (!clickable) {
                    return false;
                  }
                  document
                    .querySelectorAll('[data-codex-review-target="true"]')
                    .forEach((node) => node.removeAttribute('data-codex-review-target'));
                  clickable.setAttribute('data-codex-review-target', 'true');
                  return true;
                }
                """,
                label_text,
            )
        )
    except Exception:
        prepared = False
    if not prepared:
        return False
    locator = page.locator('[data-codex-review-target="true"]')
    if locator.count() == 0:
        return False
    try:
        locator.first.scroll_into_view_if_needed(timeout=1000)
    except Exception:
        pass
    try:
        locator.first.click(force=True, timeout=2000)
    except Exception:
        return False
    finally:
        try:
            page.evaluate(
                """
                () => {
                  document
                    .querySelectorAll('[data-codex-review-target="true"]')
                    .forEach((node) => node.removeAttribute('data-codex-review-target'));
                }
                """
            )
        except Exception:
            pass
    page.wait_for_timeout(250)
    if _click_visible_list_item(page, choice_text):
        page.wait_for_timeout(500)
        return True
    return False


def _review_answer_for_field(field: dict[str, Any], values: dict[str, Any]) -> str | None:
    key = _review_field_key(field)
    label = normalize_whitespace(field.get("label_text") or "").lower()
    current_value = normalize_whitespace(field.get("current_value") or "")
    is_required = label.startswith("*")

    if "veteran status" in label:
        return str(values["veteran_status"])
    if "company name" in label:
        return str(values["current_company"] or "Truist")
    if "county" in label:
        return str(values["county"] or "N/A")
    if key == "middle_name":
        return str(values["middle_name"] or "N/A")
    if key == "phone_type":
        return str(values["phone_type"] or current_value or "Mobile")
    if key == "over_18":
        return "Yes" if bool(values.get("over_18", True)) else "No"
    if key == "work_authorization" and values.get("work_authorization") is not None:
        return "Yes" if bool(values["work_authorization"]) else "No"
    if key == "require_sponsorship" and values.get("require_sponsorship") is not None:
        return "Yes" if bool(values["require_sponsorship"]) else "No"
    if key == "willing_to_relocate" and values.get("willing_to_relocate") is not None:
        return "Yes" if bool(values["willing_to_relocate"]) else "No"
    if key == "salary_expectations":
        return str(values["salary_expectations"] or current_value)
    if key == "start_date":
        return str(values["start_date"])
    if "summary" in label and current_value:
        return current_value
    if "cover letter" in label and current_value:
        return current_value
    if any(token in label for token in ("background check", "drug screening", "can you meet this requirement")):
        return "Yes"
    if any(token in label for token in ("if yes", "if other", "please specify", "jurisdiction")) and not current_value:
        return "N/A"

    if field.get("tag") in {"select", "custom_select"} and is_required and not _is_placeholder_choice(current_value):
        return current_value
    return None


def _complete_review_questions(page, settings: Settings) -> tuple[list[str], bool]:
    answers = _review_answer_values(settings)
    actions: list[str] = []
    for _pass in range(2):
        fields = _discover_review_fields(page)
        changed = False
        for field in fields:
            field_id = normalize_whitespace(field.get("id") or "")
            desired = _review_answer_for_field(field, answers)
            if not desired:
                continue
            if field.get("tag") != "custom_select" and not field_id:
                continue
            label = normalize_whitespace(field.get("label_text") or field_id)
            if field.get("tag") == "select":
                if _confirm_review_select(page, field_id, desired):
                    actions.append(f"select:{label}={desired}")
                    changed = True
            elif field.get("tag") == "custom_select":
                if _confirm_review_custom_select(page, label, desired):
                    actions.append(f"custom_select:{label}={desired}")
                    changed = True
            else:
                if _fill_review_text_field(page, field_id, desired):
                    actions.append(f"text:{label}={desired}")
                    changed = True
        page.wait_for_timeout(600)
        approve_button = page.locator("#approve-apply-button")
        if approve_button.count() > 0 and not approve_button.first.is_disabled():
            return actions, True
        if not changed:
            break
    approve_button = page.locator("#approve-apply-button")
    enabled = approve_button.count() > 0 and not approve_button.first.is_disabled()
    return actions, enabled


def _submit_quick_apply(page, row_index: int, timeout_ms: int) -> tuple[bool, str | None]:
    _safe_goto(page, TRACKER_URL, timeout_ms)
    page.wait_for_timeout(1500)
    button = page.locator(f"#readyToApplyApplicationRow{row_index}-one-click-apply-cta")
    if button.count() == 0:
        return False, f"Quick apply button not found for tracker row {row_index}."
    button.first.click(force=True)
    page.wait_for_timeout(1000)
    modal_apply_button = _visible_button(page, exact_name="Apply")
    if modal_apply_button is None:
        return False, "Quick apply confirmation button was not found."
    modal_apply_button.click(force=True)
    page.wait_for_timeout(4000)
    body_text = normalize_whitespace(page.locator("body").inner_text(timeout=5000)).lower()
    if "application is on its way" in body_text or "application tracker" in body_text:
        return True, None
    return True, None


def _submit_ready_review(page, application_id: str, settings: Settings, timeout_ms: int) -> tuple[bool, str | None]:
    _safe_goto(page, REVIEW_URL.format(application_id=application_id), timeout_ms)
    page.wait_for_timeout(1500)
    approve_button = page.locator("#approve-apply-button")
    if approve_button.count() == 0:
        return False, "Approve and apply button was not found on the review page."
    review_actions, enabled = _complete_review_questions(page, settings)
    approve_button = page.locator("#approve-apply-button")
    if approve_button.count() == 0:
        return False, "Approve and apply button disappeared during review automation."
    if not enabled and approve_button.first.is_disabled():
        page.wait_for_timeout(1500)
        _safe_goto(page, REVIEW_URL.format(application_id=application_id), timeout_ms)
        page.wait_for_timeout(1500)
        approve_button = page.locator("#approve-apply-button")
        if approve_button.count() == 0:
            return False, "Approve and apply button disappeared after review refresh."
        if approve_button.first.is_disabled():
            detail = ", ".join(review_actions[:6]) if review_actions else "no review fields could be auto-confirmed"
            return False, f"Review page still requires manual confirmation after automation ({detail})."
    approve_button.first.click(force=True)
    page.wait_for_timeout(5000)
    body_text = normalize_whitespace(page.locator("body").inner_text(timeout=5000)).lower()
    if TRACKER_URL in (page.url or "") or "application is on its way" in body_text:
        return True, None
    return False, "Approve and apply was clicked, but FlexJobs did not confirm submission."


def _sync_application_row(settings: Settings, row: Row, tracker_item: dict[str, Any] | None) -> None:
    upsert_prepared_application(
        settings.jobs_db_path,
        job_id=int(row["id"]),
        prepared_payload="{}",
        notes="FlexJobs native tracker sync",
        status="prepared",
    )

    application_notes = normalize_whitespace(_row_value(row, "application_notes") or "")
    is_pending_sync_note = "flexjobs native tracker pending sync" in application_notes.lower()
    is_legacy_not_found_note = "flexjobs expertapply native tracker status=not_found" in application_notes.lower()
    if tracker_item is None and (is_pending_sync_note or is_legacy_not_found_note):
        update_application_status(
            settings.jobs_db_path,
            job_ids=[int(row["id"])],
            status="reviewing",
            notes="FlexJobs native tracker pending sync",
            last_error=None,
        )
        return

    if tracker_item is None:
        update_application_status(
            settings.jobs_db_path,
            job_ids=[int(row["id"])],
            status="reviewing",
            notes="FlexJobs ExpertApply native tracker status=not_found",
            last_error=None,
        )
        return

    tracker_status = normalize_whitespace(tracker_item.get("jobApplicationStatus") or "")
    tracker_application_id = normalize_whitespace(tracker_item.get("id") or "")
    review_required = (tracker_item.get("answerCounter") or {}).get("totalReviewRequiredAnswers")
    note_parts = [
        f"FlexJobs ExpertApply native tracker status={tracker_status or 'unknown'}",
        f"tracker_application_id={tracker_application_id or 'unknown'}",
    ]
    if review_required not in (None, ""):
        note_parts.append(f"review_required_answers={review_required}")

    if tracker_status in {"Processing", "Success"}:
        local_status = "applied"
    elif tracker_status == "Fail":
        local_status = "error"
    elif tracker_status == "CancelledByUser":
        local_status = "skipped"
    else:
        local_status = "reviewing"

    update_application_status(
        settings.jobs_db_path,
        job_ids=[int(row["id"])],
        status=local_status,
        notes="; ".join(note_parts),
        last_error=None,
    )


def native_expertapply_results_as_dicts(results: list[NativeExpertApplyResult]) -> list[dict[str, Any]]:
    return [
        {
            "job_id": result.job_id,
            "title": result.title,
            "company": result.company,
            "external_job_id": result.external_job_id,
            "tracker_application_id": result.tracker_application_id,
            "initial_status": result.initial_status,
            "action": result.action,
            "final_status": result.final_status,
            "review_required_answers": result.review_required_answers,
            "submitted": result.submitted,
            "error": result.error,
        }
        for result in results
    ]


def apply_native_expertapply_jobs(
    settings: Settings,
    rows: list[Row],
    *,
    submit: bool,
    dry_run: bool,
) -> list[NativeExpertApplyResult]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    with sync_playwright() as playwright:
        browser, context, page = _connect_to_manual_chrome(playwright, settings)
        try:
            _safe_goto(page, TRACKER_URL, settings.flexjobs_timeout_ms)
            page.wait_for_timeout(1500)
            cookie_header = _cookie_header(context)
            _user_id, profile_id = _resolve_user_and_profile_id(page, cookie_header)

            row_entries: list[dict[str, Any]] = []
            results: list[NativeExpertApplyResult] = []
            for row in rows:
                try:
                    payload = _extract_selected_job_payload(row)
                    application_notes = normalize_whitespace(_row_value(row, "application_notes") or "")
                    row_entries.append(
                        {
                            "row": row,
                            "payload": payload,
                            "external_job_id": payload["jobId"],
                            "initial_status": None,
                            "action": None,
                            "error": None,
                            "has_pending_sync_note": (
                                "flexjobs native tracker pending sync" in application_notes.lower()
                            ),
                        }
                    )
                except Exception as exc:
                    results.append(
                        NativeExpertApplyResult(
                            job_id=int(row["id"]),
                            title=row["title"],
                            company=row["company"],
                            external_job_id=normalize_whitespace(row["external_id"] or ""),
                            tracker_application_id=None,
                            initial_status=None,
                            action="error",
                            final_status=None,
                            review_required_answers=None,
                            submitted=False,
                            error=str(exc),
                        )
                    )

            def refresh_summary_map() -> dict[str, dict[str, Any]]:
                return _summary_by_external_job_id(_fetch_summary(profile_id, cookie_header=cookie_header))

            summary_by_job = refresh_summary_map()
            for entry in row_entries:
                existing = summary_by_job.get(entry["external_job_id"])
                if existing is not None:
                    entry["initial_status"] = normalize_whitespace(existing.get("jobApplicationStatus") or "") or None

            if dry_run:
                for entry in row_entries:
                    existing = summary_by_job.get(entry["external_job_id"])
                    if existing is not None:
                        action = "dry_run_existing"
                    elif entry["has_pending_sync_note"]:
                        action = "awaiting_tracker_sync"
                    else:
                        action = "dry_run_create"
                    final_status = normalize_whitespace((existing or {}).get("jobApplicationStatus") or "") or None
                    review_required_answers = ((existing or {}).get("answerCounter") or {}).get("totalReviewRequiredAnswers")
                    tracker_application_id = normalize_whitespace((existing or {}).get("id") or "") or None
                    results.append(
                        NativeExpertApplyResult(
                            job_id=int(entry["row"]["id"]),
                            title=entry["row"]["title"],
                            company=entry["row"]["company"],
                            external_job_id=entry["external_job_id"],
                            tracker_application_id=tracker_application_id,
                            initial_status=entry["initial_status"],
                            action=action,
                            final_status=final_status,
                            review_required_answers=review_required_answers,
                            submitted=final_status in {"Processing", "Success"},
                            error=None,
                        )
                    )
                return results

            max_passes = 4
            for _pass in range(max_passes):
                created_any = False
                submitted_any = False

                for entry in row_entries:
                    if entry["action"] == "error":
                        continue
                    existing = summary_by_job.get(entry["external_job_id"])
                    if existing is not None:
                        if entry["initial_status"] is None:
                            entry["initial_status"] = normalize_whitespace(existing.get("jobApplicationStatus") or "") or None
                        continue
                    if entry["has_pending_sync_note"] or entry["action"] in {"created_pending_sync", "awaiting_tracker_sync"}:
                        entry["action"] = "awaiting_tracker_sync"
                        continue
                    try:
                        _http_json(
                            CREATE_URL.format(profile_id=profile_id),
                            cookie_header=cookie_header,
                            method="POST",
                            payload={"selectedJobs": [entry["payload"]]},
                        )
                        entry["action"] = "created_pending_sync"
                        created_any = True
                    except Exception as exc:
                        entry["action"] = "error"
                        entry["error"] = str(exc)

                if created_any:
                    page.wait_for_timeout(4500)
                    summary_by_job = refresh_summary_map()
                    for entry in row_entries:
                        existing = summary_by_job.get(entry["external_job_id"])
                        if existing is not None and entry["initial_status"] is None:
                            entry["initial_status"] = normalize_whitespace(existing.get("jobApplicationStatus") or "") or None

                ready_summary = _fetch_summary(
                    profile_id,
                    cookie_header=cookie_header,
                    statuses=["ReadyForQuickApply", "ReadyForReview"],
                    ready_only=True,
                )
                ready_index_map = {
                    normalize_whitespace(item.get("id") or ""): index
                    for index, item in enumerate(ready_summary)
                }
                quick_apply_index_map = {
                    normalize_whitespace(item.get("id") or ""): index
                    for index, item in enumerate(
                        [item for item in ready_summary if normalize_whitespace(item.get("jobApplicationStatus") or "") == "ReadyForQuickApply"]
                    )
                }

                for entry in row_entries:
                    if entry["action"] == "error":
                        continue
                    existing = summary_by_job.get(entry["external_job_id"])
                    if existing is None:
                        continue
                    tracker_status = normalize_whitespace(existing.get("jobApplicationStatus") or "")
                    tracker_application_id = normalize_whitespace(existing.get("id") or "")

                    if tracker_status in IN_FLIGHT_STATUSES | TERMINAL_STATUSES:
                        if entry["action"] not in {"submitted_quick_apply", "submitted_review"}:
                            entry["action"] = "existing"
                        entry["error"] = None
                        continue

                    if tracker_status == "ReadyForQuickApply" and submit and tracker_application_id in quick_apply_index_map:
                        submitted, error = _submit_quick_apply(
                            page,
                            quick_apply_index_map[tracker_application_id],
                            settings.flexjobs_timeout_ms,
                        )
                        if not submitted and tracker_application_id:
                            submitted, error = _submit_ready_review(
                                page,
                                tracker_application_id,
                                settings,
                                settings.flexjobs_timeout_ms,
                            )
                        entry["action"] = "submitted_quick_apply" if submitted else "quick_apply_blocked"
                        entry["error"] = error
                        submitted_any = submitted_any or submitted
                        page.wait_for_timeout(1500)
                        summary_by_job = refresh_summary_map()
                        ready_summary = _fetch_summary(
                            profile_id,
                            cookie_header=cookie_header,
                            statuses=["ReadyForQuickApply", "ReadyForReview"],
                            ready_only=True,
                        )
                        ready_index_map = {
                            normalize_whitespace(item.get("id") or ""): index
                            for index, item in enumerate(ready_summary)
                        }
                        quick_apply_index_map = {
                            normalize_whitespace(item.get("id") or ""): index
                            for index, item in enumerate(
                                [item for item in ready_summary if normalize_whitespace(item.get("jobApplicationStatus") or "") == "ReadyForQuickApply"]
                            )
                        }
                        continue

                    if tracker_status == "ReadyForQuickApply" and submit and tracker_application_id:
                        submitted, error = _submit_ready_review(
                            page,
                            tracker_application_id,
                            settings,
                            settings.flexjobs_timeout_ms,
                        )
                        entry["action"] = "submitted_quick_apply" if submitted else "quick_apply_blocked"
                        entry["error"] = error
                        submitted_any = submitted_any or submitted
                        page.wait_for_timeout(1500)
                        summary_by_job = refresh_summary_map()
                        ready_summary = _fetch_summary(
                            profile_id,
                            cookie_header=cookie_header,
                            statuses=["ReadyForQuickApply", "ReadyForReview"],
                            ready_only=True,
                        )
                        ready_index_map = {
                            normalize_whitespace(item.get("id") or ""): index
                            for index, item in enumerate(ready_summary)
                        }
                        quick_apply_index_map = {
                            normalize_whitespace(item.get("id") or ""): index
                            for index, item in enumerate(
                                [item for item in ready_summary if normalize_whitespace(item.get("jobApplicationStatus") or "") == "ReadyForQuickApply"]
                            )
                        }
                        continue

                    if tracker_status == "ReadyForReview":
                        if submit and tracker_application_id:
                            submitted, error = _submit_ready_review(
                                page,
                                tracker_application_id,
                                settings,
                                settings.flexjobs_timeout_ms,
                            )
                            entry["action"] = "submitted_review" if submitted else "review_submit_blocked"
                            entry["error"] = error
                            submitted_any = submitted_any or submitted
                            page.wait_for_timeout(1500)
                            summary_by_job = refresh_summary_map()
                            ready_summary = _fetch_summary(
                                profile_id,
                                cookie_header=cookie_header,
                                statuses=["ReadyForQuickApply", "ReadyForReview"],
                                ready_only=True,
                            )
                            ready_index_map = {
                                normalize_whitespace(item.get("id") or ""): index
                                for index, item in enumerate(ready_summary)
                            }
                            quick_apply_index_map = {
                                normalize_whitespace(item.get("id") or ""): index
                                for index, item in enumerate(
                                    [item for item in ready_summary if normalize_whitespace(item.get("jobApplicationStatus") or "") == "ReadyForQuickApply"]
                                )
                            }
                        elif entry["action"] not in {"submitted_review", "review_submit_blocked"}:
                            entry["action"] = "needs_manual_review"

                if not created_any and not submitted_any:
                    break

                page.wait_for_timeout(2500)
                summary_by_job = refresh_summary_map()

            summary_by_job = refresh_summary_map()
            for entry in row_entries:
                row = entry["row"]
                final_item = summary_by_job.get(entry["external_job_id"])
                tracker_application_id = normalize_whitespace((final_item or {}).get("id") or "") or None
                final_status = normalize_whitespace((final_item or {}).get("jobApplicationStatus") or "") or None
                review_required_answers = ((final_item or {}).get("answerCounter") or {}).get("totalReviewRequiredAnswers")
                submitted = final_status in {"Processing", "Success"}
                if submitted:
                    entry["error"] = None
                    if entry["action"] not in {"submitted_quick_apply", "submitted_review"}:
                        entry["action"] = "existing"
                elif final_status == "ReadyForReview" and entry["action"] not in {"review_submit_blocked", "submitted_review"}:
                    entry["action"] = "needs_manual_review"
                elif final_item is None and entry["action"] in {"created_pending_sync", "awaiting_tracker_sync", None}:
                    entry["action"] = "awaiting_tracker_sync"

                if final_item is None and entry["action"] == "awaiting_tracker_sync":
                    update_application_status(
                        settings.jobs_db_path,
                        job_ids=[int(row["id"])],
                        status="reviewing",
                        notes="FlexJobs native tracker pending sync",
                        last_error=None,
                    )
                elif entry["action"] == "error":
                    update_application_status(
                        settings.jobs_db_path,
                        job_ids=[int(row["id"])],
                        status="error",
                        notes="FlexJobs ExpertApply native automation error",
                        last_error=entry["error"],
                    )
                else:
                    _sync_application_row(settings, row, final_item)

                results.append(
                    NativeExpertApplyResult(
                        job_id=int(row["id"]),
                        title=row["title"],
                        company=row["company"],
                        external_job_id=entry["external_job_id"],
                        tracker_application_id=tracker_application_id,
                        initial_status=entry["initial_status"],
                        action=entry["action"] or "existing",
                        final_status=final_status,
                        review_required_answers=review_required_answers,
                        submitted=submitted,
                        error=entry["error"],
                    )
                )

            return results
        finally:
            browser.close()
