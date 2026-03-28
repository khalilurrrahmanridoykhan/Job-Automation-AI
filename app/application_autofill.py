from __future__ import annotations

import json
import time
from dataclasses import dataclass
from sqlite3 import Row
from typing import Any
from urllib.parse import urljoin

from .config import Settings
from .job_search import _connect_to_manual_chrome, _safe_goto
from .utils import normalize_whitespace, playwright_environment_hint


FIELD_SELECTOR = "input, textarea, select"
LOGIN_HINTS = ("login", "log in", "sign in", "signin")
NOISE_FIELD_HINTS = (
    "job alert",
    "receive an alert",
    "how often",
    "frequency",
    "days",
    "subscribe",
    "talent community",
    "save job",
    "share this job",
    "job category",
)
APPLICATION_START_HINTS = (
    "start your application",
    "apply manually",
    "autofill with resume",
    "use my last application",
    "upload cv file",
    "upload cv later",
    "without resume",
    "paste cv",
)
DEAD_PAGE_HINTS = (
    "job not found",
    "job post no longer exists",
    "the page you are looking for doesn't exist",
    "this job post no longer exists",
    "this job is no longer available",
)
APPLY_ENTRY_SELECTORS = [
    "a.dialogApplyBtn",
    "button.dialogApplyBtn",
    "a:has-text('Apply Now')",
    "button:has-text('Apply Now')",
    "a:has-text('Apply')",
    "button:has-text('Apply')",
    "[data-automation-id='applyButton']",
    "[aria-label*='apply' i]",
]


@dataclass(slots=True)
class AutofillResult:
    application_id: int
    job_id: int
    title: str
    company: str | None
    application_url: str
    platform: str
    filled_fields: list[str]
    uploaded_files: list[str]
    missing_required_fields: list[str]
    notes: list[str]
    error: str | None = None
    submitted: bool = False


def _detect_platform(url: str) -> str:
    lowered = (url or "").lower()
    if "workday" in lowered:
        return "workday"
    if "icims.com" in lowered:
        return "icims"
    if "ashbyhq.com" in lowered:
        return "ashby"
    if "smartrecruiters.com" in lowered:
        return "smartrecruiters"
    if "brassring.com" in lowered:
        return "brassring"
    if "greenhouse.io" in lowered:
        return "greenhouse"
    if "lever.co" in lowered:
        return "lever"
    return "generic"


def _packet_payload(row: Row) -> dict[str, Any]:
    raw = row["prepared_payload"]
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _split_name(full_name: str | None) -> tuple[str, str]:
    parts = [part for part in normalize_whitespace(full_name or "").split(" ") if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _field_values_from_packet(packet: dict[str, Any], settings: Settings) -> dict[str, Any]:
    candidate = packet.get("candidate", {})
    form_answers = packet.get("form_answers", {})
    first_name, last_name = _split_name(candidate.get("name"))
    account_login = normalize_whitespace(
        form_answers.get("account_login") or settings.candidate_account_login or candidate.get("email") or ""
    )
    account_password = normalize_whitespace(form_answers.get("account_password") or settings.candidate_account_password or "")

    values = {
        "first_name": first_name,
        "middle_name": normalize_whitespace(form_answers.get("middle_name") or settings.candidate_middle_name or ""),
        "last_name": last_name,
        "full_name": normalize_whitespace(candidate.get("name") or ""),
        "email": normalize_whitespace(candidate.get("email") or ""),
        "account_login": account_login,
        "account_password": account_password,
        "account_password_confirm": account_password,
        "phone": normalize_whitespace(candidate.get("phone") or ""),
        "phone_type": normalize_whitespace(form_answers.get("phone_type") or settings.candidate_phone_type or ""),
        "professional_summary": normalize_whitespace(form_answers.get("professional_summary") or ""),
        "why_this_role": normalize_whitespace(form_answers.get("why_this_role") or ""),
        "relevant_experience": normalize_whitespace(form_answers.get("relevant_experience") or ""),
        "languages": normalize_whitespace(form_answers.get("languages") or ""),
        "cover_letter": (packet.get("draft_cover_letter") or "").strip(),
        "resume_path": normalize_whitespace(candidate.get("resume_path") or ""),
        "linkedin_url": normalize_whitespace(form_answers.get("linkedin_url") or settings.candidate_linkedin_url or ""),
        "github_url": normalize_whitespace(form_answers.get("github_url") or settings.candidate_github_url or ""),
        "job_source": normalize_whitespace(form_answers.get("job_source") or "Other online job boards"),
        "over_18": (
            form_answers.get("over_18")
            if form_answers.get("over_18") is not None
            else True
        ),
        "address_type": normalize_whitespace(form_answers.get("address_type") or settings.candidate_address_type or ""),
        "address_line1": normalize_whitespace(form_answers.get("address_line1") or settings.candidate_address_line1 or ""),
        "address_line2": normalize_whitespace(form_answers.get("address_line2") or settings.candidate_address_line2 or ""),
        "city": normalize_whitespace(form_answers.get("city") or settings.candidate_location_city or ""),
        "region": normalize_whitespace(form_answers.get("region") or settings.candidate_location_region or ""),
        "postal_code": normalize_whitespace(form_answers.get("postal_code") or settings.candidate_postal_code or ""),
        "country": normalize_whitespace(form_answers.get("country") or settings.candidate_country or ""),
        "county": normalize_whitespace(form_answers.get("county") or settings.candidate_county or ""),
        "accept_terms": (
            form_answers.get("accept_terms")
            if form_answers.get("accept_terms") is not None
            else settings.candidate_accept_terms
        ),
        "salary_expectations": normalize_whitespace(form_answers.get("salary_expectations") or settings.candidate_salary_expectations or ""),
        "start_date": normalize_whitespace(form_answers.get("start_date") or settings.candidate_start_date or ""),
        "work_authorization": (
            form_answers.get("work_authorization")
            if form_answers.get("work_authorization") is not None
            else settings.candidate_work_authorized_us
        ),
        "require_sponsorship": (
            form_answers.get("require_sponsorship")
            if form_answers.get("require_sponsorship") is not None
            else settings.candidate_require_sponsorship
        ),
        "willing_to_relocate": (
            form_answers.get("willing_to_relocate")
            if form_answers.get("willing_to_relocate") is not None
            else settings.candidate_willing_to_relocate
        ),
    }
    return values


def _frame_contexts(page) -> list[Any]:
    contexts: list[Any] = [page]
    try:
        for frame in page.frames:
            if frame not in contexts:
                contexts.append(frame)
    except Exception:
        pass
    return contexts


def _is_login_page(page) -> bool:
    try:
        title = (page.title() or "").lower()
    except Exception:
        title = ""
    current_url = (page.url or "").lower()
    marker_parts: list[str] = []
    for context in _frame_contexts(page):
        try:
            marker_parts.append(normalize_whitespace(context.locator("body").inner_text(timeout=2000)[:1200]).lower())
        except Exception:
            continue
    marker_text = " ".join(marker_parts)
    if any(hint in marker_text for hint in APPLICATION_START_HINTS):
        return False
    has_password = False
    for context in _frame_contexts(page):
        try:
            if context.locator("input[type='password']").count() > 0:
                has_password = True
                break
        except Exception:
            continue
    sign_in_markers = any(hint in title or hint in current_url or hint in marker_text for hint in LOGIN_HINTS)
    if "/login" in current_url or "log back in" in marker_text or "returning candidate" in marker_text:
        sign_in_markers = True
    if "successfactors.com/careers" in current_url and "email address" in marker_text:
        sign_in_markers = True
    if "/login" in current_url and any(text in marker_text for text in ("email", "enter your information", "application faqs")):
        return True
    account_markers = any(text in marker_text for text in ("create account", "candidate profile", "forgot password"))
    return sign_in_markers and (has_password or account_markers or "email address" in marker_text)


def _candidate_fields(page) -> list[dict[str, Any]]:
    try:
        fields = _discover_fields(page)
    except Exception:
        return []
    return [field for field in fields if not _is_noise_field(field) and _field_key(field)]


def _looks_like_signup_widget(page, candidate_fields: list[dict[str, Any]] | None = None) -> bool:
    fields = candidate_fields if candidate_fields is not None else _candidate_fields(page)
    if not fields:
        return False

    keys = {
        key
        for field in fields
        for key in [_field_key(field)]
        if key
    }
    if not keys or not keys.issubset({"first_name", "last_name", "email"}):
        return False

    apply_entries = _find_apply_entries(page)
    if not apply_entries:
        return False
    return any("apply" in (entry.get("text") or "").lower() for entry in apply_entries)


def _page_body_text(page, *, limit: int = 4000) -> str:
    try:
        body = page.locator("body").inner_text(timeout=2000)
    except Exception:
        return ""
    return normalize_whitespace(body[:limit]).lower()


def _page_changed_after_apply(page, previous_url: str) -> bool:
    if (page.url or "") != (previous_url or ""):
        return True
    if _is_login_page(page):
        return True
    candidate_fields = _candidate_fields(page)
    if candidate_fields and not _looks_like_signup_widget(page, candidate_fields):
        return True
    try:
        body = normalize_whitespace(page.locator("body").inner_text(timeout=2000)).lower()
    except Exception:
        body = ""
    return any(hint in body for hint in APPLICATION_START_HINTS)


def _is_dead_job_page(page) -> bool:
    combined_parts = [(page.url or "").lower()]
    try:
        combined_parts.append(normalize_whitespace(page.title() or "").lower())
    except Exception:
        pass
    try:
        html = page.content().lower()
    except Exception:
        html = ""
    if "postingavailable: false" in html or '"postingavailable":false' in html:
        return True
    for context in _frame_contexts(page):
        try:
            combined_parts.append(normalize_whitespace(context.locator("body").inner_text(timeout=2000)[:2500]).lower())
        except Exception:
            continue
    combined = " ".join(combined_parts)
    return any(hint in combined for hint in DEAD_PAGE_HINTS)


def _discover_fields(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        (selector) => {
          const elements = Array.from(document.querySelectorAll(selector));
          return elements.map((el, index) => {
            const style = window.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            const isFile = (el.getAttribute('type') || '').toLowerCase() === 'file';
            const visible = (
              isFile ||
              (
                style.display !== 'none' &&
                style.visibility !== 'hidden' &&
                !el.disabled &&
                (rect.width > 0 || rect.height > 0)
              )
            );
            let labels = [];
            if (el.id) {
              labels = labels.concat(
                Array.from(document.querySelectorAll(`label[for="${el.id.replace(/"/g, '\\"')}"]`)).map(
                  item => item.textContent || ''
                )
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
                  .map(id => document.getElementById(id))
                  .filter(Boolean)
                  .map(item => item.textContent || '')
              );
            }
            return {
              index,
              visible,
              tag: el.tagName.toLowerCase(),
              type: (el.getAttribute('type') || '').toLowerCase(),
              value: el.getAttribute('value') || '',
              name: el.getAttribute('name') || '',
              id: el.getAttribute('id') || '',
              placeholder: el.getAttribute('placeholder') || '',
              aria_label: el.getAttribute('aria-label') || '',
              autocomplete: el.getAttribute('autocomplete') || '',
              required: !!el.required || el.getAttribute('aria-required') === 'true',
              checked: !!el.checked,
              label_text: labels.join(' '),
              section_text: (() => {
                const container = el.closest('fieldset, section, li, tr, .formField, .iCIMS_ExpandedField, .iCIMS_Field');
                return container ? (container.textContent || '').slice(0, 400) : '';
              })(),
              options: el.tagName.toLowerCase() === 'select'
                ? Array.from(el.options || []).map(option => ({
                    value: option.value || '',
                    text: option.textContent || ''
                  }))
                : [],
            };
          }).filter(item => item.visible);
        }
        """,
        FIELD_SELECTOR,
    )


def _field_key(field: dict[str, Any]) -> str | None:
    tag = field["tag"]
    field_type = (field.get("type") or "").lower()
    focused = " ".join(
        [
            field.get("label_text") or "",
            field.get("name") or "",
            field.get("id") or "",
            field.get("placeholder") or "",
            field.get("aria_label") or "",
            field.get("autocomplete") or "",
        ]
    ).lower()
    combined = " ".join(
        [
            field.get("label_text") or "",
            field.get("section_text") or "",
            field.get("name") or "",
            field.get("id") or "",
            field.get("placeholder") or "",
            field.get("aria_label") or "",
            field.get("autocomplete") or "",
        ]
    ).lower()

    if field_type in {"hidden", "submit", "button"}:
        return None
    if field_type == "password":
        if any(keyword in combined for keyword in ("re-enter", "reenter", "verify new password", "confirm password")):
            return "account_password_confirm"
        return "account_password"
    if field_type == "file":
        if any(keyword in combined for keyword in ("resume", "cv", "curriculum vitae")):
            return "resume_path"
        return None
    if "middle name" in combined:
        return "middle_name"
    if any(keyword in combined for keyword in ("where did you hear about", "how did you hear about", "job source")):
        return "job_source"
    if any(keyword in combined for keyword in ("over the age of 18", "over 18", "18 years of age", "18 years old")):
        return "over_18"
    if "linkedin" in focused:
        return "linkedin_url"
    if "github" in focused:
        return "github_url"
    if "referrer" in combined:
        return None
    if "login" in combined or "username" in combined or "user name" in combined:
        return "account_login"
    if "type" in combined and "phone" in combined:
        return "phone_type"
    if "type" in combined and "address" in combined:
        return "address_type"
    if "address line 1" in combined or "street address" in combined or "address 1" in combined:
        return "address_line1"
    if "address 2" in combined or "address line 2" in combined:
        return "address_line2"
    if "city" in combined:
        return "city"
    if any(keyword in combined for keyword in ("state", "province", "region")):
        return "region"
    if "county" in combined:
        return "county"
    if any(keyword in combined for keyword in ("zip", "postal")):
        return "postal_code"
    if "country" in combined:
        return "country"
    if "first name" in combined or "given name" in combined or "legal first name" in combined:
        return "first_name"
    if "last name" in combined or "family name" in combined or "surname" in combined or "legal last name" in combined:
        return "last_name"
    if field_type == "email" or "email" in combined:
        return "email"
    if "number" in combined and "phone" in combined:
        return "phone"
    if field_type == "tel" or any(keyword in combined for keyword in ("phone", "mobile", "telephone")):
        return "phone"
    if any(keyword in combined for keyword in ("consent to the terms", "terms and conditions", "privacy policy")):
        return "accept_terms"
    if any(keyword in combined for keyword in ("authorized to work", "legally authorized", "work authorization")):
        return "work_authorization"
    if any(keyword in combined for keyword in ("sponsorship", "visa", "require sponsor", "need sponsor")):
        return "require_sponsorship"
    if "relocate" in combined:
        return "willing_to_relocate"
    if any(keyword in combined for keyword in ("salary expectation", "desired salary", "expected salary", "salary requirement", "compensation expectation")):
        return "salary_expectations"
    if any(keyword in combined for keyword in ("start date", "available to start", "available start", "earliest start")):
        return "start_date"
    if "cover letter" in combined or "motivation letter" in combined:
        return "cover_letter"
    if any(keyword in combined for keyword in ("professional summary", "summary", "about you", "introduction")):
        return "professional_summary"
    if any(
        keyword in combined
        for keyword in (
            "why this role",
            "why this job",
            "why work with us",
            "why would you like to work with us",
            "why this company",
            "why are you interested",
            "why do you want",
            "why are you applying",
            "motivation",
        )
    ):
        return "why_this_role"
    if any(
        keyword in combined
        for keyword in (
            "relevant experience",
            "tell us about your experience",
            "work experience",
            "background",
            "qualifications",
            "experience",
        )
    ):
        return "relevant_experience"
    if "language" in combined:
        return "languages"
    if "full name" in combined:
        return "full_name"
    if "name" in combined and all(keyword not in combined for keyword in ("company", "user", "referrer")):
        return "full_name"
    return None


FINAL_SUBMIT_HINTS = (
    "submit application",
    "submit",
    "send application",
    "finish application",
    "complete application",
)
FINAL_SUBMIT_NEGATIVE_HINTS = (
    "apply manually",
    "autofill with resume",
    "use my last application",
    "save for later",
    "save draft",
    "cancel",
    "back",
    "job alert",
    "subscribe",
    "upload cv later",
    "continue",
    "next",
)
SUBMISSION_SUCCESS_HINTS = (
    "application submitted",
    "your application has been submitted",
    "your application was successfully submitted",
    "successfully submitted",
    "thank you for applying",
    "we've received your application",
    "application complete",
    "application received",
    "thanks for applying",
)


def _submit_candidate_in_context(context) -> dict[str, str] | None:
    try:
        candidate = context.evaluate(
            """
            () => {
              const nodes = Array.from(
                document.querySelectorAll("button, input[type='submit'], input[type='button'], a, [role='button']")
              );
              const ranked = nodes
                .map((node) => {
                  const text = (node.innerText || node.textContent || node.value || '').trim();
                  const href = node.getAttribute('href') || '';
                  const aria = node.getAttribute('aria-label') || '';
                  const type = (node.getAttribute('type') || '').toLowerCase();
                  const disabled = !!node.disabled || node.getAttribute('aria-disabled') === 'true';
                  const rect = node.getBoundingClientRect();
                  const style = window.getComputedStyle(node);
                  const visible = style.display !== 'none' && style.visibility !== 'hidden' && (rect.width > 0 || rect.height > 0);
                  const combined = `${text} ${href} ${aria} ${type}`.toLowerCase();
                  let score = 0;
                  if (combined.includes('submit application')) score += 260;
                  if (combined.includes('send application')) score += 220;
                  if (combined.includes('finish application')) score += 200;
                  if (combined.includes('complete application')) score += 180;
                  if (combined.includes('submit')) score += 120;
                  if (type === 'submit') score += 60;
                  if (combined.includes('apply now')) score += 40;
                  if (combined.includes('sign up') || combined.includes('job alerts') || combined.includes('talent community')) score -= 260;
                  if (combined.includes('candidate login') || combined.includes('log in') || combined.includes('login')) score -= 220;
                  if (combined.includes('continue') || combined.includes('next')) score -= 120;
                  if (combined.includes('cancel') || combined.includes('save draft') || combined.includes('save for later')) score -= 180;
                  if (combined.includes('apply manually') || combined.includes('autofill with resume') || combined.includes('use my last application')) score -= 220;
                  if (!visible) score -= 120;
                  if (disabled) score -= 200;
                  return { text, score, visible, disabled, index: nodes.indexOf(node) };
                })
                .filter((item) => item.score > 0)
                .sort((a, b) => b.score - a.score);
              if (!ranked.length) return null;
              const best = ranked[0];
              const node = nodes[best.index];
              node.click();
              return { text: best.text || 'Submit', score: String(best.score) };
            }
            """
        )
    except Exception:
        return None
    return candidate if isinstance(candidate, dict) else None


def _submission_succeeded(page) -> bool:
    current_url = (page.url or "").lower()
    if any(marker in current_url for marker in ("/submitted", "/confirmation", "/thank-you", "/thanks")):
        return True

    body = _page_body_text(page, limit=8000)
    if any(marker in body for marker in SUBMISSION_SUCCESS_HINTS):
        return True

    if not _candidate_fields(page) and any(marker in body for marker in ("thank you", "next steps", "received")):
        return True

    return False


def _submit_application_form(page, result: AutofillResult) -> bool:
    for context in _frame_contexts(page):
        candidate = _submit_candidate_in_context(context)
        if not candidate:
            continue
        result.notes.append(f"Clicked final submit control: {candidate.get('text') or 'Submit'}")
        for _ in range(10):
            page.wait_for_timeout(1000)
            if _submission_succeeded(page):
                return True
    return _submission_succeeded(page)


def _field_descriptor(field: dict[str, Any]) -> str:
    for key in ("label_text", "aria_label", "placeholder", "name", "id"):
        value = normalize_whitespace(field.get(key) or "")
        if value:
            return value
    return f"{field.get('tag', 'field')}#{field.get('index', '?')}"


def _combined_field_text(field: dict[str, Any]) -> str:
    return normalize_whitespace(
        " ".join(
            [
                field.get("label_text") or "",
                field.get("section_text") or "",
                field.get("aria_label") or "",
                field.get("placeholder") or "",
                field.get("name") or "",
                field.get("id") or "",
            ]
        )
    ).lower()


def _is_noise_field(field: dict[str, Any]) -> bool:
    descriptor = _combined_field_text(field)
    return any(hint in descriptor for hint in NOISE_FIELD_HINTS)


def _normalize_choice(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    return normalize_whitespace(str(value or "")).lower()


def _fill_text_like(page, index: int, value: str) -> bool:
    locator = page.locator(FIELD_SELECTOR).nth(index)
    try:
        locator.scroll_into_view_if_needed(timeout=1500)
    except Exception:
        pass

    try:
        locator.fill(value, timeout=2500)
        return True
    except Exception:
        pass

    try:
        return bool(
            page.evaluate(
                """
                ([selector, index, value]) => {
                  const elements = Array.from(document.querySelectorAll(selector));
                  const el = elements[index];
                  if (!el) return false;
                  el.focus();
                  if ('value' in el) {
                    el.value = '';
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    return true;
                  }
                  return false;
                }
                """,
                [FIELD_SELECTOR, index, value],
            )
        )
    except Exception:
        return False


def _fill_select(page, index: int, field: dict[str, Any], value: Any) -> bool:
    locator = page.locator(FIELD_SELECTOR).nth(index)
    desired = _normalize_choice(value)
    if not desired:
        return False

    options = field.get("options") or []
    candidates: list[str] = []
    for option in options:
        option_value = _normalize_choice(option.get("value"))
        option_text = _normalize_choice(option.get("text"))
        if not option_value and not option_text:
            continue
        if option_value == desired or option_text == desired:
            if option.get("value"):
                candidates.append(option["value"])
            if option.get("text"):
                candidates.append(option["text"])
        elif desired in option_text or desired in option_value:
            if option.get("value"):
                candidates.append(option["value"])
            if option.get("text"):
                candidates.append(option["text"])

    for candidate in candidates:
        try:
            locator.select_option(candidate, timeout=2500)
            return True
        except Exception:
            continue
    return False


def _fill_checkbox_or_radio(page, index: int, field: dict[str, Any], value: Any) -> bool:
    locator = page.locator(FIELD_SELECTOR).nth(index)
    desired = _normalize_choice(value)
    current_value = _normalize_choice(field.get("value"))
    descriptor = _normalize_choice(_field_descriptor(field))

    should_select = False
    if desired in {"yes", "true", "1"}:
        should_select = current_value in {"yes", "true", "1", "y"} or "yes" in descriptor or "authorized" in descriptor
    elif desired in {"no", "false", "0"}:
        should_select = current_value in {"no", "false", "0", "n"} or "no" in descriptor or "not" in descriptor
    else:
        should_select = desired in current_value or desired in descriptor

    if not should_select:
        return False

    try:
        locator.check(timeout=2500)
        return True
    except Exception:
        try:
            locator.click(timeout=2500)
            return True
        except Exception:
            return False


def _fill_button_group(page, question_text: str, choice_text: str) -> bool:
    try:
        return bool(
            page.evaluate(
                """
                ([questionText, choiceText]) => {
                  const wantedQuestion = (questionText || '').toLowerCase();
                  const wantedChoice = (choiceText || '').toLowerCase();
                  const labels = Array.from(document.querySelectorAll('label'));
                  const targetLabel = labels.find(label => (label.textContent || '').trim().toLowerCase().includes(wantedQuestion));
                  if (!targetLabel) return false;
                  const container = targetLabel.parentElement;
                  if (!container) return false;
                  const buttons = Array.from(container.querySelectorAll('button'));
                  const targetButton = buttons.find(button => (button.innerText || button.textContent || '').trim().toLowerCase() === wantedChoice);
                  if (!targetButton) return false;
                  targetButton.click();
                  return true;
                }
                """,
                [question_text, choice_text],
            )
        )
    except Exception:
        return False


def _upload_file(page, index: int, path: str) -> bool:
    locator = page.locator(FIELD_SELECTOR).nth(index)
    try:
        locator.set_input_files(path, timeout=3000)
        return True
    except Exception:
        return False


def _recognized_field_keys(page) -> set[str]:
    keys: set[str] = set()
    for context in _frame_contexts(page):
        try:
            fields = _discover_fields(context)
        except Exception:
            continue
        for field in fields:
            if _is_noise_field(field):
                continue
            key = _field_key(field)
            if key:
                keys.add(key)
    return keys


def _fill_custom_questions(page, values: dict[str, Any], result: AutofillResult) -> None:
    over_18 = values.get("over_18")
    if over_18 is not None and _fill_button_group(page, "are you over the age of 18", "yes" if bool(over_18) else "no"):
        if "over_18" not in result.filled_fields:
            result.filled_fields.append("over_18")


def _handle_login_gate(page, values: dict[str, Any], result: AutofillResult) -> bool:
    if not _is_login_page(page):
        return False

    body = _page_body_text(page, limit=5000)
    key_set = _recognized_field_keys(page)
    create_account_markers = (
        "create account" in body
        or "candidate profile" in body
        or "connect your account" in body
        or "enter your information" in body
    )
    if create_account_markers:
        return False

    if any(key in key_set for key in {"first_name", "last_name", "phone", "address_line1", "resume_path"}):
        return False

    for _ in range(2):
        filled_email = False
        for context in _frame_contexts(page):
            fields = _discover_fields(context)
            email_field = next(
                (
                    field
                    for field in fields
                    if not _is_noise_field(field) and _field_key(field) == "email"
                ),
                None,
            )
            if email_field is None:
                continue
            value = values.get("email")
            if value and _fill_text_like(context, int(email_field["index"]), str(value)):
                if "email" not in result.filled_fields:
                    result.filled_fields.append("email")
                result.notes.append("Filled email on sign-in gate.")
            filled_email = True
            break
        if filled_email:
            break
        page.wait_for_timeout(1500)

    result.error = (
        "Application site requires sign in or account creation before the actual form is available. "
        f"Current URL: {page.url or ''}"
    )
    return True


def _find_frame_apply_entry(page) -> dict[str, str] | None:
    for context in _frame_contexts(page):
        if context is page:
            continue
        frame_url = (getattr(context, "url", "") or "").lower()
        frame_name = (getattr(context, "name", "") or "").lower()
        if "icims" not in frame_url and "in_iframe=1" not in frame_url and "icims" not in frame_name:
            continue
        try:
            candidate = context.evaluate(
                """
                () => {
                  const nodes = Array.from(document.querySelectorAll('a, button'));
                  const ranked = nodes
                    .map((node) => {
                      const text = (node.innerText || node.textContent || '').trim();
                      const href = node.getAttribute('href') || '';
                      const cls = node.getAttribute('class') || '';
                      const combined = `${text} ${href} ${cls}`.toLowerCase();
                      let score = 0;
                      if (combined.includes('apply for this job online')) score += 200;
                      if (combined.includes('apply manually')) score += 160;
                      if (combined.includes('autofill with resume')) score += 140;
                      if (combined.includes('apply')) score += 80;
                      if (combined.includes('log back in')) score -= 100;
                      return { text, href, score };
                    })
                    .filter((item) => item.score > 0)
                    .sort((a, b) => b.score - a.score);
                  return ranked.length ? ranked[0] : null;
                }
                """
            )
        except Exception:
            continue
        if isinstance(candidate, dict):
            href = normalize_whitespace(candidate.get("href") or "")
            if not href:
                continue
            return {
                "text": normalize_whitespace(candidate.get("text") or ""),
                "href": href,
            }
    return None


def _find_apply_entry(page) -> dict[str, str] | None:
    try:
        candidates = page.evaluate(
            """
            () => {
              const nodes = Array.from(document.querySelectorAll('a, button'));
              return nodes
                .map((node) => {
                  const text = (node.innerText || node.textContent || '').trim();
                  const href = node.getAttribute('href') || '';
                  const cls = node.getAttribute('class') || '';
                  const aria = node.getAttribute('aria-label') || '';
                  const dataAutomationId = node.getAttribute('data-automation-id') || '';
                  const combined = `${text} ${href} ${cls} ${aria} ${dataAutomationId}`.toLowerCase();
                  const normalizedText = text.toLowerCase();
                  const rect = node.getBoundingClientRect();
                  const style = window.getComputedStyle(node);
                  const visible = style.display !== 'none' && style.visibility !== 'hidden' && (rect.width > 0 || rect.height > 0);
                  let score = 0;
                  if (combined.includes('apply manually')) score += 160;
                  if (combined.includes('autofill with resume')) score += 140;
                  if (combined.includes('apply now')) score += 100;
                  if (normalizedText === 'apply') score += 95;
                  if (normalizedText === 'review and apply') score += 90;
                  if (combined.includes('/apply')) score += 90;
                  if (combined.includes('dialogapplybtn')) score += 80;
                  if (combined.includes(' apply ')) score += 40;
                  if (combined.includes('use my last application')) score -= 50;
                  if (combined.includes('apply with seek')) score -= 50;
                  if (combined.includes('job alert') || combined.includes('subscribe')) score -= 200;
                  if (!visible) score -= 50;
                  return {
                    text,
                    href,
                    visible,
                    score,
                  };
                })
                .filter((item) => item.score > 0)
                .sort((a, b) => b.score - a.score);
            }
            """
        )
    except Exception:
        return None
    if isinstance(candidates, list) and candidates:
        candidate = candidates[0]
        return {
            "text": normalize_whitespace(candidate.get("text") or ""),
            "href": normalize_whitespace(candidate.get("href") or ""),
        }
    return None


def _find_apply_entries(page) -> list[dict[str, str]]:
    try:
        candidates = page.evaluate(
            """
            () => {
              const nodes = Array.from(document.querySelectorAll('a, button'));
              return nodes
                .map((node) => {
                  const text = (node.innerText || node.textContent || '').trim();
                  const href = node.getAttribute('href') || '';
                  const cls = node.getAttribute('class') || '';
                  const aria = node.getAttribute('aria-label') || '';
                  const dataAutomationId = node.getAttribute('data-automation-id') || '';
                  const combined = `${text} ${href} ${cls} ${aria} ${dataAutomationId}`.toLowerCase();
                  const normalizedText = text.toLowerCase();
                  const rect = node.getBoundingClientRect();
                  const style = window.getComputedStyle(node);
                  const visible = style.display !== 'none' && style.visibility !== 'hidden' && (rect.width > 0 || rect.height > 0);
                  let score = 0;
                  if (combined.includes('apply manually')) score += 160;
                  if (combined.includes('autofill with resume')) score += 140;
                  if (combined.includes('apply now')) score += 100;
                  if (normalizedText === 'apply') score += 95;
                  if (normalizedText === 'review and apply') score += 90;
                  if (combined.includes('/apply')) score += 90;
                  if (combined.includes('dialogapplybtn')) score += 80;
                  if (combined.includes(' apply ')) score += 40;
                  if (combined.includes('use my last application')) score -= 50;
                  if (combined.includes('apply with seek')) score -= 50;
                  if (combined.includes('job alert') || combined.includes('subscribe')) score -= 200;
                  if (!visible) score -= 50;
                  return { text, href, visible, score };
                })
                .filter((item) => item.score > 0)
                .sort((a, b) => b.score - a.score);
            }
            """
        )
    except Exception:
        return []

    entries: list[dict[str, str]] = []
    if not isinstance(candidates, list):
        return entries
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        entries.append(
            {
                "text": normalize_whitespace(candidate.get("text") or ""),
                "href": normalize_whitespace(candidate.get("href") or ""),
            }
        )
    return entries


def _find_workday_apply_manual_entry(page) -> dict[str, str] | None:
    for entry in _find_apply_entries(page):
        text = (entry.get("text") or "").lower()
        href = (entry.get("href") or "").lower()
        if "apply manually" in text or "/apply/applymanually" in href:
            return entry
    return None


def _wait_for_application_state(page, platform: str, *, timeout_ms: int) -> None:
    deadline = time.monotonic() + max(timeout_ms, 0) / 1000.0
    while time.monotonic() < deadline:
        candidate_fields = _candidate_fields(page)
        if _is_dead_job_page(page) or _is_login_page(page) or (candidate_fields and not _looks_like_signup_widget(page, candidate_fields)):
            return

        body = _page_body_text(page)
        if any(hint in body for hint in APPLICATION_START_HINTS):
            return

        if platform == "workday":
            if any(
                marker in body
                for marker in (
                    "create account",
                    "password requirements",
                    "already have an account?",
                    "email address*",
                )
            ):
                return
            if "the page you are looking for doesn't exist" in body:
                return

        page.wait_for_timeout(1000)


def _click_visible_apply_entry(page, *, href: str, text: str) -> bool:
    normalized_href = normalize_whitespace(href)
    current_url = page.url or ""
    selectors = list(APPLY_ENTRY_SELECTORS)
    if normalized_href:
        selectors.insert(0, f'a[href="{normalized_href}"]')

    for selector in selectors:
        locator = page.locator(selector)
        count = min(locator.count(), 10)
        for index in range(count):
            candidate = locator.nth(index)
            try:
                if not candidate.is_visible() or not candidate.is_enabled():
                    continue
            except Exception:
                continue

            try:
                candidate_text = normalize_whitespace(candidate.inner_text() or "")
            except Exception:
                candidate_text = ""

            try:
                candidate_href = normalize_whitespace(candidate.get_attribute("href") or "")
            except Exception:
                candidate_href = ""

            if normalized_href and candidate_href and normalized_href != candidate_href:
                continue
            if text and candidate_text and "apply" not in candidate_text.lower():
                continue

            try:
                candidate.click(timeout=3500)
                page.wait_for_timeout(2200)
                if _page_changed_after_apply(page, current_url):
                    return True
            except Exception:
                continue
    return False


def _maybe_open_application_form(page) -> str | None:
    candidate_fields = _candidate_fields(page)
    if (candidate_fields and not _looks_like_signup_widget(page, candidate_fields)) or _is_login_page(page):
        return None
    if _is_dead_job_page(page):
        return None

    current_platform = _detect_platform(page.url or "")
    if current_platform == "workday":
        manual_entry = _find_workday_apply_manual_entry(page)
        if manual_entry and manual_entry.get("href"):
            manual_url = urljoin(page.url or "", manual_entry["href"])
            if manual_url != (page.url or ""):
                _safe_goto(page, manual_url, 20000)
            _wait_for_application_state(page, "workday", timeout_ms=16000)
            return f"Opened application form via Workday manual entry: {manual_entry.get('text') or 'Apply Manually'}"

    frame_apply_entry = _find_frame_apply_entry(page)
    if frame_apply_entry:
        href = frame_apply_entry.get("href") or ""
        text = frame_apply_entry.get("text") or "Apply"
        if href:
            try:
                _safe_goto(page, urljoin(page.url or "", href), 15000)
                _wait_for_application_state(page, _detect_platform(page.url or ""), timeout_ms=8000)
                if _page_changed_after_apply(page, ""):
                    return f"Opened application form via iframe apply entry: {text}"
            except Exception:
                pass

    apply_entry = _find_apply_entry(page)
    if apply_entry:
        href = apply_entry.get("href") or ""
        text = apply_entry.get("text") or "Apply"
        try:
            current_url = page.url or ""
            success = _click_visible_apply_entry(page, href=href, text=text)
            if success:
                if current_platform == "workday":
                    manual_entry = _find_workday_apply_manual_entry(page)
                    if manual_entry and manual_entry.get("href"):
                        manual_url = urljoin(page.url or current_url, manual_entry["href"])
                        if manual_url != (page.url or ""):
                            _safe_goto(page, manual_url, 20000)
                        _wait_for_application_state(page, "workday", timeout_ms=16000)
                        return f"Opened application form via Workday apply flow: {manual_entry.get('text') or text}"
                    _wait_for_application_state(page, "workday", timeout_ms=12000)
                elif current_platform == "icims":
                    _wait_for_application_state(page, "icims", timeout_ms=8000)
                return f"Opened application form via clicked apply entry: {text}"

            if href and "/talentcommunity/apply/" not in href.lower():
                target_url = urljoin(page.url or "", href)
                _safe_goto(page, target_url, 15000)
                opened_platform = _detect_platform(page.url or target_url)
                _wait_for_application_state(
                    page,
                    opened_platform,
                    timeout_ms=16000 if opened_platform == "workday" else 8000 if opened_platform == "icims" else 3000,
                )
                if opened_platform == "workday":
                    manual_entry = _find_workday_apply_manual_entry(page)
                    if manual_entry and manual_entry.get("href"):
                        manual_url = urljoin(page.url or target_url, manual_entry["href"])
                        if manual_url != (page.url or ""):
                            _safe_goto(page, manual_url, 20000)
                            _wait_for_application_state(page, "workday", timeout_ms=16000)
                        return f"Opened application form via Workday manual entry URL: {manual_entry.get('text') or text}"
                if _page_changed_after_apply(page, href):
                    return f"Opened application form via apply entry URL: {text}"
        except Exception:
            pass

    for selector in APPLY_ENTRY_SELECTORS:
        locator = page.locator(selector)
        try:
            if locator.count() == 0:
                continue
            current_url = page.url or ""
            first = locator.first
            href = normalize_whitespace(first.get_attribute("href") or "")
            label = normalize_whitespace(first.inner_text() or "") or selector
            first.click(timeout=2500)
            opened_platform = _detect_platform(page.url or current_url)
            _wait_for_application_state(
                page,
                opened_platform,
                timeout_ms=16000 if opened_platform == "workday" else 8000 if opened_platform == "icims" else 3000,
            )
            if _page_changed_after_apply(page, current_url):
                return f"Opened application form via selector: {selector}"
            if href and "/talentcommunity/apply/" not in href.lower():
                target_url = urljoin(current_url, href)
                _safe_goto(page, target_url, 15000)
                opened_platform = _detect_platform(page.url or target_url)
                _wait_for_application_state(
                    page,
                    opened_platform,
                    timeout_ms=16000 if opened_platform == "workday" else 8000 if opened_platform == "icims" else 3000,
                )
                if _page_changed_after_apply(page, current_url):
                    return f"Opened application form via selector URL: {label}"
        except Exception:
            continue
    return None


def autofill_application_pages(
    settings: Settings,
    rows: list[Row],
    *,
    dry_run: bool = False,
    submit: bool = False,
    close_pages: bool = False,
) -> list[AutofillResult]:
    planned: list[AutofillResult] = []
    page_plans: list[tuple[Row, dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        packet = _packet_payload(row)
        page_plans.append((row, packet, _field_values_from_packet(packet, settings)))

    if dry_run:
        for row, packet, values in page_plans:
            planned.append(
                AutofillResult(
                    application_id=int(row["id"]),
                    job_id=int(row["job_id"]),
                    title=row["title"],
                    company=row["company"],
                    application_url=row["job_application_url"],
                    platform=_detect_platform(row["job_application_url"] or ""),
                    filled_fields=[key for key, value in values.items() if key != "resume_path" and value not in ("", None)],
                    uploaded_files=["resume_path"] if values.get("resume_path") else [],
                    missing_required_fields=[],
                    notes=["Dry run only. No browser fields were modified."],
                    submitted=False,
                )
            )
        return planned

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    with sync_playwright() as playwright:
        _browser, context, attached_page = _connect_to_manual_chrome(playwright, settings)
        print("Application autofill mode is open.")
        print("A normal Chrome window is being used for this flow, not a Playwright-launched browser.")
        print(f"Chrome profile: {settings.flexjobs_manual_chrome_profile_dir}")
        print(f"Attached tab: {attached_page.url or 'about:blank'}")

        for row, packet, values in page_plans:
            application_url = row["job_application_url"]
            platform = _detect_platform(application_url or "")
            result = AutofillResult(
                application_id=int(row["id"]),
                job_id=int(row["job_id"]),
                title=row["title"],
                company=row["company"],
                application_url=application_url,
                platform=platform,
                filled_fields=[],
                uploaded_files=[],
                missing_required_fields=[],
                notes=[],
                submitted=False,
            )
            planned.append(result)

            if not application_url:
                result.error = "Missing application URL."
                continue

            page = context.new_page()
            try:
                page.bring_to_front()
            except Exception:
                pass

            try:
                _safe_goto(page, application_url, settings.flexjobs_timeout_ms)
                result.platform = _detect_platform(page.url or application_url)
                _wait_for_application_state(
                    page,
                    result.platform,
                    timeout_ms=6000 if result.platform == "icims" else 9000 if result.platform == "workday" else 1800,
                )
                for _ in range(4):
                    if _handle_login_gate(page, values, result):
                        break
                    candidate_fields = _candidate_fields(page)
                    if candidate_fields and not _looks_like_signup_widget(page, candidate_fields):
                        break
                    maybe_opened = _maybe_open_application_form(page)
                    if not maybe_opened:
                        break
                    result.notes.append(maybe_opened)
                    result.platform = _detect_platform(page.url or application_url)
                    _wait_for_application_state(
                        page,
                        result.platform,
                        timeout_ms=15000 if result.platform == "workday" else 8000 if result.platform == "icims" else 2000,
                    )

                if result.error:
                    continue

                if _is_dead_job_page(page):
                    result.error = f"Application page is unavailable or expired. Current URL: {page.url or application_url}"
                    continue

                fields = _discover_fields(page)
                candidate_fields = [field for field in fields if not _is_noise_field(field) and _field_key(field)]
                if not candidate_fields:
                    result.error = (
                        "Could not find recognizable candidate application fields after opening the page. "
                        f"Current URL: {page.url or application_url}"
                    )
                    continue

                used_field_keys: set[str] = set()
                for field in fields:
                    if _is_noise_field(field):
                        continue
                    key = _field_key(field)
                    if not key:
                        continue

                    value = values.get(key)
                    if value in ("", None):
                        if field.get("required"):
                            result.missing_required_fields.append(_field_descriptor(field))
                        continue

                    if key == "resume_path":
                        if _upload_file(page, int(field["index"]), value):
                            if "resume_path" not in result.uploaded_files:
                                result.uploaded_files.append("resume_path")
                            used_field_keys.add(key)
                        continue

                    tag = field.get("tag")
                    field_type = (field.get("type") or "").lower()
                    filled = False
                    if tag == "select":
                        filled = _fill_select(page, int(field["index"]), field, value)
                    elif field_type in {"checkbox", "radio"}:
                        filled = _fill_checkbox_or_radio(page, int(field["index"]), field, value)
                    else:
                        filled = _fill_text_like(page, int(field["index"]), str(value))

                    if filled:
                        if key not in result.filled_fields:
                            result.filled_fields.append(key)
                        if field_type != "radio":
                            used_field_keys.add(key)

                _fill_custom_questions(page, values, result)

                for field in fields:
                    if _is_noise_field(field):
                        continue
                    if not field.get("required"):
                        continue
                    key = _field_key(field)
                    descriptor = _field_descriptor(field)
                    if key and (key in result.filled_fields or key in result.uploaded_files):
                        continue
                    if descriptor not in result.missing_required_fields:
                        result.missing_required_fields.append(descriptor)

                result.notes.append(f"Detected platform: {result.platform}.")
                if submit:
                    if result.missing_required_fields:
                        result.error = (
                            "Required fields are still missing: "
                            + ", ".join(result.missing_required_fields[:8])
                        )
                    else:
                        submitted = _submit_application_form(page, result)
                        result.submitted = submitted
                        if not submitted:
                            result.error = "Could not confirm final submission."
                else:
                    result.notes.append("Autofill stopped before any final submit action.")
            except Exception as exc:
                result.error = str(exc)
            finally:
                if close_pages:
                    try:
                        page.close()
                    except Exception:
                        pass

    return planned


def autofill_results_as_dicts(rows: list[AutofillResult]) -> list[dict[str, object]]:
    return [
        {
            "application_id": row.application_id,
            "job_id": row.job_id,
            "title": row.title,
            "company": row.company,
            "application_url": row.application_url,
            "platform": row.platform,
            "filled_fields": row.filled_fields,
            "uploaded_files": row.uploaded_files,
            "missing_required_fields": row.missing_required_fields,
            "notes": row.notes,
            "error": row.error,
            "submitted": row.submitted,
        }
        for row in rows
    ]
