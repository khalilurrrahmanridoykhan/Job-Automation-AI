from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT_DIR / ".env"


def load_dotenv(path: Path = ENV_PATH) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def _bool_from_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_client_cv_path(root_dir: Path, client_cv_raw: str | None) -> Path:
    raw_value = (client_cv_raw or "clientcv.pdf").strip()
    raw_path = Path(raw_value)
    if raw_path.is_absolute():
        return raw_path

    preferred_path = (root_dir / raw_path).resolve()
    if preferred_path.exists():
        return preferred_path

    pdf_candidates = sorted(root_dir.glob("*.pdf"))
    ranked_candidates = sorted(
        pdf_candidates,
        key=lambda path: (
            0 if "cv" in path.name.lower() else 1,
            0 if "resume" in path.name.lower() else 1,
            path.name.lower(),
        ),
    )
    if ranked_candidates:
        return ranked_candidates[0].resolve()

    return preferred_path


@dataclass(slots=True)
class Settings:
    root_dir: Path
    data_dir: Path
    client_cv_path: Path
    candidate_profile_path: Path
    jobs_db_path: Path
    search_titles_path: Path
    flexjobs_email: str | None
    flexjobs_password: str | None
    flexjobs_headless: bool
    flexjobs_browser: str
    flexjobs_browser_channel: str | None
    flexjobs_profile_dir: Path
    flexjobs_timeout_ms: int
    flexjobs_manual_chrome_binary: str | None
    flexjobs_manual_chrome_profile_dir: Path
    flexjobs_manual_chrome_cdp_port: int
    candidate_linkedin_url: str | None
    candidate_github_url: str | None
    candidate_middle_name: str | None
    candidate_account_login: str | None
    candidate_account_password: str | None
    candidate_phone_type: str | None
    candidate_location_city: str | None
    candidate_location_region: str | None
    candidate_postal_code: str | None
    candidate_country: str | None
    candidate_address_type: str | None
    candidate_address_line1: str | None
    candidate_address_line2: str | None
    candidate_county: str | None
    candidate_accept_terms: bool | None
    candidate_work_authorized_us: bool | None
    candidate_require_sponsorship: bool | None
    candidate_willing_to_relocate: bool | None
    candidate_salary_expectations: str | None
    candidate_start_date: str | None

    @classmethod
    def load(cls) -> "Settings":
        load_dotenv()
        root_dir = ROOT_DIR
        data_dir = root_dir / "data"

        client_cv_path = _resolve_client_cv_path(root_dir, os.getenv("CLIENT_CV_PATH"))

        profile_dir_raw = os.getenv("FLEXJOBS_PROFILE_DIR", "data/browser/flexjobs")
        flexjobs_profile_dir = (
            (root_dir / profile_dir_raw).resolve()
            if not Path(profile_dir_raw).is_absolute()
            else Path(profile_dir_raw)
        )
        manual_profile_dir_raw = os.getenv("FLEXJOBS_MANUAL_CHROME_PROFILE_DIR", "data/browser/flexjobs-manual-chrome")
        manual_chrome_profile_dir = (
            (root_dir / manual_profile_dir_raw).resolve()
            if not Path(manual_profile_dir_raw).is_absolute()
            else Path(manual_profile_dir_raw)
        )

        return cls(
            root_dir=root_dir,
            data_dir=data_dir,
            client_cv_path=client_cv_path,
            candidate_profile_path=data_dir / "candidate_profile.json",
            jobs_db_path=data_dir / "jobs.db",
            search_titles_path=data_dir / "search_titles.json",
            flexjobs_email=os.getenv("FLEXJOBS_EMAIL"),
            flexjobs_password=os.getenv("FLEXJOBS_PASSWORD"),
            flexjobs_headless=_bool_from_env("FLEXJOBS_HEADLESS", default=False),
            flexjobs_browser=os.getenv("FLEXJOBS_BROWSER", "chromium").strip().lower(),
            flexjobs_browser_channel=os.getenv("FLEXJOBS_BROWSER_CHANNEL"),
            flexjobs_profile_dir=flexjobs_profile_dir,
            flexjobs_timeout_ms=int(os.getenv("FLEXJOBS_TIMEOUT_MS", "45000")),
            flexjobs_manual_chrome_binary=os.getenv("FLEXJOBS_MANUAL_CHROME_BINARY"),
            flexjobs_manual_chrome_profile_dir=manual_chrome_profile_dir,
            flexjobs_manual_chrome_cdp_port=int(os.getenv("FLEXJOBS_MANUAL_CHROME_CDP_PORT", "9222")),
            candidate_linkedin_url=os.getenv("CANDIDATE_LINKEDIN_URL"),
            candidate_github_url=os.getenv("CANDIDATE_GITHUB_URL"),
            candidate_middle_name=os.getenv("CANDIDATE_MIDDLE_NAME"),
            candidate_account_login=os.getenv("CANDIDATE_ACCOUNT_LOGIN"),
            candidate_account_password=os.getenv("CANDIDATE_ACCOUNT_PASSWORD"),
            candidate_phone_type=os.getenv("CANDIDATE_PHONE_TYPE"),
            candidate_location_city=os.getenv("CANDIDATE_LOCATION_CITY"),
            candidate_location_region=os.getenv("CANDIDATE_LOCATION_REGION"),
            candidate_postal_code=os.getenv("CANDIDATE_POSTAL_CODE"),
            candidate_country=os.getenv("CANDIDATE_COUNTRY"),
            candidate_address_type=os.getenv("CANDIDATE_ADDRESS_TYPE"),
            candidate_address_line1=os.getenv("CANDIDATE_ADDRESS_LINE1"),
            candidate_address_line2=os.getenv("CANDIDATE_ADDRESS_LINE2"),
            candidate_county=os.getenv("CANDIDATE_COUNTY"),
            candidate_accept_terms=(
                _bool_from_env("CANDIDATE_ACCEPT_TERMS", default=False)
                if os.getenv("CANDIDATE_ACCEPT_TERMS") is not None
                else None
            ),
            candidate_work_authorized_us=(
                _bool_from_env("CANDIDATE_WORK_AUTHORIZED_US", default=False)
                if os.getenv("CANDIDATE_WORK_AUTHORIZED_US") is not None
                else None
            ),
            candidate_require_sponsorship=(
                _bool_from_env("CANDIDATE_REQUIRE_SPONSORSHIP", default=False)
                if os.getenv("CANDIDATE_REQUIRE_SPONSORSHIP") is not None
                else None
            ),
            candidate_willing_to_relocate=(
                _bool_from_env("CANDIDATE_WILLING_TO_RELOCATE", default=False)
                if os.getenv("CANDIDATE_WILLING_TO_RELOCATE") is not None
                else None
            ),
            candidate_salary_expectations=os.getenv("CANDIDATE_SALARY_EXPECTATIONS"),
            candidate_start_date=os.getenv("CANDIDATE_START_DATE"),
        )
