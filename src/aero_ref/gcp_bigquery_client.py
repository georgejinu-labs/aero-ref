"""
Build a BigQuery client using local Application Default Credentials.

Supports, in order (same as google.auth.default):
  - GOOGLE_APPLICATION_CREDENTIALS → service account JSON (set in .env)
  - File from: gcloud auth application-default login
    (Windows: %APPDATA%\\gcloud\\application_default_credentials.json)

BIGQUERY_PROJECT is the project used for BigQuery jobs and data (Client.project).

Quota / API consumption attribution (credentials quota project):
  - Default: same as BIGQUERY_PROJECT.
  - BIGQUERY_QUOTA_PROJECT: use another project ID if your org bills quota there (you must
    have roles/serviceusage.serviceUsageConsumer on that project).
  - BIGQUERY_QUOTA_PROJECT=none: omit quota_project_id (rare; may change error shape).

If you see 403 USER_PROJECT_DENIED / serviceusage.services.use:
  Your user (or service account) needs role **Service Usage Consumer** on the project that
  is charged for API use (usually BIGQUERY_PROJECT), or use a service account key that
  already has it. In Console: IAM & Admin → IAM → your principal → Grant Access →
  "Service Usage Consumer" (roles/serviceusage.serviceUsageConsumer).

CLI (sets quota project inside ADC metadata):
  gcloud auth application-default set-quota-project PROJECT_ID
"""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

from google.auth import default as google_auth_default
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import Request
from google.cloud import bigquery

# Scope required for BigQuery jobs (not only cloud-platform).
_BIGQUERY_SCOPES = ("https://www.googleapis.com/auth/bigquery",)


def _quota_project_for_credentials(data_project: str) -> str | None:
    """
    Project ID sent with credentials for API quota/billing attribution.

    Unset BIGQUERY_QUOTA_PROJECT → use data project.
    BIGQUERY_QUOTA_PROJECT=none|false|0|- → omit (pass None to google.auth.default).
    Otherwise → use that project id string.
    """
    raw = (os.getenv("BIGQUERY_QUOTA_PROJECT") or "").strip()
    if not raw:
        return data_project
    if raw.lower() in ("none", "false", "0", "-"):
        return None
    return raw


def _gcloud_application_default_path() -> Path | None:
    """Path gcloud writes for `gcloud auth application-default login`."""
    if os.name == "nt":
        appdata = os.getenv("APPDATA")
        if not appdata:
            return None
        p = Path(appdata) / "gcloud" / "application_default_credentials.json"
        return p if p.is_file() else None
    p = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    return p if p.is_file() else None


def _ensure_gcloud_adc_visible() -> None:
    """
    If GOOGLE_APPLICATION_CREDENTIALS is unset but gcloud's ADC file exists, set the env var.

    Some environments resolve credentials more reliably when the path is explicit.
    """
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    path = _gcloud_application_default_path()
    if path is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(path)


def make_bigquery_client(project_id: str) -> bigquery.Client:
    """
    Create a client using ADC with BigQuery scopes.

    Quota project for credentials defaults to project_id; override with BIGQUERY_QUOTA_PROJECT.

    Refreshes credentials once so auth errors surface before a long-running query.
    """
    _ensure_gcloud_adc_visible()
    quota = _quota_project_for_credentials(project_id)
    try:
        credentials, _ = google_auth_default(
            scopes=_BIGQUERY_SCOPES,
            quota_project_id=quota,
        )
    except DefaultCredentialsError as e:
        hint = (
            "Could not load Application Default Credentials.\n"
            "  • Run: gcloud auth application-default login\n"
            "  • Or set GOOGLE_APPLICATION_CREDENTIALS in .env to a service account JSON path.\n"
        )
        adc = _gcloud_application_default_path()
        if adc:
            hint += f"  • Found gcloud ADC file at {adc} but it was not accepted; try re-running application-default login.\n"
        else:
            hint += "  • No gcloud ADC file found (expected after application-default login).\n"
        raise RuntimeError(hint) from e

    try:
        credentials.refresh(Request())
    except Exception as e:
        raise RuntimeError(
            "Credentials loaded but refresh failed (expired or revoked token?). "
            "Run: gcloud auth application-default login"
        ) from e

    return bigquery.Client(credentials=credentials, project=project_id)


_client_lock = threading.Lock()
_cached_project_id: str | None = None
_cached_client: bigquery.Client | None = None


def get_cached_bigquery_client(project_id: str) -> bigquery.Client:
    """
    Return a process-wide BigQuery client for project_id.

    Builds once per MCP server process (avoids repeated ADC refresh + client setup on every tool call).
    """
    global _cached_project_id, _cached_client
    with _client_lock:
        if _cached_client is not None and _cached_project_id == project_id:
            return _cached_client
        print(
            "[bigquery-mcp] creating BigQuery client (first use this session; later calls reuse it)",
            file=sys.stderr,
            flush=True,
        )
        log_credential_source(project_id)
        _cached_client = make_bigquery_client(project_id)
        _cached_project_id = project_id
        return _cached_client


def log_credential_source(data_project: str) -> None:
    """Write to stderr which credential path and quota project are in use (for MCP debugging)."""
    qp = _quota_project_for_credentials(data_project)
    print(
        f"[bigquery-mcp] quota_project_id (credentials)={qp!r}, bigquery Client.project={data_project!r}",
        file=sys.stderr,
        flush=True,
    )
    sa = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if sa and Path(sa).is_file():
        print(f"[bigquery-mcp] using GOOGLE_APPLICATION_CREDENTIALS={sa!r}", file=sys.stderr, flush=True)
        return
    g = _gcloud_application_default_path()
    if g:
        print(f"[bigquery-mcp] using gcloud ADC file {g}", file=sys.stderr, flush=True)
    else:
        print(
            "[bigquery-mcp] no explicit credential file; using google.auth.default() discovery",
            file=sys.stderr,
            flush=True,
        )
