"""FastAPI application for ``undatum web``.

This module imports FastAPI. Call ``require_web_dependencies`` first.
It must not import Textual screens.
"""

from __future__ import annotations

import secrets
import shutil
import sys
import tempfile
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from ..common.errors import UndatumError
from ..tui.actions import DEFAULT_SQL_LIMIT, TUI_ACTIONS, render_cli
from ..tui.history import load_recent_paths, record_recent_path
from ..tui.services import TuiServices, clamp_sample_limit
from ..tui.session import SessionState

PACKAGE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE_DIR / "templates"
STATIC_DIR = PACKAGE_DIR / "static"
CSRF_COOKIE = "undatum_web_csrf"
API_KEY_COOKIE = "undatum_api_key"
MAX_UPLOAD_BYTES = 512 * 1024 * 1024
LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
DEFAULT_SQL = f"SELECT * FROM data LIMIT {DEFAULT_SQL_LIMIT}"


@dataclass
class ResultTable:
    """Last action result shown below the sample grid."""

    title: str
    headers: list[str]
    rows: list[list[str]]
    cli: str


@dataclass
class WebAppState:
    """One ephemeral browser session (not the Data API)."""

    options: dict[str, Any] = field(default_factory=dict)
    limit: int = 200
    work_dir: Path | None = None
    cleanup_work_dir: bool = False
    csrf_token: str = ""
    api_key: str | None = None
    history_file: Path | None = None
    session: SessionState | None = None
    result: ResultTable | None = None
    error: str | None = None
    notice: str | None = None
    job_lock: Lock = field(default_factory=Lock)
    busy: str | None = None


def remote_bind_warning(host: str) -> str | None:
    """Return a warning when the bind host is not loopback."""
    if host not in LOCAL_HOSTS:
        return (
            "Warning: undatum web is a local session tool, not a hardened public app. "
            f"Binding to {host} exposes the current dataset session."
        )
    return None


def _templates() -> Jinja2Templates:
    return Jinja2Templates(directory=str(TEMPLATES_DIR))


def _safe_upload_name(name: str | None) -> str:
    raw = Path(name or "upload").name
    if not raw or raw in {".", ".."}:
        return "upload"
    return raw.replace("\x00", "")


class _SecurityMiddleware(BaseHTTPMiddleware):
    """API-key gate and CSRF cookie."""

    def __init__(self, app, web: WebAppState) -> None:
        super().__init__(app)
        self.web = web

    async def dispatch(self, request: Request, call_next):
        web = self.web
        if request.url.path == "/healthz" or request.url.path.startswith("/static"):
            return await call_next(request)
        if web.api_key:
            provided = (
                request.headers.get("x-api-key")
                or request.cookies.get(API_KEY_COOKIE)
                or request.query_params.get("api_key")
            )
            if provided != web.api_key:
                return HTMLResponse("Unauthorized. Pass X-API-Key or ?api_key=.", status_code=401)
        response = await call_next(request)
        response.set_cookie(CSRF_COOKIE, web.csrf_token, httponly=True, samesite="strict", path="/")
        if web.api_key and request.query_params.get("api_key") == web.api_key:
            response.set_cookie(
                API_KEY_COOKIE, web.api_key, httponly=True, samesite="strict", path="/"
            )
        return response


def create_app(
    path: str | None = None,
    options: dict[str, Any] | None = None,
    limit: int | None = None,
    api_key: str | None = None,
    history_file: Path | None = None,
    work_dir: Path | None = None,
    cleanup_work_dir: bool = False,
) -> FastAPI:
    """Build the local web session app."""
    web = WebAppState(
        options=dict(options or {}),
        limit=clamp_sample_limit(limit),
        work_dir=work_dir,
        cleanup_work_dir=cleanup_work_dir,
        csrf_token=secrets.token_urlsafe(32),
        api_key=api_key or None,
        history_file=history_file,
    )
    if path:
        try:
            web.session = TuiServices().load_sample(path, web.options, web.limit)
            record_recent_path(path, web.history_file)
        except Exception as exc:
            web.error = str(exc)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if web.cleanup_work_dir and web.work_dir is not None:
            shutil.rmtree(web.work_dir, ignore_errors=True)

    app = FastAPI(
        title="undatum web",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.web = web
    app.add_middleware(_SecurityMiddleware, web=web)
    templates = _templates()
    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    def _check_csrf(request: Request, token: str) -> None:
        cookie = request.cookies.get(CSRF_COOKIE)
        if not token or token != web.csrf_token or cookie != web.csrf_token:
            raise HTTPException(status_code=403, detail="CSRF check failed")

    @contextmanager
    def _job(name: str):
        acquired = web.job_lock.acquire(blocking=False)
        if not acquired:
            web.error = f"Busy: {web.busy} is still running. Wait and retry."
            yield False
            return
        web.busy = name
        try:
            yield True
        finally:
            web.busy = None
            web.job_lock.release()

    def _redirect() -> RedirectResponse:
        return RedirectResponse("/", status_code=303)

    def _context(request: Request) -> dict[str, Any]:
        session = web.session
        grid: list[list[str]] = []
        if session is not None:
            grid = TuiServices().grid_rows(session)
        error, notice = web.error, web.notice
        web.error = None
        web.notice = None
        actions = []
        if session is not None:
            field = session.headers[0] if session.headers else "FIELD"
            actions = [
                {"title": action.title, "cli": render_cli(action, session, field)}
                for action in TUI_ACTIONS
                if action.id not in {"help", "open"}
            ]
        return {
            "request": request,
            "csrf_token": web.csrf_token,
            "session": session,
            "grid": grid,
            "result": web.result,
            "error": error,
            "notice": notice,
            "recent": load_recent_paths(web.history_file),
            "actions": actions,
            "default_sql": DEFAULT_SQL,
            "sample_limit": web.limit,
        }

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"ok": True})

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "explore.html", _context(request))

    def _require_session() -> SessionState | None:
        if web.session is None:
            web.error = "Open a dataset first"
            return None
        return web.session

    @app.post("/open")
    def open_path(
        request: Request,
        csrf_token: str = Form(...),
        path: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("open") as ok:
            if not ok:
                return _redirect()
            cleaned = path.strip()
            if not cleaned:
                web.error = "Path is required"
                return _redirect()
            try:
                web.session = TuiServices().load_sample(cleaned, web.options, web.limit)
                record_recent_path(cleaned, web.history_file)
                web.result = None
                web.notice = f"Opened {cleaned}"
            except Exception as exc:
                web.error = str(exc)
        return _redirect()

    @app.post("/upload")
    async def upload(
        request: Request,
        csrf_token: str = Form(...),
        file: UploadFile = File(...),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("upload") as ok:
            if not ok:
                return _redirect()
            if web.work_dir is None:
                web.error = "Upload is not available (no working directory)"
                return _redirect()
            name = _safe_upload_name(file.filename)
            dest = web.work_dir / name
            written = 0
            try:
                web.work_dir.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as handle:
                    while True:
                        chunk = await file.read(1024 * 1024)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > MAX_UPLOAD_BYTES:
                            handle.close()
                            dest.unlink(missing_ok=True)
                            web.error = (
                                f"Upload exceeds {MAX_UPLOAD_BYTES} bytes; open a path instead"
                            )
                            return _redirect()
                        handle.write(chunk)
                web.session = TuiServices().load_sample(str(dest), web.options, web.limit)
                record_recent_path(str(dest), web.history_file)
                web.result = None
                web.notice = f"Uploaded {name} ({written} bytes)"
            except Exception as exc:
                web.error = str(exc)
        return _redirect()

    @app.post("/filter")
    def filter_sample(
        request: Request,
        csrf_token: str = Form(...),
        expr: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("filter") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    TuiServices().apply_filter(session, expr)
                    web.notice = "Filter updated" if session.filter_expr else "Filter cleared"
                except UndatumError as exc:
                    web.error = str(exc)
        return _redirect()

    @app.post("/profile")
    def profile(request: Request, csrf_token: str = Form(...)) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("profile") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    headers, rows, cli = TuiServices().profile(session)
                    web.result = ResultTable("Profile", headers, rows, cli)
                except Exception as exc:
                    web.error = str(exc)
        return _redirect()

    @app.post("/frequency")
    def frequency(
        request: Request,
        csrf_token: str = Form(...),
        field: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("frequency") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    headers, rows, cli = TuiServices().frequency(session, field.strip())
                    web.result = ResultTable(f"Frequency · {field.strip()}", headers, rows, cli)
                except Exception as extra:
                    web.error = str(extra)
        return _redirect()

    @app.post("/sql")
    def sql(
        request: Request,
        csrf_token: str = Form(...),
        query: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("sql") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    headers, rows, cli = TuiServices().run_sql(session, query)
                    web.result = ResultTable("SQL", headers, rows, cli)
                except Exception as extra:
                    web.error = str(extra)
        return _redirect()

    @app.post("/export")
    def export_view(
        request: Request,
        csrf_token: str = Form(...),
        path: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("export") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    cli = TuiServices().export_view(session, path.strip())
                    web.notice = f"Wrote {path.strip()}"
                    session.last_cli = cli
                except Exception as extra:
                    web.error = str(extra)
        return _redirect()

    @app.post("/convert")
    def convert_save(
        request: Request,
        csrf_token: str = Form(...),
        path: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("convert") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    cli = TuiServices().convert_save(session, path.strip())
                    web.notice = f"Wrote {path.strip()}"
                    session.last_cli = cli
                except Exception as extra:
                    web.error = str(extra)
        return _redirect()

    @app.post("/validate")
    def validate_sample(
        request: Request,
        csrf_token: str = Form(...),
        rules: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("validate") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    headers, rows, cli = TuiServices().validate_sample(session, rules)
                    web.result = ResultTable("Validate sample", headers, rows, cli)
                except Exception as extra:
                    web.error = str(extra)
        return _redirect()

    @app.post("/mask")
    def mask_preview(
        request: Request,
        csrf_token: str = Form(...),
        fields: str = Form(""),
        output: str = Form(""),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("mask") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    dest = output.strip()
                    if dest:
                        cli = TuiServices().mask_write(session, dest, fields)
                        web.notice = f"Wrote {dest}"
                        session.last_cli = cli
                    else:
                        headers, rows, cli = TuiServices().mask_preview(session, fields)
                        web.result = ResultTable("Mask preview (sample)", headers, rows, cli)
                except Exception as extra:
                    web.error = str(extra)
        return _redirect()

    @app.post("/pipeline")
    def export_pipeline(
        request: Request,
        csrf_token: str = Form(...),
        path: str = Form("pipeline.yml"),
    ) -> RedirectResponse:
        _check_csrf(request, csrf_token)
        with _job("pipeline") as ok:
            session = _require_session() if ok else None
            if session is not None:
                try:
                    cli = TuiServices().export_pipeline(session, path.strip())
                    web.notice = f"Wrote {path.strip()}"
                    session.last_cli = cli
                except Exception as extra:
                    web.error = str(extra)
        return _redirect()

    return app


def run_web(
    path: str | None,
    options: dict[str, Any] | None = None,
    limit: int | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = False,
    api_key: str | None = None,
) -> None:
    """Start uvicorn for the local web session."""
    import uvicorn

    work_dir = Path(tempfile.mkdtemp(prefix="undatum-web-"))
    warning = remote_bind_warning(host)
    if warning:
        print(warning, file=sys.stderr)
    app = create_app(
        path=path,
        options=options,
        limit=limit,
        api_key=api_key,
        work_dir=work_dir,
        cleanup_work_dir=True,
    )
    url = f"http://{host}:{port}/"
    print(f"undatum web listening on {url}  (sample limit {clamp_sample_limit(limit)})")
    if open_browser:
        import webbrowser

        webbrowser.open(url)
    uvicorn.run(app, host=host, port=port, log_level="info")
