from __future__ import annotations

import base64
import json
import os
import secrets
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import mysql.connector  # type: ignore
import requests
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, session, url_for
import google.generativeai as genai


ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def _env(key: str, default: str = "") -> str:
    value = os.getenv(key, default)
    return value.strip() if value else ""


def _env_first(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key, "")
        if value and value.strip():
            return value.strip()
    return default


def _env_bool(key: str, default: bool = False) -> bool:
    raw = _env(key, "true" if default else "false").lower()
    return raw in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    app_name: str = _env("APP_NAME", "DataHub")
    secret_key: str = _env("APP_SESSION_SECRET", "change-me")
    app_base_url: str = _env("APP_BASE_URL", "http://127.0.0.1:5000")

    admin_username: str = _env("ADMIN_USERNAME", "john")
    admin_password: str = _env("ADMIN_PASSWORD", "jon6y.crae")
    admin_emails: str = _env("ADMIN_EMAILS")

    mysql_host: str = _env_first("MYSQL_HOST", "DB_HOST")
    mysql_port: int = int(_env_first("MYSQL_PORT", "DB_PORT", default="3306"))
    mysql_database: str = _env_first("MYSQL_DATABASE", "DB_NAME", "DB_DATABASE")
    mysql_user: str = _env_first("MYSQL_USER", "DB_USER")
    mysql_password: str = _env_first("MYSQL_PASSWORD", "DB_PASSWORD")
    mysql_ssl_ca: str = _env("MYSQL_SSL_CA")
    mysql_ssl_disabled: bool = _env_bool("MYSQL_SSL_DISABLED", default=False)

    google_client_id: str = _env("GOOGLE_CLIENT_ID")
    google_client_secret: str = _env("GOOGLE_CLIENT_SECRET")
    google_redirect_uri: str = _env("GOOGLE_REDIRECT_URI")

    use_github_upload: bool = _env_bool("USE_GITHUB_UPLOAD", default=True)
    github_token: str = _env("GITHUB_TOKEN")
    github_repo: str = _env("GITHUB_REPO")
    github_branch: str = _env("GITHUB_BRANCH", "main")
    github_upload_dir: str = _env("GITHUB_UPLOAD_DIR", "resources")
    gemini_api_key: str = _env("GEMINI_API_KEY")
    gemini_model: str = _env("GEMINI_MODEL", "gemini-2.5-flash")

    @property
    def mysql_enabled(self) -> bool:
        return all([self.mysql_host, self.mysql_database, self.mysql_user, self.mysql_password])

    @property
    def google_enabled(self) -> bool:
        return all([self.google_client_id, self.google_client_secret, self.google_redirect_uri])

    @property
    def github_enabled(self) -> bool:
        return self.use_github_upload and all([self.github_token, self.github_repo, self.github_branch])

    @property
    def admin_email_set(self) -> set[str]:
        return {item.strip().lower() for item in self.admin_emails.split(",") if item.strip()}

    @property
    def gemini_enabled(self) -> bool:
        return bool(self.gemini_api_key)


class MySQLStore:
    def __init__(self, settings: Settings) -> None:
        if not settings.mysql_enabled:
            raise RuntimeError("MySQL is not configured.")
        self.settings = settings

    def _connect(self):
        kwargs: dict[str, Any] = {
            "host": self.settings.mysql_host,
            "port": self.settings.mysql_port,
            "database": self.settings.mysql_database,
            "user": self.settings.mysql_user,
            "password": self.settings.mysql_password,
            "charset": "utf8mb4",
            "autocommit": False,
        }
        if not self.settings.mysql_ssl_disabled and self.settings.mysql_ssl_ca:
            kwargs["ssl_ca"] = self.settings.mysql_ssl_ca
        return mysql.connector.connect(**kwargs)

    def ensure_schema(self) -> None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS datahub_resources (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    description TEXT,
                    resource_type VARCHAR(16) NOT NULL,
                    category VARCHAR(120),
                    external_url TEXT,
                    file_name VARCHAR(255),
                    file_size BIGINT,
                    mime_type VARCHAR(150),
                    github_path TEXT,
                    view_url TEXT,
                    download_url TEXT,
                    created_by VARCHAR(120),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS datahub_google_signups (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    google_sub VARCHAR(100) UNIQUE,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    full_name VARCHAR(255),
                    picture_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS datahub_user_queries (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(sql, params)
            return list(cur.fetchall())
        finally:
            conn.close()

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(sql, params)
            return cur.fetchone()
        finally:
            conn.close()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
            return int(cur.lastrowid or 0)
        finally:
            conn.close()


class GitHubOps:
    def __init__(self, settings: Settings) -> None:
        if not settings.github_enabled:
            raise RuntimeError("GitHub upload is not configured.")
        self.repo = settings.github_repo
        self.branch = settings.github_branch
        self.upload_dir = settings.github_upload_dir.strip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"token {settings.github_token}", "Accept": "application/vnd.github+json"}
        )

    def _sha(self, path: str) -> str | None:
        resp = self.session.get(
            f"https://api.github.com/repos/{self.repo}/contents/{path}",
            params={"ref": self.branch},
            timeout=30,
        )
        if resp.status_code >= 300:
            return None
        data = resp.json()
        if isinstance(data, dict):
            return data.get("sha")
        return None

    def upload(self, filename: str, content: bytes) -> dict[str, str]:
        clean = "".join(ch for ch in filename if ch.isalnum() or ch in {".", "-", "_"})
        clean = clean or "resource.bin"
        repo_path = f"{self.upload_dir}/{uuid.uuid4().hex}_{clean}"
        payload: dict[str, Any] = {
            "message": f"[datahub] upload {clean}",
            "content": base64.b64encode(content).decode("utf-8"),
            "branch": self.branch,
        }
        sha = self._sha(repo_path)
        if sha:
            payload["sha"] = sha
        resp = self.session.put(
            f"https://api.github.com/repos/{self.repo}/contents/{repo_path}",
            json=payload,
            timeout=40,
        )
        if resp.status_code >= 300:
            raise RuntimeError("GitHub upload failed.")
        return {
            "repo_path": repo_path,
            "view_url": f"https://github.com/{self.repo}/blob/{self.branch}/{repo_path}",
            "download_url": f"https://raw.githubusercontent.com/{self.repo}/{self.branch}/{repo_path}",
        }

    def delete(self, repo_path: str) -> None:
        sha = self._sha(repo_path)
        if not sha:
            return
        payload = {"message": f"[datahub] delete {repo_path}", "sha": sha, "branch": self.branch}
        self.session.delete(
            f"https://api.github.com/repos/{self.repo}/contents/{repo_path}",
            json=payload,
            timeout=30,
        )


def google_auth_url(settings: Settings, state: str) -> str:
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }
    return f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"


def exchange_google_code(settings: Settings, code: str) -> dict[str, Any]:
    data = urllib.parse.urlencode(
        {
            "code": code,
            "client_id": settings.google_client_id,
            "client_secret": settings.google_client_secret,
            "redirect_uri": settings.google_redirect_uri,
            "grant_type": "authorization_code",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_google_profile(access_token: str) -> dict[str, Any]:
    req = urllib.request.Request(
        "https://openidconnect.googleapis.com/v1/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def create_app() -> Flask:
    settings = Settings()
    app = Flask(__name__)
    app.secret_key = settings.secret_key

    store: MySQLStore | None = None
    github: GitHubOps | None = None
    db_ready = False
    try:
        store = MySQLStore(settings)
        store.ensure_schema()
        db_ready = True
    except Exception:
        db_ready = False
    try:
        if settings.github_enabled:
            github = GitHubOps(settings)
    except Exception:
        github = None
    if settings.gemini_enabled:
        try:
            genai.configure(api_key=settings.gemini_api_key)
        except Exception:
            pass
    state: dict[str, Any] = {"store": store, "db_ready": db_ready}

    def get_db() -> MySQLStore | None:
        existing = state.get("store")
        if isinstance(existing, MySQLStore):
            try:
                existing.query_one("SELECT 1 AS ok")
                state["db_ready"] = True
                return existing
            except Exception:
                state["store"] = None
                state["db_ready"] = False
        try:
            fresh = MySQLStore(settings)
            fresh.ensure_schema()
            state["store"] = fresh
            state["db_ready"] = True
            return fresh
        except Exception:
            state["store"] = None
            state["db_ready"] = False
            return None

    def current_user() -> dict[str, Any] | None:
        user = session.get("google_user")
        return user if isinstance(user, dict) else None

    def can_see_admin() -> bool:
        user = current_user()
        if not user:
            return False
        email = str(user.get("email") or "").strip().lower()
        return email in settings.admin_email_set

    @app.context_processor
    def ctx():
        return {
            "app_name": settings.app_name,
            "current_user": current_user(),
            "admin_visible": can_see_admin(),
            "google_enabled": settings.google_enabled,
            "gemini_enabled": settings.gemini_enabled,
        }

    def ask_ai_model(context: str, query: str) -> str:
        if not settings.gemini_enabled:
            return "AI is currently unavailable."
        q = (query or "").strip()
        if len(q) < 3:
            return "Please ask a more specific question."
        if len(q) > 1000:
            return "Your question is too long. Keep it under 1000 characters."
        prompt = f"""You are an AI assistant helping users understand a learning resource.

STRICT RULES:
1. Answer using ONLY the context below.
2. If context is missing the answer, say: "I don't have that information in the provided context."
3. Do not invent details.
4. Keep answers concise and practical.

CONTEXT:
{context}

QUESTION:
{q}
"""
        try:
            model = genai.GenerativeModel(settings.gemini_model)
            response = model.generate_content(prompt)
            text = getattr(response, "text", "") or ""
            return text.strip() or "I could not generate a response right now."
        except Exception:
            return "I’m having trouble connecting to the AI service right now. Please try again."

    @app.post("/ask_ai_resource")
    def ask_ai_resource():
        if not settings.gemini_enabled:
            return {"error": "AI is not configured."}, 503

        # Light session rate-limiting: 20 requests per 5 minutes.
        now = time.time()
        if "ai_request_count" not in session:
            session["ai_request_count"] = 0
            session["ai_reset_at"] = now
        if now - float(session.get("ai_reset_at", now)) > 300:
            session["ai_request_count"] = 0
            session["ai_reset_at"] = now
        if int(session.get("ai_request_count", 0)) >= 20:
            return {"error": "Rate limit exceeded. Please wait a few minutes and try again."}, 429
        session["ai_request_count"] = int(session.get("ai_request_count", 0)) + 1

        db = get_db()
        if not db:
            return {"error": "Database unavailable."}, 503
        payload = request.get_json(silent=True) or {}
        resource_id = int(payload.get("resource_id") or 0)
        query = str(payload.get("query") or "").strip()
        if not resource_id or not query:
            return {"error": "Missing resource_id or query."}, 400
        resource = db.query_one("SELECT * FROM datahub_resources WHERE id=%s", (resource_id,))
        if not resource:
            return {"error": "Resource not found."}, 404

        context = (
            f"Title: {resource.get('title','')}\n"
            f"Type: {resource.get('resource_type','')}\n"
            f"Category: {resource.get('category','')}\n"
            f"Description: {resource.get('description','')}\n"
            f"Link: {resource.get('external_url') or resource.get('view_url') or ''}\n"
        )
        answer = ask_ai_model(context, query)
        return {"response": answer}

    @app.get("/")
    def index():
        q = request.args.get("q", "").strip()
        rtype = request.args.get("type", "").strip().lower()
        resources: list[dict[str, Any]] = []
        db = get_db()
        if db:
            where_parts: list[str] = []
            params: list[Any] = []
            if q:
                where_parts.append("(title LIKE %s OR description LIKE %s OR category LIKE %s)")
                like = f"%{q}%"
                params.extend([like, like, like])
            if rtype in {"file", "link"}:
                where_parts.append("resource_type=%s")
                params.append(rtype)
            where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
            resources = db.query_all(
                f"SELECT * FROM datahub_resources {where_sql} ORDER BY created_at DESC",
                tuple(params),
            )
        return render_template(
            "index.html",
            resources=resources,
            google_enabled=settings.google_enabled,
            search_query=q,
            search_type=rtype or "all",
        )

    @app.get("/questions")
    def questions_page():
        return render_template("questions.html")

    @app.post("/questions")
    def questions_submit():
        db = get_db()
        if not db:
            flash("Messaging is temporarily unavailable.", "danger")
            return redirect(url_for("questions_page"))
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        message = request.form.get("message", "").strip()
        if not (name and email and message):
            flash("Please fill in all fields.", "warning")
            return redirect(url_for("questions_page"))
        db.execute(
            "INSERT INTO datahub_user_queries (name, email, message) VALUES (%s, %s, %s)",
            (name, email, message),
        )
        flash("Message received.", "success")
        return redirect(url_for("questions_page"))

    @app.get("/login/google")
    def login_google():
        if not settings.google_enabled:
            flash("Google sign-in is not configured.", "warning")
            return redirect(url_for("index"))
        state = secrets.token_urlsafe(24)
        session["oauth_state"] = state
        return redirect(google_auth_url(settings, state))

    @app.get("/auth/google/callback")
    def google_callback():
        if not settings.google_enabled:
            flash("Google sign-in is not configured.", "warning")
            return redirect(url_for("index"))
        code = request.args.get("code", "").strip()
        state = request.args.get("state", "").strip()
        if not code or not state or state != session.get("oauth_state"):
            flash("Google sign-in failed. Please try again.", "danger")
            return redirect(url_for("index"))
        try:
            token = exchange_google_code(settings, code)
            profile = fetch_google_profile(token.get("access_token", ""))
            session["google_user"] = profile
            session.pop("oauth_state", None)
            db = get_db()
            if db:
                google_sub = profile.get("sub")
                email = profile.get("email")
                full_name = profile.get("name") or "Learner"
                picture_url = profile.get("picture")
                if google_sub and email:
                    existing = db.query_one("SELECT id FROM datahub_google_signups WHERE email=%s", (email,))
                    if existing:
                        db.execute(
                            "UPDATE datahub_google_signups SET google_sub=%s, full_name=%s, picture_url=%s WHERE id=%s",
                            (google_sub, full_name, picture_url, existing["id"]),
                        )
                    else:
                        db.execute(
                            "INSERT INTO datahub_google_signups (google_sub, email, full_name, picture_url) VALUES (%s, %s, %s, %s)",
                            (google_sub, email, full_name, picture_url),
                        )
            flash("Signed in with Google.", "success")
        except Exception:
            flash("Google sign-in failed. Please try again.", "danger")
        return redirect(url_for("index"))

    @app.get("/logout")
    def logout():
        session.pop("google_user", None)
        session.pop("admin_ok", None)
        session.pop("oauth_state", None)
        flash("You have signed out.", "success")
        return redirect(url_for("index"))

    @app.get("/admin/login")
    def admin_login():
        if not can_see_admin():
            flash("Admin access requires an allowlisted Google account.", "warning")
            return redirect(url_for("index"))
        return render_template("admin_login.html")

    @app.post("/admin/login")
    def admin_login_submit():
        if not can_see_admin():
            flash("Admin access requires an allowlisted Google account.", "warning")
            return redirect(url_for("index"))
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if username == settings.admin_username and password == settings.admin_password:
            session["admin_ok"] = True
            flash("Admin access granted.", "success")
            return redirect(url_for("admin_panel"))
        flash("Invalid credentials.", "danger")
        return redirect(url_for("admin_login"))

    @app.get("/admin")
    def admin_panel():
        if not can_see_admin() or not session.get("admin_ok"):
            return redirect(url_for("admin_login"))
        resources: list[dict[str, Any]] = []
        messages: list[dict[str, Any]] = []
        signups: list[dict[str, Any]] = []
        db = get_db()
        if db:
            resources = db.query_all("SELECT * FROM datahub_resources ORDER BY created_at DESC")
            messages = db.query_all("SELECT * FROM datahub_user_queries ORDER BY created_at DESC LIMIT 200")
            signups = db.query_all("SELECT * FROM datahub_google_signups ORDER BY created_at DESC LIMIT 200")
        return render_template("admin.html", resources=resources, messages=messages, signups=signups, github_enabled=bool(github))

    @app.post("/admin/resource/link")
    def admin_add_link():
        if not can_see_admin() or not session.get("admin_ok"):
            return redirect(url_for("admin_login"))
        db = get_db()
        if not db:
            flash("Database unavailable.", "danger")
            return redirect(url_for("admin_panel"))
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        category = request.form.get("category", "").strip() or "General"
        link_url = request.form.get("url", "").strip()
        if not (title and link_url):
            flash("Title and URL are required.", "warning")
            return redirect(url_for("admin_panel"))
        db.execute(
            """
            INSERT INTO datahub_resources
            (title, description, resource_type, category, external_url, view_url, download_url, created_by)
            VALUES (%s, %s, 'link', %s, %s, %s, %s, %s)
            """,
            (title, description, category, link_url, link_url, link_url, settings.admin_username),
        )
        flash("Link published.", "success")
        return redirect(url_for("admin_panel"))

    @app.post("/admin/resource/file")
    def admin_upload_file():
        if not can_see_admin() or not session.get("admin_ok"):
            return redirect(url_for("admin_login"))
        db = get_db()
        if not db:
            flash("Database unavailable.", "danger")
            return redirect(url_for("admin_panel"))
        if not github:
            flash("GitHub uploads are not configured.", "danger")
            return redirect(url_for("admin_panel"))
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        category = request.form.get("category", "").strip() or "General"
        file_obj = request.files.get("file")
        if not title or not file_obj or not file_obj.filename:
            flash("Title and file are required.", "warning")
            return redirect(url_for("admin_panel"))
        try:
            payload = file_obj.read()
            result = github.upload(file_obj.filename, payload)
            db.execute(
                """
                INSERT INTO datahub_resources
                (title, description, resource_type, category, file_name, file_size, mime_type, github_path, view_url, download_url, created_by)
                VALUES (%s, %s, 'file', %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    title,
                    description,
                    category,
                    file_obj.filename,
                    len(payload),
                    file_obj.mimetype or "application/octet-stream",
                    result["repo_path"],
                    result["view_url"],
                    result["download_url"],
                    settings.admin_username,
                ),
            )
            flash("File uploaded.", "success")
        except Exception:
            flash("Upload failed.", "danger")
        return redirect(url_for("admin_panel"))

    @app.post("/admin/resource/delete/<int:resource_id>")
    def admin_delete_resource(resource_id: int):
        if not can_see_admin() or not session.get("admin_ok"):
            return redirect(url_for("admin_login"))
        db = get_db()
        if not db:
            flash("Database unavailable.", "danger")
            return redirect(url_for("admin_panel"))
        row = db.query_one("SELECT github_path FROM datahub_resources WHERE id=%s", (resource_id,))
        if row and row.get("github_path") and github:
            github.delete(str(row["github_path"]))
        db.execute("DELETE FROM datahub_resources WHERE id=%s", (resource_id,))
        flash("Resource deleted.", "success")
        return redirect(url_for("admin_panel"))

    @app.get("/admin/resource/edit/<int:resource_id>")
    def admin_edit_resource(resource_id: int):
        if not can_see_admin() or not session.get("admin_ok"):
            return redirect(url_for("admin_login"))
        db = get_db()
        if not db:
            flash("Database unavailable.", "danger")
            return redirect(url_for("admin_panel"))
        resource = db.query_one("SELECT * FROM datahub_resources WHERE id=%s", (resource_id,))
        if not resource:
            flash("Resource not found.", "warning")
            return redirect(url_for("admin_panel"))
        return render_template("admin_edit.html", resource=resource, github_enabled=bool(github))

    @app.post("/admin/resource/edit/<int:resource_id>")
    def admin_edit_resource_submit(resource_id: int):
        if not can_see_admin() or not session.get("admin_ok"):
            return redirect(url_for("admin_login"))
        db = get_db()
        if not db:
            flash("Database unavailable.", "danger")
            return redirect(url_for("admin_panel"))
        resource = db.query_one("SELECT * FROM datahub_resources WHERE id=%s", (resource_id,))
        if not resource:
            flash("Resource not found.", "warning")
            return redirect(url_for("admin_panel"))

        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        category = request.form.get("category", "").strip() or "General"
        if not title:
            flash("Title is required.", "warning")
            return redirect(url_for("admin_edit_resource", resource_id=resource_id))

        resource_type = (resource.get("resource_type") or "").lower()
        if resource_type == "link":
            link_url = request.form.get("url", "").strip()
            if not link_url:
                flash("URL is required for link resources.", "warning")
                return redirect(url_for("admin_edit_resource", resource_id=resource_id))
            db.execute(
                """
                UPDATE datahub_resources
                SET title=%s, description=%s, category=%s, external_url=%s, view_url=%s, download_url=%s
                WHERE id=%s
                """,
                (title, description, category, link_url, link_url, link_url, resource_id),
            )
            flash("Link updated.", "success")
            return redirect(url_for("admin_panel"))

        # File resources: update metadata and optionally replace file.
        file_obj = request.files.get("file")
        if file_obj and file_obj.filename:
            if not github:
                flash("GitHub uploads are not configured; cannot replace file.", "danger")
                return redirect(url_for("admin_edit_resource", resource_id=resource_id))
            try:
                payload = file_obj.read()
                uploaded = github.upload(file_obj.filename, payload)
                old_path = str(resource.get("github_path") or "").strip()
                if old_path:
                    github.delete(old_path)
                db.execute(
                    """
                    UPDATE datahub_resources
                    SET title=%s, description=%s, category=%s, file_name=%s, file_size=%s, mime_type=%s,
                        github_path=%s, view_url=%s, download_url=%s
                    WHERE id=%s
                    """,
                    (
                        title,
                        description,
                        category,
                        file_obj.filename,
                        len(payload),
                        file_obj.mimetype or "application/octet-stream",
                        uploaded["repo_path"],
                        uploaded["view_url"],
                        uploaded["download_url"],
                        resource_id,
                    ),
                )
                flash("File resource updated (file replaced).", "success")
                return redirect(url_for("admin_panel"))
            except Exception:
                flash("Failed to replace file.", "danger")
                return redirect(url_for("admin_edit_resource", resource_id=resource_id))

        db.execute(
            "UPDATE datahub_resources SET title=%s, description=%s, category=%s WHERE id=%s",
            (title, description, category, resource_id),
        )
        flash("File resource metadata updated.", "success")
        return redirect(url_for("admin_panel"))

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
