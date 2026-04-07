# Flask DataHub (Fast Version)

Lightweight Flask app with:
- Public resources page
- Dedicated "Ask A Question" page
- Google sign-in/signup capture
- Admin panel gated by allowlisted Google email + username/password
- GitHub PAT uploads for files
- MySQL storage with `datahub_`-prefixed tables

## Quick Start

1. Copy `.env.example` to `.env` and fill real values.
2. Install:
   ```bash
   pip install -r requirements.txt
   ```
3. Run:
   ```bash
   python app.py
   ```
4. Open:
   - `http://127.0.0.1:5000`

## Google OAuth

- Use a **Web application** OAuth client in Google Cloud.
- Add redirect URI exactly:
  - Local: `http://127.0.0.1:5000/auth/google/callback`
  - Deployed: `https://your-domain/auth/google/callback`

## Notes

- This version is intentionally minimal for speed (no heavy async workers, no ORM, no extra framework layers).
