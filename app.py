"""
eduit. — Gestão Acadêmica (Flask).

Uso:
    python app.py
    Acesse http://localhost:5001
"""

import sys, os, io, warnings

warnings.filterwarnings("ignore", message=".*collation.*")

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        _orig = getattr(sys, _s)
        if hasattr(_orig, "buffer"):
            try:
                setattr(sys, _s, io.TextIOWrapper(_orig.buffer, encoding="utf-8", errors="replace", line_buffering=True))
            except Exception:
                pass

import time
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask

load_dotenv(Path(__file__).parent / ".env")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dcz-sync-default-key-change-me")
app.config["CACHE_BUST"] = str(int(time.time()))

# ── Registrar Blueprints ──────────────────────────────────────────────────

from routes.auth import auth_bp
from routes.dashboard import dashboard_bp
from routes.crm import crm_bp
from routes.upload import upload_bp
from routes.engagement import engagement_bp, register_engagement_job
from routes.config import config_bp, init_scheduler, _load_schedules_from_db
from routes.logs import logs_bp
from routes.kommo_sync import kommo_bp
from routes.match_merge import match_merge_bp
from routes.comercial_rgm import comercial_rgm_bp
from routes.ativacoes import ativacoes_bp
from routes.avisos import avisos_bp
from routes.kommo_merge_route import kommo_merge_bp
from routes.kommo_dispatcher import kommo_dispatcher_bp

app.register_blueprint(auth_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(crm_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(engagement_bp)
app.register_blueprint(config_bp)
app.register_blueprint(logs_bp)
app.register_blueprint(kommo_bp)
app.register_blueprint(match_merge_bp)
app.register_blueprint(comercial_rgm_bp)
app.register_blueprint(ativacoes_bp)
app.register_blueprint(avisos_bp)
app.register_blueprint(kommo_merge_bp)
app.register_blueprint(kommo_dispatcher_bp)

# ── Inicialização do banco ────────────────────────────────────────────────

from db import (
    _ensure_schedules_table,
    _ensure_turmas_table,
    _ensure_ciclos_table,
    _ensure_users_table,
    _ensure_xl_snapshots_table,
    _ensure_engagement_tables,
    _ensure_avisos_tables,
)

_ensure_schedules_table()
_ensure_turmas_table()
_ensure_ciclos_table()
_ensure_users_table()
_ensure_xl_snapshots_table()
_ensure_engagement_tables()
_ensure_avisos_tables()

# ── APScheduler ───────────────────────────────────────────────────────────

from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
init_scheduler(scheduler)
scheduler.start()
_load_schedules_from_db()
register_engagement_job(scheduler)

# ── Entrypoint ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001, threaded=True, use_reloader=False)
