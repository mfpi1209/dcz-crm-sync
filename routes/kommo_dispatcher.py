"""
Proxy para API do Kommo Dispatcher (projeto Kommo_chat).

Endpoints:
  GET /api/kommo-dispatcher/stats   proxy para /api/kommo/dashboard/stats
"""

import os
import logging
import requests
from flask import Blueprint, jsonify, session as flask_session

log = logging.getLogger("kommo_dispatcher")

kommo_dispatcher_bp = Blueprint("kommo_dispatcher", __name__)

KOMMO_CHAT_URL = os.getenv("KOMMO_CHAT_URL", "http://banco-kommo-dispatcher:8000").rstrip("/")


@kommo_dispatcher_bp.route("/api/kommo-dispatcher/stats", methods=["GET"])
def proxy_stats():
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    try:
        r = requests.get(f"{KOMMO_CHAT_URL}/api/kommo/dashboard/stats", timeout=10)
        r.raise_for_status()
        return jsonify(r.json())
    except requests.exceptions.Timeout:
        return jsonify({"error": "Timeout ao conectar com Kommo Dispatcher"}), 504
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Kommo Dispatcher indisponível"}), 503
    except Exception as e:
        log.warning("Erro proxy stats: %s", e)
        return jsonify({"error": str(e)}), 502
