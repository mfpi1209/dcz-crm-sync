"""
Proxy para o webhook N8N de Ativações Acadêmicas.
Evita problemas de CORS e certificado SSL no frontend.
"""

import os
import requests
from flask import Blueprint, request, jsonify

ativacoes_bp = Blueprint("ativacoes", __name__)

_N8N_URL = os.getenv(
    "ATIVACOES_WEBHOOK_URL",
    "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/dashboard",
)


@ativacoes_bp.route("/api/ativacoes/dados")
def proxy_ativacoes():
    params = {}
    if request.args.get("data_inicio"):
        params["data_inicio"] = request.args["data_inicio"]
    if request.args.get("data_fim"):
        params["data_fim"] = request.args["data_fim"]

    try:
        r = requests.get(_N8N_URL, params=params, timeout=30, verify=False)
        r.raise_for_status()
        return jsonify(r.json()), 200
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502
