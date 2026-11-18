"""Reprise de rodada real na página "Motor de Ativações".

Só lê arquivos de rodada (sem identificador de contato, sem texto) da pasta
`MOTOR_REPRISE_DIR` e devolve a conta de `motor.reprise`. Não fala com o CRM,
não lê nem grava no banco e não envia nada. Sem a pasta configurada, a página
esconde o bloco.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path

from flask import Blueprint, jsonify, request

from motor.reprise import Parametros, carregar_rodada, ler_parametros, listar_rodadas, reprisar
from routes.motor_ativacoes import _gate

logger = logging.getLogger(__name__)
motor_reprise_bp = Blueprint("motor_reprise_bp", __name__)


def _pasta() -> str:
    pasta = (os.getenv("MOTOR_REPRISE_DIR") or "").strip()
    return pasta if pasta and Path(pasta).is_dir() else ""


@lru_cache(maxsize=4)
def _rodada(pasta: str, arquivo: str, _mtime: float) -> dict:
    return carregar_rodada(pasta, arquivo)


@motor_reprise_bp.route("/api/motor/reprise/rodadas")
def api_motor_reprise_rodadas():
    negado = _gate()
    if negado:
        return negado
    pasta = _pasta()
    p = Parametros()
    return jsonify({
        "ok": True,
        "configurada": bool(pasta),
        "local": (os.getenv("MOTOR_LOCAL") or "").strip() == "1",
        "rodadas": listar_rodadas(pasta) if pasta else [],
        "padrao": {**p.__dict__, "lotes": list(p.lotes), "inicio": f"{p.inicio_min // 60:02d}:{p.inicio_min % 60:02d}"},
    })


@motor_reprise_bp.route("/api/motor/reprise", methods=["POST"])
def api_motor_reprise():
    negado = _gate()
    if negado:
        return negado
    pasta = _pasta()
    if not pasta:
        return jsonify({"ok": False, "error": "Pasta de rodadas não configurada (MOTOR_REPRISE_DIR)."}), 404
    corpo = request.get_json(silent=True) or {}
    arquivo = str(corpo.get("arquivo") or "")
    try:
        caminho = Path(pasta) / arquivo
        rodada = _rodada(pasta, arquivo, caminho.stat().st_mtime if caminho.is_file() else 0.0)
    except (OSError, ValueError, KeyError) as e:
        logger.warning("Reprise: rodada inválida (%s)", type(e).__name__)
        return jsonify({"ok": False, "error": "Não consegui ler esta rodada."}), 400
    parametros = corpo.get("parametros") if isinstance(corpo.get("parametros"), dict) else {}
    return jsonify({"ok": True, **reprisar(rodada, ler_parametros(parametros))})
