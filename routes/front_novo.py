"""Front novo (React + TypeScript) servido em /app/ (Fase 4, etapa 2, 27/11/2026).

O build do Vite (`frontend/dist`, gerado no Dockerfile) é servido pelo próprio
Flask, no mesmo endereço do painel. Por isso o login, o cookie de sessão e as
permissões são os mesmos, e não há CORS.

- Quem não está logado é mandado para o login pelo `auth.require_auth`, como no `/`.
- `/app/assets/*`: arquivos com hash no nome; cache de 1 ano (mudou o arquivo, muda o nome).
- Qualquer outro caminho em /app/ devolve o index.html, sem cache: as rotas são do
  React Router. O que cada tela pode chamar continua decidido em `permissoes.py`.
- Sem build (ex.: `python app.py` sem rodar `npm run build`): 503 explicando.
"""
from __future__ import annotations

from pathlib import Path

from flask import Blueprint, abort, redirect, send_from_directory

DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"
UM_ANO = 365 * 24 * 3600

front_novo_bp = Blueprint("front_novo", __name__)


def _sem_build():
    return (
        "Front novo não compilado. Rode `npm ci` e `npm run build` em frontend/.",
        503,
        {"Content-Type": "text/plain; charset=utf-8"},
    )


@front_novo_bp.route("/app")
def app_sem_barra():
    return redirect("/app/", code=301)


@front_novo_bp.route("/app/", defaults={"caminho": ""})
@front_novo_bp.route("/app/<path:caminho>")
def app_front(caminho: str):
    if caminho.startswith("assets/"):
        arquivo = caminho[len("assets/"):]
        if not (DIST / "assets" / arquivo).is_file():
            abort(404)
        resp = send_from_directory(DIST / "assets", arquivo, max_age=UM_ANO)
        resp.headers["Cache-Control"] = f"public, max-age={UM_ANO}, immutable"
        return resp
    if not (DIST / "index.html").is_file():
        return _sem_build()
    resp = send_from_directory(DIST, "index.html", max_age=0)
    resp.headers["Cache-Control"] = "no-cache"
    return resp
