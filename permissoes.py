"""Permissão por página conferida no servidor (Fase 1 da modernização).

Até aqui a tabela `user_permissions` só escondia itens do menu: qualquer usuário
logado chamava direto as APIs de escrita (preços, agendamentos, upload de
matriculados...). `exige_pagina` faz o servidor conferir a mesma tabela, com a
mesma regra que `routes/bwipo_precos.py` já usava:

- admin (papel `admin`) e o login de emergência (`user_id` 0) passam sempre;
- os demais passam se tiverem QUALQUER uma das páginas indicadas.

Cada rota recebe as páginas cujas telas a chamam (mapeado no front em 25/11/2026),
para nenhum usuário perder algo que já usa pela tela.
"""
from __future__ import annotations

from functools import wraps

from flask import jsonify, session

from db import get_conn

SEM_PERMISSAO = "Sem permissão para esta ação."


def tem_alguma_pagina(*paginas: str) -> bool:
    if not session.get("authenticated"):
        return False
    if (session.get("role") or "") == "admin" or session.get("user_id", 0) == 0:
        return True
    uid = session.get("user_id")
    if not uid or not paginas:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = ANY(%s) LIMIT 1",
                (uid, list(paginas)),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def exige_pagina(*paginas: str):
    """Decorator: 403 em JSON se o usuário não tiver nenhuma das páginas. Sem páginas = só admin."""
    def deco(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            if not tem_alguma_pagina(*paginas):
                return jsonify({"ok": False, "error": SEM_PERMISSAO}), 403
            return view(*args, **kwargs)
        wrapper.paginas_exigidas = paginas
        return wrapper
    return deco


def so_admin(view):
    return exige_pagina()(view)
