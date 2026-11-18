"""Preço da graduação no banco do site www.eduit.com.br (banco site_anhanguera).

O site lê `cursos.preco`; a oferta (`curso_ofertas.valor`) guarda o mesmo valor.
Os dois mudam juntos, numa transação. Casa pela `curso_ofertas.chave`, que tem o
mesmo formato da `chave1` do Wix do Eduit New (ex.: "MarketingEADTecnólogo").
Só graduação; a Pós não é tocada.

Desde 18/11 a duração também vai: a DURAÇÃO da planilha (em semestres) vira
"N semestres" em `curso_ofertas.duracao` e `cursos.duracao`. Chave com mais de
uma duração na planilha (ex.: cursos "2.0") não muda e volta em `duracao_ambigua`.

Conexão pelas variáveis SITE_EDUIT_DB_HOST/PORT/NAME/USER/PASS (sem URL: a
senha pode ter caracteres que quebram o parse de uma URL).
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import NamedTuple

_PREFIXO_CST = "Cst Em "


class SiteEduitNaoConfigurado(RuntimeError):
    """Faltam as variáveis SITE_EDUIT_DB_*."""


class Oferta(NamedTuple):
    oferta_id: int
    curso_id: int
    slug: str
    chave: str | None
    valor: Decimal | None
    preco_curso: Decimal | None = None  # cursos.preco, o que o site mostra
    duracao: str | None = None          # curso_ofertas.duracao
    duracao_curso: str | None = None    # cursos.duracao, o que o site mostra


def _alias(chave: str) -> str:
    return chave[len(_PREFIXO_CST):] if chave.startswith(_PREFIXO_CST) else _PREFIXO_CST + chave


def _preco_da_chave(chave: str, price_idx: dict[str, int]) -> int | None:
    if chave in price_idx:
        return price_idx[chave]
    return price_idx.get(_alias(chave))


def _semestres(valor) -> str | None:
    try:
        n = int(round(float(str(valor).strip().replace(",", "."))))
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    return f"{n} semestre" if n == 1 else f"{n} semestres"


def indice_duracao(pares) -> tuple[dict[str, str], set[str]]:
    """(chave, DURAÇÃO da planilha) -> {chave: "N semestres"}. Chave com durações
    diferentes fica fora do índice e volta no conjunto de ambíguas."""
    vistos: dict[str, set[str]] = {}
    for chave, valor in pares:
        txt = _semestres(valor)
        if chave and txt:
            vistos.setdefault(chave, set()).add(txt)
    idx = {k: next(iter(v)) for k, v in vistos.items() if len(v) == 1}
    return idx, {k for k, v in vistos.items() if len(v) > 1}


def _int_ou_none(valor) -> int | None:
    if valor is None:
        return None
    try:
        return int(round(float(valor)))
    except (TypeError, ValueError):
        return None


def planejar(ofertas: list[Oferta], price_idx: dict[str, int],
             dur_idx: dict[str, str] | None = None, dur_ambiguas=()) -> dict:
    """O que muda: trocas (de -> para), quantos já estão iguais e o que não casou."""
    dur_idx = dur_idx or {}
    trocas, iguais, sem_chave, sem_preco, ambigua = [], 0, [], [], []
    for of in ofertas:
        chave = (of.chave or "").strip()
        if not chave:
            sem_chave.append(of.slug)
            continue
        para = _preco_da_chave(chave, price_idx)
        if para is None:
            sem_preco.append(chave)
            continue
        de = _int_ou_none(of.valor)
        curso_ok = of.preco_curso is None or _int_ou_none(of.preco_curso) == para
        dur_para = dur_idx.get(chave) or dur_idx.get(_alias(chave))
        if chave in dur_ambiguas or _alias(chave) in dur_ambiguas:
            ambigua.append(chave)
        dur_muda = bool(dur_para) and ((of.duracao or "").strip() != dur_para
                                       or (of.duracao_curso or "").strip() != dur_para)
        if de == para and curso_ok and not dur_muda:
            iguais += 1
            continue
        t = {"oferta_id": of.oferta_id, "curso_id": of.curso_id,
             "chave": chave, "de": de, "para": para}
        if dur_muda:
            t["dur_de"] = of.duracao_curso or of.duracao
            t["dur_para"] = dur_para
        trocas.append(t)
    return {"trocas": trocas, "iguais": iguais, "sem_chave": sem_chave, "sem_preco": sem_preco,
            "duracao_ambigua": ambigua}


def carregar_ofertas(cur) -> list[Oferta]:
    cur.execute(
        """
        SELECT o.id, o.curso_id, c.slug, o.chave, o.valor, c.preco, o.duracao, c.duracao
          FROM curso_ofertas o
          JOIN cursos c ON c.id = o.curso_id
         WHERE c.tipo = 'graduacao'
         ORDER BY o.id
        """
    )
    return [Oferta(*row) for row in cur.fetchall()]


def aplicar(conn, plano: dict) -> dict:
    """Grava as trocas numa transação. Qualquer erro desfaz tudo e é relançado."""
    linhas = []
    try:
        with conn.cursor() as cur:
            for t in plano["trocas"]:
                cur.execute("UPDATE curso_ofertas SET valor = %s WHERE id = %s", (t["para"], t["oferta_id"]))
                cur.execute("UPDATE cursos SET preco = %s WHERE id = %s", (t["para"], t["curso_id"]))
                linha = {"chave": t["chave"], "de": t["de"], "para": t["para"], "ok": True}
                if t.get("dur_para"):
                    cur.execute("UPDATE curso_ofertas SET duracao = %s WHERE id = %s", (t["dur_para"], t["oferta_id"]))
                    cur.execute("UPDATE cursos SET duracao = %s WHERE id = %s", (t["dur_para"], t["curso_id"]))
                    linha.update(dur_de=t.get("dur_de"), dur_para=t["dur_para"])
                linhas.append(linha)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "atualizados": len(linhas),
        "iguais": plano["iguais"],
        "erros": 0,
        "ultimo_erro": "",
        "linhas": linhas,
        "sem_chave": list(plano["sem_chave"]),
        "sem_preco": list(plano["sem_preco"]),
        "duracao_ambigua": list(plano.get("duracao_ambigua") or []),
    }


def conectar():
    host = (os.getenv("SITE_EDUIT_DB_HOST") or "").strip()
    user = (os.getenv("SITE_EDUIT_DB_USER") or "").strip()
    password = os.getenv("SITE_EDUIT_DB_PASS") or ""
    if not (host and user and password):
        raise SiteEduitNaoConfigurado("Site eduit.com.br não configurado (SITE_EDUIT_DB_*).")
    import psycopg2

    # Nada pode prender uma das 4 threads do gunicorn: lock/statement com prazo
    # e keepalive para conexão TCP meio aberta.
    return psycopg2.connect(
        host=host,
        port=int(os.getenv("SITE_EDUIT_DB_PORT") or 5432),
        dbname=os.getenv("SITE_EDUIT_DB_NAME") or "site_anhanguera",
        user=user,
        password=password,
        connect_timeout=10,
        options="-c statement_timeout=15000 -c lock_timeout=5000",
        keepalives=1,
        keepalives_idle=30,
    )


def atualizar_site_eduit(price_idx: dict[str, int], duracoes=()) -> dict:
    """`duracoes`: pares (chave, DURAÇÃO da planilha), no formato de chave do price_idx."""
    dur_idx, ambiguas = indice_duracao(duracoes)
    conn = conectar()
    try:
        with conn.cursor() as cur:
            ofertas = carregar_ofertas(cur)
        return aplicar(conn, planejar(ofertas, price_idx, dur_idx, ambiguas))
    finally:
        conn.close()
