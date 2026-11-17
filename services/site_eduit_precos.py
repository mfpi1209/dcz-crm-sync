"""Preço da graduação no banco do site www.eduit.com.br (banco site_anhanguera).

O site lê `cursos.preco`; a oferta (`curso_ofertas.valor`) guarda o mesmo valor.
Os dois mudam juntos, numa transação. Casa pela `curso_ofertas.chave`, que tem o
mesmo formato da `chave1` do Wix do Eduit New (ex.: "MarketingEADTecnólogo").
Só graduação; a Pós não é tocada.

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


def _preco_da_chave(chave: str, price_idx: dict[str, int]) -> int | None:
    if chave in price_idx:
        return price_idx[chave]
    alt = chave[len(_PREFIXO_CST):] if chave.startswith(_PREFIXO_CST) else _PREFIXO_CST + chave
    return price_idx.get(alt)


def _int_ou_none(valor) -> int | None:
    if valor is None:
        return None
    try:
        return int(round(float(valor)))
    except (TypeError, ValueError):
        return None


def planejar(ofertas: list[Oferta], price_idx: dict[str, int]) -> dict:
    """O que muda: trocas (de -> para), quantos já estão iguais e o que não casou."""
    trocas, iguais, sem_chave, sem_preco = [], 0, [], []
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
        if de == para:
            iguais += 1
            continue
        trocas.append({"oferta_id": of.oferta_id, "curso_id": of.curso_id,
                       "chave": chave, "de": de, "para": para})
    return {"trocas": trocas, "iguais": iguais, "sem_chave": sem_chave, "sem_preco": sem_preco}


def carregar_ofertas(cur) -> list[Oferta]:
    cur.execute(
        """
        SELECT o.id, o.curso_id, c.slug, o.chave, o.valor
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
                linhas.append({"chave": t["chave"], "de": t["de"], "para": t["para"], "ok": True})
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
    }


def conectar():
    host = (os.getenv("SITE_EDUIT_DB_HOST") or "").strip()
    if not host:
        raise SiteEduitNaoConfigurado("Site eduit.com.br não configurado (SITE_EDUIT_DB_*).")
    import psycopg2

    return psycopg2.connect(
        host=host,
        port=int(os.getenv("SITE_EDUIT_DB_PORT") or 5432),
        dbname=os.getenv("SITE_EDUIT_DB_NAME") or "site_anhanguera",
        user=os.getenv("SITE_EDUIT_DB_USER") or "postgres",
        password=os.getenv("SITE_EDUIT_DB_PASS") or "",
        connect_timeout=10,
    )


def atualizar_site_eduit(price_idx: dict[str, int]) -> dict:
    conn = conectar()
    try:
        with conn.cursor() as cur:
            ofertas = carregar_ofertas(cur)
        return aplicar(conn, planejar(ofertas, price_idx))
    finally:
        conn.close()
