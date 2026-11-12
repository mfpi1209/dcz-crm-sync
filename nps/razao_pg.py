"""Gravação e leitura das tabelas nps_ (Postgres dcz_sync). Não cria tabela: ver sql/nps_aluno.sql."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional

import psycopg2.extras

from nps.coleta import Pesquisa

TABELAS = ("nps_pesquisas", "nps_coletas")
TRAVA = 2026111301  # pg_try_advisory_lock: uma coleta por vez


def tabelas_instaladas(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ANY(%s)",
            (list(TABELAS),),
        )
        return cur.fetchone()[0] == len(TABELAS)


def pegar_trava(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (TRAVA,))
        return bool(cur.fetchone()[0])


def abrir_coleta(conn, disparada_por: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE nps_coletas SET status = 'erro', fim = NOW(), erro = 'interrompida' WHERE status = 'rodando'"
        )
        cur.execute("INSERT INTO nps_coletas (disparada_por) VALUES (%s) RETURNING id", (disparada_por[:80],))
        coleta_id = cur.fetchone()[0]
    conn.commit()
    return coleta_id


def fechar_coleta(conn, coleta_id: int, *, erro: Optional[str] = None, **numeros: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE nps_coletas
               SET fim = NOW(), status = %s, erro = %s,
                   negocios_lidos = %s, pesquisas_novas = %s, respostas_novas = %s, atendentes = %s
             WHERE id = %s
            """,
            (
                "erro" if erro else "ok", (erro or None) and erro[:500],
                numeros.get("negocios_lidos", 0), numeros.get("pesquisas_novas", 0),
                numeros.get("respostas_novas", 0), numeros.get("atendentes", 0), coleta_id,
            ),
        )
    conn.commit()


def gravar(conn, pesquisas: Iterable[Pesquisa], tipo: str = "atendimento") -> tuple[int, int]:
    """Insere pesquisas novas e completa a resposta de quem ainda não tinha. A primeira resposta vale.

    Devolve (pesquisas novas, respostas novas).
    """
    novas = respostas = 0
    with conn.cursor() as cur:
        for p in pesquisas:
            cur.execute(
                """
                INSERT INTO nps_pesquisas (tipo, deal_id, contact_id, enviado_em, conversa_origem, depto)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT nps_pesquisas_uk DO NOTHING
                """,
                (tipo, p.deal_id, p.contact_id, p.enviado_em, p.conversa_origem, p.depto),
            )
            novas += cur.rowcount
            if p.nota is None:
                continue
            cur.execute(
                """
                UPDATE nps_pesquisas
                   SET nota = %s, resolvido = %s, resolvido_bruto = %s, motivo = %s,
                       respondido_em = %s, respondido_estimado = %s
                 WHERE tipo = %s AND deal_id = %s AND enviado_em = %s AND nota IS NULL
                """,
                (p.nota, p.resolvido, p.resolvido_bruto, p.motivo, p.respondido_em, p.respondido_estimado,
                 tipo, p.deal_id, p.enviado_em),
            )
            respostas += cur.rowcount
    conn.commit()
    return novas, respostas


def sem_atendente(conn, limite: int) -> list[tuple[int, str]]:
    """Respostas com conversa de origem cujo atendente ainda não foi procurado."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, conversa_origem FROM nps_pesquisas
             WHERE nota IS NOT NULL AND conversa_origem <> '' AND atendente_buscado_em IS NULL
             ORDER BY id LIMIT %s
            """,
            (limite,),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def gravar_atendente(conn, pesquisa_id: int, nome: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE nps_pesquisas SET atendente = %s, atendente_buscado_em = NOW() WHERE id = %s",
            (nome[:120] or None, pesquisa_id),
        )
    conn.commit()


def linhas(conn, de: datetime, ate: datetime, tipo: str = "atendimento") -> list[dict[str, Any]]:
    """Pesquisas enviadas no período [de, ate). A leitura é por data de envio (coorte)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT depto, atendente, nota, resolvido, resolvido_bruto, motivo,
                   to_char(enviado_em AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM') AS mes,
                   to_char(date_trunc('week', enviado_em AT TIME ZONE 'America/Sao_Paulo'), 'YYYY-MM-DD') AS semana
              FROM nps_pesquisas
             WHERE tipo = %s AND enviado_em >= %s AND enviado_em < %s
            """,
            (tipo, de, ate),
        )
        return [dict(r) for r in cur.fetchall()]


def ultima_coleta(conn) -> Optional[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM nps_coletas ORDER BY id DESC LIMIT 1")
        r = cur.fetchone()
        return dict(r) if r else None
