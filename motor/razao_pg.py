"""Livro-razão no Postgres do painel (dcz_sync, tabelas motor_ de sql/motor_ativacoes.sql)."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Callable, Mapping, Optional

import psycopg2
import psycopg2.extras

from motor.carga import RegistroCiclo
from motor.modelos import Controle, Fila, Lote, LoteAtivo
from motor.razao import Msg, RazaoBase
from motor.razao_pg_envio import RazaoPgEnvio

TABELAS = (
    "motor_controle", "motor_worker", "motor_cargas", "motor_alunos", "motor_aluno_ciclo",
    "motor_planos", "motor_lotes", "motor_mensagens", "motor_fila_leituras", "motor_diario", "motor_acoes", "motor_respostas",
)


def _json(v: Any) -> Optional[str]:
    return None if v is None else json.dumps(v, ensure_ascii=False, default=str)


def tabelas_instaladas(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ANY(%s)",
            (list(TABELAS),),
        )
        return cur.fetchone()[0] == len(TABELAS)


class RazaoPg(RazaoPgEnvio, RazaoBase):
    def __init__(self, conectar: Callable[[], Any]):
        self._conectar = conectar

    def _tx(self, dicionario: bool = False):
        return _Tx(self._conectar, dicionario)

    def instalado(self) -> bool:
        conn = self._conectar()
        try:
            return tabelas_instaladas(conn)
        finally:
            conn.close()

    # ── controle, ponto, diário, fila ─────────────────────────────────────
    def ler_controle(self) -> Controle:
        with self._tx() as cur:
            cur.execute("SELECT ligado, parada_geral, parada_motivo, atualizado_por FROM motor_controle WHERE id = 1")
            r = cur.fetchone()
        return Controle(*r) if r else Controle()

    def bater_ponto(self, info: dict) -> None:
        with self._tx() as cur:
            cur.execute(
                """
                INSERT INTO motor_worker (id, host, pid, versao, iniciado_em, ultimo_tick_em, modo, enabled, dry_run, ultimo_erro, config)
                VALUES (1, %(host)s, %(pid)s, %(versao)s, %(iniciado_em)s, NOW(), %(modo)s, %(enabled)s, %(dry_run)s, %(ultimo_erro)s, %(config)s)
                ON CONFLICT (id) DO UPDATE SET host = EXCLUDED.host, pid = EXCLUDED.pid, versao = EXCLUDED.versao,
                    iniciado_em = EXCLUDED.iniciado_em, ultimo_tick_em = NOW(), modo = EXCLUDED.modo,
                    enabled = EXCLUDED.enabled, dry_run = EXCLUDED.dry_run, ultimo_erro = EXCLUDED.ultimo_erro, config = EXCLUDED.config
                """,
                {**info, "config": _json(info.get("config"))},
            )

    def diario(self, tipo: str, autor: str, texto: str, dados: Optional[dict] = None) -> None:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO motor_diario (tipo, autor, texto, dados) VALUES (%s, %s, %s, %s)",
                (tipo, autor, texto[:2000], _json(dados)),
            )

    def registrar_fila(self, fila: Optional[Fila], decisao: str, motivo: str, erro: str = "") -> None:
        with self._tx() as cur:
            cur.execute(
                """
                INSERT INTO motor_fila_leituras (ok, aguardando, maior_espera_s, atendentes_online, sla_estourado, decisao, motivo, erro)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    fila is not None,
                    fila.aguardando if fila else None,
                    fila.maior_espera_s if fila else None,
                    fila.atendentes_online if fila else None,
                    fila.sla_estourado if fila else None,
                    decisao, motivo, (erro or None) and erro[:500],
                ),
            )
            # Retenção: 30 dias de leituras bastam para recalibrar o governador.
            cur.execute("DELETE FROM motor_fila_leituras WHERE lida_em < NOW() - INTERVAL '30 days'")

    # ── carga ─────────────────────────────────────────────────────────────
    def _ciclo_existe(self, ciclo: str) -> bool:
        with self._tx() as cur:
            cur.execute("SELECT 1 FROM motor_aluno_ciclo WHERE ciclo = %s LIMIT 1", (ciclo,))
            return cur.fetchone() is not None

    def _registros_do_ciclo(self, ciclo: str) -> dict[str, RegistroCiclo]:
        with self._tx() as cur:
            cur.execute(
                "SELECT contact_id, aberto_em, baixa_dia10_em, controle, declarou_pagamento_em, suspenso FROM motor_aluno_ciclo WHERE ciclo = %s",
                (ciclo,),
            )
            return {r[0]: RegistroCiclo(r[1], r[2], r[3], r[4], r[5]) for r in cur.fetchall()}

    def _gravar_carga(self, dia, ciclo, linhas, mudancas, resumo) -> int:
        with self._tx() as cur:
            cur.execute(
                """
                INSERT INTO motor_cargas (dia, ciclo, concluida_em, completa, ciclo_aberto, negocios, alunos, abertos_dia10, abertos_financeira, novas_baixas)
                VALUES (%s, %s, NOW(), TRUE, %s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (dia, ciclo, resumo["ciclo_aberto"], resumo["negocios"], resumo["alunos"],
                 resumo["abertos_dia10"], resumo["abertos_financeira"], resumo["novas_baixas"]),
            )
            carga_id = cur.fetchone()[0]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO motor_alunos (contact_id, deal_id, situacao, nivel, nivel_misto, calouro, dia10_aberto, financeira_aberta, tem_telefone, bloqueio, carga_id, visto_em)
                VALUES %s
                ON CONFLICT (contact_id) DO UPDATE SET
                    deal_id = EXCLUDED.deal_id, situacao = EXCLUDED.situacao, nivel = EXCLUDED.nivel, nivel_misto = EXCLUDED.nivel_misto, calouro = EXCLUDED.calouro,
                    dia10_aberto = EXCLUDED.dia10_aberto, financeira_aberta = EXCLUDED.financeira_aberta,
                    tem_telefone = EXCLUDED.tem_telefone, bloqueio = EXCLUDED.bloqueio,
                    carga_id = EXCLUDED.carga_id, visto_em = NOW()
                """,
                [
                    (cid, l.deal_id, l.situacao, l.nivel, l.nivel_misto, l.calouro, bool(l.dia10_aberto), bool(l.financeira_aberta), l.tem_telefone, l.bloqueio, carga_id)
                    for cid, l in linhas.items()
                ],
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                page_size=1000,
            )
            if mudancas:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO motor_aluno_ciclo (contact_id, ciclo, deal_id, aberto_em, baixa_dia10_em, controle, declarou_pagamento_em)
                    VALUES %s
                    ON CONFLICT (contact_id, ciclo) DO UPDATE SET
                        baixa_dia10_em = EXCLUDED.baixa_dia10_em, deal_id = EXCLUDED.deal_id, atualizado_em = NOW()
                    """,
                    [
                        (cid, ciclo, linhas[cid].deal_id, r.aberto_em, r.baixa_dia10_em, r.controle, r.declarou_pagamento_em)
                        for cid, r in mudancas.items()
                    ],
                    page_size=1000,
                )
        return carga_id

    def carga_do_dia(self, dia: date) -> Optional[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT * FROM motor_cargas WHERE dia = %s AND completa ORDER BY id DESC LIMIT 1", (dia,))
            r = cur.fetchone()
            return dict(r) if r else None

    def _retratos(self) -> list[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute(
                """
                SELECT contact_id, deal_id, situacao, nivel, nivel_misto, calouro, dia10_aberto, financeira_aberta,
                       tem_telefone, bloqueio, bloqueio_motor, pediu_silencio, lembretes_sem_reacao
                FROM motor_alunos
                WHERE carga_id = (SELECT MAX(id) FROM motor_cargas WHERE completa)
                """
            )
            return [dict(r) for r in cur.fetchall()]

    def _ciclos_desde(self, ciclo_min: str) -> dict[str, dict[str, RegistroCiclo]]:
        out: dict[str, dict[str, RegistroCiclo]] = {}
        with self._tx() as cur:
            cur.execute(
                "SELECT contact_id, ciclo, aberto_em, baixa_dia10_em, controle, declarou_pagamento_em, suspenso FROM motor_aluno_ciclo WHERE ciclo >= %s",
                (ciclo_min,),
            )
            for cid, ciclo, aberto, baixa, controle, declarou, suspenso in cur.fetchall():
                out.setdefault(cid, {})[ciclo] = RegistroCiclo(aberto, baixa, controle, declarou, suspenso)
        return out

    def _mensagens(self, ciclo: str, simulado: bool) -> dict[str, list[Msg]]:
        out: dict[str, list[Msg]] = {}
        with self._tx() as cur:
            cur.execute(
                "SELECT contact_id, dia, frente, mensagem FROM motor_mensagens WHERE ciclo = %s AND simulado = %s AND status <> 'cancelado'",
                (ciclo, simulado),
            )
            for cid, dia, frente, mensagem in cur.fetchall():
                out.setdefault(cid, []).append((dia, frente, mensagem))
        return out

    # ── plano e lotes ─────────────────────────────────────────────────────
    def plano(self, dia: date, simulado: bool) -> Optional[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT * FROM motor_planos WHERE dia = %s AND simulado = %s", (dia, simulado))
            r = cur.fetchone()
            return dict(r) if r else None

    def salvar_plano(self, *, dia: date, simulado: bool, ciclo: str, piloto: str, resumo: dict, lotes: list[dict]) -> int:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO motor_planos (dia, simulado, ciclo, piloto, resumo) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (dia, simulado, ciclo, piloto, _json(resumo)),
            )
            plano_id = cur.fetchone()[0]
            for l in lotes:
                cur.execute(
                    "INSERT INTO motor_lotes (plano_id, dia, simulado, ordem, frente, mensagem, tamanho_previsto) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (plano_id, dia, simulado, l["ordem"], l["frente"], l["mensagem"], l["tamanho"]),
                )
        return plano_id

    def adicionar_lote(self, plano_id: int, *, dia: date, simulado: bool, ordem: int, frente: str, mensagem: str, tamanho: int) -> None:
        with self._tx() as cur:
            cur.execute(
                """
                INSERT INTO motor_lotes (plano_id, dia, simulado, ordem, frente, mensagem, tamanho_previsto)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (dia, simulado, ordem) DO NOTHING
                """,
                (plano_id, dia, simulado, ordem, frente, mensagem, tamanho),
            )

    def lotes(self, dia: date, simulado: bool) -> list[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT * FROM motor_lotes WHERE dia = %s AND simulado = %s ORDER BY ordem", (dia, simulado))
            return [dict(r) for r in cur.fetchall()]

    def liberar_lote(self, lote_id: int, lote: Lote, estados: Mapping[str, str], *, ciclo: str, dia: date, simulado: bool, status: str, motivo: str, fila: Optional[dict], agora: datetime) -> int:
        """Grava as mensagens do lote e marca o lote, na mesma transação.

        O UNIQUE (contact_id, dia, simulado) recusa o lote inteiro se algum
        aluno já tiver mensagem no dia.
        """
        with self._tx() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO motor_mensagens (contact_id, ciclo, dia, simulado, frente, mensagem, variante, estado, lote_id, status) VALUES %s",
                [
                    (cid, ciclo, dia, simulado, lote.frente.value, lote.mensagem.value, lote.variante, estados.get(cid, ""), lote_id, "simulado" if simulado else "reservado")
                    for cid in lote.contact_ids
                ],
                page_size=1000,
            )
            cur.execute(
                "UPDATE motor_lotes SET status = %s, motivo = %s, tamanho = %s, liberado_em = %s, fila = %s, variante = %s, atualizado_em = NOW() WHERE id = %s",
                (status, motivo, lote.tamanho, agora, _json(fila), lote.variante, lote_id),
            )
        return lote.tamanho

    def marcar_lote(self, lote_id: int, status: str, motivo: str = "") -> None:
        with self._tx() as cur:
            cur.execute(
                "UPDATE motor_lotes SET status = %s, motivo = %s, atualizado_em = NOW() WHERE id = %s",
                (status, motivo, lote_id),
            )

    def enviados_hoje(self, dia: date, simulado: bool) -> int:
        with self._tx() as cur:
            cur.execute("SELECT COUNT(*) FROM motor_mensagens WHERE dia = %s AND simulado = %s AND status <> 'cancelado'", (dia, simulado))
            return cur.fetchone()[0]

    def ultimo_lote_liberado(self, dia: date, simulado: bool) -> Optional[LoteAtivo]:
        with self._tx() as cur:
            cur.execute(
                """
                SELECT id, liberado_em, enviados, falhas FROM motor_lotes
                WHERE dia = %s AND simulado = %s AND liberado_em IS NOT NULL
                ORDER BY liberado_em DESC LIMIT 1
                """,
                (dia, simulado),
            )
            r = cur.fetchone()
        return LoteAtivo(lote_id=r[0], liberado_em=r[1], enviados=r[2], falhas=r[3]) if r else None

    def registrar_acao(self, chave: str, tipo: str, *, lote_id: Optional[int], simulado: bool, requisicao: Optional[dict] = None) -> bool:
        with self._tx() as cur:
            cur.execute(
                """
                INSERT INTO motor_acoes (chave, tipo, lote_id, simulado, status, requisicao)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (chave) DO NOTHING
                """,
                (chave, tipo, lote_id, simulado, "simulado" if simulado else "iniciada", _json(requisicao)),
            )
            return cur.rowcount == 1



class _Tx:
    """Uma transação curta por operação (o painel não tem pool; o worker faz poucas por minuto)."""

    def __init__(self, conectar: Callable[[], Any], dicionario: bool = False):
        self._conectar = conectar
        self._dicionario = dicionario

    def __enter__(self):
        self._conn = self._conectar()
        fabrica = psycopg2.extras.RealDictCursor if self._dicionario else None
        self._cur = self._conn.cursor(cursor_factory=fabrica)
        return self._cur

    def __exit__(self, tipo, valor, tb):
        try:
            if tipo is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._cur.close()
            self._conn.close()
        return False
