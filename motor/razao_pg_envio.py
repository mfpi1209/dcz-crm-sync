"""Operações do livro-razão no Postgres usadas pelo envio real e pelo receptor de respostas.

Mixin de `motor.razao_pg.RazaoPg` (separado só para manter os arquivos pequenos).
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Optional

import psycopg2.extras

from motor.classificador import ROTULOS_DE_EXPERIENCIA
from motor.modelos import Experiencia
from motor.razao import CAMPOS_DO_LOTE, ERRO_NUMERO_SEM_WHATSAPP, LOTES_ATIVOS, STATUS_DO_CRM, efeito_da_resposta


def _json(v: Any) -> Optional[str]:
    return None if v is None else json.dumps(v, ensure_ascii=False, default=str)


class RazaoPgEnvio:
    """Espera `self._tx()` da classe que o usa."""

    # ── aprovação e encerramento do plano ─────────────────────────────────
    def aprovar_plano(self, dia: date, simulado: bool, usuario: str) -> bool:
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE motor_planos SET status = 'aprovado', aprovado_por = %s, aprovado_em = NOW()
                WHERE dia = %s AND simulado = %s AND status = 'proposto'
                """,
                (usuario, dia, simulado),
            )
            return cur.rowcount == 1

    def encerrar_plano(self, dia: date, simulado: bool) -> None:
        with self._tx() as cur:
            cur.execute("UPDATE motor_planos SET status = 'encerrado' WHERE dia = %s AND simulado = %s", (dia, simulado))

    # ── lotes em envio real ───────────────────────────────────────────────
    def lote(self, lote_id: int) -> dict:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT * FROM motor_lotes WHERE id = %s", (lote_id,))
            return dict(cur.fetchone())

    def lotes_ativos(self) -> list[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT * FROM motor_lotes WHERE NOT simulado AND status = ANY(%s) ORDER BY id", (list(LOTES_ATIVOS),))
            return [dict(r) for r in cur.fetchall()]

    def lotes_para_leitura(self) -> list[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute(
                "SELECT * FROM motor_lotes WHERE NOT simulado AND status = 'concluido' AND crm_campaign_id IS NOT NULL AND leituras < 2 ORDER BY id"
            )
            return [dict(r) for r in cur.fetchall()]

    def lotes_liberados_hoje(self, dia: date, simulado: bool) -> int:
        with self._tx() as cur:
            cur.execute("SELECT COUNT(*) FROM motor_lotes WHERE dia = %s AND simulado = %s AND liberado_em IS NOT NULL", (dia, simulado))
            return cur.fetchone()[0]

    def atualizar_lote(self, lote_id: int, **campos) -> None:
        assert campos and set(campos) <= CAMPOS_DO_LOTE, set(campos) - CAMPOS_DO_LOTE
        colunas = sorted(campos)
        with self._tx() as cur:
            cur.execute(
                "UPDATE motor_lotes SET " + ", ".join(f"{c} = %s" for c in colunas) + ", atualizado_em = NOW() WHERE id = %s",
                [campos[c] for c in colunas] + [lote_id],
            )

    def contatos_do_lote(self, lote_id: int, status: str, limite: int = 1000) -> list[str]:
        with self._tx() as cur:
            cur.execute(
                "SELECT contact_id FROM motor_mensagens WHERE lote_id = %s AND status = %s ORDER BY id LIMIT %s",
                (lote_id, status, limite),
            )
            return [r[0] for r in cur.fetchall()]

    def marcar_mensagens(self, lote_id: int, contact_ids, status: str, erro: str = "") -> None:
        ids = list(contact_ids)
        if not ids:
            return
        with self._tx() as cur:
            cur.execute(
                "UPDATE motor_mensagens SET status = %s, erro = %s WHERE lote_id = %s AND contact_id = ANY(%s)",
                (status, erro or None, lote_id, ids),
            )

    def cancelar_pendentes(self, lote_id: int) -> int:
        with self._tx() as cur:
            cur.execute(
                "UPDATE motor_mensagens SET status = 'cancelado' WHERE lote_id = %s AND status IN ('reservado', 'etiquetado')",
                (lote_id,),
            )
            return cur.rowcount

    def aplicar_destinatarios(self, lote_id: int, itens) -> int:
        """Grava status, código de erro e horários de cada destinatário. Erro 131026 bloqueia o número."""
        linhas = [
            (STATUS_DO_CRM[i["status"]], i.get("erro_codigo"), i.get("entregue_em"), i.get("lido_em"), i.get("respondeu_em"), lote_id, i["contact_id"])
            for i in itens if i.get("status") in STATUS_DO_CRM
        ]
        if not linhas:
            return 0
        with self._tx() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                UPDATE motor_mensagens SET status = %s, erro_codigo = %s, entregue_em = %s, lido_em = %s,
                       respondeu_em = COALESCE(respondeu_em, %s)
                WHERE lote_id = %s AND contact_id = %s
                """,
                linhas,
            )
            sem_whatsapp = [l[6] for l in linhas if l[1] == ERRO_NUMERO_SEM_WHATSAPP]
            if sem_whatsapp:
                cur.execute("UPDATE motor_alunos SET bloqueio_motor = 'erro_131026' WHERE contact_id = ANY(%s)", (sem_whatsapp,))
        return len(linhas)

    def acao(self, chave: str) -> Optional[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT chave, tipo, lote_id, simulado, status, requisicao, resposta FROM motor_acoes WHERE chave = %s", (chave,))
            r = cur.fetchone()
            return dict(r) if r else None

    def concluir_acao(self, chave: str, status: str, resposta: Optional[dict] = None) -> None:
        with self._tx() as cur:
            cur.execute("UPDATE motor_acoes SET status = %s, resposta = %s WHERE chave = %s", (status, _json(resposta), chave))

    # ── respostas dos alunos ──────────────────────────────────────────────
    def registrar_resposta(self, *, contact_id: str, dia: date, wa_message_id: str = "", texto_pendente: str = "",
                           rotulo: Optional[str] = None, confianca: Optional[float] = None, origem: Optional[str] = None,
                           observar: bool = False, max_por_dia: int = 3) -> Optional[int]:
        """Grava uma resposta recebida pelo webhook. Devolve o id, ou None se não é para gravar.

        Só entra resposta de quem recebeu envio real do motor nos últimos 7
        dias. Com `observar` (dia de observação), entra também quem está na
        base com a mensalidade em aberto. No máximo `max_por_dia` por aluno.
        """
        with self._tx() as cur:
            cur.execute(
                """
                SELECT id, lote_id FROM motor_mensagens
                WHERE contact_id = %s AND NOT simulado AND status <> 'cancelado' AND dia >= %s::date - 7
                ORDER BY dia DESC, id DESC LIMIT 1
                """,
                (contact_id, dia),
            )
            envio = cur.fetchone()
            if envio is None:
                if not observar:
                    return None
                cur.execute("SELECT 1 FROM motor_alunos WHERE contact_id = %s AND dia10_aberto", (contact_id,))
                if cur.fetchone() is None:
                    return None
            cur.execute("SELECT COUNT(*) FROM motor_respostas WHERE contact_id = %s AND dia = %s", (contact_id, dia))
            if cur.fetchone()[0] >= max_por_dia:
                return None
            cur.execute(
                """
                INSERT INTO motor_respostas (dia, contact_id, wa_message_id, mensagem_id, lote_id, rotulo, confianca, origem,
                                             classificada_em, texto_pendente)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CASE WHEN %s IS NULL THEN NULL ELSE NOW() END, %s)
                ON CONFLICT (wa_message_id) DO NOTHING RETURNING id
                """,
                (dia, contact_id, wa_message_id or None, envio[0] if envio else None, envio[1] if envio else None,
                 rotulo, confianca, origem, rotulo, texto_pendente or None),
            )
            r = cur.fetchone()
            if r and rotulo and envio:
                cur.execute(
                    "UPDATE motor_mensagens SET respondeu_em = COALESCE(respondeu_em, NOW()), classificacao = COALESCE(classificacao, %s), confianca = COALESCE(confianca, %s) WHERE id = %s",
                    (rotulo, confianca, envio[0]),
                )
            return r[0] if r else None

    def respostas_pendentes(self, limite: int = 50) -> list[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute(
                "SELECT id, contact_id, dia, mensagem_id, texto_pendente FROM motor_respostas WHERE rotulo IS NULL ORDER BY id LIMIT %s",
                (limite,),
            )
            return [dict(r) for r in cur.fetchall()]

    def gravar_classificacao(self, resposta_id: int, *, rotulo: str, confianca: float, origem: str, revisar: bool,
                             guardar_texto: bool, dias: int = 30) -> None:
        """Grava rótulo e confiança e apaga o texto pendente (ou o guarda mascarado, por prazo, se a chave estiver ligada)."""
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE motor_respostas SET rotulo = %s, confianca = %s, origem = %s, revisar = %s, classificada_em = NOW(),
                       texto_mascarado = CASE WHEN %s THEN texto_pendente ELSE NULL END,
                       expira_em = CASE WHEN %s THEN NOW() + make_interval(days => %s) ELSE NULL END,
                       texto_pendente = NULL
                WHERE id = %s RETURNING mensagem_id
                """,
                (rotulo, confianca, origem, revisar, guardar_texto, guardar_texto, dias, resposta_id),
            )
            r = cur.fetchone()
            if r and r[0]:
                cur.execute(
                    "UPDATE motor_mensagens SET respondeu_em = COALESCE(respondeu_em, NOW()), classificacao = COALESCE(classificacao, %s), confianca = COALESCE(confianca, %s) WHERE id = %s",
                    (rotulo, confianca, r[0]),
                )

    def aplicar_efeito(self, contact_id: str, rotulo: str, dia: date) -> None:
        efeito = efeito_da_resposta(rotulo)
        if not efeito:
            return
        campo, valor = efeito
        with self._tx() as cur:
            if campo == "bloqueio_motor":
                cur.execute("UPDATE motor_alunos SET bloqueio_motor = %s WHERE contact_id = %s", (valor, contact_id))
            elif campo == "pediu_silencio":
                cur.execute("UPDATE motor_alunos SET pediu_silencio = %s WHERE contact_id = %s", (valor, contact_id))
            else:
                coluna = "declarou_pagamento_em = %s" if campo == "declarou_pagamento" else "suspenso = TRUE"
                cur.execute(
                    f"""
                    UPDATE motor_aluno_ciclo SET {coluna}, atualizado_em = NOW()
                    WHERE contact_id = %s AND ciclo = (SELECT MAX(ciclo) FROM motor_aluno_ciclo WHERE contact_id = %s)
                    """,
                    ((dia, contact_id, contact_id) if campo == "declarou_pagamento" else (contact_id, contact_id)),
                )

    def experiencia_do_dia(self, dia: date, simulado: bool) -> Experiencia:
        with self._tx() as cur:
            cur.execute(
                "SELECT COUNT(DISTINCT contact_id) FROM motor_respostas WHERE dia = %s AND rotulo = ANY(%s)",
                (dia, sorted(ROTULOS_DE_EXPERIENCIA)),
            )
            casos = cur.fetchone()[0]
            cur.execute(
                "SELECT COALESCE(SUM(entregues), 0), COALESCE(SUM(tamanho), 0) FROM motor_lotes WHERE dia = %s AND simulado = %s AND liberado_em IS NOT NULL",
                (dia, simulado),
            )
            entregues, tamanho = cur.fetchone()
        return Experiencia(casos=casos, entregues=int(entregues) or int(tamanho))

    def limpar_textos_vencidos(self) -> int:
        """Apaga texto mascarado vencido e texto pendente esquecido há mais de um dia."""
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE motor_respostas SET texto_mascarado = NULL, texto_pendente = NULL, expira_em = NULL
                WHERE (expira_em IS NOT NULL AND expira_em < NOW())
                   OR (texto_pendente IS NOT NULL AND recebida_em < NOW() - INTERVAL '1 day')
                """
            )
            return cur.rowcount
