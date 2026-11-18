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
from motor.razao import CAMPANHA_ATIVA, CAMPOS_DO_LOTE, ERRO_NUMERO_SEM_WHATSAPP, efeito_da_resposta, status_do_destinatario


def _json(v: Any) -> Optional[str]:
    return None if v is None else json.dumps(v, ensure_ascii=False, default=str)


class RazaoPgEnvio:
    """Espera `self._tx()` da classe que o usa."""

    # ── aprovação e encerramento do plano ─────────────────────────────────
    def aprovar_plano(self, dia: date, simulado: bool, usuario: str, etiqueta: str = "") -> bool:
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE motor_planos SET status = 'aprovado', aprovado_por = %s, aprovado_em = NOW(), etiqueta_dia = %s
                WHERE dia = %s AND simulado = %s AND status = 'proposto'
                """,
                (usuario, etiqueta.strip() or None, dia, simulado),
            )
            return cur.rowcount == 1

    def atualizar_campanha(self, plano_id: int, campanha: dict) -> None:
        with self._tx() as cur:
            cur.execute("UPDATE motor_planos SET campanha = %s WHERE id = %s", (_json(campanha), plano_id))

    def planos_com_campanha_ativa(self) -> list[dict]:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT * FROM motor_planos WHERE NOT simulado AND campanha->>'estado' = ANY(%s) ORDER BY dia", (list(CAMPANHA_ATIVA),))
            return [dict(r) for r in cur.fetchall()]

    # ── grupo de controle no CRM ──────────────────────────────────────────
    def controle_pendente(self, ciclo: str, limite: int = 1000) -> list[str]:
        """Alunos do controle do ciclo que estão na base de hoje e ainda não receberam a etiqueta do mês."""
        with self._tx() as cur:
            cur.execute(
                """
                SELECT c.contact_id FROM motor_aluno_ciclo c JOIN motor_alunos a USING (contact_id)
                WHERE c.ciclo = %s AND c.controle AND NOT c.controle_etiquetado
                  AND a.carga_id = (SELECT MAX(id) FROM motor_cargas WHERE completa)
                ORDER BY c.contact_id LIMIT %s
                """,
                (ciclo, max(0, limite)),
            )
            return [r[0] for r in cur.fetchall()]

    def marcar_controle_etiquetado(self, ciclo: str, contact_ids) -> None:
        ids = list(contact_ids)
        if not ids:
            return
        with self._tx() as cur:
            cur.execute("UPDATE motor_aluno_ciclo SET controle_etiquetado = TRUE, atualizado_em = NOW() WHERE ciclo = %s AND contact_id = ANY(%s)", (ciclo, ids))

    def registrar_destinatarios(self, *, ciclo: str, dia: date, frente: str, mensagem: str, itens) -> int:
        """Grava no livro-razão quem a campanha do dia alcançou, pelo que o CRM devolve por destinatário.

        Erro 131026 bloqueia o número. Quem ficou sem envio entra como `nao_enviado`.
        """
        linhas = []
        for i in itens:
            status = status_do_destinatario(i)
            if status:
                linhas.append((i["contact_id"], ciclo, dia, frente, mensagem, status, i.get("erro_codigo"), i.get("entregue_em"), i.get("lido_em"), i.get("respondeu_em")))
        if not linhas:
            return 0
        with self._tx() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO motor_mensagens (contact_id, ciclo, dia, simulado, frente, mensagem, status, erro_codigo, entregue_em, lido_em, respondeu_em)
                VALUES %s
                ON CONFLICT (contact_id, dia, simulado) DO UPDATE SET status = EXCLUDED.status, erro_codigo = EXCLUDED.erro_codigo,
                    entregue_em = EXCLUDED.entregue_em, lido_em = EXCLUDED.lido_em,
                    respondeu_em = COALESCE(motor_mensagens.respondeu_em, EXCLUDED.respondeu_em)
                """,
                linhas,
                template="(%s, %s, %s, FALSE, %s, %s, %s, %s, %s, %s, %s)",
                page_size=200,
            )
            sem_whatsapp = [l[0] for l in linhas if l[6] == ERRO_NUMERO_SEM_WHATSAPP]
            if sem_whatsapp:
                cur.execute("UPDATE motor_alunos SET bloqueio_motor = 'erro_131026' WHERE contact_id = ANY(%s)", (sem_whatsapp,))
        return len(linhas)

    def encerrar_plano(self, dia: date, simulado: bool) -> None:
        with self._tx() as cur:
            cur.execute("UPDATE motor_planos SET status = 'encerrado' WHERE dia = %s AND simulado = %s", (dia, simulado))

    # ── janelas da campanha do dia (linhas de motor_lotes) ────────────────
    def lote(self, lote_id: int) -> dict:
        with self._tx(dicionario=True) as cur:
            cur.execute("SELECT * FROM motor_lotes WHERE id = %s", (lote_id,))
            return dict(cur.fetchone())

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
                [_json(campos[c]) if c == "fila" else campos[c] for c in colunas] + [lote_id],
            )

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
        dias. Em dia de campanha do motor, ou com `observar` (dia de
        observação), entra também quem está na base com a mensalidade em
        aberto. No máximo `max_por_dia` por aluno.
        """
        with self._tx() as cur:
            cur.execute(
                """
                SELECT id, lote_id FROM motor_mensagens
                WHERE contact_id = %s AND NOT simulado AND status NOT IN ('cancelado', 'nao_enviado') AND dia >= %s::date - 7
                ORDER BY dia DESC, id DESC LIMIT 1
                """,
                (contact_id, dia),
            )
            envio = cur.fetchone()
            if envio is None:
                if not observar:
                    cur.execute("SELECT 1 FROM motor_planos WHERE dia = %s AND NOT simulado AND campanha IS NOT NULL", (dia,))
                    if cur.fetchone() is None:
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
