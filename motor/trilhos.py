"""Trilhos: limites fixos em código. O piloto (fórmula ou agente) não altera nada daqui.

Todo lote passa por `validar_lote` imediatamente antes de ser liberado, venha
de onde vier. Se um trilho recusa, o lote não sai.

  - janela das 9h às 14h em dia útil
  - teto diário e tamanho máximo de lote
  - exclusões: situação diferente de "Em Curso", número com erro 131026, quem
    recusou mensagens, sem telefone, grupo de controle
  - no máximo 2 mensagens de pagamento por aluno por ciclo
  - nunca duas campanhas no mesmo dia para o mesmo aluno
  - uma mensagem por frente por ciclo; mensagem firme uma vez por ciclo
  - quem declarou pagamento sai dos lembretes por 5 dias úteis
  - pausa de emergência pela fila e parada geral
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Mapping, Optional

from motor.calendario import dias_uteis_entre, para_brt
from motor.config import MotorConfig
from motor.governador import antes_do_corte, dentro_da_janela
from motor.modelos import Aluno, Controle, Fila, Frente, Lote, Mensagem

SITUACAO_ATIVA = "em curso"


@dataclass(frozen=True)
class Recusa:
    codigo: str
    detalhe: str = ""

    def __str__(self) -> str:
        return f"{self.codigo}: {self.detalhe}" if self.detalhe else self.codigo


def _fold(s: str) -> str:
    n = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in n if not unicodedata.combining(c)).lower().strip()


def normalizar_nivel(s: str) -> str:
    """'  Graduação ' → 'graduacao': sem espaços, sem acento, sem diferença de maiúsculas."""
    return "".join(_fold(s).split())


def modo_efetivo(cfg: MotorConfig, controle: Controle) -> str:
    """'parado' | 'desligado' | 'simulacao' | 'real'.

    Para enviar de verdade são precisas as duas chaves: MOTOR_ENABLED=1 e
    MOTOR_DRY_RUN=0 no ambiente do worker, e o motor ligado na página.
    """
    if controle.parada_geral:
        return "parado"
    if not (cfg.enabled and controle.ligado):
        return "desligado"
    return "simulacao" if cfg.dry_run else "real"


def motivo_exclusao(
    aluno: Aluno,
    *,
    frente: Frente,
    mensagem: Mensagem,
    hoje: date,
    cfg: MotorConfig,
) -> Optional[str]:
    """Por que este aluno não pode receber esta mensagem hoje (None = pode)."""
    if aluno.controle:
        return "grupo_de_controle"
    if _fold(aluno.situacao) != SITUACAO_ATIVA:
        return "situacao_diferente_de_em_curso"
    if aluno.nivel_misto:
        return "negocio_em_dois_niveis"
    if cfg.nivel_alvo and normalizar_nivel(aluno.nivel) != cfg.nivel_alvo:
        return "fora_do_nivel_do_piloto"
    if not aluno.tem_telefone:
        return "sem_telefone"
    if aluno.bloqueio:
        return aluno.bloqueio
    if aluno.suspenso:
        return "reclamou_no_ciclo"
    if hoje in aluno.dias_com_mensagem:
        return "ja_recebeu_hoje"
    if aluno.msgs_ciclo >= cfg.max_msgs_ciclo:
        return "limite_de_mensagens_no_ciclo"
    if frente in aluno.frentes_ciclo:
        return "ja_recebeu_desta_frente_no_ciclo"
    if mensagem in aluno.mensagens_ciclo:
        return "ja_recebeu_esta_mensagem_no_ciclo"
    if aluno.declarou_pagamento_em is not None:
        if dias_uteis_entre(aluno.declarou_pagamento_em, hoje, cfg.feriados) < cfg.declarou_pagamento_du:
            return "declarou_pagamento"
    return None


def validar_lote(
    lote: Lote,
    *,
    alunos: Mapping[str, Aluno],
    agora: datetime,
    cfg: MotorConfig,
    controle: Controle,
    enviados_hoje: int,
    plano_aprovado: bool = False,
) -> list[Recusa]:
    """Última barreira antes de liberar um lote. Lista vazia = pode sair.

    Em modo real nenhum lote sai sem o plano do dia aprovado por uma pessoa.
    """
    recusas: list[Recusa] = []
    modo = modo_efetivo(cfg, controle)
    if modo == "parado":
        recusas.append(Recusa("parada_geral", controle.parada_motivo))
    elif modo == "desligado":
        recusas.append(Recusa("motor_desligado"))
    elif modo == "real" and not plano_aprovado:
        recusas.append(Recusa("plano_nao_aprovado"))
    if not dentro_da_janela(agora, cfg):
        recusas.append(Recusa("fora_da_janela", f"{cfg.janela_inicio_h}h–{cfg.janela_fim_h}h em dia útil"))
    elif not antes_do_corte(agora, cfg):
        recusas.append(Recusa("depois_do_corte", f"último lote até {cfg.corte_min} min antes das {cfg.janela_fim_h}h"))
    if lote.tamanho == 0:
        recusas.append(Recusa("lote_vazio"))
    if lote.tamanho > cfg.lote_max:
        recusas.append(Recusa("lote_acima_do_maximo", f"{lote.tamanho} > {cfg.lote_max}"))
    if enviados_hoje + lote.tamanho > cfg.teto_diario:
        recusas.append(Recusa("teto_diario", f"{enviados_hoje} + {lote.tamanho} > {cfg.teto_diario}"))
    if len(set(lote.contact_ids)) != lote.tamanho:
        recusas.append(Recusa("aluno_repetido_no_lote"))

    hoje = para_brt(agora).date()
    motivos: dict[str, int] = {}
    for cid in lote.contact_ids:
        aluno = alunos.get(cid)
        motivo = "aluno_desconhecido" if aluno is None else motivo_exclusao(
            aluno, frente=lote.frente, mensagem=lote.mensagem, hoje=hoje, cfg=cfg
        )
        if motivo:
            motivos[motivo] = motivos.get(motivo, 0) + 1
    for motivo, n in sorted(motivos.items()):
        recusas.append(Recusa("aluno_excluido", f"{motivo} × {n}"))
    return recusas


def pausa_de_emergencia(
    fila: Optional[Fila],
    cfg: MotorConfig,
    *,
    agora: datetime,
    campanha_em_andamento: bool,
    lote_ativo=None,
    experiencia=None,
) -> Optional[str]:
    """Motivo para pausar agora o que estiver em andamento (None = segue).

    Roda a cada leitura da fila e não depende do piloto: fila crítica, espera
    acima do SLA, falha de envio acima de 6%, trava de experiência no nível de
    encerrar, fim da janela ou fila às cegas.
    """
    if not campanha_em_andamento:
        return None
    from motor.governador import decidir
    from motor.modelos import Decisao

    decisao, motivo = decidir(
        fila, cfg, agora=agora, lote_ativo=lote_ativo, campanha_em_andamento=True, experiencia=experiencia
    )
    if decisao != Decisao.PAUSAR:
        return None
    return "fim da janela de envio" if motivo == "fora da janela de envio" else motivo


def contar_exclusoes(motivos: Iterable[Optional[str]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for m in motivos:
        if m:
            out[m] = out.get(m, 0) + 1
    return out
