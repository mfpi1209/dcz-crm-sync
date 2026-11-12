"""Seletor de público: quem está no alvo de cada ação e quem os trilhos tiram.

A lista é recalculada a cada lote, não uma vez por campanha: o lote das 11h já
usa as baixas importadas de manhã.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Iterable

from motor.config import MotorConfig
from motor.estados import calcular_estado, historico_com_ciclo_vencido
from motor.modelos import Acao, Aluno, Estado, Mensagem, Selecao
from motor.sorteio import no_controle
from motor.trilhos import motivo_exclusao


def com_estado(aluno: Aluno, *, ciclo: str, hoje: date, cfg: MotorConfig, controle_gravado: bool | None = None) -> Aluno:
    """Devolve o aluno com estado e sorteio do controle calculados para hoje.

    `controle_gravado` é o sorteio já registrado no livro-razão para o ciclo;
    quando existe, vale ele (trocar o sal não muda um ciclo aberto).
    """
    hist = historico_com_ciclo_vencido(
        aluno.historico,
        ciclo=ciclo,
        hoje=hoje,
        financeira_aberta=aluno.financeira_aberta,
        dia10_aberto=aluno.dia10_aberto,
    )
    estado = calcular_estado(
        hist,
        pediu_silencio=aluno.pediu_silencio,
        lembretes_sem_reacao=aluno.lembretes_sem_reacao,
    )
    controle = (
        controle_gravado
        if controle_gravado is not None
        else no_controle(aluno.contact_id, ciclo, cfg.controle_sal, cfg.controle_pct)
    )
    return replace(aluno, estado=estado, controle=controle)


def no_alvo(aluno: Aluno, acao: Acao) -> bool:
    """O aluno pertence ao público desta ação (antes das exclusões dos trilhos)?"""
    if aluno.estado not in acao.estados:
        return False
    m = acao.mensagem
    if m == Mensagem.AVISO_CALOURO:
        return aluno.primeira_mensalidade and aluno.dia10_aberto
    if m == Mensagem.LEMBRETE_DIA10:
        # Calouro na 1ª mensalidade nunca recebe o texto do dia 10.
        return aluno.dia10_aberto and not aluno.primeira_mensalidade
    if m == Mensagem.RECONHECIMENTO:
        return aluno.dia10_aberto
    if m == Mensagem.LEMBRETE_DIA25:
        # Inclui o silenciado que passou do dia 10 sem baixa.
        return aluno.dia10_aberto
    if m in (Mensagem.RECUPERACAO, Mensagem.FIRME):
        return aluno.financeira_aberta
    return False


def selecionar(alunos: Iterable[Aluno], acao: Acao, *, hoje: date, cfg: MotorConfig) -> Selecao:
    sel = Selecao(acao=acao)
    for aluno in alunos:
        if not no_alvo(aluno, acao):
            sel.fora_do_alvo += 1
            continue
        motivo = motivo_exclusao(aluno, frente=acao.frente, mensagem=acao.mensagem, hoje=hoje, cfg=cfg)
        if motivo:
            sel.exclusoes[motivo] = sel.exclusoes.get(motivo, 0) + 1
        else:
            sel.elegiveis.append(aluno)
    return sel


def contar_por_estado(alunos: Iterable[Aluno]) -> dict[str, int]:
    contagem = {e.value: 0 for e in Estado}
    for a in alunos:
        contagem[a.estado.value] += 1
    return contagem
