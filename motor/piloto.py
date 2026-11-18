"""Piloto: decide, dentro dos trilhos, o que sai agora, quanto e quando.

Duas implementações previstas:
  PilotoFormula  funciona sem chave nenhuma: prioridade fixa entre as frentes
                 e tamanho do lote pela fórmula do governador
  PilotoAgente   agente Claude (API da Anthropic) com ferramentas fechadas:
                 plano do dia, imprevistos e diário. Ponto de extensão; a
                 chamada real só entra quando a chave existir (ver AGENTS.md)

O piloto nunca fala com o CRM nem com o banco: recebe um retrato com contagens
e identificadores de lote e devolve uma ordem. Quem executa é o laço, e todo
lote ainda passa por `trilhos.validar_lote`. O piloto pode segurar quando o
governador libera; não pode liberar quando o governador segura ou pausa.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol

from motor.modelos import Decisao, Fila


@dataclass(frozen=True)
class LotePendente:
    ordem: int
    frente: str
    mensagem: str
    tamanho_previsto: int


@dataclass(frozen=True)
class Retrato:
    """Tudo o que o piloto vê numa rodada. Sem nome, telefone, CPF ou texto de aluno."""
    agora: datetime
    fila: Optional[Fila]
    decisao_governador: Decisao
    motivo_governador: str
    tamanho_sugerido: int
    pendentes: tuple[LotePendente, ...]
    enviados_hoje: int
    teto_diario: int
    lote_max: int
    por_estado: dict


@dataclass(frozen=True)
class Ordem:
    acao: str                      # "liberar" | "esperar" | "encerrar"
    motivo: str
    ordem_lote: Optional[int] = None
    tamanho: int = 0


class Piloto(Protocol):
    nome: str

    def proxima_ordem(self, retrato: Retrato) -> Ordem: ...


class PilotoFormula:
    """Piloto sem IA: solta o próximo lote planejado sempre que o governador libera."""

    nome = "piloto:formula"

    def proxima_ordem(self, retrato: Retrato) -> Ordem:
        if not retrato.pendentes:
            return Ordem("encerrar", "nenhum lote pendente no plano de hoje")
        if retrato.decisao_governador != Decisao.LIBERAR:
            return Ordem("esperar", retrato.motivo_governador)
        if retrato.enviados_hoje >= retrato.teto_diario:
            return Ordem("encerrar", f"teto diário atingido ({retrato.teto_diario})")
        if retrato.tamanho_sugerido <= 0:
            return Ordem("esperar", "equipe sem folga para absorver um novo lote")
        proximo = min(retrato.pendentes, key=lambda l: l.ordem)
        tamanho = min(retrato.tamanho_sugerido, retrato.lote_max, retrato.teto_diario - retrato.enviados_hoje)
        return Ordem("liberar", retrato.motivo_governador, ordem_lote=proximo.ordem, tamanho=tamanho)


# Ferramentas fechadas que o agente recebe. Só estas; nenhuma dá acesso ao
# token do CRM, à API livre ou à escrita no banco. `efeito` diz o que o laço
# faz com a chamada: "leitura" devolve contagens; "ordem" vira uma `Ordem`
# validada pelos trilhos; "registro" grava no diário ou avisa a operação.
FERRAMENTAS_DO_AGENTE = (
    {"nome": "ler_fila", "efeito": "leitura",
     "descricao": "Fila de atendimento agora: conversas aguardando, maior espera e atendentes online.",
     "parametros": {}},
    {"nome": "ler_estado_do_ciclo", "efeito": "leitura",
     "descricao": "Quantos alunos por estado, lotes pendentes do plano, enviados hoje e teto diário.",
     "parametros": {}},
    {"nome": "ler_resultado_do_lote", "efeito": "leitura",
     "descricao": "Enviados, entregues, lidos, respondidos e falhas de um lote já liberado.",
     "parametros": {"ordem_lote": "integer"}},
    {"nome": "lancar_proximo_lote", "efeito": "ordem",
     "descricao": "Pede a liberação de um lote pendente com o tamanho indicado. Os trilhos validam antes.",
     "parametros": {"ordem_lote": "integer", "tamanho": "integer", "motivo": "string"}},
    {"nome": "pausar_campanha", "efeito": "ordem",
     "descricao": "Pausa a campanha de um lote em andamento.",
     "parametros": {"ordem_lote": "integer", "motivo": "string"}},
    {"nome": "retomar_campanha", "efeito": "ordem",
     "descricao": "Retoma a campanha pausada de um lote, se o governador permitir.",
     "parametros": {"ordem_lote": "integer", "motivo": "string"}},
    {"nome": "registrar_decisao", "efeito": "registro",
     "descricao": "Grava no diário a decisão e o motivo.",
     "parametros": {"texto": "string"}},
    {"nome": "avisar_operacao", "efeito": "registro",
     "descricao": "Manda um aviso curto para a operação (imprevisto, resumo do dia).",
     "parametros": {"texto": "string"}},
    {"nome": "pedir_aprovacao", "efeito": "registro",
     "descricao": "Pede aprovação humana: mudança de limite, calendário, regra do seletor ou texto de template.",
     "parametros": {"assunto": "string", "proposta": "string"}},
)


class PilotoAgente:
    """Ponto de extensão do piloto agente. Ainda não chama a API.

    Quando a chave `ANTHROPIC_API_KEY` existir, esta classe passa a montar o
    `Retrato` como contexto, oferecer `FERRAMENTAS_DO_AGENTE` e converter a
    chamada de `lancar_proximo_lote` / `pausar_campanha` em `Ordem`. Em
    qualquer falha do agente o laço volta para o `PilotoFormula`.
    """

    nome = "piloto:agente"

    def __init__(self, reserva: Optional[Piloto] = None):
        self.reserva = reserva or PilotoFormula()

    def proxima_ordem(self, retrato: Retrato) -> Ordem:
        raise NotImplementedError("piloto agente ainda não implementado; use MOTOR_PILOTO=formula")


def criar_piloto(nome: Optional[str] = None) -> Piloto:
    escolhido = (nome or os.getenv("MOTOR_PILOTO") or "formula").strip().lower()
    if escolhido == "formula":
        return PilotoFormula()
    raise ValueError(f"piloto '{escolhido}' não disponível nesta versão (só 'formula')")
