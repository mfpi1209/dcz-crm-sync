"""Carga: lê os negócios do CRM e transforma em retrato do aluno e evolução do ciclo.

O dado financeiro é sempre D-1: campo "Dia 10" = Sim (não pagou até o dia 10)
e "Financeira" = Sim (não pagou até o dia 25). A baixa é o dia em que o motor
vê o Dia 10 mudar de Sim para Não.

Cuidados que vêm da F0:
  - o filtro por campo personalizado de `GET /api/deals` falha aberto (nome de
    campo errado devolve a base inteira), então a carga lê a base e confere o
    valor de cada negócio aqui, pelo id do campo;
  - a paginação é por offset sem desempate, então negócio pode faltar ou
    repetir numa varredura. A baixa nunca é deduzida por ausência: só quando o
    campo vem explicitamente "Não".
"""
from __future__ import annotations

import os
import unicodedata
from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Iterable, Mapping, Optional

from motor.config import MotorConfig
from motor.sorteio import no_controle


class CargaErro(RuntimeError):
    pass


def _fold(s: Any) -> str:
    n = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in n if not unicodedata.combining(c)).lower().strip()


def nomes_dos_campos() -> dict[str, str]:
    """`name` de cada campo personalizado do negócio no CRM (ajustável por ambiente)."""
    return {
        "dia10": (os.getenv("MOTOR_CAMPO_DIA10") or "dia").strip(),
        "financeira": (os.getenv("MOTOR_CAMPO_FINANCEIRA") or "situacaofinanceira").strip(),
        "situacao": (os.getenv("MOTOR_CAMPO_SITUACAO") or "situacao").strip(),
        "calouro": (os.getenv("MOTOR_CAMPO_CALOURO") or "calouro").strip(),
        "nivel": (os.getenv("MOTOR_CAMPO_NIVEL") or "nivel").strip(),
    }


# Etiquetas do contato que sempre tiram o aluno dos envios (comparadas sem acento e sem maiúsculas).
TAGS_DE_BLOQUEIO_FIXAS = frozenset({"sem lembretes de mensalidade", "sem mensagens automaticas"})


def tags_de_bloqueio() -> frozenset[str]:
    """Etiquetas do contato que tiram o aluno dos envios (recusou mensagens, número com erro 131026)."""
    bruto = os.getenv("MOTOR_TAGS_BLOQUEIO") or ""
    return TAGS_DE_BLOQUEIO_FIXAS | frozenset(_fold(t) for t in bruto.split(",") if t.strip())


@dataclass(frozen=True)
class MapaCampos:
    dia10: str
    financeira: str
    situacao: str
    calouro: str = ""
    nivel: str = ""


def montar_mapa(campos_por_nome: Mapping[str, str], nomes: Optional[Mapping[str, str]] = None) -> MapaCampos:
    """Resolve os ids dos campos. Sem Dia 10, Financeira ou Situação a carga não roda (nada de adivinhar)."""
    nomes = nomes or nomes_dos_campos()
    faltando = [nomes[k] for k in ("dia10", "financeira", "situacao", "nivel") if nomes[k] not in campos_por_nome]
    if faltando:
        raise CargaErro("campo de negócio não encontrado no CRM: " + ", ".join(faltando))
    return MapaCampos(
        dia10=campos_por_nome[nomes["dia10"]],
        financeira=campos_por_nome[nomes["financeira"]],
        situacao=campos_por_nome[nomes["situacao"]],
        calouro=campos_por_nome.get(nomes["calouro"], ""),
        nivel=campos_por_nome.get(nomes["nivel"], ""),
    )


@dataclass(frozen=True)
class Linha:
    """Um aluno na carga. Não carrega nome, telefone nem documento."""
    contact_id: str
    deal_id: str
    situacao: str = ""
    nivel: str = ""
    calouro: bool = False
    dia10_aberto: Optional[bool] = None       # None = campo vazio (não mexe no ciclo)
    financeira_aberta: Optional[bool] = None
    tem_telefone: bool = True
    bloqueio: Optional[str] = None
    nivel_misto: bool = False                 # contato com negócios em níveis diferentes
    etiquetas: frozenset = frozenset()        # nomes das etiquetas do contato e dos negócios dele (sem acento, minúsculas)


def sim_nao(valor: Any) -> Optional[bool]:
    v = _fold(valor)
    if v in ("sim", "s", "true", "1"):
        return True
    if v in ("nao", "n", "false", "0"):
        return False
    return None


def normalizar(deal: Mapping[str, Any], mapa: MapaCampos, bloqueios: frozenset[str] = frozenset()) -> Optional[Linha]:
    """Item de GET /api/deals → Linha. Negócio sem contato é ignorado."""
    contato = deal.get("contact") or {}
    contact_id = str(deal.get("contactId") or contato.get("id") or "").strip()
    deal_id = str(deal.get("id") or "").strip()
    if not contact_id or not deal_id:
        return None
    valores = {
        str(c.get("customFieldId")): c.get("value")
        for c in (deal.get("customFields") or [])
        if isinstance(c, dict)
    }
    # O público de campanha por etiqueta casa etiqueta no contato OU em qualquer negócio dele.
    tags = {_fold(t.get("name")) for t in list(contato.get("tags") or []) + list(deal.get("tags") or []) if isinstance(t, dict) and t.get("name")}
    return Linha(
        contact_id=contact_id,
        deal_id=deal_id,
        situacao=str(valores.get(mapa.situacao) or "").strip(),
        nivel=str(valores.get(mapa.nivel) or "").strip() if mapa.nivel else "",
        calouro=bool(sim_nao(valores.get(mapa.calouro))) if mapa.calouro else False,
        dia10_aberto=sim_nao(valores.get(mapa.dia10)),
        financeira_aberta=sim_nao(valores.get(mapa.financeira)),
        tem_telefone=bool(str(contato.get("phone") or "").strip()),
        bloqueio="recusou_mensagens" if (bloqueios and tags & bloqueios) else None,
        etiquetas=frozenset(tags),
    )


def _peso(l: Linha) -> tuple:
    """Entre vários negócios do mesmo contato, vale o que está em curso e em aberto."""
    return (_fold(l.situacao) == "em curso", bool(l.dia10_aberto), bool(l.financeira_aberta), l.deal_id)


def consolidar(linhas: Iterable[Linha]) -> dict[str, Linha]:
    """Uma linha por contato (o aluno com duas matrículas recebe uma mensagem só)."""
    por_contato: dict[str, Linha] = {}
    niveis: dict[str, set[str]] = {}
    etiquetas: dict[str, set[str]] = {}
    for l in linhas:
        etiquetas.setdefault(l.contact_id, set()).update(l.etiquetas)
        nivel = "".join(_fold(l.nivel).split())
        if nivel:
            niveis.setdefault(l.contact_id, set()).add(nivel)
        atual = por_contato.get(l.contact_id)
        if atual is None or _peso(l) > _peso(atual):
            # Bloqueio e falta de telefone valem para o contato inteiro.
            if atual is not None and atual.bloqueio and not l.bloqueio:
                l = replace(l, bloqueio=atual.bloqueio)
            por_contato[l.contact_id] = l
        elif l.bloqueio and not atual.bloqueio:
            por_contato[l.contact_id] = replace(atual, bloqueio=l.bloqueio)
    # Negócio na graduação e na pós: fica de fora, porque a regra de desconto muda de um nível para o outro.
    for cid, vistos in niveis.items():
        if len(vistos) > 1:
            por_contato[cid] = replace(por_contato[cid], nivel_misto=True)
    for cid, nomes in etiquetas.items():
        if nomes != por_contato[cid].etiquetas:
            por_contato[cid] = replace(por_contato[cid], etiquetas=frozenset(nomes))
    return por_contato


@dataclass(frozen=True)
class RegistroCiclo:
    aberto_em: date
    baixa_dia10_em: Optional[date] = None
    controle: bool = False
    declarou_pagamento_em: Optional[date] = None
    suspenso: bool = False  # reclamou no ciclo: régua suspensa
    controle_etiquetado: bool = False  # já recebeu no CRM a etiqueta do grupo de controle do mês


def evoluir_ciclo(
    reg: Optional[RegistroCiclo],
    linha: Linha,
    *,
    ciclo: str,
    hoje: date,
    cfg: MotorConfig,
    pode_abrir: bool,
) -> tuple[Optional[RegistroCiclo], Optional[str]]:
    """Aplica a carga de hoje ao registro do aluno no ciclo vigente.

    Devolve o registro novo e o que mudou: 'aberto', 'baixa', 'reaberto' ou None.
    O sorteio do controle é gravado na abertura e não muda mais no ciclo.
    """
    if reg is None:
        if pode_abrir and linha.dia10_aberto is True:
            controle = no_controle(linha.contact_id, ciclo, cfg.controle_sal, cfg.controle_pct)
            return RegistroCiclo(aberto_em=hoje, controle=controle), "aberto"
        return None, None
    if reg.baixa_dia10_em is None and linha.dia10_aberto is False:
        return replace(reg, baixa_dia10_em=hoje), "baixa"
    if reg.baixa_dia10_em is not None and linha.dia10_aberto is True:
        return replace(reg, baixa_dia10_em=None), "reaberto"
    return reg, None


def ciclo_vigente(ciclo_do_mes: str, *, abertos: int, ja_aberto: bool, cfg: MotorConfig) -> tuple[str, bool]:
    """Ciclo que a carga de hoje atualiza, e se o ciclo do mês está aberto.

    O ciclo do mês abre quando a carga da faculdade chega (salto de alunos com
    Dia 10 = Sim, por volta do dia 3 a 5). Antes disso os "Sim" ainda são do
    mês anterior.
    """
    from motor.calendario import ciclo_anterior

    if ja_aberto or abertos >= cfg.carga_min_abertos:
        return ciclo_do_mes, True
    return ciclo_anterior(ciclo_do_mes), False
