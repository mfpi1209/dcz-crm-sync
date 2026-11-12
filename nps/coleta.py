"""Coleta: lê os negócios do CRM e transforma os campos nps_* em pesquisas e respostas.

Como a pesquisa grava no CRM (ver `crm1/docs/nps-aluno.html`):
  - antes de enviar, a automação escreve no negócio a conversa e o departamento
    de origem, a hora do envio e a nota "aguardando";
  - a resposta do Formulário FLOW é gravada pelo mapeamento do próprio
    formulário (nota, resolvido, motivo), mesmo que a automação já tenha saído;
  - depois da resposta, a automação escreve a hora da resposta.

Cuidados (os mesmos da carga do Motor de Ativações):
  - o filtro por campo de `GET /api/deals` falha aberto, então a coleta lê a
    base e confere cada negócio aqui, pelo id do campo;
  - a paginação é por offset sem desempate: negócio pode repetir ou faltar numa
    varredura. Repetido é ignorado pela chave; o que faltar entra na próxima.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Optional

from nps.calculo import ler_nota, ler_resolvido, mascarar

BRT = timezone(timedelta(hours=-3))
POR_PAGINA = 1000
MAX_PAGINAS = 200
AGUARDANDO = "aguardando"

CAMPOS_PADRAO = {
    "enviado_em": "nps_enviado_em",
    "respondido_em": "nps_respondido_em",
    "nota": "nps_nota",
    "resolvido": "nps_resolvido",
    "motivo": "nps_motivo",
    "conversa": "nps_conversa_origem",
    "depto": "nps_depto_origem",
}
OBRIGATORIOS = ("enviado_em", "nota")


class ColetaErro(RuntimeError):
    pass


def nomes_dos_campos() -> dict[str, str]:
    """`name` de cada campo do negócio no CRM. Ajustável por ambiente: NPS_CAMPO_NOTA etc."""
    return {k: (os.getenv(f"NPS_CAMPO_{k.upper()}") or padrao).strip() for k, padrao in CAMPOS_PADRAO.items()}


def montar_mapa(campos_do_crm: Iterable[Mapping[str, Any]], nomes: Optional[Mapping[str, str]] = None) -> dict[str, str]:
    """Resposta de GET /api/custom-fields → {papel: id do campo}. Sem envio e nota a coleta não roda."""
    nomes = nomes or nomes_dos_campos()
    por_nome: dict[str, str] = {}
    for campo in campos_do_crm:
        if not isinstance(campo, Mapping) or not campo.get("id"):
            continue
        entidade = str(campo.get("entity") or campo.get("entityType") or "").lower()
        if entidade and entidade != "deal":
            continue
        por_nome.setdefault(str(campo.get("name") or "").strip(), str(campo["id"]))
    mapa = {papel: por_nome[nome] for papel, nome in nomes.items() if nome in por_nome}
    faltando = [nomes[p] for p in OBRIGATORIOS if p not in mapa]
    if faltando:
        raise ColetaErro("campo de negócio não encontrado no CRM: " + ", ".join(faltando))
    return mapa


def ler_momento(valor: Any) -> Optional[datetime]:
    """ISO com hora, ou só a data (campo do tipo Data): vira meio-dia de Brasília."""
    texto = str(valor or "").strip()
    if not texto or texto.lower() == AGUARDANDO:
        return None
    try:
        if len(texto) == 10:
            return datetime.strptime(texto, "%Y-%m-%d").replace(hour=12, tzinfo=BRT)
        momento = datetime.fromisoformat(texto.replace("Z", "+00:00"))
    except ValueError:
        return None
    return momento if momento.tzinfo else momento.replace(tzinfo=BRT)


@dataclass(frozen=True)
class Pesquisa:
    """Uma pesquisa enviada. Não carrega nome, telefone nem documento do aluno."""
    deal_id: str
    contact_id: str
    enviado_em: datetime
    conversa_origem: str = ""
    depto: str = ""
    nota: Optional[int] = None
    resolvido: Optional[str] = None
    resolvido_bruto: str = ""
    motivo: str = ""
    respondido_em: Optional[datetime] = None
    respondido_estimado: bool = False


def extrair(deal: Mapping[str, Any], mapa: Mapping[str, str], agora: datetime) -> Optional[Pesquisa]:
    """Item de GET /api/deals → Pesquisa. Negócio que nunca recebeu pesquisa devolve None."""
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

    def campo(papel: str) -> Any:
        return valores.get(mapa[papel]) if papel in mapa else None

    enviado_em = ler_momento(campo("enviado_em"))
    if enviado_em is None:
        return None
    nota = ler_nota(campo("nota"))
    if nota is None:
        return Pesquisa(deal_id, contact_id, enviado_em, str(campo("conversa") or "").strip(), str(campo("depto") or "").strip())

    respondido_em = ler_momento(campo("respondido_em"))
    estimado = respondido_em is None or respondido_em < enviado_em
    bruto = str(campo("resolvido") or "").strip()
    motivo = str(campo("motivo") or "").strip()
    return Pesquisa(
        deal_id=deal_id,
        contact_id=contact_id,
        enviado_em=enviado_em,
        conversa_origem=str(campo("conversa") or "").strip(),
        depto=str(campo("depto") or "").strip(),
        nota=nota,
        resolvido=ler_resolvido(bruto),
        resolvido_bruto="" if bruto.lower() == AGUARDANDO else bruto[:80],
        motivo="" if motivo.lower() == AGUARDANDO else mascarar(motivo),
        respondido_em=agora if estimado else respondido_em,
        respondido_estimado=estimado,
    )


def itens(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for chave in ("items", "deals", "messages", "customFields", "fields", "data"):
            if isinstance(payload.get(chave), list):
                return [x for x in payload[chave] if isinstance(x, dict)]
    return []


def varrer(buscar_pagina: Callable[[int], Any], mapa: Mapping[str, str], agora: datetime) -> tuple[list[Pesquisa], int]:
    """Lê todas as páginas de negócios. Devolve (pesquisas, negócios lidos). Erro em qualquer página aborta."""
    vistas: dict[tuple[str, datetime], Pesquisa] = {}
    lidos = 0
    for pagina in range(1, MAX_PAGINAS + 1):
        lote = itens(buscar_pagina(pagina))
        lidos += len(lote)
        for deal in lote:
            p = extrair(deal, mapa, agora)
            if p is not None:
                vistas.setdefault((p.deal_id, p.enviado_em), p)
        if len(lote) < POR_PAGINA:
            return list(vistas.values()), lidos
    raise ColetaErro(f"a base passou de {MAX_PAGINAS} páginas; a coleta parou para não girar sem fim")


def atendente_da_conversa(mensagens: Iterable[Mapping[str, Any]]) -> str:
    """Quem atendeu = autor da última mensagem humana enviada ao aluno na conversa de origem.

    O encerramento limpa o responsável da conversa e o gatilho não informa quem
    encerrou; o que sobra é o `senderName` das mensagens.
    """
    melhor: tuple[str, str] = ("", "")
    for m in mensagens:
        if str(m.get("direction") or "") != "out" or str(m.get("authorType") or "") != "human":
            continue
        if m.get("isPrivate") or not str(m.get("senderName") or "").strip():
            continue
        quando = str(m.get("createdAt") or "")
        if quando >= melhor[0]:
            melhor = (quando, str(m["senderName"]).strip())
    return melhor[1]
