"""Cliente do CRM acadêmico para o motor (API pública, Bearer `EDUIT_CRM_TOKEN`).

Estende `services/eduit_crm.py` (`request_full`) com:
  - limite de requisições por janela deslizante (padrão 180/min; o CRM corta
    em 240/min por token e por organização, e o balde da organização é
    dividido com o n8n e o Disparador)
  - repetição com espera em 429 (`Retry-After`) e em 5xx de leitura
  - trava de escrita: em simulação qualquer chamada que não seja GET levanta
    `EscritaBloqueada` antes de sair para a rede

Contratos conferidos no código do backend do CRM (origin/main, 12/11/2026);
ver AGENTS.md, "Motor de Ativações — F0".
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime
from typing import Any, Callable, Iterator, Optional

from motor.calendario import BRT
from motor.modelos import Fila

BASE_PADRAO = "https://integrations.bwipo.com"
FILTROS_DE_CAMPANHA_ACEITOS = frozenset({"tagIds"})

Transporte = Callable[[str, str, Any], tuple[int, dict, Any]]


class EscritaBloqueada(RuntimeError):
    """Chamada que escreve no CRM tentada com a trava de escrita ligada."""


class CrmErro(RuntimeError):
    def __init__(self, mensagem: str, status: int = 502, corpo: Any = None):
        super().__init__(mensagem)
        self.status = status
        self.corpo = corpo


def base_url() -> str:
    return (os.getenv("MOTOR_CRM_BASE_URL") or BASE_PADRAO).rstrip("/")


def transporte_padrao() -> Transporte:
    """HTTP real, pelo cliente que o painel já usa. Importado só aqui para os testes não precisarem dele."""
    from services import eduit_crm

    base = base_url()

    def _chamar(metodo: str, caminho: str, corpo: Any = None) -> tuple[int, dict, Any]:
        try:
            return eduit_crm.request_full(metodo, caminho, corpo, base=base, timeout=60)
        except eduit_crm.EduitCrmError as e:
            raise CrmErro(str(e), e.status) from e

    return _chamar


class LimiteDeTaxa:
    """Janela deslizante de 60 s: nunca mais que `rpm` chamadas em qualquer minuto."""

    def __init__(self, rpm: int, relogio: Callable[[], float] = time.monotonic, dormir: Callable[[float], None] = time.sleep):
        self.rpm = max(1, rpm)
        self._relogio = relogio
        self._dormir = dormir
        self._marcas: deque[float] = deque()
        self._trava = threading.Lock()

    def aguardar(self) -> float:
        """Espera o necessário e registra a chamada. Devolve quanto esperou."""
        with self._trava:
            esperou = 0.0
            while True:
                agora = self._relogio()
                while self._marcas and agora - self._marcas[0] >= 60.0:
                    self._marcas.popleft()
                if len(self._marcas) < self.rpm:
                    self._marcas.append(agora)
                    return esperou
                falta = 60.0 - (agora - self._marcas[0])
                self._dormir(max(falta, 0.01))
                esperou += max(falta, 0.01)


def _espera_do_429(headers: dict, corpo: Any, tentativa: int) -> float:
    for chave in ("retry-after",):
        try:
            return max(1.0, float(headers.get(chave, "")))
        except (TypeError, ValueError):
            pass
    if isinstance(corpo, dict):
        try:
            return max(1.0, float(corpo.get("retryAfterSec")))
        except (TypeError, ValueError):
            pass
    return float(min(60, 2 ** (tentativa + 1)))


def _mensagem(corpo: Any, status: int) -> str:
    if isinstance(corpo, dict):
        msg = corpo.get("message") or corpo.get("error")
        if isinstance(msg, dict):
            msg = msg.get("message")
        req = corpo.get("required")
        if msg and req:
            return f"{msg} (permissão exigida: {req})"
        if msg:
            return str(msg)
    return f"CRM HTTP {status}"


class CrmMotor:
    def __init__(
        self,
        *,
        transporte: Optional[Transporte] = None,
        rpm: int = 180,
        permitir_escrita: bool = False,
        max_tentativas: int = 6,
        espera_max_s: float = 120.0,
        relogio: Callable[[], float] = time.monotonic,
        dormir: Callable[[float], None] = time.sleep,
    ):
        self._transporte = transporte or transporte_padrao()
        self._limite = LimiteDeTaxa(rpm, relogio, dormir)
        self._dormir = dormir
        self.permitir_escrita = permitir_escrita
        self.max_tentativas = max(1, max_tentativas)
        self.espera_max_s = espera_max_s
        self.chamadas = 0

    # ── núcleo ────────────────────────────────────────────────────────────

    def _chamar(self, metodo: str, caminho: str, corpo: Any = None) -> Any:
        metodo = metodo.upper()
        if metodo != "GET" and not self.permitir_escrita:
            raise EscritaBloqueada(f"{metodo} {caminho.split('?')[0]} bloqueado: motor em simulação")
        ultimo: tuple[int, Any] = (0, None)
        for tentativa in range(self.max_tentativas):
            self._limite.aguardar()
            self.chamadas += 1
            status, headers, dados = self._transporte(metodo, caminho, corpo)
            if 200 <= status < 300:
                return dados
            ultimo = (status, dados)
            if status == 429:
                self._dormir(min(self.espera_max_s, _espera_do_429(headers or {}, dados, tentativa)))
                continue
            if status >= 500 and metodo == "GET":
                self._dormir(float(min(30, 2 ** tentativa)))
                continue
            break
        status, dados = ultimo
        raise CrmErro(_mensagem(dados, status), status, dados)

    @staticmethod
    def _qs(params: dict) -> str:
        limpos = {k: v for k, v in params.items() if v not in (None, "")}
        return ("?" + urllib.parse.urlencode(limpos)) if limpos else ""

    # ── leitura ───────────────────────────────────────────────────────────

    def fila_agora(self) -> Fila:
        """GET /api/painel/service?section=agora → fila de atendimento neste instante."""
        dados = self._chamar("GET", "/api/painel/service?section=agora&clock=business")
        return interpretar_fila(dados)

    def campos_do_negocio(self) -> dict[str, str]:
        """Nome do campo personalizado de negócio → id (a listagem de negócios só traz o id)."""
        dados = self._chamar("GET", "/api/custom-fields?entity=deal")
        itens = dados if isinstance(dados, list) else (dados or {}).get("items") or (dados or {}).get("data") or []
        return {str(c["name"]): str(c["id"]) for c in itens if isinstance(c, dict) and c.get("id") and c.get("name")}

    def negocios(
        self,
        *,
        pipeline_id: str = "",
        filtros: Optional[dict] = None,
        atualizado_desde: str = "",
        por_pagina: int = 1000,
        max_paginas: int = 200,
    ) -> Iterator[dict]:
        """GET /api/deals paginado. Levanta se a varredura passar de `max_paginas`."""
        for pagina in range(1, max_paginas + 1):
            qs = self._qs({
                "page": pagina,
                "perPage": max(1, min(1000, por_pagina)),
                "withTotal": "0",
                "pipelineId": pipeline_id,
                "updatedSince": atualizado_desde,
                "filters": json.dumps(filtros, separators=(",", ":")) if filtros else "",
            })
            dados = self._chamar("GET", "/api/deals" + qs) or {}
            itens = dados.get("items") or []
            for item in itens:
                if isinstance(item, dict):
                    yield item
            if not dados.get("hasMore") or not itens:
                return
        raise CrmErro(f"varredura de negócios passou de {max_paginas} páginas", 502)

    def etiquetas(self) -> list[dict]:
        dados = self._chamar("GET", "/api/tags")
        itens = dados if isinstance(dados, list) else (dados or {}).get("items") or []
        return [t for t in itens if isinstance(t, dict)]

    def campanha_stats(self, campanha_id: str) -> dict:
        return self._chamar("GET", f"/api/campaigns/{urllib.parse.quote(campanha_id)}/stats") or {}

    def campanha_destinatarios(self, campanha_id: str, *, status: str = "", codigo_erro: str = "", max_paginas: int = 500) -> Iterator[dict]:
        """Destinatários com status e erro. O código da Meta vem dentro de `errorMessage` ("… (code 131026)")."""
        for pagina in range(1, max_paginas + 1):
            qs = self._qs({"page": pagina, "perPage": 100, "status": status, "errorCode": codigo_erro})
            dados = self._chamar("GET", f"/api/campaigns/{urllib.parse.quote(campanha_id)}/recipients{qs}") or {}
            itens = dados.get("items") or []
            yield from (i for i in itens if isinstance(i, dict))
            if pagina >= int(dados.get("totalPages") or 0) or not itens:
                return

    # ── escrita (bloqueada em simulação) ──────────────────────────────────

    def criar_etiqueta(self, nome: str, cor: str = "#64748b") -> dict:
        return self._chamar("POST", "/api/tags", {"name": nome, "color": cor}) or {}

    def etiquetar_contato(self, contact_id: str, tag_id: str) -> None:
        """Atenção: etiqueta nova no contato dispara o gatilho `tag_added` (automações e webhooks) no CRM."""
        self._chamar("POST", f"/api/contacts/{urllib.parse.quote(contact_id)}/tags", {"tagId": tag_id})

    def desetiquetar_contato(self, contact_id: str, tag_id: str) -> None:
        self._chamar("DELETE", f"/api/contacts/{urllib.parse.quote(contact_id)}/tags", {"tagId": tag_id})

    def criar_campanha(self, *, nome: str, canal_id: str, template: str, idioma: str, componentes: Any, tag_ids: list[str], velocidade: int = 20) -> dict:
        """Cria a campanha em rascunho (não envia). O público é sempre uma etiqueta do lote.

        O CRM ignora chave de filtro que não conhece e, com filtro vazio, envia
        para todos os contatos com telefone. Por isso só `tagIds` não vazio passa.
        """
        filtros = {"tagIds": [t for t in tag_ids if t]}
        validar_filtros_de_campanha(filtros)
        corpo = {
            "name": nome,
            "type": "TEMPLATE",
            "channelId": canal_id,
            "filters": filtros,
            "templateName": template,
            "templateLanguage": idioma,
            "templateComponents": componentes,
            "sendRate": max(1, min(30, int(velocidade))),
        }
        return (self._chamar("POST", "/api/campaigns", corpo) or {}).get("campaign") or {}

    def lancar_campanha(self, campanha_id: str) -> Any:
        return self._chamar("POST", f"/api/campaigns/{urllib.parse.quote(campanha_id)}/launch")

    def pausar_campanha(self, campanha_id: str) -> Any:
        return self._chamar("POST", f"/api/campaigns/{urllib.parse.quote(campanha_id)}/pause")

    def retomar_campanha(self, campanha_id: str) -> Any:
        return self._chamar("POST", f"/api/campaigns/{urllib.parse.quote(campanha_id)}/resume")

    def cancelar_campanha(self, campanha_id: str) -> Any:
        return self._chamar("POST", f"/api/campaigns/{urllib.parse.quote(campanha_id)}/cancel")


def validar_filtros_de_campanha(filtros: Any) -> None:
    if not isinstance(filtros, dict) or set(filtros) - FILTROS_DE_CAMPANHA_ACEITOS:
        raise ValueError("filtro de campanha fora do permitido (só tagIds)")
    tags = filtros.get("tagIds")
    if not isinstance(tags, list) or not tags or not all(isinstance(t, str) and t.strip() for t in tags):
        raise ValueError("campanha sem etiqueta de lote: o CRM enviaria para todos os contatos")


def interpretar_fila(dados: Any, lida_em: Optional[datetime] = None) -> Fila:
    """Converte a seção `agora` do painel do CRM em `Fila`. Levanta se o formato não for o esperado."""
    bloco = (dados or {}).get("agora") if isinstance(dados, dict) else None
    if not isinstance(bloco, dict) or not bloco.get("ok") or not isinstance(bloco.get("data"), dict):
        erro = (bloco or {}).get("error") if isinstance(bloco, dict) else None
        raise CrmErro(f"seção 'agora' indisponível{': ' + str(erro) if erro else ''}", 502, dados)
    d = bloco["data"]
    espera = d.get("longestWait") or {}
    agentes = d.get("agents") or {}
    try:
        return Fila(
            aguardando=int(d["awaitingReply"]),
            maior_espera_s=int((espera.get("ms") or 0) // 1000),
            atendentes_online=int(agentes["online"]),
            lida_em=lida_em or datetime.now(BRT),
            sla_estourado=1 if espera.get("overSla") else 0,
        )
    except (KeyError, TypeError, ValueError) as e:
        raise CrmErro(f"formato inesperado da fila: {e}", 502, dados) from e
