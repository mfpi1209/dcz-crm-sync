"""Cliente do CRM acadêmico para o motor (API pública, Bearer `EDUIT_CRM_TOKEN`).

Usa as rotas de integração (`/api/integrations/*`: fila de atendimento e
campanhas) e as rotas públicas que já existiam (negócios, campos, etiquetas).
Contrato: nota `api-integracoes-campanhas-fila.md` e AGENTS.md.

  - orçamento de 60 chamadas por minuto em janela deslizante (o CRM corta em
    240/min por organização, dividido com as outras integrações), com reserva
    para as chamadas de prioridade alta: fila, resultado e pausa passam na
    frente de etiquetagem, destinatários e carga
  - 429: espera o `Retry-After`; se repetir, dobra a espera a cada tentativa
  - 403: o token é de um usuário de papel mínimo; a permissão que faltou (`required`)
    fica registrada e a mesma rota não é chamada de novo até o worker reiniciar
  - trava de escrita: em simulação qualquer chamada que não seja GET levanta
    `EscritaBloqueada` antes de sair para a rede
  - a resposta de destinatários traz nome e telefone: são descartados aqui
  - o cliente se dosa sozinho: as respostas normais do CRM não trazem cabeçalho de
    limite (só o 429 traz), então nada aqui depende de `X-RateLimit-*`
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Optional

from motor.calendario import BRT
from motor.modelos import Fila

BASE_PADRAO = "https://integrations.bwipo.com"
FILTROS_DE_CAMPANHA_ACEITOS = frozenset({"tagIds"})
CACHE_DA_FILA_S = 15.0
ALTA, BAIXA = "alta", "baixa"

Transporte = Callable[[str, str, Any], tuple[int, dict, Any]]


class EscritaBloqueada(RuntimeError):
    """Chamada que escreve no CRM tentada com a trava de escrita ligada."""


class CrmErro(RuntimeError):
    def __init__(self, mensagem: str, status: int = 502, corpo: Any = None):
        super().__init__(mensagem)
        self.status = status
        self.corpo = corpo

    @property
    def codigo(self) -> str:
        return str(self.corpo.get("code") or "") if isinstance(self.corpo, dict) else ""

    @property
    def permissao(self) -> str:
        """Permissão que faltou ao token, quando o CRM respondeu 403 (campo `required`)."""
        return str(self.corpo.get("required") or "") if self.status == 403 and isinstance(self.corpo, dict) else ""


_COM_ID = re.compile(r"/(campaigns|contacts|deals)/[^/?]+")


def rota_de(caminho: str) -> str:
    """'/api/contacts/abc/tags?x=1' → '/api/contacts/{id}/tags': a mesma rota para qualquer id."""
    return _COM_ID.sub(r"/\1/{id}", caminho.split("?")[0])


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
    """Janela deslizante de 60 s com duas prioridades.

    Nunca mais que `rpm` chamadas em qualquer minuto. As de prioridade baixa
    param em `rpm - reserva_alta`, para sempre sobrar espaço para ler a fila,
    ler o resultado e pausar.
    """

    def __init__(self, rpm: int, reserva_alta: int = 0, relogio: Callable[[], float] = time.monotonic, dormir: Callable[[float], None] = time.sleep):
        self.rpm = max(1, rpm)
        self.reserva_alta = max(0, min(reserva_alta, self.rpm - 1))
        self._relogio = relogio
        self._dormir = dormir
        self._marcas: deque[float] = deque()
        self._trava = threading.Lock()

    def _teto(self, prioridade: str) -> int:
        return self.rpm if prioridade == ALTA else self.rpm - self.reserva_alta

    def _limpar(self, agora: float) -> None:
        while self._marcas and agora - self._marcas[0] >= 60.0:
            self._marcas.popleft()

    def disponiveis(self, prioridade: str = BAIXA) -> int:
        """Quantas chamadas desta prioridade cabem agora sem esperar."""
        with self._trava:
            self._limpar(self._relogio())
            return max(0, self._teto(prioridade) - len(self._marcas))

    def aguardar(self, prioridade: str = ALTA) -> float:
        """Espera o necessário e registra a chamada. Devolve quanto esperou."""
        with self._trava:
            esperou = 0.0
            teto = self._teto(prioridade)
            while True:
                agora = self._relogio()
                self._limpar(agora)
                if len(self._marcas) < teto:
                    self._marcas.append(agora)
                    return esperou
                # Espera sair da janela a chamada que abre a vaga para esta prioridade.
                falta = max(60.0 - (agora - self._marcas[len(self._marcas) - teto]), 0.01)
                self._dormir(falta)
                esperou += falta


def _retry_after(headers: dict, corpo: Any) -> Optional[float]:
    try:
        return max(1.0, float(headers.get("retry-after", "")))
    except (TypeError, ValueError):
        pass
    if isinstance(corpo, dict):
        try:
            return max(1.0, float(corpo.get("retryAfterSec")))
        except (TypeError, ValueError):
            pass
    return None


def _mensagem(corpo: Any, status: int) -> str:
    if isinstance(corpo, dict):
        msg = corpo.get("message") or corpo.get("error")
        if isinstance(msg, dict):
            msg = msg.get("message")
        extras = [str(corpo[k]) for k in ("code",) if corpo.get(k)]
        if corpo.get("required"):
            extras.append(f"permissão exigida: {corpo['required']}")
        if msg:
            return f"{msg} ({'; '.join(extras)})" if extras else str(msg)
    return f"CRM HTTP {status}"


def _iso(valor: Any) -> Optional[datetime]:
    if not valor or not isinstance(valor, str):
        return None
    try:
        dt = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class CrmMotor:
    def __init__(
        self,
        *,
        transporte: Optional[Transporte] = None,
        rpm: int = 60,
        reserva_alta: int = 10,
        permitir_escrita: bool = False,
        max_tentativas: int = 6,
        espera_max_s: float = 120.0,
        relogio: Callable[[], float] = time.monotonic,
        dormir: Callable[[float], None] = time.sleep,
    ):
        self._transporte = transporte or transporte_padrao()
        self.limite = LimiteDeTaxa(rpm, reserva_alta, relogio, dormir)
        self._relogio = relogio
        self._dormir = dormir
        self.permitir_escrita = permitir_escrita
        self.max_tentativas = max(1, max_tentativas)
        self.espera_max_s = espera_max_s
        self.chamadas = 0
        self._fila_cache: Optional[tuple[float, Fila]] = None
        # Rotas que o CRM negou por falta de permissão do token (403). Não são tentadas de novo
        # até o worker reiniciar: corrigir o papel do usuário do token e reimplantar.
        self.negadas: dict[tuple[str, str], str] = {}

    # ── núcleo ────────────────────────────────────────────────────────────

    def _chamar(self, metodo: str, caminho: str, corpo: Any = None, prioridade: str = ALTA) -> Any:
        metodo = metodo.upper()
        if metodo != "GET" and not self.permitir_escrita:
            raise EscritaBloqueada(f"{metodo} {caminho.split('?')[0]} bloqueado: motor em simulação")
        chave = (metodo, rota_de(caminho))
        if chave in self.negadas:
            falta = self.negadas[chave]
            raise CrmErro(f"permissão negada antes (permissão exigida: {falta}); não tento de novo", 403, {"required": falta, "repetida": True})
        ultimo: tuple[int, Any] = (0, None)
        seguidos_429 = 0
        for tentativa in range(self.max_tentativas):
            self.limite.aguardar(prioridade)
            self.chamadas += 1
            status, headers, dados = self._transporte(metodo, caminho, corpo)
            if 200 <= status < 300:
                return dados
            ultimo = (status, dados)
            if status == 429:
                # Primeiro 429: o tempo que o CRM pediu. Se repetir, dobra a cada tentativa.
                base = _retry_after(headers or {}, dados) or 2.0
                self._dormir(min(self.espera_max_s, base * (2 ** seguidos_429)))
                seguidos_429 += 1
                continue
            seguidos_429 = 0
            if status == 403:
                self.negadas[chave] = str((dados or {}).get("required") or "não informada") if isinstance(dados, dict) else "não informada"
                break
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

    @staticmethod
    def _id(valor: str) -> str:
        return urllib.parse.quote(str(valor), safe="")

    # ── fila de atendimento ───────────────────────────────────────────────

    def fila_agora(self) -> Fila:
        """GET /api/integrations/service-queue. `lida_em` é o `asOf` da resposta, não a hora da chamada.

        O CRM guarda a resposta por 15 s; perguntar mais rápido que isso devolve
        o mesmo número, então o cliente nem chama.
        """
        agora = self._relogio()
        if self._fila_cache and agora - self._fila_cache[0] < CACHE_DA_FILA_S:
            return self._fila_cache[1]
        dados = self._chamar("GET", "/api/integrations/service-queue?clock=business", prioridade=ALTA)
        fila = interpretar_fila(dados)
        self._fila_cache = (agora, fila)
        return fila

    # ── leitura da base ───────────────────────────────────────────────────

    def campos_do_negocio(self) -> dict[str, str]:
        """Nome do campo personalizado de negócio → id (a listagem de negócios só traz o id)."""
        dados = self._chamar("GET", "/api/custom-fields?entity=deal", prioridade=BAIXA)
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
            dados = self._chamar("GET", "/api/deals" + qs, prioridade=BAIXA) or {}
            itens = dados.get("items") or []
            for item in itens:
                if isinstance(item, dict):
                    yield item
            if not dados.get("hasMore") or not itens:
                return
        raise CrmErro(f"varredura de negócios passou de {max_paginas} páginas", 502)

    def etiquetas(self) -> list[dict]:
        dados = self._chamar("GET", "/api/tags", prioridade=BAIXA)
        itens = dados if isinstance(dados, list) else (dados or {}).get("items") or []
        return [t for t in itens if isinstance(t, dict)]

    # ── campanhas: leitura ────────────────────────────────────────────────

    def campanha(self, campanha_id: str) -> dict:
        dados = self._chamar("GET", f"/api/integrations/campaigns/{self._id(campanha_id)}", prioridade=ALTA) or {}
        return dados.get("campaign") or {}

    def campanha_stats(self, campanha_id: str) -> dict:
        return self._chamar("GET", f"/api/integrations/campaigns/{self._id(campanha_id)}/stats", prioridade=ALTA) or {}

    def campanha_contagem(self, campanha_id: str, status: tuple[str, ...] = ("SENT", "DELIVERED", "READ", "FAILED")) -> dict[str, int]:
        """Quantos destinatários há em cada status, pelo campo `total` da rota de destinatários.

        É a medida do lote: o `stats` conta diferente (na campanha de 10/11 dizia
        0 pendentes e havia 183 destinatários PENDING numa campanha concluída).
        Uma chamada por status, pedindo um item só.
        """
        contagem: dict[str, int] = {}
        for s in status:
            dados = self._chamar("GET", f"/api/integrations/campaigns/{self._id(campanha_id)}/recipients?status={s}&page=1&perPage=1", prioridade=ALTA) or {}
            contagem[s] = int(dados.get("total") or 0)
        return contagem

    def campanha_destinatarios_pagina(self, campanha_id: str, pagina: int, *, status: str = "", por_pagina: int = 100) -> tuple[list[dict], int]:
        """Uma página de destinatários, já sem nome, telefone e texto de erro. Devolve (itens, total de páginas)."""
        qs = self._qs({"page": max(1, pagina), "perPage": max(1, min(100, por_pagina)), "status": status})
        dados = self._chamar("GET", f"/api/integrations/campaigns/{self._id(campanha_id)}/recipients{qs}", prioridade=BAIXA) or {}
        itens = [limpar_destinatario(i) for i in (dados.get("items") or []) if isinstance(i, dict)]
        return [i for i in itens if i["contact_id"]], int(dados.get("totalPages") or 0)

    # ── escrita (bloqueada em simulação) ──────────────────────────────────

    def criar_etiqueta(self, nome: str, cor: str = "#64748b") -> dict:
        dados = self._chamar("POST", "/api/tags", {"name": nome, "color": cor}, prioridade=ALTA) or {}
        return dados.get("tag") if isinstance(dados.get("tag"), dict) else dados

    def etiquetar_contato(self, contact_id: str, tag_id: str) -> None:
        """Atenção: etiqueta nova no contato dispara o gatilho `tag_added` (automações e webhooks) no CRM."""
        self._chamar("POST", f"/api/contacts/{self._id(contact_id)}/tags", {"tagId": tag_id}, prioridade=BAIXA)

    def desetiquetar_contato(self, contact_id: str, tag_id: str) -> None:
        self._chamar("DELETE", f"/api/contacts/{self._id(contact_id)}/tags", {"tagId": tag_id}, prioridade=BAIXA)

    def criar_campanha(self, *, nome: str, canal_id: str, template: str, idioma: str, componentes: Any, tag_ids: list[str], velocidade: int = 1) -> dict:
        """Cria a campanha em rascunho (não envia). O público é sempre a etiqueta do lote.

        Só `tagIds` não vazio passa: filtro vazio ou com outra chave alargaria o público.
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
        return (self._chamar("POST", "/api/integrations/campaigns", corpo, prioridade=ALTA) or {}).get("campaign") or {}

    def _acao(self, campanha_id: str, acao: str) -> dict:
        return self._chamar("POST", f"/api/integrations/campaigns/{self._id(campanha_id)}/{acao}", prioridade=ALTA) or {}

    def lancar_campanha(self, campanha_id: str) -> dict:
        return self._acao(campanha_id, "launch")

    def pausar_campanha(self, campanha_id: str) -> dict:
        return self._acao(campanha_id, "pause")

    def retomar_campanha(self, campanha_id: str) -> dict:
        return self._acao(campanha_id, "resume")

    def cancelar_campanha(self, campanha_id: str) -> dict:
        return self._acao(campanha_id, "cancel")


def validar_filtros_de_campanha(filtros: Any) -> None:
    if not isinstance(filtros, dict) or set(filtros) - FILTROS_DE_CAMPANHA_ACEITOS:
        raise ValueError("filtro de campanha fora do permitido (só tagIds)")
    tags = filtros.get("tagIds")
    if not isinstance(tags, list) or not tags or not all(isinstance(t, str) and t.strip() for t in tags):
        raise ValueError("campanha sem etiqueta de lote: o CRM enviaria para todos os contatos")


def limpar_destinatario(item: dict) -> dict:
    """Fica só com identificador, status, código de erro e horários. Nome, telefone e texto do erro saem aqui."""
    codigo = item.get("errorCode")
    return {
        "contact_id": str(item.get("contactId") or (item.get("contact") or {}).get("id") or ""),
        "status": str(item.get("status") or ""),
        "erro_codigo": int(codigo) if isinstance(codigo, (int, float)) or (isinstance(codigo, str) and codigo.isdigit()) else None,
        "enviado_em": _iso(item.get("sentAt")),
        "entregue_em": _iso(item.get("deliveredAt")),
        "lido_em": _iso(item.get("readAt")),
        "respondeu_em": _iso(item.get("repliedAt")),
    }


def interpretar_fila(dados: Any) -> Fila:
    """Resposta de `service-queue` → `Fila`. Aceita também o formato antigo (`{"agora": {"ok", "data"}}`)."""
    d = dados
    if isinstance(dados, dict) and isinstance(dados.get("agora"), dict):
        bloco = dados["agora"]
        if not bloco.get("ok") or not isinstance(bloco.get("data"), dict):
            raise CrmErro(f"seção 'agora' indisponível: {bloco.get('error') or ''}".rstrip(": "), 502, dados)
        d = bloco["data"]
    if not isinstance(d, dict):
        raise CrmErro("formato inesperado da fila", 502, dados)
    espera = d.get("longestWait") or {}
    agentes = d.get("agents") or {}
    try:
        return Fila(
            aguardando=int(d["awaitingReply"]),
            maior_espera_s=int((espera.get("ms") or 0) // 1000),
            atendentes_online=int(agentes["online"]),
            lida_em=_iso(d.get("asOf")) or datetime.now(BRT),
            sla_estourado=1 if espera.get("overSla") else 0,
        )
    except (KeyError, TypeError, ValueError) as e:
        raise CrmErro(f"formato inesperado da fila: {e}", 502, dados) from e
