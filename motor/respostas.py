"""Receptor de respostas: webhook de integração `message_received` do CRM.

O CRM assina o corpo com HMAC-SHA256 (`X-Eduit-Signature: sha256=<hex>`) usando
o segredo devolvido na criação do webhook. A rota do painel confere a
assinatura, mascara o texto e grava a resposta para o worker classificar.

O que fica no banco: rótulo e confiança. O texto mascarado fica só até o
worker classificar (minutos); guardar por 30 dias depende de
`MOTOR_GUARDAR_TEXTO=1`. Nome e telefone não vêm no evento.

Limite conhecido: o CRM não emite o evento quando o bot ou uma automação
assume a mensagem, e não reenvia se a entrega falhar. Serve para medir a
reação, não como fonte única de "o aluno respondeu".
"""
from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional

from motor.calendario import para_brt
from motor.classificador import Classificacao, anonimizar, classificar_botao

EVENTO = "message_received"
TAMANHO_MAXIMO = 2000


def assinatura_valida(corpo: bytes, cabecalho: str, segredo: str) -> bool:
    """Compara em tempo constante. Sem segredo configurado, nada é aceito."""
    if not segredo or not cabecalho:
        return False
    recebido = cabecalho.strip()
    if recebido.lower().startswith("sha256="):
        recebido = recebido[7:]
    esperado = hmac.new(segredo.encode("utf-8"), corpo, hashlib.sha256).hexdigest()
    return hmac.compare_digest(esperado, recebido.strip().lower())


@dataclass(frozen=True)
class Recebida:
    contact_id: str
    organizacao: str
    wa_message_id: str
    dia: date
    botao: Optional[Classificacao]   # clique em botão: já classificado, sem texto a guardar
    texto_mascarado: str             # texto livre mascarado, para o worker classificar


def interpretar(payload: Any, hoje: Optional[date] = None) -> Optional[Recebida]:
    """Evento do CRM → resposta pronta para gravar. None = não é uma mensagem de aluno aproveitável."""
    if not isinstance(payload, dict) or payload.get("event") != EVENTO:
        return None
    contact_id = str(payload.get("contactId") or "").strip()
    dados = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    conteudo = str(dados.get("content") or "").strip()
    if not contact_id or not conteudo:
        return None
    dia = hoje
    if dia is None:
        try:
            dia = para_brt(datetime.fromisoformat(str(payload.get("occurredAt")).replace("Z", "+00:00"))).date()
        except ValueError:
            dia = para_brt(datetime.now(timezone.utc)).date()
    botao = classificar_botao(conteudo)
    return Recebida(
        contact_id=contact_id,
        organizacao=str(payload.get("organizationId") or ""),
        wa_message_id=str(dados.get("waMessageId") or dados.get("metaWebhookEventId") or "")[:200],
        dia=dia,
        botao=botao,
        texto_mascarado="" if botao else anonimizar(conteudo)[:TAMANHO_MAXIMO],
    )
