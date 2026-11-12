"""Leitura da API pública do CRM acadêmico para o NPS. Só GET: este módulo não tem como escrever.

Host `NPS_CRM_BASE_URL` (padrão https://integrations.bwipo.com, o único que
aceita Bearer). Usa o mesmo `EDUIT_CRM_TOKEN` do painel. O balde de 240
requisições por minuto é dividido com n8n, Disparador e Motor, então a coleta
anda devagar (`NPS_CRM_RPM`, padrão 60).
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

_PADRAO = "https://integrations.bwipo.com"
_TIMEOUT = 40


class CrmErro(RuntimeError):
    pass


class CrmLeitura:
    def __init__(self, base: str = "", token: str = "", rpm: int = 0, dormir=time.sleep, abrir=urllib.request.urlopen):
        self._base = (base or os.getenv("NPS_CRM_BASE_URL") or _PADRAO).rstrip("/")
        self._token = (token or os.getenv("EDUIT_CRM_TOKEN") or "").strip()
        self._intervalo = 60.0 / max(1, min(rpm or int(os.getenv("NPS_CRM_RPM") or 60), 120))
        self._dormir = dormir
        self._abrir = abrir
        self._ultimo = 0.0

    def configurado(self) -> bool:
        return bool(self._token)

    def get(self, caminho: str, **params: Any) -> Any:
        if not self._token:
            raise CrmErro("EDUIT_CRM_TOKEN não configurado")
        consulta = urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
        url = f"{self._base}{caminho}" + (f"?{consulta}" if consulta else "")
        for tentativa in range(3):
            espera = self._intervalo - (time.monotonic() - self._ultimo)
            if espera > 0:
                self._dormir(espera)
            self._ultimo = time.monotonic()
            req = urllib.request.Request(
                url,
                headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json", "User-Agent": "dcz-crm-sync/nps"},
                method="GET",
            )
            try:
                with self._abrir(req, timeout=_TIMEOUT) as resp:
                    bruto = resp.read().decode("utf-8")
                    return json.loads(bruto) if bruto else None
            except urllib.error.HTTPError as e:
                if e.code == 429 and tentativa < 2:
                    self._dormir(min(float(e.headers.get("Retry-After") or 5), 60.0))
                    continue
                raise CrmErro(f"CRM respondeu HTTP {e.code} em {caminho}") from e
            except urllib.error.URLError as e:
                raise CrmErro(f"falha de conexão com o CRM em {caminho}: {e.reason}") from e
        raise CrmErro(f"CRM segue em 429 em {caminho}")

    def campos(self) -> Any:
        return self.get("/api/custom-fields")

    def negocios(self, pagina: int, por_pagina: int, pipeline_id: str = "") -> Any:
        return self.get("/api/deals", page=pagina, perPage=por_pagina, pipelineId=pipeline_id)

    def mensagens(self, conversa_id: str) -> Any:
        return self.get(f"/api/conversations/{urllib.parse.quote(conversa_id, safe='')}/messages")
