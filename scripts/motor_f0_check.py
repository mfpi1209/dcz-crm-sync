"""
F0 do Motor de Ativações: confere ao vivo a API pública do CRM acadêmico.

SÓ LEITURA: todas as chamadas são GET. Não cria campanha, não etiqueta, não
altera nada. Nunca imprime o token nem dado de aluno (só status, contagens e
nomes de campo).

Usa EDUIT_CRM_TOKEN do .env. Host: MOTOR_CRM_BASE_URL (padrão
https://integrations.bwipo.com).

Uso:
    python scripts/motor_f0_check.py
"""
import json
import os
import sys
import time
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
os.chdir(_ROOT)

from dotenv import load_dotenv

load_dotenv()

from motor.carga import nomes_dos_campos
from motor.crm import base_url
from services import eduit_crm


def _get(caminho: str):
    t0 = time.monotonic()
    status, _headers, corpo = eduit_crm.request_full("GET", caminho, base=base_url(), timeout=90)
    return status, corpo, time.monotonic() - t0


def _linha(rotulo: str, texto: str) -> None:
    print(f"{rotulo:<34} {texto}")


def main() -> int:
    if not eduit_crm.configured():
        print("EDUIT_CRM_TOKEN não está no .env. Nada foi chamado.")
        return 1
    print(f"Host: {base_url()} (só GET)\n")

    st, corpo, dt = _get("/api/painel/service?section=agora&clock=business")
    bloco = (corpo or {}).get("agora") if isinstance(corpo, dict) else None
    if st == 200 and isinstance(bloco, dict) and bloco.get("ok"):
        d = bloco["data"]
        _linha("(a) fila agora", f"200 em {dt:.1f}s · campos: {sorted(d)} · aguardando={d.get('awaitingReply')} online={(d.get('agents') or {}).get('online')}")
    else:
        _linha("(a) fila agora", f"{st} — {(corpo or {}).get('message') if isinstance(corpo, dict) else ''} (com Bearer a rota exige sessão)")

    st, corpo, dt = _get("/api/conversations?counts=1&perPage=1")
    chaves = sorted(k for k in (corpo or {}) if k != "items") if isinstance(corpo, dict) else []
    _linha("(a') conversas com contadores", f"{st} em {dt:.1f}s · chaves: {chaves}")

    st, corpo, dt = _get("/api/custom-fields?entity=deal")
    itens = corpo if isinstance(corpo, list) else (corpo or {}).get("items") or []
    nomes = {str(c.get("name")) for c in itens if isinstance(c, dict)}
    faltam = [n for n in nomes_dos_campos().values() if n not in nomes]
    _linha("(b) campos do negócio", f"{st} · {len(nomes)} campos · faltando: {faltam or 'nenhum'}")

    st, corpo, dt = _get("/api/deals?perPage=1&page=1")
    total = (corpo or {}).get("total") if isinstance(corpo, dict) else None
    _linha("(b) negócios (total)", f"{st} em {dt:.1f}s · total={total}")

    campo = nomes_dos_campos()["dia10"]
    filtro = urllib.parse.quote(json.dumps({"dealCustomFields": [{"name": campo, "operator": "eq", "value": "Sim"}]}))
    st, corpo, dt = _get(f"/api/deals?perPage=1&page=1&filters={filtro}")
    com_filtro = (corpo or {}).get("total") if isinstance(corpo, dict) else None
    aviso = " ← IGUAL ao total: o filtro falhou aberto" if total is not None and com_filtro == total else ""
    _linha(f"(b) filtro {campo}=Sim", f"{st} em {dt:.1f}s · total={com_filtro}{aviso}")

    st, corpo, dt = _get("/api/deals?perPage=1000&page=1&withTotal=0")
    n = len((corpo or {}).get("items") or []) if isinstance(corpo, dict) else 0
    if total and n:
        paginas = -(-int(total) // 1000)
        _linha("(b) página de 1.000", f"{st} em {dt:.1f}s · {n} itens · base inteira ≈ {paginas} requisições, ≈ {paginas * dt / 60:.1f} min")
    else:
        _linha("(b) página de 1.000", f"{st} em {dt:.1f}s · {n} itens")

    st, corpo, dt = _get("/api/campaigns?perPage=1")
    msg = (corpo or {}).get("message") if isinstance(corpo, dict) else ""
    exig = (corpo or {}).get("required") if isinstance(corpo, dict) else ""
    _linha("(c/e) campanhas", f"{st} — {msg or 'ok'}{' · exige ' + exig if exig else ''}")

    # Construtor de campanhas: GET sem `id` não lê nem cria rascunho. 400 DRAFT_ID_REQUIRED =
    # recurso campaign_builder_v2 ligado; 403 FEATURE_DISABLED = desligado.
    st, corpo, dt = _get("/api/campaign-builder/drafts")
    erro = (corpo or {}).get("error") if isinstance(corpo, dict) else None
    codigo = erro.get("code") if isinstance(erro, dict) else ""
    situacao = {"DRAFT_ID_REQUIRED": "recurso campaign_builder_v2 LIGADO", "FEATURE_DISABLED": "recurso campaign_builder_v2 desligado"}.get(codigo, "resposta não prevista")
    _linha("(c) construtor de campanhas", f"{st} {codigo} — {situacao}")

    st, corpo, dt = _get("/api/whatsapp-template-configs/approved")
    modelos = corpo if isinstance(corpo, list) else (corpo or {}).get("items") or (corpo or {}).get("templates") or (corpo or {}).get("data") or []
    _linha("(c) templates aprovados", f"{st} · {len(modelos) if isinstance(modelos, list) else '?'} itens")

    st, corpo, dt = _get("/api/tags?counts=1")
    tags = corpo if isinstance(corpo, list) else (corpo or {}).get("items") or []
    _linha("(f) etiquetas", f"{st} · {len(tags)} etiquetas")

    print("\nNada foi escrito no CRM.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
