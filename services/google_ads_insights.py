"""
Performance de Campanhas — leads Google e o que aconteceu com eles.

Leads: Supabase `campanhas_google` (um registro por lead, com utm_*, gclid e telefone).
Ganho/perdido do lead: até 30/09/2026 o campo do Kommo; depois, o negócio do Bwipo
(Pipeline Principal) com o mesmo telefone.
Vendas Google: banco `google_ads`, `matriculas_google` com gclid preenchido (a tabela tem
todas as vendas; polo, curso e valor são complemento). O gclid liga a venda à campanha.

Cache de 5 min por período (o gunicorn tem só 4 threads).
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

import requests
import unicodedata
from concurrent.futures import ThreadPoolExecutor

from db import get_conn, get_google_ads_conn

logger = logging.getLogger(__name__)

SUPABASE_GOOGLE_URL = "https://vtlbndvcgajcoajhcnnx.supabase.co"
SUPABASE_GOOGLE_KEY = "sb_publishable_sW0h7aqgrjiwqGqKpawm4g_FuMi5xU_"
_HEADERS = {"apikey": SUPABASE_GOOGLE_KEY, "Authorization": f"Bearer {SUPABASE_GOOGLE_KEY}"}

_TTL = 300
_cache: dict[tuple, tuple[float, dict]] = {}
_cache_lock = threading.Lock()
_period_locks: dict[tuple, threading.Lock] = {}

# gclid -> {"utm_campaign", "data_criacao"} (lead não muda de campanha; cache sem TTL)
_gclid_cache: dict[str, dict | None] = {}

SEM_UTM = "(sem utm_campaign)"

# Lead até 30/09/2026: ganho/perdido do Kommo (campo novo_ganho_perdido).
# De 01/10/2026 em diante: negócio do Bwipo pelo telefone. Mesmo corte do Repasse.
CORTE_BWIPO = date(2026, 10, 1)


def norm_campaign(value) -> str:
    """'pm_grad_ead,pm_grad_ead' -> 'pm_grad_ead' (o form duplica a UTM às vezes)."""
    if not value:
        return ""
    parts = [p.strip() for p in str(value).split(",") if p.strip()]
    seen: list[str] = []
    for p in parts:
        if p not in seen:
            seen.append(p)
    return ",".join(seen)


def _brt_bounds(from_date: str, to_date: str) -> tuple[str, str]:
    return f"{from_date}T00:00:00-03:00", f"{to_date}T23:59:59.999-03:00"


def _get(url: str, headers: dict | None = None, tentativas: int = 3) -> requests.Response:
    """GET no Supabase com nova tentativa em 5xx/timeout (o PostgREST dá 500 esporádico)."""
    for i in range(tentativas):
        try:
            resp = requests.get(url, headers=headers or _HEADERS, timeout=30)
            if resp.status_code < 500 or i == tentativas - 1:
                resp.raise_for_status()
                return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if i == tentativas - 1:
                raise
        time.sleep(0.6 * (i + 1))
    raise RuntimeError("inalcançável")


def _fetch_paginated(url_base: str, limit: int = 1000) -> list[dict]:
    """Pega o total (count=exact) e baixa as páginas em paralelo."""
    resp = _get(f"{url_base}&limit={limit}&offset=0", {**_HEADERS, "Prefer": "count=exact"})
    rows = resp.json()
    try:
        total = int((resp.headers.get("content-range") or "*/0").split("/")[-1])
    except ValueError:
        total = len(rows)
    offsets = list(range(limit, total, limit))
    if not offsets:
        return rows

    def page(off):
        return _get(f"{url_base}&limit={limit}&offset={off}").json()

    with ThreadPoolExecutor(max_workers=6) as ex:
        for batch in ex.map(page, offsets):
            rows.extend(batch)
    return rows


def fetch_google_leads(from_date: str, to_date: str) -> list[dict]:
    """Leads Google criados no período (dia em BRT)."""
    ini, fim = _brt_bounds(from_date, to_date)
    return _fetch_paginated(
        f"{SUPABASE_GOOGLE_URL}/rest/v1/campanhas_google"
        f"?select=utm_campaign,utm_source,utm_medium,novo_ganho_perdido,gclid,data_criacao,telefone"
        f"&data_criacao=gte.{ini}&data_criacao=lte.{fim}")


def _tel_norm(t) -> str:
    """Mesmo formato do bwipo_deals.phone_norm: DDD + número, sem o 55."""
    d = re.sub(r"\D", "", str(t or ""))
    if d.startswith("55") and len(d) >= 12:
        d = d[2:]
    return d if len(d) >= 10 else ""


def _bwipo_negocios(tels: list[str]) -> dict[str, list[tuple]]:
    """telefone -> [(status, created_at, updated_at)] dos negócios do Pipeline Principal."""
    out: dict[str, list[tuple]] = defaultdict(list)
    if not tels:
        return out
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.phone_norm, upper(COALESCE(d.status, '')), d.created_at, d.updated_at
                FROM bwipo_deals d
                WHERE NOT d.is_deleted
                  AND d.phone_norm = ANY(%s)
                  AND position('principal' in lower(COALESCE(d.pipeline_name, ''))) > 0
                """,
                (tels,),
            )
            for tel, status, criado, atualizado in cur.fetchall():
                out[tel].append((status, criado, atualizado))
    finally:
        conn.close()
    return out


def _status_lead(lead: dict, negocios: dict[str, list[tuple]]) -> str:
    """ganho | perdido | aberto | sem_negocio.

    Lead até 30/09/2026: campo novo_ganho_perdido (vinha do Kommo).
    Lead de 01/10/2026 em diante: negócio do Bwipo com o mesmo telefone no Pipeline Principal.
    Se a pessoa tem mais de um negócio, vale o mais recente criado a partir de 1 dia antes do lead;
    sem nenhum assim, o último atualizado (o Bwipo reaproveita o negócio do contato).
    """
    try:
        criado = datetime.fromisoformat(lead["data_criacao"])
    except (TypeError, ValueError):
        criado = None
    if criado and criado.date() < CORTE_BWIPO:
        ngp = (lead.get("novo_ganho_perdido") or "").strip().lower()
        return ngp if ngp in ("ganho", "perdido") else "aberto"

    lista = negocios.get(_tel_norm(lead.get("telefone")))
    if not lista:
        return "sem_negocio"
    if criado:
        recentes = [n for n in lista if n[1] and n[1] >= criado - timedelta(days=1)]
        escolhido = max(recentes, key=lambda n: n[1]) if recentes else \
            max(lista, key=lambda n: n[2] or n[1] or criado)
    else:
        escolhido = max(lista, key=lambda n: n[2] or n[1])
    return {"WON": "ganho", "LOST": "perdido"}.get(escolhido[0], "aberto")


def _lookup_chunk(chunk: list[str]) -> dict[str, dict | None]:
    q = "gclid=in.(" + ",".join(f'"{g}"' for g in chunk) + ")"
    resp = _get(f"{SUPABASE_GOOGLE_URL}/rest/v1/campanhas_google?select=gclid,utm_campaign,data_criacao&{q}")
    achados: dict[str, list[dict]] = defaultdict(list)
    for row in resp.json():
        achados[row["gclid"]].append(row)
    out: dict[str, dict | None] = {}
    for g in chunk:
        leads = achados.get(g)
        if not leads:
            out[g] = None
            continue
        # primeiro lead com campanha; senão o primeiro lead
        leads.sort(key=lambda r: r.get("data_criacao") or "")
        com_camp = [r for r in leads if norm_campaign(r.get("utm_campaign"))]
        escolhido = (com_camp or leads)[0]
        out[g] = {"utm_campaign": norm_campaign(escolhido.get("utm_campaign")),
                  "data_criacao": leads[0].get("data_criacao")}
    return out


def _lookup_gclids(gclids: list[str]) -> None:
    falta = [g for g in gclids if g not in _gclid_cache]
    chunks = [falta[i:i + 80] for i in range(0, len(falta), 80)]
    if not chunks:
        return
    with ThreadPoolExecutor(max_workers=6) as ex:
        for res in ex.map(_lookup_chunk, chunks):
            _gclid_cache.update(res)


def _fetch_matriculas(from_date: str, to_date: str) -> list[dict]:
    conn = get_google_ads_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT matricula_id,
                       (data_matricula AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                       data_matricula, lower(coalesce(nivel, '')) AS nivel,
                       NULLIF(btrim(gclid), '') AS gclid, curso, polo, valor
                FROM matriculas_google
                WHERE (data_matricula AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s AND %s
                """,
                (from_date, to_date),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def _pct(a, b) -> float:
    return round(a * 100.0 / b, 1) if b else 0.0


def _sem_acento(s: str) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


_POLO_LIXO = {"polo mais proximo", "mais proximo", "nao encontrado", ""}
_CURSO_LIXO = {"nao encontrado", ""}


def _titulo(s: str) -> str:
    minus = {"da", "de", "do", "das", "dos", "e", "em"}
    pal = s.lower().split()
    return " ".join(p if (i and p in minus) else p[:1].upper() + p[1:] for i, p in enumerate(pal))


def _polo_nome(polo: str) -> str | None:
    """'50 - POLO SP_BARRA FUNDA' e 'barra funda' -> 'Barra Funda' (o feed mudou em 27/10)."""
    s = polo.split(" - ", 1)[1] if re.match(r"^\d+ - ", polo) else polo
    s = re.sub(r"^(CEB\s+)?POLO\s+", "", s.strip(), flags=re.I)
    s = re.sub(r"^SP_", "", s, flags=re.I)
    s = re.sub(r"\s*\(.*?\)", "", s).replace("_", " - ")
    s = re.sub(r"\s+\d+$", "", re.sub(r"\s+", " ", s).strip())  # 'Santana 2' -> 'Santana'
    if _sem_acento(s).lower() in _POLO_LIXO:
        return None
    return _titulo(s)


def _curso_nome(curso: str) -> str | None:
    """'CST EM GESTÃO DE RECURSOS HUMANOS' e 'Gestão de Recursos Humanos' -> mesmo nome."""
    s = re.sub(r"\s*\((BACHARELADO|LICENCIATURA|TECNOL[OÓ]GICO|TECN[OÓ]LOGO)\)", "", curso.strip(), flags=re.I)
    s = re.sub(r"^CST\s+EM\s+", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    if _sem_acento(s).lower() in _CURSO_LIXO:
        return None
    return _titulo(s)


def _leads_unicos(brutos: list[dict]) -> tuple[list[dict], dict]:
    """Uma linha por pessoa, só lead que veio do Google.

    - Lead do Google = tem gclid ou utm_source com "google". Linha sem gclid e sem UTM
      (ou de outra fonte, como chatgpt.com e ig) não é lead de anúncio Google.
    - A mesma pessoa chega várias vezes (formulário enviado de novo, "recadastro",
      às vezes 5 linhas no mesmo segundo). Vale a primeira do período, pelo telefone;
      sem telefone válido, pelo gclid. A campanha vem da primeira linha que tiver uma.
    """
    descartados = Counter()
    por_pessoa: dict[str, dict] = {}
    for l in sorted(brutos, key=lambda r: r.get("data_criacao") or ""):
        gclid = (l.get("gclid") or "").strip()
        if not gclid and "google" not in (l.get("utm_source") or "").lower():
            descartados["sem_sinal_google"] += 1
            continue
        chave = _tel_norm(l.get("telefone")) or (f"gclid:{gclid}" if gclid else f"linha:{id(l)}")
        atual = por_pessoa.get(chave)
        if atual is None:
            por_pessoa[chave] = dict(l)
            continue
        descartados["repetido"] += 1
        for campo in ("utm_campaign", "utm_source", "utm_medium", "gclid"):
            if not atual.get(campo) and l.get(campo):
                atual[campo] = l[campo]
    return list(por_pessoa.values()), dict(descartados)


def _build(from_date: str, to_date: str) -> dict:
    brutos = fetch_google_leads(from_date, to_date)
    leads, descartados = _leads_unicos(brutos)
    negocios = _bwipo_negocios(sorted({t for t in (_tel_norm(l.get("telefone")) for l in leads) if t}))
    matriculas = _fetch_matriculas(from_date, to_date)
    _lookup_gclids(sorted({m["gclid"] for m in matriculas if m["gclid"]}))

    # ---- leads do período e o que aconteceu com eles ----
    camp: dict[str, dict] = {}

    def slot(nome: str, source=None, medium=None) -> dict:
        key = nome or SEM_UTM
        if key not in camp:
            camp[key] = {"utm_campaign": key, "utm_source": None, "utm_medium": None,
                         "novos": 0, "ganhos": 0, "perdidos": 0, "sem_negocio": 0,
                         "matriculas": 0, "matriculas_grad": 0, "matriculas_pos": 0}
        c = camp[key]
        if c["utm_source"] is None and source:
            c["utm_source"] = norm_campaign(source)
        if c["utm_medium"] is None and medium:
            c["utm_medium"] = norm_campaign(medium)
        return c

    tot = Counter()
    for lead in leads:
        c = slot(norm_campaign(lead.get("utm_campaign")), lead.get("utm_source") or "google", lead.get("utm_medium"))
        st = _status_lead(lead, negocios)
        tot[st] += 1
        if st == "ganho":
            c["ganhos"] += 1
        elif st == "perdido":
            c["perdidos"] += 1
        else:
            c["novos"] += 1          # em aberto (inclui quem não tem negócio no Bwipo)
            if st == "sem_negocio":
                c["sem_negocio"] += 1

    # ---- vendas Google (gclid preenchido) ----
    cursos: Counter = Counter()
    polos: Counter = Counter()
    n_google = 0
    for m in matriculas:
        if not m["gclid"]:
            continue
        n_google += 1
        nivel = "pos" if m["nivel"].startswith("pos") else "grad"
        nome_curso = _curso_nome(m["curso"]) if m["curso"] else None
        if nome_curso:
            cursos[nome_curso] += 1
        nome_polo = _polo_nome(m["polo"]) if m["polo"] else None
        if nome_polo:
            polos[nome_polo] += 1
        info = _gclid_cache.get(m["gclid"])
        if info:
            c = slot(info["utm_campaign"])
            c["matriculas"] += 1
            c[f"matriculas_{nivel}"] += 1

    campanhas = list(camp.values())
    for c in campanhas:
        c["leads"] = c["novos"] + c["ganhos"] + c["perdidos"]
        c["conv_matricula_pct"] = _pct(c["matriculas"], c["leads"])

    return {
        "campaigns": campanhas,
        "insights": {
            "kpis": {
                "leads": len(leads),
                "linhas_brutas": len(brutos),
                "repetidos": descartados.get("repetido", 0),
                "sem_sinal_google": descartados.get("sem_sinal_google", 0),
                "ganhos": tot["ganho"],
                "perdidos": tot["perdido"],
                "abertos": tot["aberto"] + tot["sem_negocio"],
                "sem_negocio": tot["sem_negocio"],
                "matriculas": n_google,
                "conv_pct": _pct(n_google, len(leads)),
            },
            "top_cursos": [{"nome": k, "n": v} for k, v in cursos.most_common(6)],
            "top_polos": [{"nome": k, "n": v} for k, v in polos.most_common(6)],
            "top_campanhas": sorted(
                [{"nome": c["utm_campaign"], "leads": c["leads"], "matriculas": c["matriculas"],
                  "conv_pct": c["conv_matricula_pct"]} for c in campanhas if c["matriculas"]],
                key=lambda x: (-x["matriculas"], -x["conv_pct"]))[:6],
        },
    }


def google_campaigns(from_date: str, to_date: str) -> dict:
    key = (from_date, to_date)
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < _TTL:
            return hit[1]
        lock = _period_locks.setdefault(key, threading.Lock())
    with lock:  # requests simultâneos do mesmo período montam uma vez só
        with _cache_lock:
            hit = _cache.get(key)
            if hit and time.time() - hit[0] < _TTL:
                return hit[1]
        data = _build(from_date, to_date)
        with _cache_lock:
            _cache[key] = (time.time(), data)
            for k in [k for k, (t, _) in _cache.items() if time.time() - t > _TTL]:
                _cache.pop(k, None)
        return data
