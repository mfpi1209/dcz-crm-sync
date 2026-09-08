"""
eduit. â€” Dashboard Comercial.

Upload de CSV de matrículas (Power BI), integração com dados do Match & Merge,
ranking de agentes comerciais via Kommo, e dashboard com KPIs e comparativos.

Endpoints:
  POST /api/comercial-rgm/upload        upload CSV e importa para o banco
  GET  /api/comercial-rgm/data          dados filtrados (KPIs + evolução + ranking)
  GET  /api/comercial-rgm/filters       listas de polos, níveis e agentes
  GET  /api/comercial-rgm/snapshot-info info do último upload
  POST /api/comercial-rgm/sync-users    sincroniza usuários do Kommo
"""

import os
import csv
from collections import defaultdict
import io
import logging
import re
import threading
import time
import requests
from datetime import datetime, date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, session, g, has_request_context

from helpers import normalize_polo_display

from services.atividade_kommo import (
    horas_media_por_consultor,
    fetch_atividade_periodo,
)

logger = logging.getLogger(__name__)

comercial_rgm_bp = Blueprint("comercial_rgm", __name__)

MM_TIPO_MAT_VALIDOS = (
    'INGRESSANTE', 'NOVA MATRICULA', 'NOVA MATRÃCULA', 'RETORNO', 'RECOMPRA'
)

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)

KOMMO_DB_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)

KOMMO_BASE_URL = os.getenv("KOMMO_BASE_URL", "https://eduitbr.kommo.com").rstrip("/")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")
# Campo RGM no Kommo (custom field id) â€” usado na busca por RGM na API
KOMMO_RGM_FIELD_ID = int(os.getenv("KOMMO_RGM_FIELD_ID", "31776"))


def _kommo_api_v4() -> str:
    b = KOMMO_BASE_URL.rstrip("/")
    return b if b.endswith("/api/v4") else f"{b}/api/v4"


def _kommo_uid_int(v):
    """IDs de usuário Kommo: o ranking usa int; o PG pode devolver tipos mistos."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _kommo_mini_sync_lead_flask(lead_id: int) -> tuple[dict | None, str | None]:
    """
    Mesmo pipeline que `python kommo_lib/sync_one_lead.py <id>`:
    KommoAPIClient -> SQLite (kommo_lib) + PostgreSQL kommo_sync.
    """
    import sys
    from pathlib import Path

    kl = Path(__file__).resolve().parents[1] / "kommo_lib"
    p = str(kl)
    if p not in sys.path:
        sys.path.insert(0, p)
    from sync_one_lead import mini_sync_lead

    return mini_sync_lead(lead_id)


def _crgm_conflito_overrides() -> dict:
    """Mapa rgm normalizado → user_id de comercial_rgm_conflito_resolucao."""
    out = {}
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT rgm, user_id FROM comercial_rgm_conflito_resolucao")
        for rgm_raw, uid in cur.fetchall():
            nk = _normalize_rgm(rgm_raw)
            if nk and uid:
                out[nk] = int(uid)
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("conflito_resolucao load: %s", e)
    return out


def _apply_conflito_overrides_to_rgm_map(rgm_to_uid: dict) -> None:
    """Sobrescreve rgm→uid com resolução manual / mini-sync (in-place)."""
    for nk, uid in _crgm_conflito_overrides().items():
        rgm_to_uid[nk] = uid


def _pin_rgm_attribution(rgm: str, user_id: int, user_name: str = "", resolved_by: str = "mini_sync") -> bool:
    """Fixa o crédito da venda neste RGM para o responsável do lead sincronizado.

    Sobrevive ao próximo sync e tira o RGM de qualquer outro 142 que o
    DISTINCT ON (142 + id DESC) ainda escolheria.
    """
    nk = _normalize_rgm(rgm)
    uid = _kommo_uid_int(user_id)
    if not nk or len(nk) != 8 or not uid:
        return False
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO comercial_rgm_conflito_resolucao
                (rgm, user_id, user_name, resolved_at, resolved_by)
            VALUES (%s, %s, %s, NOW(), %s)
            ON CONFLICT (rgm) DO UPDATE
              SET user_id = EXCLUDED.user_id,
                  user_name = EXCLUDED.user_name,
                  resolved_at = NOW(),
                  resolved_by = EXCLUDED.resolved_by
            """,
            (nk, uid, user_name or None, resolved_by),
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("mini_sync pin RGM %s → uid=%s (%s) by=%s", nk, uid, user_name, resolved_by)
        return True
    except Exception as e:
        logger.warning("pin_rgm_attribution rgm=%s: %s", nk, e)
        return False


def _kommo_sibling_lead_ids_for_rgm(rgm_clean: str, except_id: int) -> list[int]:
    """Outros leads no kommo_sync com o mesmo RGM (pra refrescar e não deixar crédito velho)."""
    nk = _normalize_rgm(rgm_clean)
    if not nk or len(nk) != 8:
        return []
    try:
        found, _err = _kommo_resolve_lead_id_by_rgm(nk)
        return [i for i in (found or []) if i and int(i) != int(except_id)]
    except Exception as e:
        logger.warning("sibling leads rgm=%s: %s", nk, e)
        return []


def _kommo_resolve_lead_id_by_rgm(rgm_clean: str) -> tuple[list[int], str | None]:
    """Retorna (lista de lead_ids, None) ou ([], mensagem_erro)."""
    ids: list[int] = []
    try:
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            """
            SELECT DISTINCT id FROM (
                SELECT l.id FROM leads l
                JOIN lead_custom_field_values lcf ON lcf.lead_id = l.id
                  AND lower(lcf.field_name) = 'rgm'
                WHERE length(regexp_replace(COALESCE((lcf.values_json->0)->>'value',''), '[^0-9]', '', 'g')) = 8
                  AND regexp_replace((lcf.values_json->0)->>'value', '[^0-9]', '', 'g') = %s
                UNION
                SELECT l.id FROM leads l,
                     LATERAL jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) x
                WHERE lower(x->>'field_name') = 'rgm'
                  AND length(regexp_replace(COALESCE(x->'values'->0->>'value',''), '[^0-9]', '', 'g')) = 8
                  AND regexp_replace(x->'values'->0->>'value', '[^0-9]', '', 'g') = %s
            ) t ORDER BY id DESC LIMIT 15
            """,
            (rgm_clean, rgm_clean),
        )
        ids = [r[0] for r in cur.fetchall()]
        cur.close()
        kc.close()
    except Exception as e:
        logger.warning("kommo PG busca RGM: %s", e)
    if ids:
        return ids, None
    if not KOMMO_TOKEN:
        return [], "RGM não encontrado na base local. Configure KOMMO_TOKEN para buscar na API."
    api = _kommo_api_v4()
    headers = {
        "Authorization": f"Bearer {KOMMO_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        r = requests.post(
            f"{api}/leads/list",
            headers=headers,
            json={
                "limit": 50,
                "filter": {
                    "custom_fields_values": [
                        {"field_id": KOMMO_RGM_FIELD_ID, "values": [{"value": rgm_clean}]}
                    ]
                },
            },
            timeout=45,
        )
        if r.status_code == 200:
            emb = r.json().get("_embedded", {})
            for L in emb.get("leads", []):
                if L.get("id"):
                    ids.append(int(L["id"]))
            ids = list(dict.fromkeys(ids))
            if ids:
                return ids, None
    except Exception as e:
        logger.warning("Kommo leads/list RGM: %s", e)
    for page in range(1, 9):
        try:
            time.sleep(0.1)
            r = requests.get(
                f"{api}/leads",
                headers={"Authorization": f"Bearer {KOMMO_TOKEN}", "Accept": "application/json"},
                params={"limit": 250, "page": page, "query": rgm_clean},
                timeout=30,
            )
            if r.status_code != 200:
                break
            data = r.json()
            for L in data.get("_embedded", {}).get("leads", []):
                for cf in L.get("custom_fields_values") or []:
                    if str(cf.get("field_name", "")).lower() != "rgm":
                        continue
                    v = re.sub(r"[^0-9]", "", str((cf.get("values") or [{}])[0].get("value", "")))
                    if v == rgm_clean and L.get("id"):
                        ids.append(int(L["id"]))
            ids = list(dict.fromkeys(ids))
            if ids:
                return ids, None
            if "next" not in data.get("_links", {}):
                break
        except Exception as e:
            logger.warning("Kommo leads query page %s: %s", page, e)
            break
    return [], (
        "Não achamos esse RGM na base nem nas primeiras páginas da API. "
        "Use o ID do lead (número após # na URL do Kommo)."
    )


def _pg():
    return psycopg2.connect(**DB_DSN)


# =============================================================================
# Cache backend in-memory para /api/comercial-rgm/data (Fase 3)
# -----------------------------------------------------------------------------
# Memoiza a resposta por combinacao de filtros com TTL curto. Invalidado por:
#   - novos uploads (xl_snapshots de matriculados/inadimplentes etc)
#   - sync Kommo
#   - CRUD de metas (comercial_metas / premiacao_*)
# Quem precisar invalidar manualmente chama clear_crgm_data_cache().
# =============================================================================
import threading as _crgm_threading

_CRGM_DATA_CACHE: dict = {}
_CRGM_DATA_CACHE_VER = 7  # bump quando a lógica de contagem mudar (ex.: outliers / fora_padrao)
_CRGM_DATA_CACHE_TTL_S = 120  # segundos
_CRGM_DATA_CACHE_LOCK = _crgm_threading.Lock()


def _crgm_cache_key_from_args() -> tuple:
    return (
        _CRGM_DATA_CACHE_VER,
        request.args.get("polo", ""),
        request.args.get("nivel", ""),
        request.args.get("dt_ini", ""),
        request.args.get("dt_fim", ""),
        request.args.get("ciclo", ""),
        request.args.get("turma", ""),
    )


def _crgm_cache_get(key: tuple):
    import time as _t
    now = _t.time()
    with _CRGM_DATA_CACHE_LOCK:
        entry = _CRGM_DATA_CACHE.get(key)
        if entry and (now - entry[0]) <= _CRGM_DATA_CACHE_TTL_S:
            return entry[1]
        if entry:
            _CRGM_DATA_CACHE.pop(key, None)
    return None


def _crgm_cache_set(key: tuple, payload):
    import time as _t
    with _CRGM_DATA_CACHE_LOCK:
        _CRGM_DATA_CACHE[key] = (_t.time(), payload)


def _crgm_cache_key_prefixed(prefix: str) -> tuple:
    """Chave de cache prefixada para os sub-endpoints (/data/kpis, /data/agentes, /data/grids)."""
    return (prefix,) + _crgm_cache_key_from_args()


def _crgm_parse_date(val):
    if not val:
        return None
    if isinstance(val, date):
        return val
    s = str(val).strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _crgm_normalize_period(dt_ini, dt_fim):
    """Garante dt_ini <= dt_fim (troca se cadastro em ciclos_comercial estiver invertido)."""
    d0 = _crgm_parse_date(dt_ini)
    d1 = _crgm_parse_date(dt_fim)
    if not d0 or not d1:
        return dt_ini, dt_fim
    if d0 > d1:
        logger.warning("_crgm_normalize_period: intervalo invertido %s > %s — trocando", d0, d1)
        d0, d1 = d1, d0
    return d0.isoformat(), d1.isoformat()


def _crgm_validate_period(dt_inicio, dt_fim):
    d0 = _crgm_parse_date(dt_inicio)
    d1 = _crgm_parse_date(dt_fim)
    if not d0 or not d1:
        return "Datas inválidas (use AAAA-MM-DD)"
    if d0 > d1:
        return "dt_fim deve ser igual ou posterior a dt_inicio"
    return None


def clear_crgm_data_cache(reason: str = ""):
    """API publica para invalidar o cache do dashboard comercial."""
    with _CRGM_DATA_CACHE_LOCK:
        n = len(_CRGM_DATA_CACHE)
        _CRGM_DATA_CACHE.clear()
        _CRGM_CICLO_PFX_CACHE.clear()
    if n:
        logger.info("CRGM /data cache LIMPO (%d entradas) motivo=%s", n, reason or "manual")


@comercial_rgm_bp.after_request
def _crgm_auto_invalidate_cache(response):
    """Invalida automaticamente o cache do /data quando qualquer endpoint do
    blueprint comercial_rgm faz mutacao (POST/PUT/DELETE) com sucesso (2xx).
    Excecoes: o proprio /data e /cache/clear nao acionam invalidacao."""
    try:
        if request.method in ("POST", "PUT", "DELETE", "PATCH") and 200 <= response.status_code < 300:
            path = (request.path or "").rstrip("/")
            if path.endswith("/api/comercial-rgm/data") or path.endswith("/api/comercial-rgm/cache/clear"):
                return response
            if "/api/comercial-rgm/" in path or "/api/dist-consultor/" in path:
                clear_crgm_data_cache(reason=f"{request.method} {path}")
    except Exception:
        pass
    return response


def _crgm_excluded_rgms(_unused=None) -> set:
    """Retorna conjunto de RGMs normalizados cujo registro mais recente (maior id)
    no snapshot atual NÃƒO é EM CURSO. Usa conexão própria para não contaminar
    transações do chamador em caso de erro."""
    _conn = None
    try:
        _conn = _pg()
        _cur = _conn.cursor()
        _cur.execute("""
            SELECT
                regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g') AS rgm,
                UPPER(TRIM(COALESCE(r.data->>'situacao','')))                   AS situacao
            FROM (
                SELECT DISTINCT ON (regexp_replace(COALESCE(r2.data->>'rgm',''), '[^0-9]', '', 'g'))
                    r2.data,
                    r2.id
                FROM xl_rows r2
                JOIN xl_snapshots s ON s.id = r2.snapshot_id
                WHERE s.id = (
                    SELECT id FROM xl_snapshots WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
                )
                  AND COALESCE(r2.data->>'rgm','') ~ '[0-9]'
                ORDER BY
                    regexp_replace(COALESCE(r2.data->>'rgm',''), '[^0-9]', '', 'g'),
                    -- Em transferências internas, prioriza linha EM CURSO
                    -- sobre TRANSFERIDO/CANCELADO para o aluno não ser excluído.
                    CASE
                        WHEN UPPER(TRIM(COALESCE(r2.data->>'situacao',''))) = 'EM CURSO' THEN 0
                        WHEN UPPER(TRIM(COALESCE(r2.data->>'situacao',''))) IN ('TRANCADO','SEM EVOLUCAO','SEM EVOLUÇÃO') THEN 1
                        ELSE 2
                    END,
                    r2.id DESC
            ) r
            WHERE UPPER(TRIM(COALESCE(r.data->>'situacao',''))) != 'EM CURSO'
        """)
        excluded = set()
        for rgm_raw, _ in _cur.fetchall():
            n = _normalize_rgm(rgm_raw)
            if n:
                excluded.add(n)
        _cur.close()
        return excluded
    except Exception as e:
        logger.warning("_crgm_excluded_rgms: %s", e)
        return set()
    finally:
        if _conn:
            try:
                _conn.close()
            except Exception:
                pass


def _crgm_dashboard_rgm_list(dt_ini: str, dt_fim: str,
                              polo: str = None, nivel: str = None) -> list[str]:
    """Retorna lista de RGMs únicos para o período, alinhada a Matrículas Oficiais
    (all_snapshots=True, situacao_filter='EM CURSO'). Garante que o total bate com
    o ranking e KPIs do Dashboard Comercial após a correção de ciclo."""
    dt_ini, dt_fim = _crgm_normalize_period(dt_ini, dt_fim)
    try:
        conn = _pg()
        excluded = _crgm_excluded_rgms(conn)
        rows = _crgm_periodo_data_oficial(
            dt_ini=dt_ini, dt_fim=dt_fim,
            polo=polo, nivel=nivel, conn=conn,
            situacao_filter="EM CURSO",
        )
        conn.close()

        seen: set[str] = set()
        rgm_list: list[str] = []
        for row in rows:
            n = _normalize_rgm(row.get("rgm"))
            if n and n not in seen and n not in excluded:
                seen.add(n)
                rgm_list.append(n)

        # Overrides de conflito manuais (mesmos do Dashboard Comercial)
        # — não afetam a lista de RGMs, só a atribuição ao usuário; retornamos
        #   a lista bruta para que os endpoints façam o mapeamento próprio.
        return rgm_list
    except Exception as e:
        logger.warning("_crgm_dashboard_rgm_list: %s", e)
        return []


_LATEST_RGMS_CACHE = {"snapshot_id": None, "rgms": set()}
_LATEST_RGMS_LOCK = threading.Lock()


def _crgm_latest_snapshot_rgms(conn=None):
    """RGMs normalizados presentes no snapshot 'matriculados' mais recente.

    Cacheado por snapshot_id: a varredura só refaz quando entra um upload novo.
    Sem isso, cada agente do ranking repetiria o scan do snapshot inteiro.
    """
    _conn = None
    _own = conn is None
    try:
        _conn = conn if conn is not None else _pg()
        with _conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM xl_snapshots WHERE tipo = 'matriculados' "
                "ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return set()
            snap_id = row[0]

            with _LATEST_RGMS_LOCK:
                if _LATEST_RGMS_CACHE["snapshot_id"] == snap_id:
                    return _LATEST_RGMS_CACHE["rgms"]

            cur.execute("""
                SELECT DISTINCT regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g')
                FROM xl_rows r
                WHERE r.snapshot_id = %s
                  AND COALESCE(r.data->>'rgm','') ~ '[0-9]'
            """, (snap_id,))
            rgms = {r[0] for r in cur.fetchall() if r[0]}

        with _LATEST_RGMS_LOCK:
            _LATEST_RGMS_CACHE["snapshot_id"] = snap_id
            _LATEST_RGMS_CACHE["rgms"] = rgms
        return rgms
    except Exception as e:
        logger.warning("_crgm_latest_snapshot_rgms: %s", e)
        return set()
    finally:
        if _conn is not None and _own:
            try:
                _conn.close()
            except Exception:
                pass


def _crgm_periodo_data(
    dt_ini=None, dt_fim=None, polo=None, nivel=None, ciclo_filter=None, turma=None,
    conn=None, *, all_snapshots=False, situacao_filter=None, require_latest_presence=False,
    mark_missing_as_transferido=False,
):
    """
    Retorna TODOS os RGMs únicos do período (registro mais recente por id),
    aplicando filtros de tipo_matricula, empresa e ciclo, mas SEM filtro de situação
    por padrão.
    Retorna lista de dicts: {rgm, nome, situacao, data_matricula, polo, nivel, ciclo}

    Quando `conn` é fornecida, reutiliza a conexão (não abre/fecha). Caso contrário,
    abre uma conexão própria via _pg() e fecha ao final.

    Parâmetros adicionais:
    - all_snapshots (bool): se True, lê TODOS os snapshots 'matriculados' em vez de
      apenas o mais recente, espelhando o dedupe de _fetch_agent_matriculas (Matrículas
      Oficiais). Garante que alunos presentes em snapshots intermediários mas ausentes
      do snapshot mais recente sejam incluídos. Padrão: False (comportamento anterior).
    - situacao_filter (str | None): se fornecido (ex. 'EM CURSO'), filtra a camada
      deduplicada pela situação exata (UPPER). Padrão: None (sem filtro de situação).
    - require_latest_presence (bool): quando True, mantém apenas RGMs ainda presentes
      no CSV mais recente. Evita que alunos que sumiram do relatório atual (ex. transferência
      de polo) continuem contando via last-seen de snapshots antigos.
    - mark_missing_as_transferido (bool): mesma detecção do anterior, mas em vez de
      descartar quem sumiu do CSV atual, marca a situação como TRANSFERIDO. Sai do
      EM CURSO (não conta venda) e entra na evasão.
    - Bypass de ciclo com datas: quando dt_ini ou dt_fim estão presentes, o recorte por
      ciclo_atual_comercial NÃO é aplicado — o filtro de data é suficiente. Com
      ciclo_filter explícito, aplica filtro textual de ciclo SIAA.
    """
    dt_ini, dt_fim = _crgm_normalize_period(dt_ini, dt_fim)
    _conn = None
    _own_conn = conn is None
    try:
        _conn = conn if conn is not None else _pg()
        cur = _conn.cursor()

        # Filtros extras aplicados na camada deduplicated
        outer_conds = []
        params = []

        if dt_ini:
            outer_conds.append("data_matricula >= %s")
            params.append(dt_ini)
        if dt_fim:
            outer_conds.append("data_matricula <= %s")
            params.append(dt_fim)
        if polo:
            pass  # filtro por nome canônico após fetch
        if nivel:
            outer_conds.append("nivel = %s")
            params.append(nivel)
        if turma:
            outer_conds.append("turma = %s")
            params.append(turma)
        if ciclo_filter:
            outer_conds.append("ciclo = %s")
            params.append(ciclo_filter)
        elif not dt_ini and not dt_fim:
            # Sem intervalo explícito: escopo = ciclo(s) comercial(is) atuais.
            # Com dt_ini/dt_fim (ex.: comparativo 6m/1a), o recorte é só pela data.
            outer_conds.append(
                "ciclo IN (SELECT ciclo FROM ciclo_atual_comercial)"
            )
        if situacao_filter:
            outer_conds.append("situacao = %s")
            params.append(situacao_filter.upper())

        outer_where = ("WHERE " + " AND ".join(outer_conds)) if outer_conds else ""

        # A checagem de presença no CSV atual é feita em Python (um SELECT no snapshot
        # mais recente), não com EXISTS correlacionado — este varria o histórico inteiro
        # em JSONB e travava as rotas por minutos.
        check_latest = require_latest_presence or mark_missing_as_transferido
        if all_snapshots:
            snapshot_cond = "s.tipo = 'matriculados'"
            # s.id DESC garante que, no DISTINCT ON, o snapshot mais recente tem
            # prioridade quando dois snapshots contêm o mesmo RGM — espelhando o
            # ORDER BY s.id DESC de _fetch_agent_matriculas.
            snapshot_order = "s.id DESC,"
        else:
            snapshot_cond = "s.id = (SELECT id FROM xl_snapshots WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1)"
            snapshot_order = ""

        sql = f"""
            SELECT rgm, nome, situacao, data_matricula, polo, nivel, ciclo, tipo_matricula
            FROM (
                SELECT DISTINCT ON (regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'))
                    regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g')  AS rgm,
                    NULLIF(TRIM(COALESCE(r.data->>'nome','')), '')                  AS nome,
                    UPPER(TRIM(COALESCE(r.data->>'situacao','')))                   AS situacao,
                    UPPER(TRIM(COALESCE(r.data->>'tipo_matricula','')))             AS tipo_matricula,
                    CASE
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                            THEN to_date(r.data->>'data_mat','DD/MM/YYYY')
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN (r.data->>'data_mat')::date
                        ELSE NULL
                    END AS data_matricula,
                    CASE
                        WHEN COALESCE(r.data->>'nivel','')   ~* 'p[oó]s'                                        THEN 'Pós-Graduação'
                        WHEN COALESCE(r.data->>'negocio','') ~* 'p[oó]s'                                        THEN 'Pós-Graduação'
                        WHEN COALESCE(r.data->>'curso','')   ~* '(mba|especializa|p.s.gradua|lato.sensu|stricto)' THEN 'Pós-Graduação'
                        ELSE 'Graduação'
                    END AS nivel,
                    TRIM(regexp_replace(COALESCE(r.data->>'polo',''), '^[0-9]+\\s*[-]\\s*', '')) AS polo,
                    NULLIF(TRIM(COALESCE(r.data->>'ciclo','')), '')                AS ciclo,
                    NULLIF(TRIM(COALESCE(r.data->>'curso','')), '')                AS turma
                FROM xl_rows r
                JOIN xl_snapshots s ON s.id = r.snapshot_id
                WHERE {snapshot_cond}
                  AND COALESCE(r.data->>'rgm','') ~ '[0-9]'
                  AND UPPER(TRIM(COALESCE(r.data->>'tipo_matricula','')))
                      = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])
                  AND TRIM(COALESCE(r.data->>'empresa','')) ~ '^(12|7) -'
                ORDER BY
                    regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'),
                    {snapshot_order}
                    -- Em transferências internas o aluno aparece 2x: prioriza
                    -- a linha que ainda está EM CURSO sobre TRANSFERIDO/CANCELADO.
                    CASE
                        WHEN UPPER(TRIM(COALESCE(r.data->>'situacao',''))) = 'EM CURSO' THEN 0
                        WHEN UPPER(TRIM(COALESCE(r.data->>'situacao',''))) IN ('TRANCADO','SEM EVOLUCAO','SEM EVOLUÇÃO') THEN 1
                        ELSE 2
                    END,
                    -- Desempate: matrícula mais recente.
                    CASE
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                            THEN to_date(r.data->>'data_mat','DD/MM/YYYY')
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN (r.data->>'data_mat')::date
                        ELSE NULL
                    END DESC NULLS LAST,
                    r.id DESC
            ) deduped
            {outer_where}
        """

        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()

        result = []
        for rgm, nome, situacao, dm, polo_v, nivel_v, ciclo_v, tipo_v in rows:
            if not rgm:
                continue
            try:
                dt_str = dm.isoformat() if hasattr(dm, "isoformat") else str(dm)[:10]
            except Exception:
                dt_str = None
            result.append({
                "rgm": rgm,
                "nome": nome or "",
                "situacao": situacao or "",
                "data_matricula": dt_str,
                "polo": polo_v or "",
                "nivel": nivel_v or "",
                "ciclo": ciclo_v or "",
                "tipo_matricula": tipo_v or "",
            })
        if polo:
            result = [r for r in result if normalize_polo_display(r.get("polo") or "") == polo]

        if check_latest:
            latest_rgms = _crgm_latest_snapshot_rgms(_conn)
            if latest_rgms:
                if mark_missing_as_transferido:
                    for r in result:
                        if r["rgm"] not in latest_rgms:
                            # Sumir do relatório atual = transferência para outro polo.
                            r["situacao"] = "TRANSFERIDO"
                            r["sumiu_do_csv"] = True
                    # situacao_filter foi aplicado no SQL, antes da marcação.
                    if situacao_filter:
                        alvo = situacao_filter.upper()
                        result = [r for r in result if r["situacao"] == alvo]
                else:
                    result = [r for r in result if r["rgm"] in latest_rgms]
        return result

    except Exception as e:
        logger.warning("_crgm_periodo_data: %s", e)
        return []
    finally:
        if _conn and _own_conn:
            try:
                _conn.close()
            except Exception:
                pass


def _crgm_periodo_data_oficial(
    dt_ini=None, dt_fim=None, polo=None, nivel=None,
    ciclo_filter=None, turma=None, conn=None,
    situacao_filter=None, require_latest_presence=False,
    mark_missing_as_transferido=False,
):
    """Universo alinhado a Matrículas Oficiais: usa all_snapshots=True para garantir
    que RGMs presentes em qualquer snapshot 'matriculados' (não apenas o mais recente)
    sejam incluídos. Quando há dt_ini ou dt_fim, o filtro por ciclo_atual_comercial é
    bypassado automaticamente (já existia em _crgm_periodo_data). Use este helper em
    todas as funções que devem bater com o total de Matrículas Oficiais.
    """
    return _crgm_periodo_data(
        dt_ini=dt_ini, dt_fim=dt_fim, polo=polo, nivel=nivel,
        ciclo_filter=ciclo_filter, turma=turma, conn=conn,
        all_snapshots=True,
        situacao_filter=situacao_filter,
        require_latest_presence=require_latest_presence,
        mark_missing_as_transferido=mark_missing_as_transferido,
    )


def comercial_periodo_vendas_resumo(dt_ini=None, dt_fim=None, polo=None, nivel=None):
    """
    Mesma agregação de /api/comercial-rgm/data (KPI vendas bruto + evolução EM CURSO).
    Usado pelo painel Suporte Comercial em Minha Performance.
    """
    rows = _crgm_periodo_data(dt_ini=dt_ini, dt_fim=dt_fim, polo=polo, nivel=nivel) or []
    rgms_periodo = set()
    rgms_bruto = set()
    day_rgms = defaultdict(set)
    day_rgms_bruto = defaultdict(set)
    for row in rows:
        n = row.get("rgm")
        if not n:
            continue
        rgms_bruto.add(n)
        dm = row.get("data_matricula")
        dt = None
        if dm:
            try:
                dt = date.fromisoformat(str(dm)[:10])
            except (ValueError, TypeError):
                dt = None
        if dt:
            day_rgms_bruto[dt].add(n)
        if row.get("situacao") == "EM CURSO":
            rgms_periodo.add(n)
            if dt:
                day_rgms[dt].add(n)

    # Filtrar outliers das contagens (mesmo critério do Dashboard Comercial)
    _vr_dom_pfx   = _crgm_effective_dominant_prefix(list(rgms_periodo) or list(rgms_bruto))
    _vr_overrides = _load_outlier_contagem_overrides()
    _rgms_contando = {r for r in rgms_periodo if _rgm_conta_para_venda(r, _vr_dom_pfx, _vr_overrides)}
    day_rgms_contando = {d: s & _rgms_contando for d, s in day_rgms.items()}
    day_rgms_contando = {d: s for d, s in day_rgms_contando.items() if s}

    day_counts = {d: len(s) for d, s in day_rgms_contando.items()}
    dias = len(day_counts) or 1
    vendas = len(rgms_bruto)
    vendas_liquidas = len(_rgms_contando)
    media_diaria = round(vendas_liquidas / dias, 1) if dias else 0
    evolucao = [{"data": d.isoformat(), "count": c} for d, c in sorted(day_counts.items())]
    return {
        "vendas": vendas,
        "vendas_liquidas": vendas_liquidas,
        "media_diaria": media_diaria,
        "dias": dias,
        "evolucao": evolucao,
        "mat_by_date": day_counts,
    }


def _pg_kommo():
    return psycopg2.connect(**KOMMO_DB_DSN)


# â”€â”€ Schema â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS comercial_rgm (
    id              SERIAL PRIMARY KEY,
    rgm             TEXT,
    polo            TEXT,
    nivel           TEXT,
    modalidade      TEXT,
    data_matricula  DATE,
    ciclo           TEXT,
    turma           TEXT,
    financeiro      TEXT,
    valor_real      NUMERIC(12,2),
    mes_pagamento   TEXT,
    tipo_pagamento  TEXT,
    uploaded_at     TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_crgm_data  ON comercial_rgm(data_matricula);
CREATE INDEX IF NOT EXISTS idx_crgm_polo  ON comercial_rgm(polo);
CREATE INDEX IF NOT EXISTS idx_crgm_nivel ON comercial_rgm(nivel);

CREATE TABLE IF NOT EXISTS kommo_users (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    email       TEXT,
    synced_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mm_snapshots (
    id               SERIAL PRIMARY KEY,
    snapshot_id      TEXT NOT NULL,
    executed_at      TIMESTAMP DEFAULT NOW(),
    nivel            TEXT,
    total_inscritos  INTEGER,
    total_matriculados INTEGER,
    total_cruzados   INTEGER
);

CREATE TABLE IF NOT EXISTS mm_inscritos_hist (
    id SERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    tipo TEXT, status TEXT, dt_pag_insc TEXT, inscricao TEXT,
    nome TEXT, sexo TEXT, cpf TEXT, rg TEXT,
    curso_raw TEXT, curso_limpo TEXT, grau_curso TEXT, modalidade TEXT,
    polo_raw TEXT, polo_normalizado TEXT, marca_instituicao TEXT,
    data_inscr DATE, data_prova DATE,
    telefone TEXT, telefone_res TEXT, telefone_com TEXT,
    email TEXT, cep TEXT, endereco TEXT, bairro TEXT, cidade TEXT, estado TEXT,
    data_pagamento TEXT, data_matricula TEXT,
    situacao_raw TEXT, situacao_final TEXT,
    observacao TEXT, captador TEXT, trimestre_ingresso TEXT,
    chave_preco TEXT, preco_balcao TEXT, area_curso TEXT, semestres TEXT,
    arquivo_origem TEXT, uploaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mmih_snap ON mm_inscritos_hist(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_mmih_cpf  ON mm_inscritos_hist(cpf);
CREATE INDEX IF NOT EXISTS idx_mmih_data ON mm_inscritos_hist(data_inscr);

CREATE TABLE IF NOT EXISTS mm_matriculados_hist (
    id SERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    tipo TEXT, nome TEXT, cpf TEXT, rgm TEXT, rg TEXT, sexo TEXT, data_nasc TEXT,
    polo_captador TEXT, tipo_polo TEXT, polo_aulas TEXT,
    curso_raw TEXT, curso_limpo TEXT,
    prouni TEXT, serie TEXT, data_matricula TEXT, ano_tri_ingresso TEXT,
    tipo_matricula TEXT, situacao_raw TEXT, situacao TEXT,
    fone_res TEXT, fone_com TEXT, fone_cel TEXT, email TEXT, email_ad TEXT,
    endereco TEXT, bairro TEXT, cidade TEXT,
    arquivo_origem TEXT, uploaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mmhm_snap ON mm_matriculados_hist(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_mmhm_cpf  ON mm_matriculados_hist(cpf);

CREATE INDEX IF NOT EXISTS idx_mmhm_data ON mm_matriculados_hist(data_matricula);
"""

METAS_CATEGORIAS = [
    {"id": "matriculas",  "label": "Matrículas"},
    {"id": "inscricoes",  "label": "Inscrições"},
    {"id": "valor",       "label": "Valor vendido (R$)"},
    {"id": "novos_leads", "label": "Novos leads"},
    {"id": "conversao",   "label": "Taxa conversão (%)"},
]

_METAS_DDL = """
CREATE TABLE IF NOT EXISTS comercial_metas (
    id                  SERIAL PRIMARY KEY,
    user_id             INTEGER NOT NULL,
    user_name           TEXT,
    meta                NUMERIC NOT NULL DEFAULT 0,
    meta_intermediaria  NUMERIC NOT NULL DEFAULT 0,
    supermeta           NUMERIC NOT NULL DEFAULT 0,
    categoria           TEXT NOT NULL DEFAULT 'matriculas',
    dt_inicio           DATE NOT NULL,
    dt_fim              DATE NOT NULL,
    descricao           TEXT DEFAULT '',
    created_at          TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cm_user ON comercial_metas(user_id);
CREATE INDEX IF NOT EXISTS idx_cm_dates ON comercial_metas(dt_inicio, dt_fim);
CREATE INDEX IF NOT EXISTS idx_cm_cat ON comercial_metas(categoria);
"""


def _ensure_table():
    conn = _pg()
    cur = conn.cursor()
    cur.execute(_CREATE_SQL)
    conn.commit()

    # Migrate comercial_metas
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'comercial_metas' AND column_name = 'dt_inicio'
        """)
        has_dt_inicio = cur.fetchone() is not None

        if not has_dt_inicio:
            cur.execute("DROP TABLE IF EXISTS comercial_metas CASCADE")
            conn.commit()

        cur.execute(_METAS_DDL)
        conn.commit()

        # Add missing columns incrementally
        for col, defn in [
            ("categoria", "TEXT NOT NULL DEFAULT 'matriculas'"),
            ("meta_intermediaria", "NUMERIC NOT NULL DEFAULT 0"),
            ("supermeta", "NUMERIC NOT NULL DEFAULT 0"),
        ]:
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'comercial_metas' AND column_name = %s
            """, (col,))
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE comercial_metas ADD COLUMN {col} {defn}")
                conn.commit()
                logger.info("comercial_metas: added '%s' column", col)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_cm_cat ON comercial_metas(categoria)")
        conn.commit()

        # Tabela de resolucoes de conflito de atribuicao de RGMs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS comercial_rgm_conflito_resolucao (
                rgm         TEXT PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                user_name   TEXT,
                resolved_at TIMESTAMPTZ DEFAULT NOW(),
                resolved_by TEXT DEFAULT 'manual'
            )
        """)
        conn.commit()

        # Tabela de override: RGMs outlier (prefixo abaixo do dominante) marcados manualmente
        cur.execute("""
            CREATE TABLE IF NOT EXISTS comercial_rgm_outlier_contagem (
                rgm         TEXT PRIMARY KEY,
                counted_at  TIMESTAMPTZ DEFAULT NOW(),
                counted_by  TEXT
            )
        """)
        conn.commit()

        # Ajustes de consultor no nível do dashboard (renomear / ocultar / excluir).
        # Não mexe no Kommo (que é a fonte de verdade e reverteria no sync):
        #   display_name -> renomeia o consultor só na exibição do dashboard
        #   hidden       -> some do ranking e do filtro AGENTE
        #   reassign_to  -> "excluído": leads/matrículas deste uid passam a contar
        #                   para o uid destino (Admin Sistema) em todo o dashboard
        cur.execute("""
            CREATE TABLE IF NOT EXISTS comercial_consultor_ajuste (
                kommo_user_id INTEGER PRIMARY KEY,
                display_name  TEXT,
                hidden        BOOLEAN NOT NULL DEFAULT FALSE,
                reassign_to   INTEGER,
                updated_at    TIMESTAMPTZ DEFAULT NOW(),
                updated_by    TEXT
            )
        """)
        conn.commit()

        # Ensure unique constraint for batch upsert
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uq_cm_user_period_cat'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE comercial_metas
                ADD CONSTRAINT uq_cm_user_period_cat
                UNIQUE (user_id, dt_inicio, dt_fim, categoria)
            """)
            conn.commit()
            logger.info("comercial_metas: added unique constraint uq_cm_user_period_cat")

        # Ensure meta column is NUMERIC
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_name = 'comercial_metas' AND column_name = 'meta'
        """)
        row = cur.fetchone()
        if row and row[0] == 'integer':
            cur.execute("ALTER TABLE comercial_metas ALTER COLUMN meta TYPE NUMERIC")
            conn.commit()
            logger.info("comercial_metas: changed 'meta' to NUMERIC")

    except Exception as e:
        conn.rollback()
        logger.warning("comercial_metas migration: %s", e)
        try:
            cur.execute("DROP TABLE IF EXISTS comercial_metas CASCADE")
            conn.commit()
            cur.execute(_METAS_DDL)
            conn.commit()
        except Exception as e2:
            conn.rollback()
            logger.error("comercial_metas create failed: %s", e2)

    cur.close()
    conn.close()


try:
    _ensure_table()
except Exception as _e:
    logger.warning("comercial_rgm: could not ensure tables at startup: %s", _e)


# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _normalize_polo(polo: str) -> str:
    """Remove prefixo numérico e 'CEB ' do nome do polo para unificar duplicatas."""
    if not polo:
        return polo
    # Remove código numérico inicial: '1876 - ' ou '43 - '
    p = re.sub(r'^\d+\s*[-—]\s*', '', polo.strip())
    # Remove prefixo 'CEB ': 'CEB POLO SP_...' → 'POLO SP_...'
    p = re.sub(r'^CEB\s+', '', p, flags=re.IGNORECASE)
    return p.strip()


# Expressão SQL que normaliza a coluna polo da mesma forma que _normalize_polo()
_POLO_SQL = "regexp_replace(regexp_replace(polo, E'^\\\\d+\\\\s*[-\\u2013]\\\\s*', ''), '^CEB\\s+', '', 'i')"


def _normalize_rgm(val):
    """Normalize RGM: strip non-digits, remove leading zeros."""
    if not val:
        return None
    digits = re.sub(r"\D", "", str(val))
    if not digits:
        return None
    try:
        return str(int(digits))
    except ValueError:
        return digits


def _compute_dominant_rgm_prefix(rgms):
    """Retorna o prefixo de 2 dígitos mais frequente entre os RGMs fornecidos (como int), ou None."""
    from collections import Counter
    c = Counter()
    for rgm in rgms:
        n = _normalize_rgm(rgm) if not isinstance(rgm, str) or not rgm.isdigit() else rgm
        if n and len(n) >= 2 and n[:2].isdigit():
            c[n[:2]] += 1
    if not c:
        return None
    return int(c.most_common(1)[0][0])


def _is_rgm_prefix_outlier(rgm, dominant_prefix):
    """Retorna True se o prefixo do RGM for MENOR que o prefixo dominante."""
    if dominant_prefix is None:
        return False
    n = _normalize_rgm(rgm)
    if not n or len(n) < 2 or not n[:2].isdigit():
        return False
    return int(n[:2]) < dominant_prefix


_CRGM_CICLO_PFX_CACHE: dict = {}
_CRGM_CICLO_PFX_TTL_S = 300


def _crgm_ciclo_dominant_prefix():
    """Prefixo dominante apurado sobre o(s) ciclo(s) comercial(is) atual(is) INTEIRO(s).

    Independe de polo/nível/turma e da janela de datas da tela: a série de RGM é
    sequencial na instituição, então recortá-la só adiciona instabilidade.
    Retorna int ou None quando não há dados do ciclo atual.

    Usa conexão própria de propósito: se a consulta falhar, uma conexão emprestada
    ficaria com a transação abortada e derrubaria as queries seguintes do chamador.
    """
    import time as _t
    now = _t.time()
    cached = _CRGM_CICLO_PFX_CACHE.get("v")
    if cached and (now - cached[0]) <= _CRGM_CICLO_PFX_TTL_S:
        return cached[1]

    sql = """
        WITH rgms AS (
            SELECT DISTINCT regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g') AS rgm
            FROM xl_rows r
            JOIN xl_snapshots s ON s.id = r.snapshot_id
            WHERE s.tipo = 'matriculados'
              AND COALESCE(r.data->>'rgm','') ~ '[0-9]'
              AND UPPER(TRIM(COALESCE(r.data->>'situacao',''))) = 'EM CURSO'
              AND UPPER(TRIM(COALESCE(r.data->>'tipo_matricula','')))
                  = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])
              AND TRIM(COALESCE(r.data->>'empresa','')) ~ '^(12|7) -'
              AND NULLIF(TRIM(COALESCE(r.data->>'ciclo','')), '')
                  IN (SELECT ciclo FROM ciclo_atual_comercial)
        )
        SELECT LEFT(rgm, 2) AS pfx, COUNT(*) AS n
        FROM rgms
        WHERE LENGTH(rgm) >= 2 AND LEFT(rgm, 2) ~ '^[0-9]{2}$'
        GROUP BY 1
        -- Empate: vence o prefixo menor (mais inclusivo — não derruba a série anterior).
        ORDER BY n DESC, pfx ASC
        LIMIT 1
    """
    pfx = None
    _c = None
    try:
        _c = _pg()
        with _c.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        if row and row[0]:
            pfx = int(row[0])
    except Exception as e:
        logger.warning("_crgm_ciclo_dominant_prefix: %s", e)
    finally:
        if _c is not None:
            try:
                _c.close()
            except Exception:
                pass

    _CRGM_CICLO_PFX_CACHE["v"] = (now, pfx)
    return pfx


def _crgm_effective_dominant_prefix(periodo_rgms):
    """Régua de outlier: o MENOR entre o dominante do período e o do ciclo atual.

    O dominante do período sozinho quebra na virada da série de RGM (ex.: 49 -> 50):
    numa janela curta a série nova vira maioria e a anterior — que ainda é do ciclo
    corrente — passa a ser tratada como outlier. Usar o mínimo mantém períodos
    históricos com a régua antiga (nada que contava antes deixa de contar).
    """
    periodo_pfx = _compute_dominant_rgm_prefix(periodo_rgms)
    ciclo_pfx = _crgm_ciclo_dominant_prefix()
    candidatos = [p for p in (periodo_pfx, ciclo_pfx) if p is not None]
    return min(candidatos) if candidatos else None


def _load_outlier_contagem_overrides(conn=None):
    """Retorna set de RGMs normalizados que foram marcados pelo admin como 'contar para venda'."""
    out = set()
    _own = conn is None
    try:
        _c = conn if conn is not None else _pg()
        with _c.cursor() as cur:
            cur.execute("SELECT rgm FROM comercial_rgm_outlier_contagem")
            for (r,) in cur.fetchall():
                n = _normalize_rgm(r)
                if n:
                    out.add(n)
        if _own:
            _c.close()
    except Exception as e:
        logger.warning("outlier_contagem_overrides load: %s", e)
    return out


def _rgm_conta_para_venda(rgm, dominant_prefix, overrides):
    """Retorna True se o RGM deve contar para o total de vendas.
    Outliers (prefixo < dominante) só contam se estiverem em overrides (admin 'Contar venda').
    """
    n = _normalize_rgm(rgm)
    if not n:
        return False
    if not _is_rgm_prefix_outlier(n, dominant_prefix):
        return True
    return n in overrides


def _crgm_fora_padrao_rows(_periodo_rows, dominant_prefix, overrides, apenas_nao_conta=True):
    """EM CURSO com prefixo RGM abaixo do dominante (fora do padrão do ciclo)."""
    out = []
    for row in _periodo_rows:
        if row.get("situacao") != "EM CURSO":
            continue
        rgm = row.get("rgm")
        if not rgm or not _is_rgm_prefix_outlier(rgm, dominant_prefix):
            continue
        conta = _rgm_conta_para_venda(rgm, dominant_prefix, overrides)
        if apenas_nao_conta and conta:
            continue
        n = _normalize_rgm(rgm)
        prefix = n[:2] if n and len(n) >= 2 and n[:2].isdigit() else ""
        out.append({
            "rgm": rgm,
            "nome": row.get("nome") or "",
            "polo": row.get("polo") or "",
            "data_matricula": row.get("data_matricula"),
            "prefixo": prefix,
            "conta_para_meta": conta,
            "situacao": row.get("situacao") or "",
        })
    return out


def _crgm_kommo_lookup_rgms(rgms):
    """Mapa rgm normalizado → (user_id, user_name) via Kommo."""
    rgm_to_uid = {}
    uid_to_nome = {}
    lookup = sorted({_normalize_rgm(r) for r in rgms if _normalize_rgm(r)})
    if not lookup:
        return rgm_to_uid, uid_to_nome
    try:
        ek_conn = _pg_kommo()
        ek_cur = ek_conn.cursor()
        ek_cur.execute("""
            SELECT DISTINCT ON (v.rgm) v.rgm, l.responsible_user_id,
                   u.name AS user_name
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE l.responsible_user_id IS NOT NULL
              AND v.rgm = ANY(%s)
            ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
        """, (lookup,))
        for row_k in ek_cur.fetchall():
            nk = _normalize_rgm(row_k[0])
            if nk:
                rgm_to_uid[nk] = row_k[1]
                if row_k[1] and row_k[2]:
                    uid_to_nome[row_k[1]] = row_k[2]
        ek_cur.close()
        ek_conn.close()
    except Exception as ek_e:
        logger.warning("_crgm_kommo_lookup_rgms: %s", ek_e)

    _apply_conflito_overrides_to_rgm_map(rgm_to_uid)

    # Consultores excluídos: reatribui os RGMs para Admin Sistema + renomeações
    _apply_reassign_to_rgm_map(rgm_to_uid)
    _aj = _load_consultor_ajustes()
    for _u, _a in _aj.items():
        if _a.get("display_name"):
            uid_to_nome[_u] = _a["display_name"]
    _missing_names = [t for t in set(_consultor_reassign_map().values()) if t not in uid_to_nome]
    if _missing_names:
        uid_to_nome.update(_fetch_kommo_user_names(_missing_names))
    return rgm_to_uid, uid_to_nome


def _crgm_build_fora_padrao_data(rows, dominant_prefix, rgm_to_uid, uid_to_nome):
    """Pacote fora_padrao no mesmo formato de evasao (total/por_agente/itens)."""
    empty = {
        "total": 0,
        "dominant_prefix": dominant_prefix,
        "por_prefixo": {},
        "por_agente": [],
        "itens": [],
    }
    if not rows:
        return empty
    por_prefixo = defaultdict(int)
    por_agente = defaultdict(list)
    itens = []
    for row in rows:
        px = row.get("prefixo") or "?"
        por_prefixo[px] += 1
        uid = rgm_to_uid.get(_normalize_rgm(row.get("rgm")))
        agente = uid_to_nome.get(uid, "Não identificado") if uid else "Não identificado"
        por_agente[agente].append(row)
        itens.append({**row, "agente": agente})
    return {
        "total": len(rows),
        "dominant_prefix": dominant_prefix,
        "por_prefixo": dict(por_prefixo),
        "por_agente": [
            {
                "agente": ag_nome,
                "total": len(ag_itens),
                "itens": [
                    {
                        "rgm": i["rgm"],
                        "nome": i["nome"],
                        "polo": i.get("polo") or "",
                        "prefixo": i.get("prefixo") or "",
                        "data_matricula": i.get("data_matricula"),
                        "conta_para_meta": i.get("conta_para_meta", False),
                    }
                    for i in ag_itens
                ],
            }
            for ag_nome, ag_itens in sorted(por_agente.items(), key=lambda x: -len(x[1]))
        ],
        "itens": itens,
    }


def crgm_outlier_context(dt_ini=None, dt_fim=None, polo=None, nivel=None, excluded_rgms=None, conn=None):
    """Retorna (dominant_prefix, overrides) usando os RGMs EM CURSO do período.
    dominant_prefix: int ou None
    overrides: set de RGMs normalizados marcados como 'contar venda'
    """
    try:
        rows = _crgm_periodo_data(dt_ini=dt_ini, dt_fim=dt_fim, polo=polo, nivel=nivel, conn=conn)
        em_curso_rgms = [r["rgm"] for r in rows if r.get("situacao") == "EM CURSO"]
        if not em_curso_rgms:
            em_curso_rgms = [r["rgm"] for r in rows]
        dominant_prefix = _crgm_effective_dominant_prefix(em_curso_rgms)
    except Exception as e:
        logger.warning("crgm_outlier_context: %s", e)
        dominant_prefix = None

    _own_conn = conn is None
    overrides = _load_outlier_contagem_overrides(conn=None if _own_conn else conn)
    return dominant_prefix, overrides


def _parse_date_br(s):
    """Parse dd/mm/yyyy or dd/m/yyyy to date object."""
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal_br(s):
    """Parse '33,62' or '1.234,56' to float."""
    if not s or not s.strip():
        return None
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _shift_months(d, months):
    """Desloca uma data por N meses.
    Se d é o último dia do mês, o resultado também é o último dia do mês alvo.
    """
    import calendar
    is_last = d.day == calendar.monthrange(d.year, d.month)[1]
    m = d.month + months
    y = d.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    max_day = calendar.monthrange(y, m)[1]
    return date(y, m, max_day if is_last else min(d.day, max_day))


def _safe_date(year, month, day):
    """Cria date ajustando dia para o máximo do mês (ex: 29/Fev → 28/Fev)."""
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day, max_day))


COL_MAP = {
    "RGM": "rgm",
    "Polo": "polo",
    "Nível": "nivel",
    "N\xedvel": "nivel",
    "Modalidade": "modalidade",
    "Data de Matrícula": "data_matricula",
    "Data de Matr\xedcula": "data_matricula",
    "Ciclo": "ciclo",
    "Turma": "turma",
    "Financeiro": "financeiro",
    "Valor Real": "valor_real",
    "Mês Pagamento": "mes_pagamento",
    "M\xeas Pagamento": "mes_pagamento",
    "Tipo de Pagamento": "tipo_pagamento",
}


def populate_comercial_from_snapshot(snapshot_id=None):
    """Auto-populate comercial_rgm from the latest matriculados snapshot.

    Filters by tipo_matricula (INGRESSANTE, NOVA MATRÃCULA, RETORNO, RECOMPRA)
    and merges with existing comercial_rgm data (new RGMs only).
    Called automatically after a matriculados upload.
    """
    import json
    import unicodedata
    import re as _re

    _POS_RE = _re.compile(r'p[oó]s', _re.IGNORECASE)
    _POS_CURSO_RE = _re.compile(
        r'(mba|especializa.+o|p[oó]s.gradua|lato.sensu|stricto)',
        _re.IGNORECASE,
    )

    def _classify(data):
        if data.get("nivel") and _POS_RE.search(data["nivel"]):
            return "Pós-Graduação"
        if _POS_RE.search(data.get("negocio", "") or ""):
            return "Pós-Graduação"
        if _POS_CURSO_RE.search(data.get("curso", "") or ""):
            return "Pós-Graduação"
        return "Graduação"

    conn = _pg()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT nome, dt_inicio, dt_fim FROM ciclos_comercial
            ORDER BY dt_inicio
        """)
        ciclos = cur.fetchall()

        cur.execute("""
            SELECT nome, nivel, dt_inicio, dt_fim FROM turmas_comercial
            ORDER BY dt_inicio
        """)
        turmas = cur.fetchall()

        def _resolve_ciclo(dt_matricula):
            if not dt_matricula:
                return None
            for nome, dt_ini, dt_end in ciclos:
                if dt_ini <= dt_matricula <= dt_end:
                    return nome
            return None

        def _resolve_turma(dt_matricula, nivel_aluno):
            if not dt_matricula:
                return None
            for nome, nivel_turma, dt_ini, dt_end in turmas:
                if dt_ini <= dt_matricula <= dt_end and nivel_turma == nivel_aluno:
                    return nome
            for nome, nivel_turma, dt_ini, dt_end in turmas:
                if dt_ini <= dt_matricula <= dt_end:
                    return nome
            return None

        if snapshot_id:
            snap_id = snapshot_id
        else:
            cur.execute("""
                SELECT id FROM xl_snapshots
                WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                logger.warning("populate_comercial: no matriculados snapshot found")
                return 0
            snap_id = row[0]

        cur.execute(
            "SELECT data FROM xl_rows WHERE snapshot_id = %s",
            (snap_id,),
        )
        xl_rows = cur.fetchall()
        if not xl_rows:
            return 0

        cur.execute("SELECT rgm FROM comercial_rgm")
        existing_rgms = {r[0] for r in cur.fetchall() if r[0]}

        new_rows = []
        for (data_json,) in xl_rows:
            d = data_json if isinstance(data_json, dict) else json.loads(data_json)

            tipo_mat = (d.get("tipo_matricula") or "").strip().upper()
            if tipo_mat != "INGRESSANTE":
                continue

            situacao = (d.get("situacao") or "").strip().upper()
            if situacao == "TRANSFERIDO":
                continue

            rgm = _normalize_rgm(d.get("rgm") or d.get("rgm_digits"))
            if not rgm or rgm in existing_rgms:
                continue

            raw_date = d.get("data_mat", "")
            dt = _parse_date_br(raw_date) if raw_date else None
            if dt is None and raw_date:
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
                    try:
                        dt = datetime.strptime(raw_date.strip()[:10], fmt).date()
                        break
                    except (ValueError, TypeError):
                        continue

            polo_raw = d.get("polo", "") or ""
            polo = _re.sub(r'^\d+\s*[-—]\s*', '', polo_raw).strip() or None
            nivel = _classify(d)
            modalidade = (d.get("modalidade") or "").strip() or None
            ciclo = _resolve_ciclo(dt)
            turma = _resolve_turma(dt, nivel)

            new_rows.append((rgm, polo, nivel, modalidade, dt, ciclo, turma,
                             None, None, None, None))
            existing_rgms.add(rgm)

        if not new_rows:
            logger.info("populate_comercial: no new commercial records to add")
            return 0

        cols = ["rgm", "polo", "nivel", "modalidade", "data_matricula", "ciclo",
                "turma", "financeiro", "valor_real", "mes_pagamento", "tipo_pagamento"]
        sql = f"INSERT INTO comercial_rgm ({', '.join(cols)}) VALUES %s"
        tpl = "(" + ", ".join(["%s"] * len(cols)) + ")"
        psycopg2.extras.execute_values(cur, sql, new_rows, template=tpl, page_size=2000)
        conn.commit()
        logger.info("populate_comercial: added %d new rows from snapshot %s", len(new_rows), snap_id)
        return len(new_rows)

    except Exception as e:
        conn.rollback()
        logger.exception("populate_comercial error: %s", e)
        return 0
    finally:
        cur.close()
        conn.close()


def _import_csv(stream, encoding="utf-8-sig"):
    """Parse CSV stream and insert rows into comercial_rgm. Returns count."""
    reader = csv.DictReader(stream)

    rows = []
    for raw in reader:
        row = {}
        for csv_col, val in raw.items():
            db_col = COL_MAP.get(csv_col)
            if not db_col:
                continue
            row[db_col] = val
        row["rgm"] = _normalize_rgm(row.get("rgm"))
        if not row["rgm"]:
            continue

        row["data_matricula"] = _parse_date_br(row.get("data_matricula", ""))
        row["valor_real"] = _parse_decimal_br(row.get("valor_real", ""))

        for k in ("polo", "nivel", "modalidade", "ciclo", "turma",
                   "financeiro", "mes_pagamento", "tipo_pagamento"):
            row.setdefault(k, None)
            if row[k] is not None:
                row[k] = row[k].strip() or None

        rows.append(row)

    if not rows:
        return 0

    conn = _pg()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE comercial_rgm RESTART IDENTITY")

    cols = ["rgm", "polo", "nivel", "modalidade", "data_matricula", "ciclo",
            "turma", "financeiro", "valor_real", "mes_pagamento", "tipo_pagamento"]
    sql = f"INSERT INTO comercial_rgm ({', '.join(cols)}) VALUES %s"
    tpl = "(" + ", ".join(["%s"] * len(cols)) + ")"

    values = [tuple(r.get(c) for c in cols) for r in rows]
    psycopg2.extras.execute_values(cur, sql, values, template=tpl, page_size=2000)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("comercial_rgm: imported %d rows", len(rows))
    return len(rows)


# â”€â”€ Endpoints â€” Congelar â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/congelar", methods=["POST"])
def crgm_congelar():
    """Congela dados da view comercial_rgm_atual para a tabela comercial_rgm_congelados
    e avança o ciclo ativo para o próximo.

    Body JSON: { "nivel": "Graduação" | "Pós-Graduação" }
    """
    body = request.json or {}
    nivel = (body.get("nivel") or "").strip()

    if nivel not in ("Graduação", "Pós-Graduação"):
        return jsonify({"error": "Selecione Graduação ou Pós-Graduação"}), 400

    conn = _pg()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT ciclo FROM ciclo_atual_comercial WHERE nivel = %s",
            (nivel,),
        )
        row = cur.fetchone()
        ciclo_atual = row[0] if row else None

        cur.execute(
            "SELECT COUNT(*) FROM comercial_rgm_atual WHERE nivel = %s",
            (nivel,),
        )
        total_source = cur.fetchone()[0]

        if total_source == 0:
            return jsonify({
                "error": f"Nenhum registro de {nivel} no ciclo {ciclo_atual or '?'}"
            }), 400

        cur.execute("""
            INSERT INTO comercial_rgm_congelados
                (rgm, polo, nivel, modalidade, data_matricula, ciclo, turma)
            SELECT rgm, polo, nivel, modalidade, data_matricula, ciclo, turma
            FROM comercial_rgm_atual
            WHERE nivel = %s
              AND rgm NOT IN (
                  SELECT rgm FROM comercial_rgm_congelados WHERE rgm IS NOT NULL
              )
        """, (nivel,))
        inserted = cur.rowcount

        next_ciclo = None
        if ciclo_atual:
            cur.execute("""
                SELECT ciclo FROM (
                    SELECT DISTINCT TRIM(data->>'ciclo') AS ciclo
                    FROM xl_rows r
                    JOIN xl_snapshots s ON s.id = r.snapshot_id
                    WHERE s.id = (SELECT id FROM xl_snapshots
                                  WHERE tipo = 'matriculados'
                                  ORDER BY id DESC LIMIT 1)
                      AND TRIM(data->>'ciclo') SIMILAR TO '\\d{4}/\\d'
                ) sub
                WHERE ciclo > %s
                ORDER BY ciclo
                LIMIT 1
            """, (ciclo_atual,))
            nxt = cur.fetchone()
            if nxt:
                next_ciclo = nxt[0]
                cur.execute(
                    "UPDATE ciclo_atual_comercial SET ciclo = %s WHERE nivel = %s",
                    (next_ciclo, nivel),
                )

        conn.commit()

        logger.info(
            "congelar: %d novos registros congelados (%s ciclo %s). Próximo ciclo: %s",
            inserted, nivel, ciclo_atual, next_ciclo or "nenhum",
        )
        return jsonify({
            "ok": True,
            "nivel": nivel,
            "ciclo_congelado": ciclo_atual,
            "total_view": total_source,
            "congelados": inserted,
            "proximo_ciclo": next_ciclo,
        })
    except Exception as e:
        conn.rollback()
        logger.exception("congelar error")
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@comercial_rgm_bp.route("/api/comercial-rgm/ciclo-atual")
def crgm_ciclo_atual():
    """Returns current active cycle per nivel."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT nivel, ciclo FROM ciclo_atual_comercial ORDER BY nivel")
        rows = cur.fetchall()
        cur.close()
        return jsonify({
            "ok": True,
            "ciclos": {r[0]: r[1] for r in rows},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# â”€â”€ Endpoints â€” Ciclos â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/ciclos")
def crgm_ciclos_list():
    """List all commercial cycles (dimension)."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nome, ano, semestre, dt_inicio, dt_fim, ativo, descricao
            FROM ciclos_comercial ORDER BY dt_inicio DESC
        """)
        rows = cur.fetchall()
        cur.close()
        ciclos = []
        for r in rows:
            di, df = _crgm_normalize_period(r[4].isoformat(), r[5].isoformat())
            ciclos.append({
                "id": r[0], "nome": r[1], "ano": r[2], "semestre": r[3],
                "dt_inicio": di, "dt_fim": df,
                "ativo": r[6], "descricao": r[7],
            })
        return jsonify({"ok": True, "ciclos": ciclos})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/ciclos", methods=["POST"])
def crgm_ciclos_create():
    """Create a new commercial cycle (auto-derives ano, semestre, descricao).

    Also updates ciclo_atual_comercial for the selected nivel(s).
    Body: { nome, nivel ("Graduação"|"Pós-Graduação"|"Ambos"), dt_inicio, dt_fim, ativo }
    """
    body = request.json or {}
    nome = (body.get("nome") or "").strip()
    nivel_target = (body.get("nivel") or "Ambos").strip()
    dt_inicio = body.get("dt_inicio", "")
    dt_fim = body.get("dt_fim", "")
    ativo = body.get("ativo", False)

    if not nome or not dt_inicio or not dt_fim:
        return jsonify({"error": "nome, dt_inicio e dt_fim são obrigatórios"}), 400

    period_err = _crgm_validate_period(dt_inicio, dt_fim)
    if period_err:
        return jsonify({"error": period_err}), 400

    ano, semestre, descricao = None, None, nome
    parts = nome.replace("/", ".").split(".")
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        y = int(parts[0])
        ano = y if y > 100 else 2000 + y
        semestre = int(parts[1])
        descricao = f"{semestre}Âº Semestre {ano}"

    conn = _pg()
    try:
        cur = conn.cursor()
        if ativo:
            cur.execute("UPDATE ciclos_comercial SET ativo = FALSE WHERE ativo = TRUE")

        cur.execute("""
            INSERT INTO ciclos_comercial (nome, ano, semestre, dt_inicio, dt_fim, ativo, descricao)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (nome) DO NOTHING
            RETURNING id
        """, (nome, ano, semestre, dt_inicio, dt_fim, ativo, descricao))
        row = cur.fetchone()
        new_id = row[0] if row else None

        nivels = []
        if nivel_target == "Ambos":
            nivels = ["Graduação", "Pós-Graduação"]
        elif nivel_target in ("Graduação", "Pós-Graduação"):
            nivels = [nivel_target]

        for nv in nivels:
            cur.execute("""
                INSERT INTO ciclo_atual_comercial (nivel, ciclo)
                VALUES (%s, %s)
                ON CONFLICT (nivel) DO UPDATE SET ciclo = EXCLUDED.ciclo
            """, (nv, nome))

        conn.commit()
        cur.close()
        return jsonify({"ok": True, "id": new_id, "ciclo_atual_atualizado": nivels})
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return jsonify({"error": f"Ciclo '{nome}' já existe"}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/ciclos/<int:ciclo_id>", methods=["PUT"])
def crgm_ciclos_update(ciclo_id):
    """Update a commercial cycle."""
    body = request.json or {}
    conn = _pg()
    try:
        cur = conn.cursor()
        fields, vals = [], []
        for col in ("nome", "dt_inicio", "dt_fim"):
            if col in body:
                fields.append(f"{col} = %s")
                vals.append(body[col])
        if "ativo" in body:
            if body["ativo"]:
                cur.execute("UPDATE ciclos_comercial SET ativo = FALSE WHERE ativo = TRUE")
            fields.append("ativo = %s")
            vals.append(body["ativo"])

        if not fields:
            return jsonify({"error": "Nenhum campo para atualizar"}), 400

        cur.execute(
            "SELECT dt_inicio, dt_fim FROM ciclos_comercial WHERE id = %s",
            (ciclo_id,),
        )
        current = cur.fetchone()
        if not current:
            return jsonify({"error": "Ciclo não encontrado"}), 404
        new_ini = body.get("dt_inicio", current[0].isoformat())
        new_fim = body.get("dt_fim", current[1].isoformat())
        period_err = _crgm_validate_period(new_ini, new_fim)
        if period_err:
            return jsonify({"error": period_err}), 400

        vals.append(ciclo_id)
        cur.execute(
            f"UPDATE ciclos_comercial SET {', '.join(fields)} WHERE id = %s",
            vals,
        )
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/ciclos/<int:ciclo_id>", methods=["DELETE"])
def crgm_ciclos_delete(ciclo_id):
    """Delete a commercial cycle."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM ciclos_comercial WHERE id = %s", (ciclo_id,))
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# â”€â”€ Endpoints â€” Turmas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/turmas")
def crgm_turmas_list():
    """List all turmas (monthly cohorts), optionally filtered by ciclo and/or nivel."""
    ciclo_id = request.args.get("ciclo_id", "")
    nivel = request.args.get("nivel", "")
    conn = _pg()
    try:
        cur = conn.cursor()
        wheres, params = [], []
        if ciclo_id:
            wheres.append("t.ciclo_id = %s")
            params.append(ciclo_id)
        if nivel:
            wheres.append("t.nivel = %s")
            params.append(nivel)
        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT t.id, t.nome, t.nivel, t.ciclo_id, c.nome, t.dt_inicio, t.dt_fim
            FROM turmas_comercial t
            LEFT JOIN ciclos_comercial c ON c.id = t.ciclo_id
            {w}
            ORDER BY t.nivel, t.dt_inicio
        """, params)
        rows = cur.fetchall()
        cur.close()
        return jsonify({
            "ok": True,
            "turmas": [
                {"id": r[0], "nome": r[1], "nivel": r[2], "ciclo_id": r[3],
                 "ciclo_nome": r[4], "dt_inicio": r[5].isoformat(),
                 "dt_fim": r[6].isoformat()}
                for r in rows
            ],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/turmas/stats")
def crgm_turmas_stats():
    """Contagens de matrículas por turma usando snapshots históricos."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nivel, dt_inicio, dt_fim
            FROM turmas_comercial
            ORDER BY nivel, dt_inicio
        """)
        turmas = cur.fetchall()

        # Todos os snapshots de matriculados disponíveis
        cur.execute("""
            SELECT id, uploaded_at::date FROM xl_snapshots
            WHERE tipo = 'matriculados' ORDER BY id
        """)
        snaps = cur.fetchall()
        snap_latest = snaps[-1][0] if snaps else None

        # Raw strings: \d chega ao PostgreSQL sem ser consumido pelo Python
        # empresa 12=Grad, 7=Pos UCS, 79=Pos UCS-CL — todos começam com (12|7x)
        _NIVEL_SQL = r"""CASE
            WHEN coalesce(r.data->>'nivel','') ~* 'p[oó]s'
              OR coalesce(r.data->>'negocio','') ~* 'p[oó]s'
              OR coalesce(r.data->>'curso','') ~* '(mba|especializa|p[oó]s.gradua|lato.sensu|stricto)'
            THEN 'Pós-Graduação' ELSE 'Graduação' END"""

        _DM_SQL = r"""CASE
            WHEN (r.data->>'data_mat') ~ '^\d{2}/\d{2}/\d{4}$'
                THEN to_date(r.data->>'data_mat', 'DD/MM/YYYY')
            WHEN (r.data->>'data_mat') ~ '^\d{4}-\d{2}-\d{2}'
                THEN (r.data->>'data_mat')::date
            ELSE NULL END"""

        _EMP_SQL = r"trim(coalesce(r.data->>'empresa','')) ~ '^(12|7[0-9]*) -'"

        def _count_snap(snap_id, nivel, dt_ini, dt_fim):
            cur.execute(f"""
                SELECT COUNT(DISTINCT regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g'))
                FROM xl_rows r
                WHERE r.snapshot_id = %s
                  AND upper(trim(coalesce(r.data->>'situacao',''))) = 'EM CURSO'
                  AND upper(trim(coalesce(r.data->>'tipo_matricula','')))
                      = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])
                  AND {_EMP_SQL}
                  AND coalesce(r.data->>'rgm','') ~ '[0-9]'
                  AND {_NIVEL_SQL} = %s
                  AND {_DM_SQL} BETWEEN %s AND %s
            """, (snap_id, nivel, dt_ini, dt_fim))
            return cur.fetchone()[0] or 0

        stats = {}
        for tid, tnivel, dt_ini, dt_fim in turmas:
            snaps_ate_dtfim = [s[0] for s in snaps if s[1] <= dt_fim]
            snap_id_periodo = snaps_ate_dtfim[-1] if snaps_ate_dtfim else None

            mat_periodo = _count_snap(snap_id_periodo, tnivel, dt_ini, dt_fim) if snap_id_periodo else None
            em_curso = _count_snap(snap_latest, tnivel, dt_ini, dt_fim) if snap_latest else None

            stats[tid] = {
                "mat_periodo": mat_periodo,
                "em_curso_hoje": em_curso,
                "sem_dados": snap_id_periodo is None,
            }

        cur.close()
        return jsonify({"ok": True, "stats": stats})
    except Exception as e:
        logger.exception("turmas stats error")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/turmas", methods=["POST"])
def crgm_turmas_create():
    """Create a new turma."""
    body = request.json or {}
    nome = (body.get("nome") or "").strip()
    nivel = (body.get("nivel") or "Graduação").strip()
    ciclo_id = body.get("ciclo_id")
    dt_inicio = body.get("dt_inicio", "")
    dt_fim = body.get("dt_fim", "")

    if not nome or not dt_inicio or not dt_fim:
        return jsonify({"error": "nome, dt_inicio e dt_fim são obrigatórios"}), 400

    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO turmas_comercial (nome, nivel, ciclo_id, dt_inicio, dt_fim)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (nome, nivel, ciclo_id or None, dt_inicio, dt_fim))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return jsonify({"ok": True, "id": new_id})
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return jsonify({"error": f"Turma '{nome}' ({nivel}) já existe nesse ciclo"}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/turmas/<int:turma_id>", methods=["DELETE"])
def crgm_turmas_delete(turma_id):
    """Delete a turma."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM turmas_comercial WHERE id = %s", (turma_id,))
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# â”€â”€ Endpoints â€” Upload & Data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/upload", methods=["POST"])
def crgm_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    fname_lower = f.filename.lower()

    if fname_lower.endswith(".csv"):
        try:
            raw = f.read()
            for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
                try:
                    text = raw.decode(enc)
                    break
                except (UnicodeDecodeError, ValueError):
                    continue
            else:
                return jsonify({"error": "Encoding não suportado"}), 400

            stream = io.StringIO(text)
            count = _import_csv(stream)
            return jsonify({"ok": True, "rows": count, "filename": f.filename})
        except Exception as e:
            logger.exception("comercial_rgm upload CSV error")
            return jsonify({"error": str(e)}), 500

    elif fname_lower.endswith((".xlsx", ".xlsm")):
        try:
            from routes.upload import _save_xl_snapshot
            import tempfile, shutil
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            f.save(tmp.name)
            tmp.close()

            row_count = _save_xl_snapshot(tmp.name, f.filename, "matriculados")
            added = populate_comercial_from_snapshot()

            try:
                os.unlink(tmp.name)
            except OSError:
                pass

            return jsonify({
                "ok": True,
                "filename": f.filename,
                "snapshot_rows": row_count,
                "comercial_added": added,
            })
        except Exception as e:
            logger.exception("comercial_rgm upload XLSX error")
            return jsonify({"error": str(e)}), 500

    else:
        return jsonify({"error": "Aceitos: .csv ou .xlsx"}), 400


@comercial_rgm_bp.route("/api/comercial-rgm/populate-from-matriculados", methods=["POST"])
def crgm_populate():
    """Manually trigger population of comercial_rgm from latest matriculados snapshot."""
    try:
        added = populate_comercial_from_snapshot()
        return jsonify({"ok": True, "added": added})
    except Exception as e:
        logger.exception("populate_comercial endpoint error")
        return jsonify({"error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/snapshot-info")
def crgm_snapshot_info():
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*), MIN(data_matricula), MAX(data_matricula),
                   MAX(uploaded_at)
            FROM comercial_rgm
        """)
        row = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM mm_inscritos_hist")
        mm_insc = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM mm_matriculados_hist")
        mm_mat = cur.fetchone()[0] or 0

        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "total": row[0] or 0,
            "min_date": row[1].isoformat() if row[1] else None,
            "max_date": row[2].isoformat() if row[2] else None,
            "uploaded_at": row[3].isoformat() if row[3] else None,
            "mm_inscritos": mm_insc,
            "mm_matriculados": mm_mat,
        })
    except Exception as e:
        logger.exception("snapshot-info error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/sync-users", methods=["POST"])
def crgm_sync_users():
    """Sync Kommo users via API v4 and store in both databases."""
    if not KOMMO_TOKEN:
        return jsonify({"error": "KOMMO_TOKEN não configurado"}), 500
    try:
        headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
        url = f"{KOMMO_BASE_URL}/api/v4/users"
        all_users = []
        page = 1
        while True:
            resp = requests.get(url, headers=headers, params={"page": page, "limit": 250}, timeout=15)
            logger.info("sync-users page %d -> status %d", page, resp.status_code)
            if resp.status_code != 200:
                logger.warning("sync-users API returned %d: %s", resp.status_code, resp.text[:300])
                break
            data = resp.json()
            embedded = data.get("_embedded", {}).get("users", [])
            if not embedded:
                break
            all_users.extend(embedded)
            page += 1

        if not all_users:
            return jsonify({"ok": True, "synced": 0, "msg": "Nenhum usuário retornado pela API"})

        conn = _pg()
        cur = conn.cursor()
        for u in all_users:
            cur.execute("""
                INSERT INTO kommo_users (id, name, email, synced_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, synced_at = NOW()
            """, (u["id"], u.get("name", ""), u.get("email", "")))
        conn.commit()
        cur.close()
        conn.close()

        try:
            kconn = _pg_kommo()
            kcur = kconn.cursor()
            kcur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY, name TEXT, email TEXT,
                    lang TEXT, rights_json JSONB, synced_at TEXT
                )
            """)
            for u in all_users:
                kcur.execute("""
                    INSERT INTO users (id, name, email, synced_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, synced_at = NOW()
                """, (u["id"], u.get("name", ""), u.get("email", "")))
            kconn.commit()
            kcur.close()
            kconn.close()
        except Exception as e:
            logger.warning("sync-users kommo_sync write: %s", e)

        return jsonify({"ok": True, "synced": len(all_users)})
    except Exception as e:
        logger.exception("sync-users error")
        return jsonify({"error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/filters")
def crgm_filters():
    try:
        conn = _pg()
        cur = conn.cursor()
        # Usa apenas comercial_rgm_atual (xl_rows) — fonte principal do dashboard
        cur.execute("""
            SELECT DISTINCT polo FROM comercial_rgm_atual
            WHERE polo IS NOT NULL AND polo != ''
            ORDER BY polo
        """)
        _polo_set = {}
        for (p,) in cur.fetchall():
            n = normalize_polo_display(p)
            if n and n not in _polo_set:
                _polo_set[n] = True
        polos = sorted(_polo_set.keys())
        cur.execute("SELECT DISTINCT nivel FROM comercial_rgm WHERE nivel IS NOT NULL ORDER BY nivel")
        niveis = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT ciclo FROM comercial_rgm WHERE ciclo IS NOT NULL ORDER BY ciclo")
        ciclos = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id, name FROM kommo_users ORDER BY name")
        agentes = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
        cur.close()
        conn.close()

        if not agentes:
            agentes = [{"id": k, "name": v} for k, v in sorted(_KNOWN_USERS.items(), key=lambda x: x[1])]

        # Aplica ajustes de consultor: renomeia e remove ocultos/excluídos do dropdown
        _aj = _load_consultor_ajustes()
        _hidden = _consultor_hidden_uids()
        agentes = [
            {"id": a["id"], "name": (_aj.get(a["id"], {}).get("display_name") or a["name"])}
            for a in agentes if a["id"] not in _hidden
        ]

        return jsonify({"ok": True, "polos": polos, "niveis": niveis, "ciclos": ciclos, "agentes": agentes})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/consultores", methods=["GET"])
def crgm_consultores_list():
    """Lista consultores (usuários Kommo) com contagem de leads + ajustes do dashboard."""
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "Sem permissão"}), 403
    try:
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT u.id, u.name, u.email,
                   COUNT(l.id) FILTER (WHERE l.is_deleted = FALSE) AS leads
            FROM users u
            LEFT JOIN leads l ON l.responsible_user_id = u.id
            GROUP BY u.id, u.name, u.email
            ORDER BY leads DESC NULLS LAST, u.name
        """)
        base = [
            {"id": int(r[0]), "name": (r[1] or f"User #{r[0]}"), "email": (r[2] or ""), "leads": int(r[3] or 0)}
            for r in kcur.fetchall()
        ]
        kcur.close()
        kconn.close()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    aj = _load_consultor_ajustes()
    admin_uid = _admin_sistema_uid()
    name_by_uid = {c["id"]: c["name"] for c in base}
    out = []
    for c in base:
        a = aj.get(c["id"], {})
        rt = a.get("reassign_to")
        out.append({
            **c,
            "display_name": a.get("display_name"),
            "hidden": bool(a.get("hidden")),
            "reassign_to": rt,
            "reassign_to_name": (name_by_uid.get(rt) if rt else None),
            "excluido": rt is not None,
            "is_admin_sistema": c["id"] == admin_uid,
        })
    return jsonify({"ok": True, "consultores": out, "admin_sistema_uid": admin_uid})


@comercial_rgm_bp.route("/api/comercial-rgm/consultores/<int:uid>", methods=["POST"])
def crgm_consultor_save(uid):
    """Upsert do ajuste de um consultor (renomear / ocultar / excluir).

    Body JSON: { display_name, hidden, excluir }. 'excluir=true' reatribui os
    leads para o Admin Sistema e oculta o consultor. Estado completo (o front
    envia sempre o estado desejado atual).
    """
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "Sem permissão"}), 403
    body = request.get_json(silent=True) or {}
    display_name = (body.get("display_name") or "").strip() or None
    hidden = bool(body.get("hidden"))
    excluir = bool(body.get("excluir"))
    admin_uid = _admin_sistema_uid()
    reassign_to = None
    if excluir:
        if uid == admin_uid:
            return jsonify({"ok": False, "error": "Não é possível excluir o Admin Sistema"}), 400
        reassign_to = admin_uid
        hidden = True
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO comercial_consultor_ajuste
                (kommo_user_id, display_name, hidden, reassign_to, updated_at, updated_by)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (kommo_user_id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                hidden       = EXCLUDED.hidden,
                reassign_to  = EXCLUDED.reassign_to,
                updated_at   = NOW(),
                updated_by   = EXCLUDED.updated_by
        """, (uid, display_name, hidden, reassign_to, (session.get("username") or "admin")))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True})


@comercial_rgm_bp.route("/api/comercial-rgm/consultores/<int:uid>", methods=["DELETE"])
def crgm_consultor_reset(uid):
    """Remove o ajuste (restaura o consultor ao padrão: nome do Kommo, visível, sem reatribuição)."""
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM comercial_consultor_ajuste WHERE kommo_user_id = %s", (uid,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True})


_KNOWN_USERS = {
    8239958:  "Fran",
    8240165:  "Isabela",
    8240189:  "Juliana",
    8240438:  "Claudia",
    8261837:  "Admin",
    9718419:  "Felipe",
    10329248: "Andreina",
    10729260: "Jessica",
    11741316: "Bruno",
    12158628: "Hugo",
    12209212: "Gabriela",
    12908868: "Diogo",
    13018348: "Kamily",
    13304804: "T.I",
    14205944: "Thainá",
    14464488: "Tamires",
    14482884: "Eduardo",
    14546744: "Suporte",
    14546760: "Jessyca",
    14932700: "Beatriz",
}


def _distribuicao_consultor_aliases(consultor: str) -> list:
    """Variantes de nome na distribuicao_por_consultor (n8n vs mapa Kommo)."""
    c = (consultor or "").strip().lower()
    if not c:
        return []
    out = {c}
    if c == "kamily":
        out.add("kamilly")
    elif c == "kamilly":
        out.add("kamily")
    return list(out)


# ── Hierarquia "Minha Performance" para Distribuição por Consultor ─────────
#
# Não-admin só pode ver seus próprios dados. Resolvemos:
#   1. session["user_id"] -> app_users.kommo_user_id
#   2. kommo_user_id -> nome canônico em distribuicao_por_consultor (id_consultor)
#                       (mesmo nome que a webhook do n8n devolve)
#                       fallback: kommo_sync.users.name -> primeiro nome
def _dist_consultor_kommo_uid_for_session():
    """Retorna o kommo_user_id do usuário logado, ou None."""
    uid = session.get("user_id", 0)
    if not uid:
        return None
    try:
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute("SELECT kommo_user_id FROM app_users WHERE id = %s", (uid,))
            row = cur.fetchone()
        conn.close()
        return _kommo_uid_int(row[0]) if row else None
    except Exception as e:
        logger.warning("dist_consultor: kommo_user_id lookup failed: %s", e)
        return None


def _dist_consultor_name_for_kommo_uid(kommo_uid: int) -> str | None:
    """Resolve o nome canônico do consultor (mesma string da webhook n8n).

    Ordem: distribuicao_por_consultor.consultor (mais recente) → users.name no
    Kommo PG (primeiro nome) → kommo_users.name no app PG (primeiro nome).
    """
    if not kommo_uid:
        return None
    # 1) Nome usado na própria webhook (mais confiável)
    try:
        kconn = _pg_kommo()
        try:
            with kconn.cursor() as kcur:
                kcur.execute(
                    """
                    SELECT TRIM(consultor)
                    FROM distribuicao_por_consultor
                    WHERE id_consultor = %s
                      AND consultor IS NOT NULL
                      AND TRIM(consultor) != ''
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                    """,
                    (kommo_uid,),
                )
                row = kcur.fetchone()
                if row and row[0]:
                    return row[0].strip()
        finally:
            kconn.close()
    except Exception as e:
        logger.warning("dist_consultor: dist_por_consultor lookup failed: %s", e)

    # 2) users.name no Kommo PG → primeiro nome
    try:
        kconn = _pg_kommo()
        try:
            with kconn.cursor() as kcur:
                kcur.execute("SELECT name FROM users WHERE id = %s", (kommo_uid,))
                row = kcur.fetchone()
                if row and row[0]:
                    return row[0].strip().split()[0]
        finally:
            kconn.close()
    except Exception as e:
        logger.warning("dist_consultor: users.name lookup failed: %s", e)

    # 3) kommo_users.name no app PG → primeiro nome
    try:
        conn = _pg()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM kommo_users WHERE id = %s", (kommo_uid,))
                row = cur.fetchone()
                if row and row[0]:
                    return row[0].strip().split()[0]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("dist_consultor: kommo_users lookup failed: %s", e)

    # 4) _KNOWN_USERS hardcoded
    if kommo_uid in _KNOWN_USERS:
        n = _KNOWN_USERS[kommo_uid]
        if n:
            return n.strip().split()[0]
    return None


def _dist_consultor_is_admin() -> bool:
    return session.get("role") == "admin"


def _dist_consultor_session_info() -> dict:
    """Resumo de identidade da sessão para o ACL do dashboard."""
    is_admin = _dist_consultor_is_admin()
    kuid = _dist_consultor_kommo_uid_for_session()
    nome = None if is_admin else _dist_consultor_name_for_kommo_uid(kuid)
    return {
        "is_admin": is_admin,
        "kommo_user_id": kuid,
        "consultor_nome": nome,
    }


def _dist_consultor_acl(arg_consultor: str | None) -> tuple[str | None, dict]:
    """Aplica a regra de visibilidade.

    - Admin → respeita o `consultor` que veio na query string.
    - Não-admin → ignora o que veio e força o nome do consultor logado.
                   Se não conseguir resolver o nome, devolve string sentinela
                   "__no_access__" para garantir resultado vazio.
    """
    info = _dist_consultor_session_info()
    if info["is_admin"]:
        c = (arg_consultor or "").strip()
        return (c or None, info)
    nome = (info["consultor_nome"] or "").strip()
    if not nome:
        return ("__no_access__", info)
    return (nome, info)


@comercial_rgm_bp.route("/api/dist-consultor/me")
def dist_consultor_me():
    """Identidade do usuário logado para o dashboard de Distribuição.

    Mesma regra de hierarquia da Minha Performance:
      - Admin: vê tudo (consultor_nome = null).
      - Demais: só veem o próprio funil.
    """
    info = _dist_consultor_session_info()
    return jsonify({
        "ok": True,
        "is_admin":       info["is_admin"],
        "kommo_user_id":  info["kommo_user_id"],
        "consultor_nome": info["consultor_nome"],
    })


def _dist_consultor_owner_key(
    uid,
    lead_id,
    dist_name_map: dict,
    uid_to_dist_name: dict,
    kommo_name: str,
    status_id,
) -> str:
    """Consultor 'dono' para cards/modal de distribuição.

    Fechado ganho (142): prioriza responsável atual no Kommo (mapa conhecido → nome
    do usuário → última dist. do próprio lead). Não usa dist. n8n herdada de outro
    lead com o mesmo uid — evita atribuir venda ao consultor errado.

    Demais estágios: mantém prioridade com vínculo n8n por uid.
    """
    kn = (_KNOWN_USERS.get(uid) if uid and uid in _KNOWN_USERS else None)
    dist_uid = (uid_to_dist_name.get(uid) if uid else None)
    dist_lead = (dist_name_map.get(lead_id) if lead_id else None)
    km = (kommo_name or "N/A").strip()

    if status_id == 142:
        return kn or (km if km != "N/A" else None) or dist_lead or dist_uid or "N/A"
    return kn or dist_uid or dist_lead or (km if km != "N/A" else None) or "N/A"


def _kommo_date_iso(raw) -> str | None:
    """Normaliza datas do Kommo (timestamp/datetime/date/string) para YYYY-MM-DD."""
    if raw is None:
        return None
    try:
        import datetime as _dt
        if isinstance(raw, (int, float)):
            return str(_dt.datetime.utcfromtimestamp(raw).date())
        if hasattr(raw, "date"):
            return str(raw.date())
        s = str(raw).strip()
        if not s:
            return None
        return s[:10]
    except Exception:
        return None


def _date_in_period(date_value, start_date: str, end_date: str) -> bool:
    if not date_value:
        return False
    return start_date <= str(date_value)[:10] <= end_date


def _fetch_dist_consultor_received_maps(kcur, lead_ids: list) -> tuple[dict, dict]:
    """Datas em que o responsável atual recebeu o lead, geral e antes do ganho."""
    received_at_map = {}
    received_at_won_map = {}
    if not lead_ids:
        return received_at_map, received_at_won_map

    kcur.execute("""
        SELECT DISTINCT ON (lrh.lead_id)
            lrh.lead_id,
            (lrh.changed_at AT TIME ZONE 'America/Sao_Paulo')::date AS received_date
        FROM lead_responsible_history lrh
        JOIN leads l ON l.id = lrh.lead_id
        WHERE lrh.lead_id = ANY(%s)
          AND lrh.to_user_id = l.responsible_user_id
        ORDER BY lrh.lead_id, lrh.changed_at DESC
    """, (lead_ids,))
    received_at_map = {r[0]: str(r[1]) for r in kcur.fetchall()}

    kcur.execute("""
        SELECT DISTINCT ON (lrh.lead_id)
            lrh.lead_id,
            (lrh.changed_at AT TIME ZONE 'America/Sao_Paulo')::date AS received_date
        FROM lead_responsible_history lrh
        JOIN leads l ON l.id = lrh.lead_id
        WHERE lrh.lead_id = ANY(%s)
          AND l.status_id = 142
          AND l.closed_at IS NOT NULL
          AND l.closed_at > 0
          AND lrh.to_user_id = l.responsible_user_id
          AND lrh.changed_at <= to_timestamp(l.closed_at)
        ORDER BY lrh.lead_id, lrh.changed_at DESC
    """, (lead_ids,))
    received_at_won_map = {r[0]: str(r[1]) for r in kcur.fetchall()}

    return received_at_map, received_at_won_map


def _dist_consultor_period_date(
    lead_id,
    status_id,
    created_at_raw,
    dist_in_period: dict,
    received_at_map: dict,
    received_at_won_map: dict,
    start_date: str,
    end_date: str,
) -> str | None:
    """Data que coloca a venda no grupo 'Desta semana'."""
    if lead_id in dist_in_period:
        return str(dist_in_period[lead_id])

    received = (received_at_won_map.get(lead_id) if status_id == 142 else None) \
        or received_at_map.get(lead_id)
    if _date_in_period(received, start_date, end_date):
        return str(received)[:10]

    created = _kommo_date_iso(created_at_raw)
    if _date_in_period(created, start_date, end_date):
        return created

    return None


def _fetch_kommo_user_names(user_ids):
    """Get user names: known map -> kommo_sync.users -> dcz_sync.kommo_users -> API."""
    user_map = {}
    if not user_ids:
        return user_map

    for uid in user_ids:
        if uid in _KNOWN_USERS:
            user_map[uid] = _KNOWN_USERS[uid]

    missing = [uid for uid in user_ids if uid not in user_map]
    if not missing:
        return user_map

    try:
        conn = _pg_kommo()
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users WHERE id = ANY(%s)", (missing,))
        for r in cur.fetchall():
            user_map[r[0]] = r[1]
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("fetch user names from kommo_sync.users: %s", e)

    missing = [uid for uid in user_ids if uid not in user_map]
    if missing:
        try:
            conn = _pg()
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM kommo_users WHERE id = ANY(%s)", (missing,))
            for r in cur.fetchall():
                user_map[r[0]] = r[1]
            cur.close()
            conn.close()
        except Exception:
            pass

    missing = [uid for uid in user_ids if uid not in user_map]
    if missing and KOMMO_TOKEN:
        try:
            headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
            all_resp = requests.get(
                f"{KOMMO_BASE_URL}/api/v4/users",
                headers=headers, params={"limit": 250}, timeout=15
            )
            if all_resp.status_code == 200:
                api_users = all_resp.json().get("_embedded", {}).get("users", [])
                for u in api_users:
                    uid = u.get("id")
                    if uid in missing:
                        user_map[uid] = u.get("name", f"User #{uid}")
        except Exception as e:
            logger.warning("fetch user names from API: %s", e)

    # Renomeação por ajuste do dashboard (sobrepõe o nome do Kommo)
    _aj = _load_consultor_ajustes()
    for uid in list(user_map.keys()):
        dn = _aj.get(uid, {}).get("display_name")
        if dn:
            user_map[uid] = dn

    return user_map


# ---------------------------------------------------------------------------
# Ajustes de consultor (dashboard-only): renomear / ocultar / excluir
# ---------------------------------------------------------------------------

_ADMIN_SISTEMA_FALLBACK_UID = 8261837


def _admin_sistema_uid():
    """kommo_user_id do 'Admin Sistema' (destino padrão de reatribuição).

    Resolve por nome em kommo_sync.users; cai no fallback 8261837 se não achar.
    """
    if has_request_context() and hasattr(g, "_crgm_admin_uid"):
        return g._crgm_admin_uid
    uid = _ADMIN_SISTEMA_FALLBACK_UID
    try:
        conn = _pg_kommo()
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE name ILIKE 'admin sistema' ORDER BY id LIMIT 1")
        r = cur.fetchone()
        if r:
            uid = int(r[0])
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("_admin_sistema_uid: %s", e)
    if has_request_context():
        g._crgm_admin_uid = uid
    return uid


def _load_consultor_ajustes():
    """Mapa kommo_user_id -> {display_name, hidden, reassign_to} (override do dashboard).

    Cacheado por request via flask.g para evitar múltiplos SELECT.
    """
    if has_request_context() and hasattr(g, "_crgm_cons_ajustes"):
        return g._crgm_cons_ajustes
    out = {}
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT kommo_user_id, display_name, hidden, reassign_to FROM comercial_consultor_ajuste"
        )
        for uid, dn, hidden, rt in cur.fetchall():
            out[int(uid)] = {
                "display_name": ((dn or "").strip() or None),
                "hidden": bool(hidden),
                "reassign_to": (int(rt) if rt is not None else None),
            }
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("_load_consultor_ajustes: %s", e)
    if has_request_context():
        g._crgm_cons_ajustes = out
    return out


def _consultor_reassign_map():
    """{uid_origem: uid_destino} para consultores excluídos (reassign_to preenchido)."""
    return {
        u: a["reassign_to"]
        for u, a in _load_consultor_ajustes().items()
        if a.get("reassign_to")
    }


def _consultor_hidden_uids():
    """uids que NÃO devem aparecer no ranking/filtro (ocultos OU excluídos)."""
    return {
        u
        for u, a in _load_consultor_ajustes().items()
        if a.get("hidden") or a.get("reassign_to")
    }


def _apply_reassign_to_rgm_map(rgm_to_uid):
    """Remapeia os valores de rgm_to_uid conforme consultores excluídos (in-place)."""
    rmap = _consultor_reassign_map()
    if not rmap:
        return rgm_to_uid
    for k in list(rgm_to_uid.keys()):
        v = rgm_to_uid.get(k)
        if v in rmap:
            rgm_to_uid[k] = rmap[v]
    return rgm_to_uid


def _date_to_epoch(dt_str):
    """Convert 'YYYY-MM-DD' to Unix epoch int, or None."""
    if not dt_str:
        return None
    try:
        return int(datetime.strptime(dt_str, "%Y-%m-%d").timestamp())
    except Exception:
        return None


def _build_agent_ranking(dt_ini=None, dt_fim=None, polo=None):
    """Build agent ranking by cross-referencing CSV matrículas with Kommo leads.

    Logic (matches the BI):
      1. kommo_sync: leads with status=142 (Ganho) -> extract RGM from custom fields
         -> build RGM->responsible_user_id map
      2. dcz_sync: comercial_rgm (CSV) filtered by date/polo
         -> count matrículas per agent using the RGM map
      3. Also include CRM-only stats (total leads, novos, perdidos, ativos)
    """
    try:
        # --- Step 1: build RGM -> responsible_user_id from Kommo leads ---
        kconn = _pg_kommo()
        kcur = kconn.cursor()

        # Build RGM->user map from TWO sources (lead_custom_field_values + leads.custom_fields_json)
        # Source 1: lead_custom_field_values (case-insensitive)
        kcur.execute("""
            SELECT regexp_replace(lcf.values_json->0->>'value', '[^0-9]', '', 'g') AS rgm,
                   l.responsible_user_id,
                   l.status_id
            FROM lead_custom_field_values lcf
            JOIN leads l ON l.id = lcf.lead_id AND l.is_deleted = FALSE
            WHERE LOWER(lcf.field_name) = 'rgm'
              AND lcf.values_json->0->>'value' IS NOT NULL
              AND lcf.values_json->0->>'value' != ''
            ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
        """)
        rgm_to_user = {}
        src1_count = 0
        for row in kcur.fetchall():
            rgm, uid = _normalize_rgm(row[0]), row[1]
            if rgm and uid and rgm not in rgm_to_user:
                rgm_to_user[rgm] = uid
                src1_count += 1

        # Source 2: leads.custom_fields_json (fallback for leads not in cf_values table)
        kcur.execute("""
            SELECT regexp_replace(cf_elem->'values'->0->>'value', '[^0-9]', '', 'g') AS rgm,
                   l.responsible_user_id,
                   l.status_id
            FROM leads l,
                 jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) cf_elem
            WHERE l.is_deleted = FALSE
              AND LOWER(cf_elem->>'field_name') = 'rgm'
              AND cf_elem->'values'->0->>'value' IS NOT NULL
              AND cf_elem->'values'->0->>'value' != ''
            ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
        """)
        src2_count = 0
        for row in kcur.fetchall():
            rgm, uid = _normalize_rgm(row[0]), row[1]
            if rgm and uid and rgm not in rgm_to_user:
                rgm_to_user[rgm] = uid
                src2_count += 1

        logger.info("rgm_to_user map: %d total (%d from cf_values, %d extra from custom_fields_json)",
                     len(rgm_to_user), src1_count, src2_count)

        # --- CRM totals per agent (all-time) ---
        ep_ini = _date_to_epoch(dt_ini)
        ep_fim = _date_to_epoch(dt_fim)
        if ep_fim is not None:
            ep_fim += 86399

        kcur.execute("""
            SELECT l.responsible_user_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN l.status_id = 142 THEN 1 ELSE 0 END) AS ganhos,
                   SUM(CASE WHEN l.status_id = 143 THEN 1 ELSE 0 END) AS perdidos,
                   SUM(CASE WHEN l.status_id NOT IN (142, 143) THEN 1 ELSE 0 END) AS ativos,
                   SUM(CASE WHEN l.status_id = 143 AND l.closed_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.closed_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.closed_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS perdidos_periodo,
                   SUM(CASE WHEN l.created_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.created_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.created_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS novos_periodo
            FROM leads l
            WHERE l.responsible_user_id IS NOT NULL
                  AND l.is_deleted = FALSE
            GROUP BY l.responsible_user_id
        """, {"ep_ini": ep_ini, "ep_fim": ep_fim})
        crm_stats = {}
        for r in kcur.fetchall():
            crm_stats[r[0]] = {
                "total": r[1], "ganhos": r[2], "perdidos": r[3],
                "ativos": r[4], "perdidos_periodo": r[5], "novos_periodo": r[6],
            }
        kcur.close()
        kconn.close()

        # --- Step 2: count matrículas per agent via RGM ---
        # Sources: comercial_rgm (CSV upload) + mm_matriculados (M&M upload)
        conn = _pg()
        cur = conn.cursor()

        all_rgms = set()
        cpf_to_rgm = {}

        # Source A: CSV (comercial_rgm)
        csv_where = []
        csv_params = []
        if dt_ini:
            csv_where.append("data_matricula >= %s")
            csv_params.append(dt_ini)
        if dt_fim:
            csv_where.append("data_matricula <= %s")
            csv_params.append(dt_fim)
        if polo:
            csv_where.append(f"{_POLO_SQL} = %s")
            csv_params.append(_normalize_polo(polo))
        csv_w = ("WHERE " + " AND ".join(csv_where)) if csv_where else ""

        cur.execute(f"SELECT rgm FROM comercial_rgm {csv_w}", csv_params)
        for r in cur.fetchall():
            n = _normalize_rgm(r[0])
            if n:
                all_rgms.add(n)

        # Source B: M&M matriculados (dedup via set)
        mm_where = ["UPPER(COALESCE(tipo_matricula,'')) IN %s"]
        mm_params = [MM_TIPO_MAT_VALIDOS]
        if dt_ini:
            mm_where.append("data_matricula >= %s")
            mm_params.append(dt_ini)
        if dt_fim:
            mm_where.append("data_matricula <= %s")
            mm_params.append(dt_fim)
        if polo:
            mm_where.append("polo_aulas = %s")
            mm_params.append(polo)
        mm_w = "WHERE " + " AND ".join(mm_where)

        cur.execute(f"SELECT rgm, cpf, nome FROM mm_matriculados {mm_w}", mm_params)
        nome_to_rgm = {}
        for r in cur.fetchall():
            rgm = _normalize_rgm(r[0])
            if rgm:
                all_rgms.add(rgm)
                if r[1] and r[1].strip():
                    cpf_to_rgm[r[1].strip()] = rgm
                if r[2] and r[2].strip():
                    nome_to_rgm[r[2].strip().upper()] = rgm

        cur.close()
        conn.close()

        pre_fallback = len(rgm_to_user)

        # Fallback 1: CPF -> Kommo contact -> lead -> responsible_user_id
        unmatched_rgms = {r for r in all_rgms if r not in rgm_to_user}
        if unmatched_rgms and cpf_to_rgm:
            try:
                kconn2 = _pg_kommo()
                kcur2 = kconn2.cursor()
                # Source A: contact_custom_field_values
                kcur2.execute("""
                    SELECT
                        regexp_replace(ccf.values_json->0->>'value', '[^0-9]', '', 'g') AS cpf,
                        l.responsible_user_id
                    FROM contact_custom_field_values ccf
                    JOIN contacts c ON c.id = ccf.contact_id AND c.is_deleted = FALSE
                    JOIN lead_contacts lc ON lc.contact_id = c.id
                    JOIN leads l ON l.id = lc.lead_id AND l.is_deleted = FALSE
                    WHERE LOWER(ccf.field_name) IN ('cpf')
                      AND ccf.values_json->0->>'value' IS NOT NULL
                      AND ccf.values_json->0->>'value' != ''
                    ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
                """)
                cpf_to_uid = {}
                for row in kcur2.fetchall():
                    cpf_val, uid = row[0], row[1]
                    if cpf_val and uid and cpf_val not in cpf_to_uid:
                        cpf_to_uid[cpf_val] = uid

                # Source B: contacts.custom_fields_json (fallback)
                kcur2.execute("""
                    SELECT regexp_replace(cf_elem->'values'->0->>'value', '[^0-9]', '', 'g') AS cpf,
                           l.responsible_user_id
                    FROM contacts c,
                         jsonb_array_elements(COALESCE(c.custom_fields_json, '[]'::jsonb)) cf_elem,
                         lead_contacts lc,
                         leads l
                    WHERE c.is_deleted = FALSE
                      AND LOWER(cf_elem->>'field_name') = 'cpf'
                      AND cf_elem->'values'->0->>'value' IS NOT NULL
                      AND cf_elem->'values'->0->>'value' != ''
                      AND lc.contact_id = c.id
                      AND l.id = lc.lead_id AND l.is_deleted = FALSE
                    ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
                """)
                for row in kcur2.fetchall():
                    cpf_val, uid = row[0], row[1]
                    if cpf_val and uid and cpf_val not in cpf_to_uid:
                        cpf_to_uid[cpf_val] = uid

                kcur2.close()
                kconn2.close()

                cpf_added = 0
                for cpf, rgm in cpf_to_rgm.items():
                    if rgm not in rgm_to_user and cpf in cpf_to_uid:
                        rgm_to_user[rgm] = cpf_to_uid[cpf]
                        cpf_added += 1
                logger.info("CPF fallback: %d CPFs in Kommo, %d new RGM->user mapped", len(cpf_to_uid), cpf_added)
            except Exception as e:
                logger.warning("CPF fallback error: %s", e)

        # Fallback 2: nome (student name) -> Kommo lead name or contact name
        unmatched_rgms = {r for r in all_rgms if r not in rgm_to_user}
        if unmatched_rgms and nome_to_rgm:
            try:
                kconn3 = _pg_kommo()
                kcur3 = kconn3.cursor()
                kcur3.execute("""
                    SELECT UPPER(l.name), l.responsible_user_id
                    FROM leads l
                    WHERE l.is_deleted = FALSE
                      AND l.name IS NOT NULL AND l.name != ''
                      AND l.responsible_user_id IS NOT NULL
                    ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
                """)
                nome_to_uid = {}
                for row in kcur3.fetchall():
                    n, uid = row[0], row[1]
                    if n and uid and n not in nome_to_uid:
                        nome_to_uid[n] = uid
                kcur3.close()
                kconn3.close()

                nome_added = 0
                for nome, rgm in nome_to_rgm.items():
                    if rgm not in rgm_to_user and nome in nome_to_uid:
                        rgm_to_user[rgm] = nome_to_uid[nome]
                        nome_added += 1
                logger.info("Nome fallback: %d names in Kommo, %d new RGM->user mapped", len(nome_to_uid), nome_added)
            except Exception as e:
                logger.warning("Nome fallback error: %s", e)

        logger.info("RGM->user total: %d (base: %d, +fallbacks: %d)",
                     len(rgm_to_user), pre_fallback, len(rgm_to_user) - pre_fallback)

        mat_per_agent = {}
        matched_count = 0
        unmatched_sample = []
        for rgm in all_rgms:
            uid = rgm_to_user.get(rgm)
            if uid:
                mat_per_agent[uid] = mat_per_agent.get(uid, 0) + 1
                matched_count += 1
            elif len(unmatched_sample) < 10:
                unmatched_sample.append(rgm)
        if unmatched_sample:
            logger.info("Sample unmatched RGMs (%d total): %s",
                        len(all_rgms) - matched_count, unmatched_sample)

        # --- Step 3: merge CRM stats + CSV matrículas ---
        all_uids = set(crm_stats.keys()) | set(mat_per_agent.keys())
        user_map = _fetch_kommo_user_names(list(all_uids))

        ranking = []
        for uid in all_uids:
            cs = crm_stats.get(uid, {})
            total = cs.get("total", 0)
            ganhos = cs.get("ganhos", 0)
            perdidos = cs.get("perdidos", 0)
            ativos = cs.get("ativos", 0)
            mat_periodo = mat_per_agent.get(uid, 0)
            perdidos_p = cs.get("perdidos_periodo", 0)
            novos_p = cs.get("novos_periodo", 0)
            name = user_map.get(uid, f"User #{uid}")
            taxa = round(ganhos / total * 100, 1) if total > 0 else 0
            ranking.append({
                "user_id": uid,
                "nome": name,
                "total": total,
                "ganhos": ganhos,
                "perdidos": perdidos,
                "ativos": ativos,
                "taxa_conversao": taxa,
                "matriculas_periodo": mat_periodo,
                "perdidos_periodo": perdidos_p,
                "novos_periodo": novos_p,
            })

        ranking.sort(key=lambda x: x["matriculas_periodo"], reverse=True)
        logger.info(
            "Agent ranking: %d agents, %d unique RGMs, %d matched (%.0f%%)",
            len(ranking), len(all_rgms),
            sum(mat_per_agent.values()),
            sum(mat_per_agent.values()) / max(len(all_rgms), 1) * 100
        )
        return ranking
    except Exception as e:
        logger.warning("agent ranking error: %s", e)
        import traceback
        logger.warning(traceback.format_exc())
        return []


def _build_agent_ranking_completa_vw(
    dt_ini=None, dt_fim=None, polo=None, nivel=None, ciclo=None, turma=None,
    excluded_rgms: set = None, crm_dt_ini=None, crm_dt_fim=None, conn=None,
):
    """Matrículas (all_snapshots, alinhado a Matrículas Oficiais) × responsável em
    vw_leads_rgm. Sem match → transferencia/regresso.

    Fonte de matrículas: _crgm_periodo_data_oficial (all_snapshots=True, bypass de
    ciclo_atual quando datas estão presentes). A view comercial_rgm_atual permanece
    intacta para fins de congelamento de ciclo.

    Quando `conn` é fornecida, reutiliza a conexão Postgres principal (não abre/fecha).
    """
    TR = -1
    _own_conn = conn is None
    try:
        conn = conn if conn is not None else _pg()
        if excluded_rgms is None:
            excluded_rgms = _crgm_excluded_rgms(conn)

        # Substitui SELECT FROM comercial_rgm_atual por _crgm_periodo_data_oficial
        # com situacao_filter='EM CURSO' (a view já filtrava só EM CURSO).
        _pd_rows = _crgm_periodo_data_oficial(
            dt_ini=dt_ini, dt_fim=dt_fim, polo=polo, nivel=nivel,
            ciclo_filter=ciclo, turma=turma, conn=conn,
            situacao_filter="EM CURSO",
            require_latest_presence=True,
        )
        rgm_nome = {}
        rgm_date_map = {}      # rgm → 'YYYY-MM-DD', para matriculas_grid (EM CURSO)
        rgm_date_map_all = {}  # rgm → 'YYYY-MM-DD', todos incl. excluídos (para bruto)
        for _row in _pd_rows:
            n = _normalize_rgm(_row.get("rgm"))
            if not n:
                continue
            _dm_str = _row.get("data_matricula")
            # Captura data de TODOS os RGMs (incluindo excluídos) para o grid bruto
            if _dm_str and n not in rgm_date_map_all:
                rgm_date_map_all[n] = str(_dm_str)[:10]
            if n in rgm_nome or n in excluded_rgms:
                continue
            rgm_nome[n] = (_row.get("nome") or "").strip()
            if _dm_str:
                rgm_date_map[n] = str(_dm_str)[:10]

        # Regra de recuperação (janela ancorada no início da meta = dia 01 do mês):
        # mantém apenas o caso (a) cancelamento após a meta. O caso antigo de
        # "sumiu do CSV mais recente" foi removido porque, operacionalmente, esses
        # alunos representam transferência para outro polo e não devem continuar
        # contando para a carteira atual.
        # Antes a janela era limitada a uploads feitos ATÉ dt_fim, o que perdia matrículas
        # de um dia passado cujo primeiro relatório só chegou depois daquele dia.
        if dt_fim:
            _meta_start = ((dt_ini or dt_fim) or "")[:7] + "-01"
            _NIVEL_CASE = """CASE
                WHEN coalesce(r.data->>'nivel','') ~* 'p[oó]s'
                  OR coalesce(r.data->>'negocio','') ~* 'p[oó]s'
                  OR coalesce(r.data->>'curso','') ~* '(mba|especializa|p[oó]s.gradua|lato.sensu|stricto)'
                THEN 'Pós-Graduação' ELSE 'Graduação' END"""
            # Regex com classe [0-9] em string normal — NÃO usar E'\\d' (em E-string
            # o Postgres colapsa \d -> d e a regex nunca casa datas dd/mm/yyyy).
            _DM_EXPR = """CASE
                WHEN (r.data->>'data_mat') ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                    THEN to_date(r.data->>'data_mat', 'DD/MM/YYYY')
                WHEN (r.data->>'data_mat') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN (r.data->>'data_mat')::date
                ELSE NULL END"""
            supp_cw = [
                "s.tipo = 'matriculados'",
                "s.uploaded_at::date >= %s",
                "upper(trim(coalesce(r.data->>'situacao',''))) = 'EM CURSO'",
                "upper(trim(coalesce(r.data->>'tipo_matricula',''))) = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])",
                "trim(coalesce(r.data->>'empresa','')) ~ '^(12|7) -'",
                "coalesce(r.data->>'rgm','') ~ '\\d'",
                """regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g') IN (
                    SELECT regexp_replace(coalesce(rl.data->>'rgm',''), '[^0-9]', '', 'g')
                    FROM xl_rows rl
                    WHERE rl.snapshot_id = (
                        SELECT id FROM xl_snapshots
                        WHERE tipo = 'matriculados'
                        ORDER BY id DESC LIMIT 1
                    )
                      AND coalesce(rl.data->>'rgm','') ~ '[0-9]'
                )""",
                f"""(({_NIVEL_CASE} = 'Graduação'
                    AND trim(r.data->>'ciclo') = (SELECT ciclo FROM ciclo_atual_comercial WHERE nivel='Graduação'))
                   OR ({_NIVEL_CASE} = 'Pós-Graduação'
                    AND trim(r.data->>'ciclo') = (SELECT ciclo FROM ciclo_atual_comercial WHERE nivel='Pós-Graduação')))""",
            ]
            supp_cp = [_meta_start]
            if dt_ini:
                supp_cw.append(f"{_DM_EXPR} >= %s")
                supp_cp.append(dt_ini)
            # Limite superior: respeita o dia/período filtrado. A janela ampliada de
            # uploads não deve trazer matrículas com data_matricula fora do período.
            supp_cw.append(f"{_DM_EXPR} <= %s")
            supp_cp.append(dt_fim)
            if polo:
                supp_cw.append("trim(regexp_replace(coalesce(r.data->>'polo',''), E'^\\d+\\s*[-–]\\s*', '')) = %s")
                supp_cp.append(_normalize_polo(polo))
            if nivel:
                supp_cw.append(f"{_NIVEL_CASE} = %s")
                supp_cp.append(nivel)
            # Quando há ciclo manual ou filtro de datas, bypass do filtro automático
            # ciclo_atual_comercial (datas já limitam o escopo sem precisar do ciclo atual).
            if ciclo or dt_ini or dt_fim:
                supp_cw = [c for c in supp_cw if 'ciclo_atual_comercial' not in c]
            if ciclo:
                supp_cw.append("trim(coalesce(r.data->>'ciclo','')) = %s")
                supp_cp.append(ciclo)
            if turma:
                supp_cw.append("nullif(trim(coalesce(r.data->>'curso','')), '') = %s")
                supp_cp.append(turma)
            # Exclui RGMs já contabilizados na query principal
            already = tuple(rgm_nome.keys()) if rgm_nome else ('__NONE__',)
            supp_cw.append("regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g') != ALL(%s)")
            supp_cp.append(list(already))
            # Respeita o dedup de PÓS multi-ciclo: NÃO recupera RGM que está EM CURSO
            # no ÚLTIMO relatório em QUALQUER ciclo (ex.: pós rebaixado para 2026/1 pelo
            # dedup — presente e EM CURSO, só que noutro ciclo). Recupera apenas sumiço
            # real (ausente do último relatório) ou cancelado-pós-meta (presente, mas
            # não-EM CURSO). Sem isso, a recuperação desfazia o dedup.
            supp_cw.append("""regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g') NOT IN (
                SELECT regexp_replace(coalesce(r2.data->>'rgm',''), '[^0-9]', '', 'g')
                FROM xl_rows r2
                WHERE r2.snapshot_id = (SELECT id FROM xl_snapshots WHERE tipo='matriculados' ORDER BY id DESC LIMIT 1)
                  AND upper(trim(coalesce(r2.data->>'situacao',''))) = 'EM CURSO'
                  AND regexp_replace(coalesce(r2.data->>'rgm',''), '[^0-9]', '', 'g') <> ''
            )""")
            supp_where = "WHERE " + " AND ".join(supp_cw)
            _rgm_nome_antes_supp = len(rgm_nome)
            try:
                cur2 = conn.cursor() if not conn.closed else _pg().cursor()
                cur2.execute(f"""
                    SELECT DISTINCT ON (rgm_norm) rgm_norm, nome, dm
                    FROM (
                        SELECT
                            regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g') AS rgm_norm,
                            nullif(trim(coalesce(r.data->>'nome','')), '') AS nome,
                            {_DM_EXPR} AS dm,
                            s.uploaded_at
                        FROM xl_rows r
                        JOIN xl_snapshots s ON s.id = r.snapshot_id
                        {supp_where}
                    ) t
                    WHERE rgm_norm != ''
                    ORDER BY rgm_norm, uploaded_at DESC
                """, supp_cp)
                for rgm_raw, nome, dm in cur2.fetchall():
                    n = _normalize_rgm(rgm_raw)
                    if n and n not in rgm_nome:
                        rgm_nome[n] = (nome or "").strip()
                        # Alimenta rgm_date_map com a data de matrícula do RGM
                        # recuperado, para ele entrar no matriculas_grid (usado pelo
                        # cross-filter por dia no front). Sem isso o RGM contava no
                        # período mas sumia ao filtrar por um dia específico.
                        if dm is not None and n not in rgm_date_map:
                            try:
                                rgm_date_map[n] = dm.isoformat()[:10] if hasattr(dm, 'isoformat') else str(dm)[:10]
                            except Exception:
                                pass
                cur2.close()
                logger.info(
                    "ranking: +%d RGMs recuperados (cancelado-pós-meta / sumiço do SIAA)",
                    len(rgm_nome) - _rgm_nome_antes_supp,
                )
            except Exception as _se:
                logger.warning("ranking supp cancelados: %s", _se)

        mat_rows = list(rgm_nome.items())
        if _own_conn:
            conn.close()

        # Otimização: filtrar mapa RGM→consultor apenas pelos RGMs realmente usados
        # neste request (mat_rows + excluded_rgms), em vez de varrer toda a vw_leads_rgm.
        _rgms_para_lookup = sorted({n for n, _ in mat_rows} | set(excluded_rgms or ()))
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        if _rgms_para_lookup:
            kcur.execute("""
                SELECT DISTINCT ON (v.rgm) v.rgm, l.responsible_user_id
                FROM vw_leads_rgm v
                JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                WHERE l.responsible_user_id IS NOT NULL
                  AND v.rgm = ANY(%s)
                ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
            """, (_rgms_para_lookup,))
        rgm_to_uid = {}
        for row in kcur.fetchall():
            nk = _normalize_rgm(row[0])
            if nk and row[1]:
                rgm_to_uid[nk] = row[1]

        _apply_conflito_overrides_to_rgm_map(rgm_to_uid)

        # Consultores excluídos: reatribui os leads/matrículas para Admin Sistema
        _apply_reassign_to_rgm_map(rgm_to_uid)

        _crm_ini = crm_dt_ini or dt_ini
        _crm_fim = crm_dt_fim or dt_fim
        ep_ini = _date_to_epoch(_crm_ini)
        ep_fim = _date_to_epoch(_crm_fim)
        if ep_fim is not None:
            ep_fim += 86399
        # CRM no ranking: período DE/ATÉ em leads.created_at / closed_at (epoch).
        # total / novos = criados no período; ganhos / perdidos = fechados no período (Kommo).
        # Conv.% = matrículas no período (CSV×Kommo) / total de leads criados no período — não usa ganhos 142.
        #
        # Otimização (Fase 3): pre-filtra leads que tem AO MENOS uma janela relevante
        # (created_at OU closed_at dentro do periodo). Linhas totalmente fora ja saem
        # 0 em todos os SUMs — descarta-las antes economiza ~70% das linhas agregadas.
        _rk_extra_where = ""
        _rk_params = {"ep_ini": ep_ini, "ep_fim": ep_fim}
        if ep_ini is not None and ep_fim is not None:
            _rk_extra_where = (
                " AND ((l.created_at IS NOT NULL AND l.created_at BETWEEN %(ep_ini)s AND %(ep_fim)s)"
                "   OR (l.closed_at  IS NOT NULL AND l.closed_at  BETWEEN %(ep_ini)s AND %(ep_fim)s))"
            )
        elif ep_ini is not None:
            _rk_extra_where = (
                " AND ((l.created_at IS NOT NULL AND l.created_at >= %(ep_ini)s)"
                "   OR (l.closed_at  IS NOT NULL AND l.closed_at  >= %(ep_ini)s))"
            )
        elif ep_fim is not None:
            _rk_extra_where = (
                " AND ((l.created_at IS NOT NULL AND l.created_at <= %(ep_fim)s)"
                "   OR (l.closed_at  IS NOT NULL AND l.closed_at  <= %(ep_fim)s))"
            )

        kcur.execute(f"""
            SELECT l.responsible_user_id,
                   SUM(CASE WHEN l.created_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.created_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.created_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS total_periodo,
                   SUM(CASE WHEN l.status_id = 142 AND l.closed_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.closed_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.closed_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS ganhos_periodo,
                   SUM(CASE WHEN l.status_id = 143 AND l.closed_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.closed_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.closed_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS perdidos_periodo
            FROM leads l
            WHERE l.responsible_user_id IS NOT NULL AND NOT l.is_deleted
              {_rk_extra_where}
            GROUP BY l.responsible_user_id
        """, _rk_params)
        crm_stats = {}
        for r in kcur.fetchall():
            tot_p = int(r[1] or 0)
            g_p = int(r[2] or 0)
            perd_p = int(r[3] or 0)
            crm_stats[r[0]] = {
                "total": tot_p,
                "ganhos": g_p,
                "perdidos": perd_p,
                "perdidos_periodo": perd_p,
                "novos_periodo": tot_p,
            }
        kcur.close()
        kconn.close()

        # Consultor excluído: soma as stats de CRM dele no destino (Admin Sistema)
        _reassign_cs = _consultor_reassign_map()
        if _reassign_cs:
            _merged_cs = {}
            for _u, _cs in crm_stats.items():
                _t = _reassign_cs.get(_u, _u)
                if _t in _merged_cs:
                    for _kk, _vv in _cs.items():
                        _merged_cs[_t][_kk] = _merged_cs[_t].get(_kk, 0) + _vv
                else:
                    _merged_cs[_t] = dict(_cs)
            crm_stats = _merged_cs

        # Calcular prefixo dominante e overrides para filtrar outliers nas contagens
        _ranking_all_rgms = [n for n, _ in mat_rows]
        _ranking_dom_pfx = _crgm_effective_dominant_prefix(_ranking_all_rgms) or 99
        _ranking_overrides = _load_outlier_contagem_overrides()

        mat_per_agent = {}
        transferencia_itens = []
        for rgm, nome in mat_rows:
            uid = rgm_to_uid.get(rgm)
            _conta = _rgm_conta_para_venda(rgm, _ranking_dom_pfx, _ranking_overrides)
            if uid:
                if _conta:
                    mat_per_agent[uid] = mat_per_agent.get(uid, 0) + 1
            else:
                if _conta:
                    transferencia_itens.append({"rgm": rgm, "nome": nome})
        tr_count = len(transferencia_itens)
        if tr_count:
            mat_per_agent[TR] = tr_count

        _hidden_uids = _consultor_hidden_uids()
        uids_real = [u for u in (set(crm_stats) | set(mat_per_agent)) if u != TR and u not in _hidden_uids]
        user_map = _fetch_kommo_user_names(uids_real)
        ranking = []
        for uid in uids_real:
            cs = crm_stats.get(uid, {})
            t, g = cs.get("total", 0), cs.get("ganhos", 0)
            m = mat_per_agent.get(uid, 0)
            ranking.append({
                "user_id": uid,
                "nome": user_map.get(uid, f"User #{uid}"),
                "total": t,
                "ganhos": g,
                "perdidos": cs.get("perdidos", 0),
                "taxa_conversao": round(m / t * 100, 1) if t > 0 else 0,
                "matriculas_periodo": m,
                "perdidos_periodo": cs.get("perdidos_periodo", 0),
                "novos_periodo": cs.get("novos_periodo", 0),
            })
        if tr_count:
            ranking.append({
                "user_id": TR,
                "nome": "transferencia/regresso",
                "total": 0,
                "ganhos": 0,
                "perdidos": 0,
                "taxa_conversao": 0.0,
                "matriculas_periodo": tr_count,
                "perdidos_periodo": 0,
                "novos_periodo": 0,
                "is_transferencia": True,
            })
        ranking.sort(key=lambda x: x["matriculas_periodo"], reverse=True)

        # Decisão: incluir transferencia/regresso (user_id=-1) em matriculas_grid para completude.
        # RGMs suplementares (recuperados de xl_rows) agora carregam a data de matrícula
        # (rgm_date_map) e entram no grid — o cross-filter por dia passa a contá-los.
        # count = bruto (inclui excluídos/evasão); count_liquido = EM CURSO apenas.
        grid_acc_bruto = defaultdict(int)
        grid_acc_liq   = defaultdict(int)
        # EM CURSO (não-excluídos); count_liquido exclui outliers sem override
        for rgm, _nome in mat_rows:
            dt_str = rgm_date_map.get(rgm)
            if not dt_str:
                continue
            uid = rgm_to_uid.get(rgm)
            if uid is None:
                uid = TR
            grid_acc_bruto[(dt_str, uid)] += 1
            if _rgm_conta_para_venda(rgm, _ranking_dom_pfx, _ranking_overrides):
                grid_acc_liq[(dt_str, uid)] += 1
        # Excluídos (evasão) adicionados apenas ao bruto
        for ev_rgm in (excluded_rgms or set()):
            dt_str = rgm_date_map_all.get(ev_rgm)
            if not dt_str:
                continue
            uid = rgm_to_uid.get(ev_rgm)
            if uid is None:
                uid = TR
            grid_acc_bruto[(dt_str, uid)] += 1
        all_grid_keys = sorted(set(grid_acc_bruto) | set(grid_acc_liq))
        matriculas_grid = [
            {"data": k[0], "user_id": k[1],
             "count": grid_acc_bruto.get(k, 0),
             "count_liquido": grid_acc_liq.get(k, 0)}
            for k in all_grid_keys
        ]

        return ranking, {
            "titulo": "transferencia/regresso",
            "total": tr_count,
            "itens": sorted(transferencia_itens, key=lambda x: x["rgm"]),
        }, matriculas_grid
    except Exception as e:
        logger.warning("ranking completa/vw: %s", e)
        return [], {"titulo": "transferencia/regresso", "total": 0, "itens": []}, []


@comercial_rgm_bp.route("/api/comercial-rgm/cache/clear", methods=["POST"])
def crgm_cache_clear():
    """Invalida manualmente o cache em memoria do /data."""
    clear_crgm_data_cache(reason="endpoint manual")
    return jsonify({"ok": True})


@comercial_rgm_bp.route("/api/comercial-rgm/data")
def crgm_data():
    polo       = request.args.get("polo", "")
    nivel      = request.args.get("nivel", "")
    dt_ini     = request.args.get("dt_ini", "")
    dt_fim     = request.args.get("dt_fim", "")
    dt_ini, dt_fim = _crgm_normalize_period(dt_ini, dt_fim)
    ciclo_nome = request.args.get("ciclo", "")
    turma_nome = request.args.get("turma", "")

    # ---- Cache (Fase 3): atende com TTL curto sem recalcular ----
    _cache_key = _crgm_cache_key_from_args()
    _no_cache  = request.args.get("no_cache") == "1"
    if not _no_cache:
        _cached = _crgm_cache_get(_cache_key)
        if _cached is not None:
            logger.info("CRGM /data CACHE HIT key=%s", _cache_key)
            return jsonify(_cached)

    # ---- Fase 4: wrapper composto — 3 funções em paralelo via ThreadPool ----
    import time as _time_mod
    _crgm_t_start = _time_mod.perf_counter()
    _CRGM_WRAPPER_SENTINEL_ = True  # marca que entramos no bloco de computo
    try:
        from concurrent.futures import ThreadPoolExecutor as _CRGM_TPE
        with _CRGM_TPE(max_workers=3) as _pool:
            _fut_kpis    = _pool.submit(_crgm_compute_kpis,    polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome)
            _fut_agentes = _pool.submit(_crgm_compute_agentes, polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome)
            _fut_grids   = _pool.submit(_crgm_compute_grids,   polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome)

            result_kpis    = _fut_kpis.result()
            result_agentes = _fut_agentes.result()
            result_grids   = _fut_grids.result()

        _payload = {
            "ok": True,
            "metas_aviso": result_agentes["metas_aviso"],
            "kpis": result_kpis["kpis"],
            "evolucao": result_kpis["evolucao"],
            "evolucao_bruto": result_kpis["evolucao_bruto"],
            "evolucao_prev": result_kpis["evolucao_prev"],
            "ranking_polo": result_kpis["ranking_polo"],
            "ranking_ciclo": result_kpis["ranking_ciclo"],
            "ranking_agentes": result_agentes["ranking_agentes"],
            "transferencia_regresso": result_agentes["transferencia_regresso"],
            "evasao": result_grids["evasao"],
            "fora_padrao": result_grids["fora_padrao"],
            "matriculas_grid": result_agentes["matriculas_grid"],
            "leads_grid": result_grids["leads_grid"],
            "evasao_grid": result_kpis["evasao_grid"],
            "fora_padrao_grid": result_kpis["fora_padrao_grid"],
            "daily_history": result_agentes["daily_history"],
        }
        _crgm_cache_set(_cache_key, _payload)
        return jsonify(_payload)
    except Exception as e:
        logger.exception("comercial_rgm data error")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        try:
            logger.info("CRGM /data TOTAL %.2fs", _time_mod.perf_counter() - _crgm_t_start)
        except Exception:
            pass


# ===========================================================================
# Fase 4 — helpers de computo independentes + 3 sub-endpoints paralelos
# ===========================================================================

def _crgm_resolve_ciclo_pd_dates(ciclo_nome, turma_nome, dt_ini, dt_fim):
    """Resolve ciclo_dt_ini/fim e _pd_dt_ini/_pd_dt_fim (lógica compartilhada pelos 3 helpers)."""
    ciclo_dt_ini = None
    ciclo_dt_fim = None
    if ciclo_nome:
        try:
            _cc = _pg()
            _cc_cur = _cc.cursor()
            _cc_cur.execute(
                "SELECT dt_inicio, dt_fim FROM ciclos_comercial WHERE nome = %s LIMIT 1",
                (ciclo_nome,),
            )
            _cc_row = _cc_cur.fetchone()
            if _cc_row:
                ciclo_dt_ini = _cc_row[0].isoformat() if hasattr(_cc_row[0], 'isoformat') else str(_cc_row[0])[:10]
                ciclo_dt_fim = _cc_row[1].isoformat() if hasattr(_cc_row[1], 'isoformat') else str(_cc_row[1])[:10]
            _cc_cur.close()
            _cc.close()
        except Exception as _ce:
            logger.warning("ciclo dates lookup: %s", _ce)

    if ciclo_nome and turma_nome and ciclo_dt_ini and ciclo_dt_fim:
        _pd_dt_ini = ciclo_dt_ini
        _pd_dt_fim = ciclo_dt_fim
    else:
        _pd_dt_ini = dt_ini or None
        _pd_dt_fim = dt_fim or None
    return _crgm_normalize_period(_pd_dt_ini, _pd_dt_fim)


def _crgm_build_periodo_sets(ciclo_all, _pd_dt_ini, _pd_dt_fim):
    """Deriva _periodo_rows e os conjuntos rgms_bruto / rgms_periodo / evasao_rows."""
    if _pd_dt_ini or _pd_dt_fim:
        _periodo_rows = []
        for row in ciclo_all:
            dm_str = row.get("data_matricula")
            if not dm_str:
                continue
            dm_key = str(dm_str)[:10]
            if _pd_dt_ini and dm_key < _pd_dt_ini:
                continue
            if _pd_dt_fim and dm_key > _pd_dt_fim:
                continue
            _periodo_rows.append(row)
    else:
        _periodo_rows = list(ciclo_all)

    rgms_periodo = set()
    rgms_bruto   = set()
    evasao_rows  = []
    day_rgms       = defaultdict(set)
    day_rgms_bruto = defaultdict(set)
    polo_rgms      = defaultdict(set)

    for row in _periodo_rows:
        n = row["rgm"]
        if not n:
            continue
        rgms_bruto.add(n)
        try:
            dt = date.fromisoformat(row["data_matricula"][:10]) if row["data_matricula"] else None
        except (ValueError, TypeError):
            dt = None
        if dt:
            day_rgms_bruto[dt].add(n)
        if row["situacao"] == "EM CURSO":
            rgms_periodo.add(n)
            if dt:
                day_rgms[dt].add(n)
            if row["polo"]:
                canon = normalize_polo_display(row["polo"])
                if canon:
                    polo_rgms[canon].add(n)
        else:
            evasao_rows.append(row)

    return _periodo_rows, rgms_periodo, rgms_bruto, evasao_rows, day_rgms, day_rgms_bruto, polo_rgms


def _crgm_compare_table_filters(period_ini, period_fim, polo=None, nivel=None, turma=None):
    cw = ["data_matricula >= %s", "data_matricula <= %s"]
    cp = [period_ini, period_fim]
    if polo:
        cw.append(f"{_POLO_SQL} = %s")
        cp.append(_normalize_polo(polo))
    if nivel:
        cw.append("nivel = %s")
        cp.append(nivel)
    if turma:
        cw.append("turma = %s")
        cp.append(turma)
    return cw, cp


def _crgm_count_bruto_from_table(conn, period_ini, period_fim, polo=None, nivel=None, turma=None):
    """Fallback histórico via comercial_rgm quando o snapshot não cobre o período."""
    cw, cp = _crgm_compare_table_filters(period_ini, period_fim, polo, nivel, turma)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT rgm FROM comercial_rgm WHERE {' AND '.join(cw)}",
            cp,
        )
        return len({_normalize_rgm(r[0]) for r in cur.fetchall() if _normalize_rgm(r[0])})
    finally:
        cur.close()


def _crgm_day_bruto_counts_from_table(conn, iso_dates, polo=None, nivel=None, turma=None):
    cw = ["data_matricula::date = ANY(%s::date[])"]
    cp: list = [iso_dates]
    if polo:
        cw.append(f"{_POLO_SQL} = %s")
        cp.append(_normalize_polo(polo))
    if nivel:
        cw.append("nivel = %s")
        cp.append(nivel)
    if turma:
        cw.append("turma = %s")
        cp.append(turma)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT data_matricula::date AS d, COUNT(DISTINCT rgm) AS c
            FROM comercial_rgm
            WHERE {' AND '.join(cw)}
            GROUP BY 1
            """,
            cp,
        )
        out = {}
        for row in cur.fetchall():
            k = row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])[:10]
            out[k] = int(row[1])
        return out
    finally:
        cur.close()


def _crgm_count_bruto_compare(conn, period_ini, period_fim, polo=None, nivel=None, turma=None):
    """Matrículas bruto no intervalo — xl_rows (KPI) com fallback em comercial_rgm."""
    rows = _crgm_periodo_data(
        dt_ini=period_ini,
        dt_fim=period_fim,
        polo=polo,
        nivel=nivel,
        turma=turma,
        conn=conn,
    )
    _, _, rgms_bruto, *_ = _crgm_build_periodo_sets(rows, period_ini, period_fim)
    count_xl = len(rgms_bruto)
    if count_xl > 0:
        return count_xl
    return _crgm_count_bruto_from_table(
        conn, period_ini, period_fim, polo, nivel, turma
    )


def _crgm_day_bruto_counts(target_dates, polo=None, nivel=None, turma=None, conn=None):
    """Contagem bruta por dia — xl_rows com fallback em comercial_rgm por data."""
    if not target_dates:
        return {}
    iso_dates = sorted({str(d)[:10] for d in target_dates if d})
    if not iso_dates:
        return {}
    own_conn = conn is None
    db = conn if conn is not None else _pg()
    try:
        rows = _crgm_periodo_data(
            dt_ini=iso_dates[0],
            dt_fim=iso_dates[-1],
            polo=polo,
            nivel=nivel,
            turma=turma,
            conn=db,
        )
        _, _, _, _, _, day_rgms_bruto, _ = _crgm_build_periodo_sets(
            rows, iso_dates[0], iso_dates[-1]
        )
        out = {}
        for d in iso_dates:
            try:
                dt_key = date.fromisoformat(d)
            except (ValueError, TypeError):
                out[d] = 0
                continue
            out[d] = len(day_rgms_bruto.get(dt_key, set()))

        missing = [d for d in iso_dates if out.get(d, 0) == 0]
        if missing:
            table_counts = _crgm_day_bruto_counts_from_table(
                db, missing, polo, nivel, turma
            )
            for d in missing:
                if table_counts.get(d, 0) > 0:
                    out[d] = table_counts[d]
        return out
    finally:
        if own_conn:
            db.close()


def _crgm_compute_kpis(polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome) -> dict:
    """Calcula KPIs, evolucao, ranking_polo/ciclo e evasao_grid."""
    import time as _time_mod
    _t_start = _time_mod.perf_counter()
    _t_lap   = _t_start

    def _lap(label):
        nonlocal _t_lap
        _now = _time_mod.perf_counter()
        logger.info("CRGM kpis/[%s] %.2fs", label, _now - _t_lap)
        _t_lap = _now

    _pd_dt_ini, _pd_dt_fim = _crgm_resolve_ciclo_pd_dates(ciclo_nome, turma_nome, dt_ini, dt_fim)

    conn = _pg()
    cur  = conn.cursor()

    if _pd_dt_ini or _pd_dt_fim:
        ciclo_all = _crgm_periodo_data_oficial(
            dt_ini=_pd_dt_ini, dt_fim=_pd_dt_fim,
            polo=polo or None, nivel=nivel or None,
            turma=turma_nome or None, conn=conn,
            mark_missing_as_transferido=True,
        )
    else:
        ciclo_all = _crgm_periodo_data_oficial(
            polo=polo or None, nivel=nivel or None,
            turma=turma_nome or None, conn=conn,
            mark_missing_as_transferido=True,
        )
    _lap("ciclo_all_xl_rows")

    (
        _periodo_rows, rgms_periodo, rgms_bruto, evasao_rows,
        day_rgms, day_rgms_bruto, polo_rgms,
    ) = _crgm_build_periodo_sets(ciclo_all, _pd_dt_ini, _pd_dt_fim)
    _lap("derive_periodo_inmem")

    # Filtrar outliers (prefixo abaixo do dominante sem override manual) das contagens
    _kpi_dom_pfx  = _crgm_effective_dominant_prefix(list(rgms_periodo) or list(rgms_bruto))
    _kpi_overrides = _load_outlier_contagem_overrides()
    _rgms_contando = {r for r in rgms_periodo if _rgm_conta_para_venda(r, _kpi_dom_pfx, _kpi_overrides)}
    day_rgms_contando   = {d: s & _rgms_contando for d, s in day_rgms.items()}
    day_rgms_contando   = {d: s for d, s in day_rgms_contando.items() if s}
    polo_rgms_contando  = {p: s & _rgms_contando for p, s in polo_rgms.items()}
    polo_rgms_contando  = {p: s for p, s in polo_rgms_contando.items() if s}

    vendas          = len(rgms_bruto)
    vendas_liquidas = len(_rgms_contando)
    _excluded       = rgms_bruto - rgms_periodo

    evasao_grid_acc = defaultdict(int)
    for _ev in evasao_rows:
        _ev_dt = _ev.get("data_matricula")
        if not _ev_dt:
            continue
        _ev_tipo = (_ev.get("situacao") or "OUTROS").strip()
        evasao_grid_acc[(str(_ev_dt)[:10], _ev_tipo)] += 1
    evasao_grid = [
        {"data": k[0], "tipo": k[1], "count": v}
        for k, v in sorted(evasao_grid_acc.items())
    ]

    fora_padrao_rows = _crgm_fora_padrao_rows(
        _periodo_rows, _kpi_dom_pfx, _kpi_overrides, apenas_nao_conta=True,
    )
    fora_padrao_grid_acc = defaultdict(int)
    for _fp in fora_padrao_rows:
        _fp_dt = _fp.get("data_matricula")
        if not _fp_dt:
            continue
        fora_padrao_grid_acc[(str(_fp_dt)[:10], _fp.get("prefixo") or "?")] += 1
    fora_padrao_grid = [
        {"data": k[0], "prefixo": k[1], "count": v}
        for k, v in sorted(fora_padrao_grid_acc.items())
    ]

    all_kpi_rgms     = _rgms_contando
    day_counts       = {d: len(s) for d, s in day_rgms_contando.items()}
    day_counts_bruto = {d: len(s) for d, s in day_rgms_bruto.items()}
    polo_counts      = {p: len(s) for p, s in polo_rgms_contando.items()}
    dias             = len(day_counts) or 1
    media_diaria     = round(vendas_liquidas / dias, 1) if dias else 0

    # --- Ticket médio via Kommo lead price ---
    ticket_medio = 0.0
    try:
        kconn = _pg_kommo()
        kcur  = kconn.cursor()
        kcur.execute("""
            SELECT rgm_val, price FROM (
                SELECT regexp_replace(lcf.values_json->0->>'value', '[^0-9]', '', 'g') AS rgm_val,
                       l.price
                FROM lead_custom_field_values lcf
                JOIN leads l ON l.id = lcf.lead_id AND l.status_id = 142 AND l.is_deleted = FALSE
                WHERE LOWER(lcf.field_name) = 'rgm'
                  AND lcf.values_json->0->>'value' IS NOT NULL
                  AND lcf.values_json->0->>'value' != ''
                  AND l.price IS NOT NULL AND l.price > 0
                UNION ALL
                SELECT regexp_replace(cf_elem->'values'->0->>'value', '[^0-9]', '', 'g'),
                       l.price
                FROM leads l,
                     jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) cf_elem
                WHERE l.status_id = 142 AND l.is_deleted = FALSE
                  AND LOWER(cf_elem->>'field_name') = 'rgm'
                  AND cf_elem->'values'->0->>'value' IS NOT NULL
                  AND cf_elem->'values'->0->>'value' != ''
                  AND l.price IS NOT NULL AND l.price > 0
            ) sub WHERE rgm_val IS NOT NULL AND rgm_val != ''
        """)
        rgm_price = {}
        for r in kcur.fetchall():
            n = _normalize_rgm(r[0])
            if n and n not in rgm_price:
                rgm_price[n] = r[1]
        kcur.close()
        kconn.close()
        prices = [rgm_price[rgm] for rgm in all_kpi_rgms if rgm in rgm_price and rgm_price[rgm] > 0]
        if prices:
            ticket_medio = round((sum(prices) / len(prices)) * 0.30, 2)
    except Exception as e:
        logger.warning("kpis ticket_medio_kommo: %s", e)
    _lap("ticket_medio_kommo")

    # --- MM Inscritos no período ---
    mm_insc_count    = 0
    insc_where_hist  = []
    insc_params_hist = []
    if dt_ini:
        insc_where_hist.append("data_inscr >= %s")
        insc_params_hist.append(dt_ini)
    if dt_fim:
        insc_where_hist.append("data_inscr <= %s")
        insc_params_hist.append(dt_fim)
    if polo:
        insc_where_hist.append("polo_normalizado = %s")
        insc_params_hist.append(polo)
    insc_w_hist = ("WHERE " + " AND ".join(insc_where_hist)) if insc_where_hist else ""

    insc_where_cur  = []
    insc_params_cur = []
    if dt_ini:
        insc_where_cur.append("data_inscr >= %s")
        insc_params_cur.append(dt_ini)
    if dt_fim:
        insc_where_cur.append("data_inscr <= %s")
        insc_params_cur.append(dt_fim)
    if polo:
        insc_where_cur.append("polo_normalizado = %s")
        insc_params_cur.append(polo)
    insc_w_cur = ("WHERE " + " AND ".join(insc_where_cur)) if insc_where_cur else ""

    try:
        cur.execute(f"""
            SELECT COUNT(DISTINCT cpf) FROM (
                SELECT cpf FROM mm_inscritos_hist {insc_w_hist}
                UNION
                SELECT cpf FROM mm_inscritos {insc_w_cur}
            ) sub WHERE cpf IS NOT NULL
        """, insc_params_hist + insc_params_cur)
        mm_insc_count = cur.fetchone()[0] or 0
    except Exception:
        try:
            cur.execute(f"SELECT COUNT(*) FROM mm_inscritos_hist {insc_w_hist}", insc_params_hist)
            mm_insc_count = cur.fetchone()[0] or 0
        except Exception:
            mm_insc_count = 0
    _lap("mm_inscritos")

    # --- Comparações: mesmo intervalo filtrado deslocado 6m / 12m (+ YTD) ---
    # Mesma fonte xl_rows do KPI principal (_crgm_periodo_data), só com datas deslocadas.
    vendas_6m = None
    vendas_1a = None
    vendas_ytd = None
    vendas_prev_ytd = None
    compare_6m_period = None
    compare_1a_period = None

    if _pd_dt_ini and _pd_dt_fim:
        try:
            d_ini = date.fromisoformat(_pd_dt_ini)
            d_fim = date.fromisoformat(_pd_dt_fim)
            d_ini_6m = _shift_months(d_ini, -6)
            d_fim_6m = _shift_months(d_fim, -6)
            d_ini_1a = _shift_months(d_ini, -12)
            d_fim_1a = _shift_months(d_fim, -12)
            compare_6m_period = f"{d_ini_6m.isoformat()} → {d_fim_6m.isoformat()}"
            compare_1a_period = f"{d_ini_1a.isoformat()} → {d_fim_1a.isoformat()}"
            _polo = polo or None
            _nivel = nivel or None
            _turma = turma_nome or None
            vendas_6m = _crgm_count_bruto_compare(
                conn, d_ini_6m.isoformat(), d_fim_6m.isoformat(), _polo, _nivel, _turma
            )
            vendas_1a = _crgm_count_bruto_compare(
                conn, d_ini_1a.isoformat(), d_fim_1a.isoformat(), _polo, _nivel, _turma
            )
            vendas_ytd = _crgm_count_bruto_compare(
                conn, date(d_fim.year, 1, 1).isoformat(), d_fim.isoformat(), _polo, _nivel, _turma
            )
            prev_year = d_fim.year - 1
            vendas_prev_ytd = _crgm_count_bruto_compare(
                conn,
                date(prev_year, 1, 1).isoformat(),
                _safe_date(prev_year, d_fim.month, d_fim.day).isoformat(),
                _polo,
                _nivel,
                _turma,
            )
        except Exception as exc:
            logger.warning("kpis comparativos: %s", exc)

    pct_6m = (
        round((vendas / vendas_6m - 1) * 100, 1)
        if vendas_6m is not None and vendas_6m > 0
        else None
    )
    pct_1a = (
        round((vendas / vendas_1a - 1) * 100, 1)
        if vendas_1a is not None and vendas_1a > 0
        else None
    )
    pct_ytd = (
        round((vendas_ytd / vendas_prev_ytd - 1) * 100, 1)
        if vendas_ytd is not None and vendas_prev_ytd and vendas_prev_ytd > 0
        else None
    )
    _lap("comparativos_6m_1a_ytd")

    evolucao = [{"data": d.isoformat(), "count": c} for d, c in sorted(day_counts.items())]
    all_dates_bruto = sorted(set(day_counts_bruto.keys()) | set(day_counts.keys()))
    evolucao_bruto  = [{"data": d.isoformat(), "count": day_counts_bruto.get(d, 0)} for d in all_dates_bruto]

    evolucao_prev = []
    if _pd_dt_ini and _pd_dt_fim:
        try:
            d_ini   = date.fromisoformat(_pd_dt_ini)
            d_fim_d = date.fromisoformat(_pd_dt_fim)
            prev_ini = _shift_months(d_ini,   -12)
            prev_fim = _shift_months(d_fim_d, -12)

            prev_rows = _crgm_periodo_data(
                dt_ini=prev_ini.isoformat(),
                dt_fim=prev_fim.isoformat(),
                polo=polo or None,
                nivel=nivel or None,
                turma=turma_nome or None,
                conn=conn,
            )
            _, _, _, _, _, prev_day_rgms_bruto, _ = _crgm_build_periodo_sets(
                prev_rows, prev_ini.isoformat(), prev_fim.isoformat()
            )
            prev_day_counts = {d: len(s) for d, s in prev_day_rgms_bruto.items()}
            evolucao_prev = [{"data": d.isoformat(), "count": c}
                             for d, c in sorted(prev_day_counts.items())]
        except Exception as exc:
            logger.warning("kpis evolucao_prev: %s", exc)
    _lap("evolucao_prev")

    ranking_polo = [{"nome": p, "total": c}
                    for p, c in sorted(polo_counts.items(), key=lambda x: -x[1])]

    ciclo_bruto    = defaultdict(set)
    ciclo_em_curso = defaultdict(set)
    for row in ciclo_all:
        n = _normalize_rgm(row.get("rgm") or "")
        if not n:
            continue
        c = (row.get("ciclo") or "").strip() or "(sem ciclo)"
        ciclo_bruto[c].add(n)
        if row.get("situacao") == "EM CURSO":
            ciclo_em_curso[c].add(n)
    ranking_ciclo = [
        {"nome": c, "bruto": len(ciclo_bruto[c]), "total": len(ciclo_em_curso.get(c, set()))}
        for c in sorted(ciclo_bruto, key=lambda k: -len(ciclo_bruto[k]))
    ]

    cur.close()
    conn.close()
    _lap("ranking_ciclo")

    logger.info("CRGM kpis TOTAL %.2fs", _time_mod.perf_counter() - _t_start)
    return {
        "kpis": {
            "vendas": vendas,
            "vendas_liquidas": vendas_liquidas,
            "vendas_6m": vendas_6m,
            "pct_6m": pct_6m,
            "vendas_1a": vendas_1a,
            "pct_1a": pct_1a,
            "vendas_ytd": vendas_ytd,
            "vendas_prev_ytd": vendas_prev_ytd,
            "pct_ytd": pct_ytd,
            "delta_6m": (vendas - vendas_6m) if vendas_6m is not None else None,
            "delta_1a": (vendas - vendas_1a) if vendas_1a is not None else None,
            "compare_6m_period": compare_6m_period,
            "compare_1a_period": compare_1a_period,
            "ticket_medio": ticket_medio,
            "media_diaria": media_diaria,
            "dias": dias,
            "mm_inscritos": mm_insc_count,
            "dominant_prefix": _kpi_dom_pfx,
            "fora_padrao_total": len(fora_padrao_rows),
        },
        "evolucao": evolucao,
        "evolucao_bruto": evolucao_bruto,
        "evolucao_prev": evolucao_prev,
        "ranking_polo": ranking_polo,
        "ranking_ciclo": ranking_ciclo,
        "evasao_grid": evasao_grid,
        "fora_padrao_grid": fora_padrao_grid,
    }


def _crgm_compute_agentes(polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome) -> dict:
    """Calcula ranking_agentes, transferencia_regresso, matriculas_grid, metas_aviso, daily_history."""
    import time as _time_mod
    _t_start = _time_mod.perf_counter()
    _t_lap   = _t_start

    def _lap(label):
        nonlocal _t_lap
        _now = _time_mod.perf_counter()
        logger.info("CRGM agentes/[%s] %.2fs", label, _now - _t_lap)
        _t_lap = _now

    _pd_dt_ini, _pd_dt_fim = _crgm_resolve_ciclo_pd_dates(ciclo_nome, turma_nome, dt_ini, dt_fim)

    # Minimal setup: só precisa de _excluded para passar ao ranking
    conn = _pg()
    if _pd_dt_ini or _pd_dt_fim:
        ciclo_all = _crgm_periodo_data_oficial(
            dt_ini=_pd_dt_ini, dt_fim=_pd_dt_fim,
            polo=polo or None, nivel=nivel or None,
            turma=turma_nome or None, conn=conn,
            require_latest_presence=True,
        )
    else:
        ciclo_all = _crgm_periodo_data_oficial(
            polo=polo or None, nivel=nivel or None,
            turma=turma_nome or None, conn=conn,
            require_latest_presence=True,
        )
    _lap("ciclo_all_xl_rows")

    _periodo_rows = []
    if _pd_dt_ini or _pd_dt_fim:
        for row in ciclo_all:
            dm_str = row.get("data_matricula")
            if not dm_str:
                continue
            dm_key = str(dm_str)[:10]
            if _pd_dt_ini and dm_key < _pd_dt_ini:
                continue
            if _pd_dt_fim and dm_key > _pd_dt_fim:
                continue
            _periodo_rows.append(row)
    else:
        _periodo_rows = list(ciclo_all)
    _lap("derive_periodo_inmem")

    rgms_periodo = set()
    rgms_bruto   = set()
    for row in _periodo_rows:
        n = row["rgm"]
        if not n:
            continue
        rgms_bruto.add(n)
        if row["situacao"] == "EM CURSO":
            rgms_periodo.add(n)
    _excluded = rgms_bruto - rgms_periodo
    conn.close()

    # --- Ranking agentes ---
    ranking_agentes, transferencia_regresso, matriculas_grid = _build_agent_ranking_completa_vw(
        _pd_dt_ini, _pd_dt_fim, polo or None, nivel or None, None, None,
        excluded_rgms=_excluded,
        crm_dt_ini=dt_ini or None,
        crm_dt_fim=dt_fim or None,
    )
    _lap("ranking_vw")

    # --- Metas por agente ---
    metas_by_cat       = {}
    campanha_meta_uids = set()
    metas_load_error   = None
    has_camp_overlap   = False
    camp_defs          = None
    try:
        conn2 = _pg()
        cur2  = conn2.cursor()

        cur2.execute("""
            SELECT pcm.kommo_user_id, pcm.meta, pcm.meta_intermediaria, pcm.supermeta
            FROM premiacao_campanha_meta pcm
            JOIN premiacao_campanha pc ON pc.id = pcm.campanha_id
            WHERE COALESCE(pc.ativa, TRUE)
              AND pc.dt_inicio <= %s AND pc.dt_fim >= %s
        """, (dt_fim or '9999-12-31', dt_ini or '1900-01-01'))
        for r in cur2.fetchall():
            uid = _kommo_uid_int(r[0])
            if uid is None:
                continue
            campanha_meta_uids.add(uid)
            metas_by_cat.setdefault("matriculas", {})
            metas_by_cat["matriculas"][uid] = {
                "meta": float(r[1]),
                "intermediaria": float(r[2]),
                "supermeta": float(r[3]),
            }

        cur2.execute("""
            SELECT user_id, meta, COALESCE(meta_intermediaria,0),
                   COALESCE(supermeta,0), categoria
            FROM comercial_metas
            WHERE dt_inicio <= %s AND dt_fim >= %s
        """, (dt_fim or '9999-12-31', dt_ini or '1900-01-01'))
        for r in cur2.fetchall():
            uid = _kommo_uid_int(r[0])
            if uid is None:
                continue
            cat = r[4] or "matriculas"
            if cat == "matriculas" and uid in campanha_meta_uids:
                continue
            metas_by_cat.setdefault(cat, {})
            prev = metas_by_cat[cat].get(uid, {"meta": 0, "intermediaria": 0, "supermeta": 0})
            prev["meta"]          += float(r[1])
            prev["intermediaria"] += float(r[2])
            prev["supermeta"]     += float(r[3])
            metas_by_cat[cat][uid] = prev

        if dt_ini and dt_fim:
            cur2.execute("""
                SELECT def_meta_intermediaria, def_meta, def_supermeta
                FROM premiacao_campanha
                WHERE COALESCE(ativa, TRUE)
                  AND dt_inicio <= %s::date AND dt_fim >= %s::date
                ORDER BY dt_inicio DESC
                LIMIT 1
            """, (dt_fim, dt_ini))
            row_c = cur2.fetchone()
            if row_c:
                has_camp_overlap = True
                camp_defs = row_c

        cur2.close()
        conn2.close()
    except Exception as e:
        logger.warning("agentes metas por periodo: %s", e)
        metas_load_error = str(e)
    _lap("metas_por_periodo")

    mat_metas = metas_by_cat.get("matriculas", {})
    if camp_defs:
        d_i = float(camp_defs[0] or 0)
        d_m = float(camp_defs[1] or 0)
        d_s = float(camp_defs[2] or 0)
        if d_i > 0 or d_m > 0 or d_s > 0:
            for ag in ranking_agentes:
                uid = ag.get("user_id")
                if uid == -1:
                    continue
                uki = _kommo_uid_int(uid)
                if uki is None:
                    continue
                if uki not in mat_metas:
                    mat_metas[uki] = {
                        "meta": d_m,
                        "intermediaria": d_i,
                        "supermeta": d_s,
                    }
    for ag in ranking_agentes:
        uid = ag["user_id"]
        if uid == -1:
            ag["meta"]             = 0
            ag["meta_intermediaria"] = 0
            ag["supermeta"]        = 0
            ag["metas_cat"]        = {}
            continue
        uki = _kommo_uid_int(uid)
        m   = mat_metas.get(uki, {}) if uki is not None else {}
        ag["meta"]             = m.get("meta", 0)
        ag["meta_intermediaria"] = m.get("intermediaria", 0)
        ag["supermeta"]        = m.get("supermeta", 0)
        ag["metas_cat"]        = {}
        for cat, users in metas_by_cat.items():
            if uki is not None and uki in users:
                ag["metas_cat"][cat] = users[uki]

    # --- Atividade Kommo: horas médias por consultor ---
    try:
        _ativ_dt_ini = datetime.strptime(_pd_dt_ini, "%Y-%m-%d").date() if _pd_dt_ini else None
        _ativ_dt_fim = datetime.strptime(_pd_dt_fim, "%Y-%m-%d").date() if _pd_dt_fim else None
    except Exception:
        _ativ_dt_ini = _ativ_dt_fim = None
    if _ativ_dt_ini and _ativ_dt_fim:
        try:
            _ativ_map = horas_media_por_consultor(_ativ_dt_ini, _ativ_dt_fim)
        except Exception as _e:
            logger.warning("agentes horas_media_por_consultor falhou: %s", _e)
            _ativ_map = {}
    else:
        _ativ_map = {}
    for ag in ranking_agentes:
        uid = ag.get("user_id")
        uki = _kommo_uid_int(uid)
        if uki is not None and uki != -1:
            ag["horas_media"] = _ativ_map.get(uki, {}).get("horas_media")
        else:
            ag["horas_media"] = None
    _lap("horas_media_kommo")

    # --- daily_history: comparativos dia-a-dia (vs 6m e 1 ano) ---
    daily_history: dict = {}
    try:
        _unique_grid_dates = list({g["data"] for g in matriculas_grid if g.get("data")})
        if _unique_grid_dates:
            _target_map: dict = {}
            _all_targets: set = set()
            for _ds in _unique_grid_dates:
                try:
                    _d    = date.fromisoformat(_ds)
                    _d_6m = _shift_months(_d, -6)
                    _d_1y = _shift_months(_d, -12)
                    _target_map[_ds] = (_d_6m.isoformat(), _d_1y.isoformat())
                    _all_targets.add(_d_6m.isoformat())
                    _all_targets.add(_d_1y.isoformat())
                except Exception:
                    pass
            if _all_targets:
                _hist_day_counts = _crgm_day_bruto_counts(
                    sorted(_all_targets),
                    polo=polo or None,
                    nivel=nivel or None,
                    turma=turma_nome or None,
                )
                for _ds, (_d6m, _d1y) in _target_map.items():
                    daily_history[_ds] = {
                        "vs6m": _hist_day_counts.get(_d6m),
                        "vs1y": _hist_day_counts.get(_d1y),
                    }
    except Exception as _dhe:
        logger.warning("agentes daily_history: %s", _dhe)
        daily_history = {}
    _lap("daily_history")

    metas_aviso = None
    if metas_load_error:
        metas_aviso = (
            "Não foi possível carregar as metas deste período. "
            "Verifique o log do servidor ou a conexão com o banco."
        )
    elif (
        not mat_metas
        and not has_camp_overlap
        and any(
            (a.get("matriculas_periodo") or 0) > 0 and a.get("user_id") != -1
            for a in ranking_agentes
        )
    ):
        metas_aviso = (
            "Nenhuma meta de matrículas cadastrada para o intervalo de datas dos filtros "
            "(ou campanha sem sobreposição). Cadastre na Premiação (metas por agente ou pré-definição) "
            "ou ajuste dt início/fim."
        )

    logger.info("CRGM agentes TOTAL %.2fs", _time_mod.perf_counter() - _t_start)
    return {
        "ranking_agentes": ranking_agentes,
        "transferencia_regresso": transferencia_regresso,
        "matriculas_grid": matriculas_grid,
        "metas_aviso": metas_aviso,
        "daily_history": daily_history,
    }


def _crgm_compute_grids(polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome) -> dict:
    """Calcula leads_grid e evasao (objeto completo: total/por_tipo/por_agente/itens)."""
    import time as _time_mod
    from concurrent.futures import ThreadPoolExecutor as _CRGM_TPE
    _t_start = _time_mod.perf_counter()
    _t_lap   = _t_start

    def _lap(label):
        nonlocal _t_lap
        _now = _time_mod.perf_counter()
        logger.info("CRGM grids/[%s] %.2fs", label, _now - _t_lap)
        _t_lap = _now

    _pd_dt_ini, _pd_dt_fim = _crgm_resolve_ciclo_pd_dates(ciclo_nome, turma_nome, dt_ini, dt_fim)

    conn = _pg()
    if _pd_dt_ini or _pd_dt_fim:
        ciclo_all = _crgm_periodo_data_oficial(
            dt_ini=_pd_dt_ini, dt_fim=_pd_dt_fim,
            polo=polo or None, nivel=nivel or None,
            turma=turma_nome or None, conn=conn,
            mark_missing_as_transferido=True,
        )
    else:
        ciclo_all = _crgm_periodo_data_oficial(
            polo=polo or None, nivel=nivel or None,
            turma=turma_nome or None, conn=conn,
            mark_missing_as_transferido=True,
        )
    _lap("ciclo_all_xl_rows")

    _periodo_rows = []
    if _pd_dt_ini or _pd_dt_fim:
        for row in ciclo_all:
            dm_str = row.get("data_matricula")
            if not dm_str:
                continue
            dm_key = str(dm_str)[:10]
            if _pd_dt_ini and dm_key < _pd_dt_ini:
                continue
            if _pd_dt_fim and dm_key > _pd_dt_fim:
                continue
            _periodo_rows.append(row)
    else:
        _periodo_rows = list(ciclo_all)
    _lap("derive_periodo_inmem")

    evasao_rows = []
    for row in _periodo_rows:
        if row["rgm"] and row["situacao"] != "EM CURSO":
            evasao_rows.append(row)

    _fp_dom_pfx = _crgm_effective_dominant_prefix(
        [r["rgm"] for r in _periodo_rows if r.get("situacao") == "EM CURSO"]
        or [r["rgm"] for r in _periodo_rows if r.get("rgm")]
    )
    _fp_overrides = _load_outlier_contagem_overrides()
    fora_padrao_rows = _crgm_fora_padrao_rows(
        _periodo_rows, _fp_dom_pfx, _fp_overrides, apenas_nao_conta=True,
    )
    conn.close()

    def _task_leads_grid():
        try:
            _lg_conn = _pg_kommo()
            _lg_cur  = _lg_conn.cursor()
            _ep_lg_ini = _date_to_epoch(dt_ini or None)
            _ep_lg_fim = _date_to_epoch(dt_fim or None)
            if _ep_lg_fim is not None:
                _ep_lg_fim += 86399
            _lg_cw = [
                "l.responsible_user_id IS NOT NULL",
                "NOT l.is_deleted",
                "l.created_at IS NOT NULL",
            ]
            _lg_cp: list = []
            if _ep_lg_ini is not None:
                _lg_cw.append("l.created_at >= %s")
                _lg_cp.append(_ep_lg_ini)
            if _ep_lg_fim is not None:
                _lg_cw.append("l.created_at <= %s")
                _lg_cp.append(_ep_lg_fim)
            _lg_where = "WHERE " + " AND ".join(_lg_cw)
            _lg_cur.execute(
                f"""
                SELECT (to_timestamp(l.created_at) AT TIME ZONE 'America/Sao_Paulo')::date AS d,
                       l.responsible_user_id,
                       COUNT(*) AS c
                FROM leads l
                {_lg_where}
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
                _lg_cp,
            )
            _out = [
                {
                    "data": (row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])),
                    "user_id": int(row[1]),
                    "count": int(row[2]),
                }
                for row in _lg_cur.fetchall()
            ]
            _lg_cur.close()
            _lg_conn.close()
            return _out
        except Exception as _lg_e:
            logger.warning("grids leads_grid: %s", _lg_e)
            return []

    def _task_rgm_kommo_lookup():
        rgms = {ev["rgm"] for ev in evasao_rows if ev.get("rgm")}
        rgms |= {fp["rgm"] for fp in fora_padrao_rows if fp.get("rgm")}
        return _crgm_kommo_lookup_rgms(rgms)

    with _CRGM_TPE(max_workers=2) as _pool:
        _fut_leads  = _pool.submit(_task_leads_grid)
        _fut_rgm    = _pool.submit(_task_rgm_kommo_lookup)
        leads_grid  = _fut_leads.result()
        ev_rgm_to_uid, ev_uid_to_nome = _fut_rgm.result()
    _lap("parallel_leads_rgm_lookup")

    evasao_data = {"total": 0, "por_tipo": {}, "por_agente": [], "itens": []}
    if evasao_rows:
        por_tipo   = defaultdict(int)
        por_agente = defaultdict(list)
        for ev in evasao_rows:
            sit      = ev["situacao"] or "OUTROS"
            por_tipo[sit] += 1
            uid_ev   = ev_rgm_to_uid.get(ev["rgm"])
            nome_ev  = ev_uid_to_nome.get(uid_ev, "Não identificado") if uid_ev else "Não identificado"
            por_agente[nome_ev].append(ev)

        evasao_data = {
            "total": len(evasao_rows),
            "por_tipo": dict(por_tipo),
            "por_agente": [
                {"agente": ag_nome, "total": len(itens),
                 "itens": [{"rgm": i["rgm"], "nome": i["nome"],
                            "situacao": i["situacao"], "data_matricula": i["data_matricula"]}
                           for i in itens]}
                for ag_nome, itens in sorted(por_agente.items(), key=lambda x: -len(x[1]))
            ],
            "itens": [
                {"rgm": ev["rgm"], "nome": ev["nome"],
                 "situacao": ev["situacao"], "data_matricula": ev["data_matricula"],
                 "agente": ev_uid_to_nome.get(ev_rgm_to_uid.get(ev["rgm"]), "Não identificado")}
                for ev in evasao_rows
            ],
        }
    _lap("evasao_kommo_lookup")

    fora_padrao_data = _crgm_build_fora_padrao_data(
        fora_padrao_rows, _fp_dom_pfx, ev_rgm_to_uid, ev_uid_to_nome,
    )
    _lap("fora_padrao_build")

    logger.info("CRGM grids TOTAL %.2fs", _time_mod.perf_counter() - _t_start)
    return {
        "leads_grid": leads_grid,
        "evasao": evasao_data,
        "fora_padrao": fora_padrao_data,
    }


@comercial_rgm_bp.route("/api/comercial-rgm/data/kpis")
def crgm_data_kpis():
    polo       = request.args.get("polo", "")
    nivel      = request.args.get("nivel", "")
    dt_ini     = request.args.get("dt_ini", "")
    dt_fim     = request.args.get("dt_fim", "")
    ciclo_nome = request.args.get("ciclo", "")
    turma_nome = request.args.get("turma", "")

    _cache_key = _crgm_cache_key_prefixed("kpis")
    _no_cache  = request.args.get("no_cache") == "1"
    if not _no_cache:
        _cached = _crgm_cache_get(_cache_key)
        if _cached is not None:
            logger.info("CRGM /data/kpis CACHE HIT key=%s", _cache_key)
            return jsonify(_cached)

    try:
        result  = _crgm_compute_kpis(polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome)
        payload = {"ok": True, **result}
        _crgm_cache_set(_cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        logger.exception("comercial_rgm data/kpis error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/data/agentes")
def crgm_data_agentes():
    polo       = request.args.get("polo", "")
    nivel      = request.args.get("nivel", "")
    dt_ini     = request.args.get("dt_ini", "")
    dt_fim     = request.args.get("dt_fim", "")
    ciclo_nome = request.args.get("ciclo", "")
    turma_nome = request.args.get("turma", "")

    _cache_key = _crgm_cache_key_prefixed("agentes")
    _no_cache  = request.args.get("no_cache") == "1"
    if not _no_cache:
        _cached = _crgm_cache_get(_cache_key)
        if _cached is not None:
            logger.info("CRGM /data/agentes CACHE HIT key=%s", _cache_key)
            return jsonify(_cached)

    try:
        result  = _crgm_compute_agentes(polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome)
        payload = {"ok": True, **result}
        _crgm_cache_set(_cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        logger.exception("comercial_rgm data/agentes error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/data/grids")
def crgm_data_grids():
    polo       = request.args.get("polo", "")
    nivel      = request.args.get("nivel", "")
    dt_ini     = request.args.get("dt_ini", "")
    dt_fim     = request.args.get("dt_fim", "")
    ciclo_nome = request.args.get("ciclo", "")
    turma_nome = request.args.get("turma", "")

    _cache_key = _crgm_cache_key_prefixed("grids")
    _no_cache  = request.args.get("no_cache") == "1"
    if not _no_cache:
        _cached = _crgm_cache_get(_cache_key)
        if _cached is not None:
            logger.info("CRGM /data/grids CACHE HIT key=%s", _cache_key)
            return jsonify(_cached)

    try:
        result  = _crgm_compute_grids(polo, nivel, dt_ini, dt_fim, ciclo_nome, turma_nome)
        payload = {"ok": True, **result}
        _crgm_cache_set(_cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        logger.exception("comercial_rgm data/grids error")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Interação de leads (quantos leads recebidos falaram ≥1x) ─────────────
# Fonte: view vw_lead_interacao no projeto Supabase do comercial_feedback
# (sql/lead_interacao.sql). A view devolve (lead_id, dia) dos dias em que o
# cliente enviou >=1 mensagem, já em America/Sao_Paulo.
_COM_FB_URL = os.getenv(
    "SUPABASE_FEEDBACK_URL", "https://vtlbndvcgajcoajhcnnx.supabase.co"
).rstrip("/")
_COM_FB_KEY = os.getenv(
    "SUPABASE_FEEDBACK_KEY", "sb_publishable_sW0h7aqgrjiwqGqKpawm4g_FuMi5xU_"
)


def _fetch_lead_interacao(lead_ids: list, dt_ini: str, dt_fim: str) -> dict:
    """Retorna {lead_id: set(dias)} para os leads com >=1 msg de cliente no período."""
    out: dict = {}
    if not (_COM_FB_URL and _COM_FB_KEY and lead_ids):
        return out
    headers = {
        "apikey": _COM_FB_KEY,
        "Authorization": f"Bearer {_COM_FB_KEY}",
        "Accept": "application/json",
    }
    CHUNK = 400
    for i in range(0, len(lead_ids), CHUNK):
        lote = lead_ids[i:i + CHUNK]
        params = [
            ("select", "lead_id,dia"),
            ("lead_id", f"in.({','.join(str(x) for x in lote)})"),
            ("dia", f"gte.{dt_ini}"),
            ("dia", f"lte.{dt_fim}"),
        ]
        try:
            r = requests.get(
                f"{_COM_FB_URL}/rest/v1/vw_lead_interacao",
                headers=headers, params=params, timeout=30,
            )
            r.raise_for_status()
            for row in r.json() or []:
                lid = row.get("lead_id")
                dia = row.get("dia")
                if lid is None or not dia:
                    continue
                out.setdefault(int(lid), set()).add(str(dia)[:10])
        except Exception as e:
            logger.warning("interacao-leads: falha lote %d: %s", i // CHUNK, e)
    return out


@comercial_rgm_bp.route("/api/comercial-rgm/interacao-leads")
def crgm_interacao_leads():
    """Por agente: quantos leads criados no período interagiram >=1x no período.

    Resposta: { ok, por_agente: { "<uid>": { "total": N, "por_dia": {dia: n} } } }
    """
    dt_ini = request.args.get("dt_ini", "")
    dt_fim = request.args.get("dt_fim", "")
    if not (dt_ini and dt_fim):
        return jsonify({"ok": True, "por_agente": {}})

    _cache_key = _crgm_cache_key_prefixed("interacao")
    if request.args.get("no_cache") != "1":
        _cached = _crgm_cache_get(_cache_key)
        if _cached is not None:
            return jsonify(_cached)

    try:
        # Mesma base do leads_grid: leads criados no período, não deletados
        ep_ini = _date_to_epoch(dt_ini)
        ep_fim = _date_to_epoch(dt_fim) + 86399
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute(
            """
            SELECT l.id, l.responsible_user_id
            FROM leads l
            WHERE l.responsible_user_id IS NOT NULL
              AND NOT l.is_deleted
              AND l.created_at IS NOT NULL
              AND l.created_at >= %s
              AND l.created_at <= %s
            """,
            (ep_ini, ep_fim),
        )
        uid_leads: dict = {}
        for lid, uid in kcur.fetchall():
            if lid is not None and uid is not None:
                uid_leads.setdefault(int(uid), []).append(int(lid))
        kcur.close()
        kconn.close()

        todos = sorted({lid for ids in uid_leads.values() for lid in ids})
        inter = _fetch_lead_interacao(todos, dt_ini, dt_fim)

        por_agente = {}
        for uid, ids in uid_leads.items():
            por_dia: dict = {}
            total = 0
            for lid in ids:
                dias = inter.get(lid)
                if not dias:
                    continue
                total += 1
                for d in dias:
                    por_dia[d] = por_dia.get(d, 0) + 1
            if total:
                por_agente[str(uid)] = {"total": total, "por_dia": por_dia}

        payload = {"ok": True, "por_agente": por_agente}
        _crgm_cache_set(_cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        logger.exception("comercial_rgm interacao-leads error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.get("/api/comercial-rgm/atividade-kommo")
def crgm_atividade_kommo():
    """Retorna linhas detalhadas de atividade Kommo do Supabase para o período."""
    try:
        dt_ini = datetime.strptime(request.args.get("dt_ini", ""), "%Y-%m-%d").date()
        dt_fim = datetime.strptime(request.args.get("dt_fim", ""), "%Y-%m-%d").date()
    except Exception:
        return jsonify({"ok": False, "error": "dt_ini/dt_fim obrigatórios (YYYY-MM-DD)"}), 400
    user_id_raw = request.args.get("user_id")
    user_id = int(user_id_raw) if (user_id_raw and user_id_raw.isdigit()) else None
    detalhado = request.args.get("detalhado") == "1"
    linhas = fetch_atividade_periodo(dt_ini, dt_fim, user_id=user_id, incluir_intervalos=detalhado)
    user_ids = sorted({int(r["created_by"]) for r in linhas if r.get("created_by") is not None})
    nomes = _fetch_kommo_user_names(user_ids) if user_ids else {}
    for r in linhas:
        uid = r.get("created_by")
        if uid is not None:
            r["nome"] = nomes.get(int(uid), f"User #{uid}")
    return jsonify({
        "ok": True,
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "incluir_intervalos": detalhado,
        "linhas": linhas,
    })


@comercial_rgm_bp.route("/api/comercial-rgm/agente-detalhe")
def crgm_agente_detalhe():
    """Lista as matrículas do período para um agente específico (ou transferencia/regresso)."""
    from flask import Response as _FlaskResponse

    try:
        user_id  = request.args.get("user_id", "")
        dt_ini   = request.args.get("dt_ini", "")
        dt_fim   = request.args.get("dt_fim", "")
        polo     = request.args.get("polo", "")
        nivel    = request.args.get("nivel", "")
        ciclo    = request.args.get("ciclo", "")
        turma    = request.args.get("turma", "")
        fmt      = request.args.get("fmt", "json")   # json | csv

        try:
            uid = int(user_id) if user_id not in ("", "-1") else -1
        except ValueError:
            return jsonify({"ok": False, "error": "user_id inválido"}), 400

        # 1. Buscar todos os RGMs do período (mesma lógica do ranking)
        where, params = [], []
        if nivel:   where.append("nivel = %s");          params.append(nivel)
        if dt_ini:  where.append("data_matricula >= %s"); params.append(dt_ini)
        if dt_fim:  where.append("data_matricula <= %s"); params.append(dt_fim)
        if ciclo:   where.append("ciclo = %s");          params.append(ciclo)
        if turma:   where.append("turma = %s");          params.append(turma)
        w = ("WHERE " + " AND ".join(where)) if where else ""

        try:
            conn = _pg()
            cur  = conn.cursor()
            cur.execute(
                f"SELECT rgm, nome, polo, nivel, data_matricula, ciclo, turma, tipo_matricula "
                f"FROM comercial_rgm_atual {w} ORDER BY data_matricula DESC NULLS LAST",
                params,
            )
            rows = cur.fetchall()
            _det_excluded = _crgm_excluded_rgms(conn)

            # CPF + telefone via xl_rows (último snapshot matriculados)
            rgm_extra = {}
            try:
                cur.execute("""
                    SELECT
                        regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g') AS rgm,
                        NULLIF(TRIM(COALESCE(r.data->>'cpf','')), '')           AS cpf,
                        COALESCE(
                            NULLIF(TRIM(COALESCE(r.data->>'fone_cel','')), ''),
                            NULLIF(TRIM(COALESCE(r.data->>'fone_res','')), ''),
                            NULLIF(TRIM(COALESCE(r.data->>'fone_com','')), '')
                        ) AS telefone
                    FROM xl_rows r
                    JOIN xl_snapshots s ON s.id = r.snapshot_id
                    WHERE s.id = (SELECT id FROM xl_snapshots WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1)
                      AND COALESCE(r.data->>'rgm','') ~ '[0-9]'
                """)
                for r_extra in cur.fetchall():
                    nk = _normalize_rgm(r_extra[0])
                    if nk and nk not in rgm_extra:
                        rgm_extra[nk] = {"cpf": r_extra[1] or "", "telefone": r_extra[2] or ""}
            except Exception as _ex:
                logger.warning("agente-detalhe cpf/tel: %s", _ex)

            cur.close(); conn.close()
        except Exception as e:
            logger.warning("agente-detalhe db: %s", e)
            return jsonify({"ok": False, "error": str(e)}), 500

        # 2. Mapa RGM → responsible_user_id (Kommo)
        try:
            kconn = _pg_kommo()
            kcur  = kconn.cursor()
            kcur.execute("""
                SELECT DISTINCT ON (v.rgm) v.rgm, l.responsible_user_id
                FROM vw_leads_rgm v
                JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                WHERE l.responsible_user_id IS NOT NULL
                ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
            """)
            rgm_to_uid = {}
            for row in kcur.fetchall():
                nk = _normalize_rgm(row[0])
                if nk and row[1]:
                    rgm_to_uid[nk] = row[1]
            kcur.close(); kconn.close()
        except Exception as e:
            logger.warning("agente-detalhe kommo: %s", e)
            rgm_to_uid = {}

        _apply_conflito_overrides_to_rgm_map(rgm_to_uid)

        # Consultores excluídos: reatribui para Admin Sistema
        _apply_reassign_to_rgm_map(rgm_to_uid)

        # 3. Calcular prefixo dominante e overrides de contagem de outliers
        all_rgms = [_normalize_rgm(row[0]) for row in rows if row[0]]
        dominant_prefix = _crgm_effective_dominant_prefix(all_rgms) or 99
        _outlier_overrides = _load_outlier_contagem_overrides()

        # 4. Filtrar linhas do agente solicitado
        seen = set()
        resultado = []
        for row in rows:
            rgm_raw, nome, p_polo, p_nivel, dm, ciclo_v, turma_v, tipo_mat = row
            n = _normalize_rgm(rgm_raw)
            if not n or n in seen or n in _det_excluded:
                continue
            if polo and normalize_polo_display(p_polo or "") != polo:
                continue
            seen.add(n)
            assigned_uid = rgm_to_uid.get(n)
            if uid == -1:
                if assigned_uid is not None:
                    continue
            else:
                if assigned_uid != uid:
                    continue
            try:
                data_str = dm.isoformat() if hasattr(dm, "isoformat") else str(dm)[:10]
            except Exception:
                data_str = ""
            outlier = _is_rgm_prefix_outlier(n, dominant_prefix)
            conta_venda = n in _outlier_overrides
            conta_para_meta = _rgm_conta_para_venda(n, dominant_prefix, _outlier_overrides)
            extra = rgm_extra.get(n, {})
            resultado.append({
                "rgm": rgm_raw or "",
                "nome": nome or "",
                "cpf": extra.get("cpf", ""),
                "telefone": extra.get("telefone", ""),
                "polo": p_polo or "",
                "nivel": p_nivel or "",
                "data_matricula": data_str,
                "ciclo": ciclo_v or "",
                "turma": turma_v or "",
                "tipo_matricula": tipo_mat or "",
                "outlier": outlier,
                "conta_venda": conta_venda,
                "conta_para_meta": conta_para_meta,
            })

        total_contando = sum(1 for r in resultado if r["conta_para_meta"])

        if fmt == "csv":
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=";")
            writer.writerow(["RGM", "Nome", "CPF", "Telefone", "Polo", "Nível", "Data Matrícula", "Ciclo", "Turma", "Tipo Matrícula", "Outlier RGM", "Conta Venda", "Conta Meta"])
            for r in resultado:
                writer.writerow([r["rgm"], r["nome"], r["cpf"], r["telefone"],
                                 r["polo"], r["nivel"],
                                 r["data_matricula"], r["ciclo"], r["turma"],
                                 r["tipo_matricula"],
                                 "SIM" if r["outlier"] else "",
                                 "SIM" if r["conta_venda"] else "",
                                 "SIM" if r["conta_para_meta"] else "NÃO"])
            safe_uid = str(uid).replace("-", "neg")
            return _FlaskResponse(
                buf.getvalue(),
                mimetype="text/csv; charset=utf-8",
                headers={"Content-Disposition": f"attachment; filename=matriculas_agente_{safe_uid}.csv"},
            )

        return jsonify({
            "ok": True,
            "total": len(resultado),
            "total_contando": total_contando,
            "total_contavel": total_contando,  # alias p/ front legado
            "dominant_prefix": dominant_prefix,
            "itens": resultado,
        })

    except Exception as e:
        logger.exception("agente-detalhe erro inesperado: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/outlier/contar-venda", methods=["POST", "DELETE"])
def crgm_outlier_contar_venda():
    """Admin: marcar (POST) ou desmarcar (DELETE) um RGM outlier para contar nas vendas."""
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "Apenas administradores podem executar esta ação"}), 403
    try:
        body = request.get_json(force=True) or {}
        rgm_raw = body.get("rgm", "")
        rgm = _normalize_rgm(rgm_raw)
        if not rgm:
            return jsonify({"ok": False, "error": "RGM inválido"}), 400
        conn = _pg()
        cur = conn.cursor()
        if request.method == "POST":
            counted_by = session.get("username") or session.get("user", "admin")
            cur.execute("""
                INSERT INTO comercial_rgm_outlier_contagem (rgm, counted_at, counted_by)
                VALUES (%s, NOW(), %s)
                ON CONFLICT (rgm) DO UPDATE SET counted_at = NOW(), counted_by = EXCLUDED.counted_by
            """, (rgm, counted_by))
            conn.commit()
            cur.close(); conn.close()
            clear_crgm_data_cache(reason=f"outlier contar-venda POST rgm={rgm}")
            return jsonify({"ok": True, "rgm": rgm, "acao": "contando"})
        else:  # DELETE
            cur.execute("DELETE FROM comercial_rgm_outlier_contagem WHERE rgm = %s", (rgm,))
            conn.commit()
            cur.close(); conn.close()
            clear_crgm_data_cache(reason=f"outlier contar-venda DELETE rgm={rgm}")
            return jsonify({"ok": True, "rgm": rgm, "acao": "removido"})
    except Exception as e:
        logger.exception("outlier/contar-venda erro: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/categorias")
def crgm_metas_categorias():
    return jsonify({"ok": True, "categorias": METAS_CATEGORIAS})


@comercial_rgm_bp.route("/api/comercial-rgm/diagnostics")
def crgm_diagnostics():
    """Diagnostic endpoint to debug RGM matching between CSV/MM and Kommo."""
    try:
        # RGMs from CSV
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT rgm FROM comercial_rgm")
        csv_rgms = set()
        csv_raw_samples = []
        for r in cur.fetchall():
            raw = r[0]
            n = _normalize_rgm(raw)
            if n:
                csv_rgms.add(n)
                if len(csv_raw_samples) < 5:
                    csv_raw_samples.append({"raw": raw, "normalized": n})

        # RGMs from MM
        cur.execute("SELECT rgm, tipo_matricula FROM mm_matriculados LIMIT 500")
        mm_rgms = set()
        mm_raw_samples = []
        for r in cur.fetchall():
            n = _normalize_rgm(r[0])
            if n:
                mm_rgms.add(n)
                if len(mm_raw_samples) < 5:
                    mm_raw_samples.append({"raw": r[0], "normalized": n, "tipo": r[1]})

        cur.close()
        conn.close()

        # RGMs from Kommo (source 1: cf_values)
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT lcf.field_name,
                   lcf.values_json->0->>'value' AS raw_val,
                   l.responsible_user_id, l.status_id
            FROM lead_custom_field_values lcf
            JOIN leads l ON l.id = lcf.lead_id AND l.is_deleted = FALSE
            WHERE LOWER(lcf.field_name) = 'rgm'
              AND lcf.values_json->0->>'value' IS NOT NULL
              AND lcf.values_json->0->>'value' != ''
        """)
        kommo_cf_rgms = {}
        cf_samples = []
        for r in kcur.fetchall():
            n = _normalize_rgm(r[1])
            if n:
                kommo_cf_rgms[n] = r[2]
                if len(cf_samples) < 5:
                    cf_samples.append({"field_name": r[0], "raw": r[1], "normalized": n,
                                       "user_id": r[2], "status": r[3]})

        # RGMs from Kommo (source 2: custom_fields_json)
        kcur.execute("""
            SELECT cf_elem->>'field_name' AS fname,
                   cf_elem->'values'->0->>'value' AS raw_val,
                   l.responsible_user_id, l.status_id
            FROM leads l,
                 jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) cf_elem
            WHERE l.is_deleted = FALSE
              AND LOWER(cf_elem->>'field_name') = 'rgm'
              AND cf_elem->'values'->0->>'value' IS NOT NULL
              AND cf_elem->'values'->0->>'value' != ''
        """)
        kommo_json_rgms = {}
        json_samples = []
        for r in kcur.fetchall():
            n = _normalize_rgm(r[1])
            if n:
                kommo_json_rgms[n] = r[2]
                if len(json_samples) < 5:
                    json_samples.append({"field_name": r[0], "raw": r[1], "normalized": n,
                                          "user_id": r[2], "status": r[3]})

        # Also check distinct field_name values that contain 'rgm'
        kcur.execute("""
            SELECT DISTINCT field_name FROM lead_custom_field_values
            WHERE LOWER(field_name) LIKE '%rgm%'
        """)
        rgm_field_names = [r[0] for r in kcur.fetchall()]

        kcur.close()
        kconn.close()

        all_kommo = set(kommo_cf_rgms.keys()) | set(kommo_json_rgms.keys())
        all_base = csv_rgms | mm_rgms
        matched = all_base & all_kommo
        unmatched = all_base - all_kommo

        return jsonify({
            "ok": True,
            "csv_rgms": len(csv_rgms),
            "mm_rgms": len(mm_rgms),
            "all_base_rgms": len(all_base),
            "kommo_cf_values_rgms": len(kommo_cf_rgms),
            "kommo_json_rgms": len(kommo_json_rgms),
            "kommo_total_unique": len(all_kommo),
            "matched": len(matched),
            "unmatched": len(unmatched),
            "match_rate": f"{len(matched)/max(len(all_base),1)*100:.1f}%",
            "rgm_field_names_in_kommo": rgm_field_names,
            "samples": {
                "csv": csv_raw_samples,
                "mm": mm_raw_samples,
                "kommo_cf": cf_samples,
                "kommo_json": json_samples,
                "unmatched": sorted(list(unmatched))[:15],
                "matched": sorted(list(matched))[:15],
            }
        })
    except Exception as e:
        logger.exception("diagnostics error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas", methods=["GET"])
def crgm_get_metas():
    try:
        conn = _pg()
        cur = conn.cursor()
        dt_ini = request.args.get("dt_ini", "")
        dt_fim = request.args.get("dt_fim", "")
        categoria = request.args.get("categoria", "")
        wheres = []
        params = []
        if dt_ini and dt_fim:
            wheres.append("dt_inicio <= %s AND dt_fim >= %s")
            params.extend([dt_fim, dt_ini])
        if categoria:
            wheres.append("categoria = %s")
            params.append(categoria)
        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT id, user_id, user_name, meta,
                   COALESCE(meta_intermediaria,0), COALESCE(supermeta,0),
                   dt_inicio, dt_fim, descricao, categoria
            FROM comercial_metas {w}
            ORDER BY dt_inicio DESC, categoria, user_name
        """, params)
        rows = [{"id": r[0], "user_id": r[1], "user_name": r[2],
                 "meta": float(r[3]), "meta_intermediaria": float(r[4]),
                 "supermeta": float(r[5]),
                 "dt_inicio": r[6].isoformat() if r[6] else None,
                 "dt_fim": r[7].isoformat() if r[7] else None,
                 "descricao": r[8], "categoria": r[9] or "matriculas"}
                for r in cur.fetchall()]

        premiacao_campanhas = []
        pcm_sums = {}
        try:
            cur.execute("""
                SELECT campanha_id,
                       COALESCE(SUM(meta), 0),
                       COALESCE(SUM(meta_intermediaria), 0),
                       COALESCE(SUM(supermeta), 0),
                       COUNT(*)::int
                FROM premiacao_campanha_meta
                GROUP BY campanha_id
            """)
            for pr in cur.fetchall():
                pcm_sums[int(pr[0])] = {
                    "meta": float(pr[1]),
                    "meta_intermediaria": float(pr[2]),
                    "supermeta": float(pr[3]),
                    "agentes": int(pr[4]),
                }
        except Exception as e:
            logger.warning("premiacao_campanha_meta sums: %s", e)

        camp_rows = []
        row_fmt = "basic"
        try:
            cur.execute("""
                SELECT id, nome, dt_inicio, dt_fim,
                       def_meta_intermediaria, def_meta, def_supermeta,
                       COALESCE(ativa, TRUE)
                FROM premiacao_campanha
                ORDER BY dt_inicio DESC
            """)
            camp_rows = cur.fetchall()
            row_fmt = "full"
        except Exception as e:
            logger.warning("premiacao_campanha (cols def_*): %s", e)
            try:
                cur.execute("""
                    SELECT id, nome, dt_inicio, dt_fim, COALESCE(ativa, TRUE)
                    FROM premiacao_campanha
                    ORDER BY dt_inicio DESC
                """)
                camp_rows = cur.fetchall()
            except Exception as e2:
                logger.warning("premiacao_campanha (basico): %s", e2)

        tier_by_cid = {}
        try:
            cur.execute(
                "SELECT campanha_id, tier, valor_por_mat FROM premiacao_tier_bonus"
            )
            for tr in cur.fetchall():
                cid_t = int(tr[0])
                tier_by_cid.setdefault(cid_t, {})[tr[1]] = float(tr[2])
        except Exception as e:
            logger.warning("premiacao_tier_bonus: %s", e)

        for r in camp_rows:
            try:
                if row_fmt == "full":
                    cid = int(r[0])
                    di, dm, ds = r[4], r[5], r[6]
                    ativa = bool(r[7])
                else:
                    cid = int(r[0])
                    di, dm, ds = None, None, None
                    ativa = bool(r[4])
                premiacao_campanhas.append({
                    "id": cid,
                    "nome": r[1],
                    "dt_inicio": r[2].isoformat() if r[2] else None,
                    "dt_fim": r[3].isoformat() if r[3] else None,
                    "ativa": ativa,
                    "metas_padrao": {
                        "meta_intermediaria": float(di) if di is not None else None,
                        "meta": float(dm) if dm is not None else None,
                        "supermeta": float(ds) if ds is not None else None,
                    },
                    "pcm_totais": pcm_sums.get(cid, {
                        "meta": 0.0, "meta_intermediaria": 0.0, "supermeta": 0.0, "agentes": 0,
                    }),
                    "tiers": tier_by_cid.get(cid, {}),
                })
            except Exception as row_e:
                logger.warning("premiacao_campanha linha ignorada: %s | row=%s", row_e, r)

        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "metas": rows,
            "categorias": METAS_CATEGORIAS,
            "premiacao_campanhas": premiacao_campanhas,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas", methods=["POST"])
def crgm_save_metas():
    data = request.get_json(force=True)
    metas = data.get("metas", [])
    if not metas:
        return jsonify({"error": "Nenhuma meta enviada"}), 400
    valid_cats = {c["id"] for c in METAS_CATEGORIAS}
    try:
        conn = _pg()
        cur = conn.cursor()
        saved = 0
        for m in metas:
            uid = int(m["user_id"])
            meta_val = float(m.get("meta", 0))
            intermediaria = float(m.get("meta_intermediaria", 0))
            supermeta = float(m.get("supermeta", 0))
            name = m.get("user_name", "")
            dt_inicio = m.get("dt_inicio")
            dt_fim = m.get("dt_fim")
            descricao = m.get("descricao", "")
            categoria = m.get("categoria", "matriculas")
            if categoria not in valid_cats:
                categoria = "matriculas"
            if not dt_inicio or not dt_fim:
                continue
            if meta_val <= 0 and intermediaria <= 0 and supermeta <= 0:
                continue
            cur.execute("""
                INSERT INTO comercial_metas
                    (user_id, user_name, meta, meta_intermediaria, supermeta,
                     categoria, dt_inicio, dt_fim, descricao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (uid, name, meta_val, intermediaria, supermeta,
                  categoria, dt_inicio, dt_fim, descricao))
            saved += 1
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": saved})
    except Exception as e:
        logger.exception("save metas error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/batch", methods=["PUT"])
def crgm_update_metas_batch():
    """Salva metas de múltiplos agentes de uma vez para um período."""
    try:
        body    = request.json or {}
        dt_ini  = body.get("dt_inicio")
        dt_fim  = body.get("dt_fim")
        descr   = body.get("descricao", "")
        cat     = body.get("categoria", "matriculas")
        items   = body.get("items", [])   # [{user_id, user_name, meta, meta_intermediaria, supermeta}]

        if not dt_ini or not dt_fim or not items:
            return jsonify({"ok": False, "error": "dt_inicio, dt_fim e items são obrigatórios"}), 400

        conn = _pg()
        cur  = conn.cursor()
        saved = 0
        for it in items:
            uid   = it.get("user_id")
            uname = it.get("user_name", "")
            meta  = float(it.get("meta", 0) or 0)
            interm= float(it.get("meta_intermediaria", 0) or 0)
            sup   = float(it.get("supermeta", 0) or 0)
            if not uid:
                continue
            cur.execute("""
                INSERT INTO comercial_metas
                    (user_id, user_name, meta, meta_intermediaria, supermeta,
                     dt_inicio, dt_fim, descricao, categoria)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id, dt_inicio, dt_fim, categoria)
                DO UPDATE SET
                    meta               = EXCLUDED.meta,
                    meta_intermediaria = EXCLUDED.meta_intermediaria,
                    supermeta          = EXCLUDED.supermeta,
                    user_name          = EXCLUDED.user_name,
                    descricao          = EXCLUDED.descricao
            """, (uid, uname, meta, interm, sup, dt_ini, dt_fim, descr, cat))
            saved += 1

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": saved})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/<int:meta_id>", methods=["PUT"])
def crgm_update_meta(meta_id):
    try:
        body = request.json or {}
        meta_val   = float(body.get("meta", 0))
        interm_val = float(body.get("meta_intermediaria", 0))
        super_val  = float(body.get("supermeta", 0))
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            UPDATE comercial_metas
            SET meta = %s, meta_intermediaria = %s, supermeta = %s
            WHERE id = %s
        """, (meta_val, interm_val, super_val, meta_id))
        conn.commit()
        updated = cur.rowcount
        cur.close()
        conn.close()
        return jsonify({"ok": True, "updated": updated})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/<int:meta_id>", methods=["DELETE"])
def crgm_delete_meta(meta_id):
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM comercial_metas WHERE id = %s", (meta_id,))
        conn.commit()
        deleted = cur.rowcount
        cur.close()
        conn.close()
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# â”€â”€ Atualizar 1 lead (Kommo → PostgreSQL) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/kommo-sync-lead", methods=["POST"])
def crgm_kommo_sync_lead():
    """
    Sincronização pontual de 1 lead (mesmo fluxo que `python kommo_lib/sync_one_lead.py <id>`):
    API Kommo -> SQLite local do kommo_lib + PostgreSQL kommo_sync.
    Body JSON: { "lead_id": 20796123 } OU { "rgm": "48411612" }
    Se vários leads com o mesmo RGM, retorna lista para escolher (ou use lead_id).
    """
    if not KOMMO_TOKEN:
        return jsonify({"ok": False, "error": "KOMMO_TOKEN não configurado no servidor."}), 500
    try:
        body = request.get_json(force=True, silent=True) or {}
        lead_id = body.get("lead_id")
        rgm = body.get("rgm")

        if lead_id is not None and str(lead_id).strip():
            try:
                lid = int(lead_id)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "ID do lead inválido."}), 400
        else:
            lid = None

        rgm_clean = re.sub(r"[^0-9]", "", str(rgm or ""))
        if lid is None and len(rgm_clean) == 8:
            found, err = _kommo_resolve_lead_id_by_rgm(rgm_clean)
            if err:
                return jsonify({"ok": False, "error": err}), 404
            if len(found) > 1:
                return jsonify({
                    "ok": False,
                    "error": "Vários leads com esse RGM. Informe o ID do lead correto.",
                    "lead_ids": found,
                }), 409
            lid = found[0]
        elif lid is None:
            return jsonify({
                "ok": False,
                "error": "Informe o ID do lead (Kommo) ou o RGM com 8 dígitos.",
            }), 400

        lead, sync_err = _kommo_mini_sync_lead_flask(lid)
        if not lead:
            return jsonify({"ok": False, "error": sync_err or "Falha na sincronização pontual do lead."}), 404

        cfs = lead.get("custom_fields_values") or []
        rgm_out = None
        for cf in cfs:
            if str(cf.get("field_name", "")).lower() == "rgm":
                rgm_out = (cf.get("values") or [{}])[0].get("value")
                break

        pipeline = None
        try:
            kc = _pg_kommo()
            kcur = kc.cursor()
            kcur.execute(
                "SELECT p.name FROM pipelines p WHERE p.id = %s",
                (lead.get("pipeline_id"),),
            )
            pr = kcur.fetchone()
            pipeline = pr[0] if pr else None
            kcur.close()
            kc.close()
        except Exception:
            pass

        st = lead.get("status_id")
        status_txt = "Ganho" if st == 142 else "Perdido" if st == 143 else f"Ativo ({st})"

        # O verde do upsert NÃO mudava o crédito: o ranking escolhe
        # DISTINCT ON (rgm) o 142 de maior id. Se outro lead (lixo/autolead)
        # ainda tem o mesmo RGM, a venda fica na pessoa errada. Ao sincronizar
        # este lead em Ganho, ele passa a ser a fonte do crédito e os irmãos
        # são atualizados pra soltar RGM velho no kommo_sync.
        pinned = False
        siblings_synced = 0
        rgm_pin = re.sub(r"[^0-9]", "", str(rgm_out or ""))
        uid_pin = _kommo_uid_int(lead.get("responsible_user_id"))
        if st == 142 and len(rgm_pin) == 8 and uid_pin:
            names = _fetch_kommo_user_names([uid_pin])
            pinned = _pin_rgm_attribution(
                rgm_pin, uid_pin, names.get(uid_pin) or "", resolved_by="mini_sync",
            )
            for sib in _kommo_sibling_lead_ids_for_rgm(rgm_pin, int(lead["id"]))[:8]:
                try:
                    time.sleep(0.2)
                    _sib_lead, _sib_err = _kommo_mini_sync_lead_flask(int(sib))
                    if _sib_lead:
                        siblings_synced += 1
                except Exception as _se:
                    logger.warning("mini_sync sibling %s: %s", sib, _se)

        pin_txt = ""
        if pinned:
            pin_txt = (
                " Crédito da venda fixado neste responsável"
                " (sai de qualquer outro lead com o mesmo RGM)."
            )
            if siblings_synced:
                pin_txt += f" {siblings_synced} lead(s) com o mesmo RGM também atualizado(s)."

        return jsonify({
            "ok": True,
            "lead_id": lead["id"],
            "nome_card": lead.get("name"),
            "rgm": rgm_out,
            "pipeline": pipeline,
            "pipeline_id": lead.get("pipeline_id"),
            "status": status_txt,
            "pinned": pinned,
            "siblings_synced": siblings_synced,
            "msg": (
                "Sincronização pontual concluída (SQLite kommo_lib + PostgreSQL kommo_sync)."
                + pin_txt
                + " Recarregue o dashboard para ver o ranking."
            ),
        })
    except Exception as e:
        logger.exception("kommo-sync-lead")
        return jsonify({"ok": False, "error": str(e)}), 500


# â”€â”€ Duplicatas (cross-ref comercial_rgm_completa x leads) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/duplicatas")
def crgm_duplicatas():
    """Detect RGMs from comercial_rgm_completa that map to multiple Kommo leads.

    Two-phase approach for performance:
      1) Lightweight pass: find RGMs with >1 lead (rgm + lead_id only)
      2) Detail pass: fetch lead info only for the duplicated RGMs
    """
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT rgm FROM comercial_rgm_completa WHERE rgm IS NOT NULL AND rgm <> ''")
        completa_rgms = {r[0] for r in cur.fetchall()}
        cur.close()
        conn.close()

        if not completa_rgms:
            return jsonify({"ok": True, "duplicatas": [], "total": 0})

        kconn = _pg_kommo()
        kcur = kconn.cursor()

        kcur.execute("""
            SELECT rgm_clean, lead_id
            FROM (
                SELECT l.id AS lead_id,
                       regexp_replace((lcf.values_json->0)->>'value', '[^0-9]', '', 'g') AS rgm_clean
                FROM leads l
                JOIN lead_custom_field_values lcf
                  ON lcf.lead_id = l.id
                 AND lower(lcf.field_name) = 'rgm'
                 AND (lcf.values_json->0)->>'value' IS NOT NULL
                 AND (lcf.values_json->0)->>'value' <> ''
                 AND length(regexp_replace((lcf.values_json->0)->>'value', '[^0-9]', '', 'g')) = 8
                JOIN pipelines p ON p.id = l.pipeline_id
                 AND p.name IN ('Funil de vendas', 'Licenciado')
                WHERE l.is_deleted = false
            ) sub
        """)

        rgm_leads = {}
        for rgm, lid in kcur.fetchall():
            if rgm in completa_rgms:
                rgm_leads.setdefault(rgm, []).append(lid)

        dup_rgm_leads = {k: v for k, v in rgm_leads.items() if len(v) > 1}

        if not dup_rgm_leads:
            kcur.close()
            kconn.close()
            return jsonify({"ok": True, "duplicatas": [], "total": 0,
                            "total_completa_rgms": len(completa_rgms)})

        all_dup_ids = []
        for ids in dup_rgm_leads.values():
            all_dup_ids.extend(ids)

        kcur.execute("""
            SELECT l.id,
                   COALESCE(u.name, 'N/A'),
                   l.price,
                   p.name,
                   CASE l.status_id WHEN 142 THEN 'Ganho' WHEN 143 THEN 'Perdido' ELSE 'Ativo' END
            FROM leads l
            JOIN pipelines p ON p.id = l.pipeline_id
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE l.id = ANY(%s)
        """, (all_dup_ids,))

        lead_info = {}
        for r in kcur.fetchall():
            lead_info[r[0]] = {
                "lead_id": r[0], "consultora": r[1], "preco": r[2],
                "pipeline": r[3], "status": r[4],
            }
        kcur.close()
        kconn.close()

        duplicatas = []
        for rgm, ids in sorted(dup_rgm_leads.items(), key=lambda x: -len(x[1])):
            leads = [lead_info.get(lid, {"lead_id": lid, "consultora": "?", "preco": 0, "pipeline": "?", "status": "?"})
                     for lid in sorted(ids, reverse=True)]
            duplicatas.append({"rgm": rgm, "count": len(leads), "leads": leads})

        return jsonify({
            "ok": True,
            "duplicatas": duplicatas,
            "total": len(duplicatas),
            "total_completa_rgms": len(completa_rgms),
        })
    except Exception as e:
        logger.exception("duplicatas error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/matriculas-sem-data")
def crgm_matriculas_sem_data():
    """Leads do Kommo com RGM mas sem data de matrícula (campo Matrícula, field_id=31772).

    Filtra pelos RGMs que aparecem no CSV (comercial_rgm_atual) dentro do ciclo/período
    selecionado, depois cruza com o Kommo sync para verificar quais não têm data preenchida.
    Parâmetros: ciclo, turma, dt_ini, dt_fim (todos opcionais).
    """
    ciclo_nome = request.args.get("ciclo", "").strip()
    turma_nome = request.args.get("turma", "").strip()
    dt_ini     = request.args.get("dt_ini", "").strip() or None
    dt_fim     = request.args.get("dt_fim", "").strip() or None

    try:
        conn = _pg()
        cur  = conn.cursor()

        # Resolve datas do ciclo selecionado
        ciclo_dt_ini = ciclo_dt_fim = None
        if ciclo_nome:
            cur.execute(
                "SELECT dt_inicio, dt_fim FROM ciclos_comercial WHERE nome = %s LIMIT 1",
                (ciclo_nome,),
            )
            row = cur.fetchone()
            if row:
                ciclo_dt_ini = row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])[:10]
                ciclo_dt_fim = row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])[:10]

        # Datas efetivas para filtrar o CSV
        pd_ini = ciclo_dt_ini or dt_ini
        pd_fim = ciclo_dt_fim or dt_fim

        # Monta WHERE para comercial_rgm_atual (CSV filtrado por ciclo/período)
        csv_where  = [
            "UPPER(TRIM(COALESCE(tipo_matricula,''))) = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])",
        ]
        csv_params = []

        if pd_ini:
            csv_where.append("data_matricula >= %s::date")
            csv_params.append(pd_ini)
        if pd_fim:
            csv_where.append("data_matricula <= %s::date")
            csv_params.append(pd_fim)
        if turma_nome:
            csv_where.append("UPPER(TRIM(COALESCE(ciclo,''))) = UPPER(%s)")
            csv_params.append(turma_nome)

        csv_where_sql = " AND ".join(csv_where)

        # Busca RGMs válidos no CSV dentro do período
        cur.execute(f"""
            SELECT DISTINCT rgm, nome, polo, nivel, ciclo, tipo_matricula, situacao
            FROM comercial_rgm_atual
            WHERE {csv_where_sql}
              AND rgm IS NOT NULL AND rgm != ''
            ORDER BY rgm
        """, csv_params)
        csv_rows = cur.fetchall()
        cur.close()
        conn.close()

        if not csv_rows:
            return jsonify({"ok": True, "itens": [], "total": 0,
                            "aviso": "Nenhuma matrícula no CSV para o período selecionado."})

        # Monta set de RGMs para consultar no Kommo
        csv_by_rgm = {
            r[0]: {"rgm": r[0], "nome": r[1] or "", "polo": r[2] or "",
                   "nivel": r[3] or "", "ciclo": r[4] or "",
                   "tipo_matricula": r[5] or "", "situacao": r[6] or ""}
            for r in csv_rows
        }
        rgm_list = list(csv_by_rgm.keys())

        # Consulta Kommo sync: quais desses RGMs TÊM data de matrícula preenchida
        kconn = _pg_kommo()
        kcur  = kconn.cursor()

        kcur.execute("""
            SELECT
                regexp_replace(
                    COALESCE((lcf_rgm.values_json -> 0) ->> 'value', ''),
                    '[^0-9]', '', 'g'
                ) AS rgm,
                COALESCE(u.name, 'N/A') AS consultora,
                NULLIF(TRIM((lcf_mat.values_json -> 0) ->> 'value'), '') AS ts_mat
            FROM leads l
            JOIN lead_custom_field_values lcf_rgm
                ON lcf_rgm.lead_id = l.id
               AND lower(lcf_rgm.field_name) = 'rgm'
            LEFT JOIN lead_custom_field_values lcf_mat
                ON lcf_mat.lead_id = l.id
               AND lcf_mat.field_id = 31772
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE l.is_deleted = false
              AND length(regexp_replace(
                    COALESCE((lcf_rgm.values_json -> 0) ->> 'value', ''),
                    '[^0-9]', '', 'g')) = 8
              AND regexp_replace(
                    COALESCE((lcf_rgm.values_json -> 0) ->> 'value', ''),
                    '[^0-9]', '', 'g') = ANY(%s)
        """, (rgm_list,))

        kommo_rows = kcur.fetchall()
        kcur.close()
        kconn.close()

        # Monta mapa rgm -> consultora e flag se tem data no Kommo
        kommo_map = {}
        for krow in kommo_rows:
            rgm_k, consultora, ts_mat = krow[0], krow[1], krow[2]
            if rgm_k not in kommo_map:
                kommo_map[rgm_k] = {"consultora": consultora or "N/A", "tem_data": False}
            if ts_mat:
                try:
                    import datetime as _dt
                    ts_val = int(ts_mat)
                    data_fmt = _dt.datetime.fromtimestamp(ts_val).strftime("%d/%m/%Y")
                    kommo_map[rgm_k]["tem_data"] = True
                    kommo_map[rgm_k]["data_kommo"] = data_fmt
                except Exception:
                    pass

        # Resultado: RGMs do CSV que NÃO têm data de matrícula no Kommo
        itens = []
        for rgm, csv_info in sorted(csv_by_rgm.items()):
            k = kommo_map.get(rgm, {})
            if k.get("tem_data"):
                continue
            itens.append({
                "rgm":           rgm,
                "nome":          csv_info["nome"],
                "polo":          csv_info["polo"],
                "nivel":         csv_info["nivel"],
                "ciclo":         csv_info["ciclo"],
                "tipo_matricula": csv_info["tipo_matricula"],
                "situacao":      csv_info["situacao"],
                "consultora":    k.get("consultora", "Não encontrado no Kommo"),
                "no_kommo":      bool(k),
            })

        return jsonify({"ok": True, "itens": itens, "total": len(itens)})
    except Exception as e:
        logger.exception("matriculas-sem-data")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Conflitos de atribuição ───────────────────────────────────────────────────

@comercial_rgm_bp.route("/api/comercial-rgm/conflitos")
def crgm_conflitos():
    """Retorna RGMs do painel atual (filtrado por data) com múltiplos agentes no Kommo."""
    dt_ini = request.args.get("dt_ini", "")
    dt_fim = request.args.get("dt_fim", "")
    polo   = request.args.get("polo", "")
    nivel  = request.args.get("nivel", "")
    try:
        # 1. RGMs e nomes do painel no período filtrado
        conn = _pg()
        cur = conn.cursor()
        cw, cp = [], []
        if polo:
            cw.append(f"{_POLO_SQL} = %s"); cp.append(_normalize_polo(polo))
        if nivel:
            cw.append("nivel = %s"); cp.append(nivel)
        if dt_ini:
            cw.append("data_matricula >= %s"); cp.append(dt_ini)
        if dt_fim:
            cw.append("data_matricula <= %s"); cp.append(dt_fim)
        w = ("WHERE " + " AND ".join(cw)) if cw else ""
        cur.execute(
            f"SELECT rgm, nome, data_matricula FROM comercial_rgm_atual {w} ORDER BY data_matricula DESC NULLS LAST",
            cp,
        )
        rgm_info = {}
        for rgm, nome, dm in cur.fetchall():
            n = _normalize_rgm(rgm)
            if n and n not in rgm_info:
                rgm_info[n] = {"nome": (nome or "").strip(), "data_matricula": dm.isoformat() if dm else None}
        cur.close()
        conn.close()

        if not rgm_info:
            return jsonify({"ok": True, "conflitos": [], "total": 0})

        # 2. Todos os leads ativos para esses RGMs no Kommo
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT v.rgm, l.id AS lead_id, l.responsible_user_id,
                   l.status_id, u.name AS agente_nome,
                   ps.name AS status_nome
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            LEFT JOIN users u ON u.id = l.responsible_user_id
            LEFT JOIN pipeline_statuses ps ON ps.id = l.status_id
            WHERE l.responsible_user_id IS NOT NULL
            ORDER BY v.rgm,
                     CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END,
                     l.id DESC
        """)

        from collections import defaultdict
        rgm_leads_map = defaultdict(list)
        for rgm_raw, lead_id, uid, status_id, agente, status_nome in kcur.fetchall():
            nk = _normalize_rgm(rgm_raw)
            if nk and nk in rgm_info:
                rgm_leads_map[nk].append({
                    "lead_id": lead_id,
                    "user_id": uid,
                    "agente": agente or f"User #{uid}",
                    "status_id": status_id,
                    "status_nome": status_nome or "",
                })

        # 3. Resolucoes ja salvas
        kcur.close(); kconn.close()
        conn2 = _pg()
        cur2 = conn2.cursor()
        cur2.execute("SELECT rgm, user_id FROM comercial_rgm_conflito_resolucao")
        resolucoes = {_normalize_rgm(r[0]): r[1] for r in cur2.fetchall() if _normalize_rgm(r[0])}
        cur2.close(); conn2.close()

        # 4. Filtra apenas RGMs com agentes diferentes
        conflitos = []
        for rgm, leads in rgm_leads_map.items():
            agentes_set = {l["user_id"] for l in leads}
            if len(agentes_set) <= 1:
                continue
            # Vencedor atual (primeiro da lista, já ordenado)
            uid_atual = leads[0]["user_id"]
            # Override salvo
            uid_override = resolucoes.get(rgm)
            info = rgm_info[rgm]
            conflitos.append({
                "rgm": rgm,
                "nome_aluno": info["nome"],
                "data_matricula": info["data_matricula"],
                "user_id_atual": uid_atual,
                "user_id_resolucao": uid_override,
                "resolvido": uid_override is not None,
                "leads": leads,
            })

        conflitos.sort(key=lambda x: (x["resolvido"], x["nome_aluno"]))
        return jsonify({"ok": True, "conflitos": conflitos, "total": len(conflitos),
                        "total_nao_resolvidos": sum(1 for c in conflitos if not c["resolvido"])})
    except Exception as e:
        logger.exception("conflitos error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/conflitos/resolver", methods=["POST"])
def crgm_conflitos_resolver():
    """Salva resoluções de conflito: [{rgm, user_id, user_name}]."""
    data = request.get_json(force=True) or {}
    items = data.get("items", [])
    if not items:
        return jsonify({"ok": False, "error": "items vazios"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        for item in items:
            rgm = _normalize_rgm(str(item.get("rgm", "")))
            uid = item.get("user_id")
            nome = item.get("user_name", "")
            if not rgm or not uid:
                continue
            cur.execute("""
                INSERT INTO comercial_rgm_conflito_resolucao (rgm, user_id, user_name, resolved_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (rgm) DO UPDATE
                  SET user_id = EXCLUDED.user_id,
                      user_name = EXCLUDED.user_name,
                      resolved_at = NOW()
            """, (rgm, uid, nome))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": len(items)})
    except Exception as e:
        logger.exception("conflitos resolver error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/conflitos/resolver", methods=["DELETE"])
def crgm_conflitos_resolver_delete():
    """Remove uma resolução de conflito pelo RGM."""
    data = request.get_json(force=True) or {}
    rgm = _normalize_rgm(str(data.get("rgm", "")))
    if not rgm:
        return jsonify({"ok": False, "error": "rgm obrigatório"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM comercial_rgm_conflito_resolucao WHERE rgm = %s", (rgm,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/rgm-atribuicao")
def crgm_rgm_atribuicao():
    """Consulta quem está com o RGM no Kommo e quem recebe crédito no dashboard."""
    rgm_n = _normalize_rgm(request.args.get("rgm", ""))
    if not rgm_n:
        return jsonify({"ok": False, "error": "Informe um RGM válido"}), 400

    academico = None
    override = None
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT nome, polo, data_matricula, situacao, nivel, ciclo, tipo_matricula
            FROM comercial_rgm_atual
            WHERE regexp_replace(COALESCE(rgm, ''), '[^0-9]', '', 'g') = %s
            ORDER BY data_matricula DESC NULLS LAST
            LIMIT 1
            """,
            (rgm_n,),
        )
        row = cur.fetchone()
        if row:
            academico = {
                "nome": row[0],
                "polo": row[1],
                "data_matricula": row[2].isoformat() if row[2] else None,
                "situacao": row[3],
                "nivel": row[4],
                "ciclo": row[5],
                "tipo_matricula": row[6],
            }
        # Fallback: quando não encontrado na view (ciclo diferente do atual), busca em
        # todos os snapshots matriculados via DISTINCT ON (mesmo dedupe de Matrículas Oficiais).
        if academico is None:
            try:
                cur.execute("""
                    SELECT nome, polo, data_matricula, situacao, nivel, ciclo, tipo_matricula
                    FROM (
                        SELECT DISTINCT ON (regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'))
                            NULLIF(TRIM(COALESCE(r.data->>'nome','')), '')                  AS nome,
                            TRIM(regexp_replace(COALESCE(r.data->>'polo',''), '^[0-9]+\\s*[-]\\s*', '')) AS polo,
                            CASE
                                WHEN (r.data->>'data_mat') ~ E'^\\d{2}/\\d{2}/\\d{4}$'
                                    THEN to_date(r.data->>'data_mat','DD/MM/YYYY')
                                WHEN (r.data->>'data_mat') ~ E'^\\d{4}-\\d{2}-\\d{2}'
                                    THEN (r.data->>'data_mat')::date
                                ELSE NULL
                            END AS data_matricula,
                            UPPER(TRIM(COALESCE(r.data->>'situacao','')))   AS situacao,
                            CASE
                                WHEN COALESCE(r.data->>'nivel','')   ~* 'p[oó]s'                                         THEN 'Pós-Graduação'
                                WHEN COALESCE(r.data->>'negocio','') ~* 'p[oó]s'                                         THEN 'Pós-Graduação'
                                WHEN COALESCE(r.data->>'curso','')   ~* '(mba|especializa|p.s.gradua|lato.sensu|stricto)' THEN 'Pós-Graduação'
                                ELSE 'Graduação'
                            END AS nivel,
                            NULLIF(TRIM(COALESCE(r.data->>'ciclo','')), '') AS ciclo,
                            UPPER(TRIM(COALESCE(r.data->>'tipo_matricula',''))) AS tipo_matricula
                        FROM xl_rows r
                        JOIN xl_snapshots s ON s.id = r.snapshot_id
                        WHERE s.tipo = 'matriculados'
                          AND regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g') = %s
                          AND UPPER(TRIM(COALESCE(r.data->>'tipo_matricula','')))
                              = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])
                          AND TRIM(COALESCE(r.data->>'empresa','')) ~ '^(12|7) -'
                        ORDER BY
                            regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'),
                            s.id DESC,
                            CASE WHEN UPPER(TRIM(COALESCE(r.data->>'situacao',''))) = 'EM CURSO' THEN 0
                                 WHEN UPPER(TRIM(COALESCE(r.data->>'situacao',''))) IN ('TRANCADO','SEM EVOLUCAO','SEM EVOLUÇÃO') THEN 1
                                 ELSE 2 END,
                            CASE WHEN (r.data->>'data_mat') ~ E'^\\d{2}/\\d{2}/\\d{4}$'
                                     THEN to_date(r.data->>'data_mat','DD/MM/YYYY')
                                 WHEN (r.data->>'data_mat') ~ E'^\\d{4}-\\d{2}-\\d{2}'
                                     THEN (r.data->>'data_mat')::date
                                 ELSE NULL END DESC NULLS LAST,
                            r.id DESC
                    ) sub
                    LIMIT 1
                """, (rgm_n,))
                fb = cur.fetchone()
                if fb:
                    academico = {
                        "nome": fb[0],
                        "polo": fb[1],
                        "data_matricula": fb[2].isoformat() if fb[2] else None,
                        "situacao": fb[3],
                        "nivel": fb[4],
                        "ciclo": fb[5],
                        "tipo_matricula": fb[6],
                    }
            except Exception as _fb_e:
                logger.warning("rgm-atribuicao fallback all_snapshots: %s", _fb_e)
        cur.execute(
            """
            SELECT user_id, user_name, resolved_by, resolved_at
            FROM comercial_rgm_conflito_resolucao
            WHERE rgm = %s
            """,
            (rgm_n,),
        )
        ov = cur.fetchone()
        if ov:
            override = {
                "user_id": ov[0],
                "user_name": ov[1],
                "resolved_by": ov[2],
                "resolved_at": ov[3].isoformat() if ov[3] else None,
            }
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("rgm-atribuicao academico/override: %s", e)

    leads = []
    kommo_erro = None
    try:
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute(
            """
            SELECT l.id, l.responsible_user_id, u.name, ps.name, l.status_id
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND l.is_deleted = FALSE
            LEFT JOIN users u ON u.id = l.responsible_user_id
            LEFT JOIN pipeline_statuses ps ON ps.id = l.status_id
            WHERE regexp_replace(COALESCE(v.rgm, ''), '[^0-9]', '', 'g') = %s
              AND l.responsible_user_id IS NOT NULL
            ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
            """,
            (rgm_n,),
        )
        for lead_id, uid, agente, status_nome, status_id in kcur.fetchall():
            leads.append({
                "lead_id": lead_id,
                "user_id": uid,
                "agente": agente or f"User #{uid}",
                "status_nome": status_nome or "",
                "status_id": status_id,
            })
        kcur.close()
        kconn.close()
    except Exception as e:
        logger.exception("rgm-atribuicao kommo")
        kommo_erro = str(e)

    agentes_ids = {l["user_id"] for l in leads if l.get("user_id")}
    creditado = None
    if override and override.get("user_id"):
        origem = override.get("resolved_by") or "conflito_manual"
        origem_label = {
            "ajuste_aprovado": "Ajuste de matrícula aprovado",
            "conflito_manual": "Vendas em Conflito",
            "manual": "Vendas em Conflito",
        }.get(origem, origem)
        creditado = {
            "user_id": override["user_id"],
            "agente": override.get("user_name") or f"User #{override['user_id']}",
            "origem": origem,
            "origem_label": origem_label,
        }
    elif leads:
        creditado = {
            "user_id": leads[0]["user_id"],
            "agente": leads[0]["agente"],
            "origem": "kommo",
            "origem_label": "Responsável no Kommo (lead prioritário)",
        }

    return jsonify({
        "ok": True,
        "rgm": rgm_n,
        "academico": academico,
        "leads": leads,
        "kommo_erro": kommo_erro,
        "conflito": len(agentes_ids) > 1,
        "override": override,
        "creditado_para": creditado,
        "no_kommo": bool(leads),
        "no_dashboard": academico is not None,
    })


# ── Distribuição por Consultor — fechamentos no período ───────────────────────

@comercial_rgm_bp.route("/api/dist-consultor/fechadas-periodo")
def dist_consultor_fechadas_periodo():
    """Matrículas com data_matricula (CSV) no período, divididas em:
      - do_periodo: lead foi distribuído ao consultor dentro do período
      - fora_periodo: lead foi distribuído ANTES do período (ou não rastreado)
      - total: soma dos dois (= mesmo número do Dashboard Comercial)

    Parâmetros: start_date (YYYY-MM-DD), end_date (YYYY-MM-DD), polo (opcional), nivel (opcional).
    """
    start_date = request.args.get("start_date", "").strip()
    end_date   = request.args.get("end_date", "").strip()
    polo       = request.args.get("polo", "").strip() or None
    nivel      = request.args.get("nivel", "").strip() or None
    consultor, _acl_info = _dist_consultor_acl(request.args.get("consultor"))
    if consultor == "__no_access__":
        return jsonify({"ok": True, "data": [], "total": 0})
    if not start_date or not end_date:
        return jsonify({"ok": False, "error": "start_date e end_date obrigatórios"}), 400

    try:
        import datetime as _dt
        _dt.date.fromisoformat(start_date)
        _dt.date.fromisoformat(end_date)
    except ValueError:
        return jsonify({"ok": False, "error": "Datas inválidas. Use YYYY-MM-DD"}), 400

    try:
        # 1. RGMs do período — mesma fonte do Dashboard Comercial
        # (comercial_rgm_atual: xl_rows EM CURSO + ciclo_atual + dedup + excluded)
        rgm_list = _crgm_dashboard_rgm_list(start_date, end_date, polo=polo, nivel=nivel)

        if not rgm_list:
            return jsonify({"ok": True, "data": [], "total": 0})

        # 2. No Kommo: para cada RGM, descobre lead_id + consultor responsável
        kconn = _pg_kommo()
        kcur  = kconn.cursor()
        kcur.execute("""
            SELECT DISTINCT ON (v.rgm)
                v.rgm,
                v.lead_id,
                COALESCE(u.name, 'N/A') AS consultor,
                l.responsible_user_id   AS id_consultor,
                l.status_id               AS status_id,
                l.created_at              AS lead_created_at
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE v.rgm = ANY(%s)
            ORDER BY v.rgm,
                     CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END,
                     l.id DESC
        """, (rgm_list,))
        rgm_rows = kcur.fetchall()  # [(rgm, lead_id, consultor, uid, status_id, created_at)]

        # 3. Busca lead_ids que foram distribuídos no período
        lead_ids = [r[1] for r in rgm_rows if r[1]]
        distributed_in_period = {}
        dist_name_map = {}   # lead_id -> nome canônico de distribuicao_por_consultor (trimmed)
        if lead_ids:
            # 3a. Quais leads foram distribuídos no período
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead,
                    ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date AS dist_date
                FROM distribuicao_por_consultor
                WHERE ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date
                      BETWEEN %s::date AND %s::date
                  AND id_lead = ANY(%s)
                ORDER BY id_lead, "timestamp" DESC
            """, (start_date, end_date, lead_ids))
            distributed_in_period = {r[0]: r[1] for r in kcur.fetchall()}

            # 3b. Nome canônico (mais recente) de cada lead na tabela de distribuição
            # Usado para garantir que o frontend faça match pelo mesmo nome da webhook
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead, TRIM(consultor)
                FROM distribuicao_por_consultor
                WHERE id_lead = ANY(%s)
                  AND consultor IS NOT NULL AND TRIM(consultor) != ''
                ORDER BY id_lead, "timestamp" DESC
            """, (lead_ids,))
            for lead_id_val, dist_name in kcur.fetchall():
                dist_name_map[lead_id_val] = dist_name

        received_at_map, received_at_won_map = _fetch_dist_consultor_received_maps(kcur, lead_ids)

        kcur.close()
        kconn.close()

        # 4. Agrupa por consultor, separando do_periodo vs fora_periodo
        # Usa o nome de distribuicao_por_consultor (mesma chave da webhook) para garantir
        # match correto no frontend, com fallback para users.name do Kommo.
        from collections import defaultdict

        # Mapa auxiliar: responsible_user_id → nome webhook (via leads já mapeados)
        uid_to_dist_name = {}
        for rgm, lead_id, kommo_name, uid, status_id, created_at_raw in rgm_rows:
            if lead_id and uid and lead_id in dist_name_map:
                uid_to_dist_name[uid] = dist_name_map[lead_id]

        contagem = defaultdict(lambda: {
            "consultor": "", "id_consultor": None,
            "do_periodo": 0, "fora_periodo": 0, "total": 0
        })
        matched_rgms = {r[0] for r in rgm_rows}
        _ov = _crgm_conflito_overrides()
        _ov_names = _fetch_kommo_user_names(list({int(v) for v in _ov.values()})) if _ov else {}
        for rgm, lead_id, kommo_name, uid, status_id, created_at_raw in rgm_rows:
            nk = _normalize_rgm(rgm)
            if nk and nk in _ov:
                uid = _ov[nk]
                kommo_name = _ov_names.get(uid) or kommo_name
            key = _dist_consultor_owner_key(
                uid, lead_id, dist_name_map, uid_to_dist_name, kommo_name, status_id,
            )
            c = contagem[key]
            c["consultor"]    = key
            c["id_consultor"] = uid
            c["total"]       += 1
            if lead_id and _dist_consultor_period_date(
                lead_id, status_id, created_at_raw, distributed_in_period,
                received_at_map, received_at_won_map, start_date, end_date,
            ):
                c["do_periodo"]   += 1
            else:
                c["fora_periodo"] += 1

        # RGMs do Dashboard Comercial sem match no Kommo → agrupados em "Sem consultor"
        # para que o total bata com matriculas-por-origem e com o card "Matrículas no Período"
        # Não-admin: NÃO inclui "Sem consultor" — somente leads do próprio consultor.
        sem_match = [r for r in rgm_list if r not in matched_rgms]
        if sem_match and not consultor:
            c = contagem["Sem consultor"]
            c["consultor"]    = "Sem consultor"
            c["id_consultor"] = None
            c["total"]       += len(sem_match)
            c["fora_periodo"] += len(sem_match)

        # Hierarquia: filtra resultado pelo consultor solicitado (admin) ou logado.
        if consultor:
            allowed = set(_distribuicao_consultor_aliases(consultor))
            contagem = {
                k: v for k, v in contagem.items()
                if (k or "").strip().lower() in allowed
            }

        result = sorted(contagem.values(), key=lambda x: -x["total"])
        return jsonify({
            "ok": True,
            "data": result,
            "total": sum(r["total"] for r in result)
        })
    except Exception as e:
        logger.exception("dist-consultor-fechadas-periodo")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Distribuição por Consultor — matrículas no período por origem ─────────────

@comercial_rgm_bp.route("/api/dist-consultor/matriculas-por-origem")
def dist_consultor_matriculas_por_origem():
    """Matrículas com data_matricula no período, agrupadas por origem, separadas em:
      - do_periodo: lead foi distribuído ao consultor dentro do período
      - fora_periodo: lead foi distribuído ANTES do período
      - total: soma dos dois (= mesmo número do Dashboard Comercial)
    """
    start_date = request.args.get("start_date", "").strip()
    end_date   = request.args.get("end_date", "").strip()
    polo       = request.args.get("polo", "").strip() or None
    nivel      = request.args.get("nivel", "").strip() or None
    consultor, _acl_info = _dist_consultor_acl(request.args.get("consultor"))
    if consultor == "__no_access__":
        return jsonify({"ok": True, "data": [], "total_do_periodo": 0,
                        "total_fora_periodo": 0, "total": 0})
    if not start_date or not end_date:
        return jsonify({"ok": False, "error": "start_date e end_date obrigatórios"}), 400

    try:
        import datetime as _dt
        _dt.date.fromisoformat(start_date)
        _dt.date.fromisoformat(end_date)
    except ValueError:
        return jsonify({"ok": False, "error": "Datas inválidas. Use YYYY-MM-DD"}), 400

    try:
        # 1. RGMs do período — mesma fonte do Dashboard Comercial
        # (comercial_rgm_atual: xl_rows EM CURSO + ciclo_atual + dedup + excluded)
        rgm_list = _crgm_dashboard_rgm_list(start_date, end_date, polo=polo, nivel=nivel)

        if not rgm_list:
            return jsonify({"ok": True, "data": [], "total_do_periodo": 0,
                            "total_fora_periodo": 0, "total": 0})

        # 2. Para cada RGM, busca lead_id no Kommo
        kconn = _pg_kommo()
        kcur  = kconn.cursor()
        kcur.execute("""
            SELECT DISTINCT ON (v.rgm)
                v.rgm,
                v.lead_id,
                COALESCE(u.name, 'N/A') AS consultor_kommo,
                l.responsible_user_id AS uid,
                l.status_id,
                l.created_at
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE v.rgm = ANY(%s)
            ORDER BY v.rgm,
                     CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END,
                     l.id DESC
        """, (rgm_list,))
        lead_rows = kcur.fetchall()
        rgm_to_lead = {r[0]: r[1] for r in lead_rows if r[1]}
        lead_meta = {r[1]: {"status_id": r[4], "created_at": r[5]} for r in lead_rows if r[1]}
        lead_ids = list(rgm_to_lead.values())

        # 3. Busca origem + se foi distribuído no período por lead_id
        distributed_in_period = {}
        lead_to_origem = {}   # lead_id -> origem (da distribuição mais recente)
        lead_origin_source = {}  # lead_id -> "n8n" | "kommo"
        lead_ids_com_qualquer_dist = set()   # lead_ids que aparecem em distribuicao_por_consultor
        dist_name_map = {}     # lead_id -> último consultor n8n (para owner key)
        if lead_ids:
            # Quais foram distribuídos no período
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead,
                    ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date AS dist_date
                FROM distribuicao_por_consultor
                WHERE ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date
                      BETWEEN %s::date AND %s::date
                  AND id_lead = ANY(%s)
                ORDER BY id_lead, "timestamp" DESC
            """, (start_date, end_date, lead_ids))
            distributed_in_period = {r[0]: r[1] for r in kcur.fetchall()}

            # Origem mais recente de cada lead (sem filtro de período)
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead, TRIM(COALESCE(origem, ''))
                FROM distribuicao_por_consultor
                WHERE id_lead = ANY(%s)
                ORDER BY id_lead, "timestamp" DESC
            """, (lead_ids,))
            for lead_id_val, origem in kcur.fetchall():
                lead_ids_com_qualquer_dist.add(lead_id_val)
                if origem:
                    lead_to_origem[lead_id_val] = origem
                    lead_origin_source[lead_id_val] = "n8n"

            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead, TRIM(consultor)
                FROM distribuicao_por_consultor
                WHERE id_lead = ANY(%s)
                  AND consultor IS NOT NULL AND TRIM(consultor) != ''
                ORDER BY id_lead, "timestamp" DESC
            """, (lead_ids,))
            for lead_id_val, dist_name in kcur.fetchall():
                dist_name_map[lead_id_val] = dist_name

            # Fallback: para leads sem origem no n8n, busca campo "Origem" no Kommo
            sem_origem_ids = [lid for lid in lead_ids if lid not in lead_to_origem]
            if sem_origem_ids:
                # Fonte 1: custom_fields_json
                kcur.execute("""
                    SELECT DISTINCT l.id,
                        TRIM((cf_elem.value -> 'values' -> 0) ->> 'value')
                    FROM leads l
                    CROSS JOIN LATERAL jsonb_array_elements(
                        COALESCE(l.custom_fields_json, '[]'::jsonb)
                    ) cf_elem(value)
                    WHERE l.id = ANY(%s)
                      AND lower(cf_elem.value ->> 'field_name') IN ('origem', 'origem-new')
                      AND TRIM(COALESCE((cf_elem.value -> 'values' -> 0) ->> 'value', '')) != ''
                """, (sem_origem_ids,))
                for lead_id_val, origem_kommo in kcur.fetchall():
                    if origem_kommo and lead_id_val not in lead_to_origem:
                        lead_to_origem[lead_id_val] = origem_kommo
                        lead_origin_source[lead_id_val] = "kommo"

                # Fonte 2: lead_custom_field_values (para os que não achamos na fonte 1)
                ainda_sem = [lid for lid in sem_origem_ids if lid not in lead_to_origem]
                if ainda_sem:
                    kcur.execute("""
                        SELECT DISTINCT lcf.lead_id,
                            TRIM((lcf.values_json -> 0) ->> 'value')
                        FROM lead_custom_field_values lcf
                        WHERE lcf.lead_id = ANY(%s)
                          AND lower(lcf.field_name) IN ('origem', 'origem-new')
                          AND TRIM(COALESCE((lcf.values_json -> 0) ->> 'value', '')) != ''
                    """, (ainda_sem,))
                    for lead_id_val, origem_kommo in kcur.fetchall():
                        if origem_kommo and lead_id_val not in lead_to_origem:
                            lead_to_origem[lead_id_val] = origem_kommo
                            lead_origin_source[lead_id_val] = "kommo"

        received_at_map, received_at_won_map = _fetch_dist_consultor_received_maps(kcur, lead_ids)

        kcur.close()
        kconn.close()

        uid_to_dist_name = {}
        for rgm, lead_id, kommo_name, uid, status_id, created_at_raw in lead_rows:
            if lead_id and uid and lead_id in dist_name_map:
                uid_to_dist_name[uid] = dist_name_map[lead_id]
        rgm_to_owner = {}
        for rgm, lead_id, kommo_name, uid, status_id, created_at_raw in lead_rows:
            rgm_to_owner[rgm] = _dist_consultor_owner_key(
                uid, lead_id, dist_name_map, uid_to_dist_name, kommo_name, status_id,
            )

        # 4. Agrupa por origem (case-insensitive — n8n usa minúsculo, Kommo usa
        # maiúsculo; precisamos unificar). Mantemos a primeira grafia vista
        # (preferindo a do n8n quando existir) como display.
        from collections import defaultdict
        contagem = defaultdict(lambda: {
            "origem": "", "do_periodo": 0, "fora_periodo": 0, "total": 0,
            "leads_fallback": 0,
        })
        # Display canônico por chave lowercase. Prioriza grafia do n8n
        # (distribuicao_por_consultor) sobre a do Kommo.
        origem_display = {}
        # Primeiro registra as grafias do n8n
        for _lid, _orig in lead_to_origem.items():
            if _lid in lead_ids_com_qualquer_dist and _orig:
                key = _orig.strip().lower()
                if key and key not in origem_display:
                    origem_display[key] = _orig.strip()
        # Depois completa com grafias vindas do Kommo (fallback) se ainda não houver
        for _lid, _orig in lead_to_origem.items():
            if _orig:
                key = _orig.strip().lower()
                if key and key not in origem_display:
                    origem_display[key] = _orig.strip()

        for rgm in rgm_list:
            lead_id = rgm_to_lead.get(rgm)
            owner = rgm_to_owner.get(rgm)
            if consultor and (owner or "").strip().lower() not in _distribuicao_consultor_aliases(consultor):
                continue
            if not lead_id:
                if consultor:
                    continue
                origem_key = "sem lead no kommo"
                origem_label = "Sem lead no Kommo"
            else:
                raw = lead_to_origem.get(lead_id)
                if raw:
                    origem_key = raw.strip().lower()
                    origem_label = origem_display.get(origem_key, raw.strip())
                else:
                    origem_key = "sem origem"
                    origem_label = "Sem origem preenchida"
            c = contagem[origem_key]
            c["origem"] = origem_label
            c["total"] += 1
            meta = lead_meta.get(lead_id, {})
            period_date = lead_id and _dist_consultor_period_date(
                lead_id, meta.get("status_id"), meta.get("created_at"), distributed_in_period,
                received_at_map, received_at_won_map, start_date, end_date,
            )
            if period_date:
                c["do_periodo"] += 1
                if lead_origin_source.get(lead_id) == "kommo":
                    c["leads_fallback"] += 1
            else:
                c["fora_periodo"] += 1

        result = sorted(contagem.values(), key=lambda x: -x["total"])
        return jsonify({
            "ok": True,
            "data": result,
            "total_do_periodo":   sum(r["do_periodo"]   for r in result),
            "total_fora_periodo": sum(r["fora_periodo"] for r in result),
            "total":              sum(r["total"]        for r in result),
        })
    except Exception as e:
        logger.exception("dist-consultor-matriculas-por-origem")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/dist-consultor/sem-origem")
def dist_consultor_sem_origem():
    """Lista os RGMs do Dashboard Comercial que não têm registro em
    distribuicao_por_consultor (origem desconhecida).
    Retorna dados do aluno de comercial_rgm_atual + lead_id do Kommo quando disponível.
    """
    start_date = request.args.get("start_date", "").strip()
    end_date   = request.args.get("end_date",   "").strip()
    consultor, _acl_info = _dist_consultor_acl(request.args.get("consultor"))
    if consultor == "__no_access__":
        return jsonify({"ok": True, "data": [], "total": 0})
    if not start_date or not end_date:
        return jsonify({"ok": False, "error": "start_date e end_date obrigatórios"}), 400
    try:
        import datetime as _dt
        _dt.date.fromisoformat(start_date)
        _dt.date.fromisoformat(end_date)
    except ValueError:
        return jsonify({"ok": False, "error": "Datas inválidas. Use YYYY-MM-DD"}), 400

    try:
        rgm_list = _crgm_dashboard_rgm_list(start_date, end_date)
        if not rgm_list:
            return jsonify({"ok": True, "data": [], "total": 0})

        # Detalhes do aluno na base interna
        conn = _pg()
        cur  = conn.cursor()
        cur.execute(
            "SELECT DISTINCT ON (rgm) rgm, nome, data_matricula, polo, nivel "
            "FROM comercial_rgm_atual "
            "WHERE data_matricula BETWEEN %s AND %s "
            "  AND rgm = ANY(%s) "
            "ORDER BY rgm, data_matricula DESC NULLS LAST",
            (start_date, end_date, rgm_list)
        )
        aluno_map = {r[0]: {"rgm": r[0], "nome": r[1] or "—",
                             "data_matricula": str(r[2]) if r[2] else "—",
                             "polo": r[3] or "—", "nivel": r[4] or "—"}
                     for r in cur.fetchall()}
        cur.close()
        conn.close()

        # Lead_id + data de criação no Kommo (quando disponível)
        kconn = _pg_kommo()
        kcur  = kconn.cursor()
        kcur.execute(
            "SELECT DISTINCT ON (v.rgm) v.rgm, v.lead_id, "
            "  to_char(to_timestamp(l.created_at) AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS lead_criado, "
            "  l.responsible_user_id, "
            "  l.status_id, "
            "  l.created_at "
            "FROM vw_leads_rgm v "
            "JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted "
            "WHERE v.rgm = ANY(%s) "
            "ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC",
            (rgm_list,)
        )
        # kommo_map: rgm -> {lead_id, lead_criado, responsible_user_id, status_id, created_at}
        kommo_map = {}
        uid_set = set()
        for row in kcur.fetchall():
            if row[1]:
                kommo_map[row[0]] = {
                    "lead_id": row[1],
                    "lead_criado": row[2] or "—",
                    "responsible_user_id": row[3],
                    "status_id": row[4],
                    "created_at": row[5],
                }
                if row[3]:
                    uid_set.add(row[3])

        # Resolve nomes dos responsáveis via _KNOWN_USERS + fallback DB
        user_names = _fetch_kommo_user_names(list(uid_set)) if uid_set else {}
        for data in kommo_map.values():
            uid = data.pop("responsible_user_id", None)
            data["responsavel"] = user_names.get(uid, "—") if uid else "—"

        # Quais lead_ids têm origem no n8n (distribuicao_por_consultor)
        lead_ids_all = [v["lead_id"] for v in kommo_map.values()]
        lead_ids_com_origem_n8n = set()
        # Origem do campo "Origem" no Kommo (custom_fields_json) — fallback
        lead_to_origem_kommo = {}
        distributed_in_period = {}
        received_at_map = {}
        received_at_won_map = {}
        if lead_ids_all:
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead,
                    ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date AS dist_date
                FROM distribuicao_por_consultor
                WHERE ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date
                      BETWEEN %s::date AND %s::date
                  AND id_lead = ANY(%s)
                ORDER BY id_lead, "timestamp" DESC
            """, (start_date, end_date, lead_ids_all))
            distributed_in_period = {r[0]: r[1] for r in kcur.fetchall()}
            received_at_map, received_at_won_map = _fetch_dist_consultor_received_maps(kcur, lead_ids_all)

            kcur.execute(
                "SELECT DISTINCT id_lead FROM distribuicao_por_consultor "
                "WHERE id_lead = ANY(%s) AND origem IS NOT NULL AND TRIM(origem) != ''",
                (lead_ids_all,)
            )
            lead_ids_com_origem_n8n = {r[0] for r in kcur.fetchall()}

            # Busca campo "Origem" direto do Kommo (custom_fields_json + lead_custom_field_values)
            sem_n8n = [lid for lid in lead_ids_all if lid not in lead_ids_com_origem_n8n]
            if sem_n8n:
                # Fonte 1: custom_fields_json
                kcur.execute("""
                    SELECT DISTINCT l.id,
                        TRIM((cf_elem.value -> 'values' -> 0) ->> 'value')
                    FROM leads l
                    CROSS JOIN LATERAL jsonb_array_elements(
                        COALESCE(l.custom_fields_json, '[]'::jsonb)
                    ) cf_elem(value)
                    WHERE l.id = ANY(%s)
                      AND lower(cf_elem.value ->> 'field_name') IN ('origem', 'origem-new')
                      AND TRIM(COALESCE((cf_elem.value -> 'values' -> 0) ->> 'value', '')) != ''
                """, (sem_n8n,))
                for lead_id_val, origem_kommo in kcur.fetchall():
                    if origem_kommo and lead_id_val not in lead_to_origem_kommo:
                        lead_to_origem_kommo[lead_id_val] = origem_kommo

                # Fonte 2: lead_custom_field_values
                ainda_sem = [lid for lid in sem_n8n if lid not in lead_to_origem_kommo]
                if ainda_sem:
                    kcur.execute("""
                        SELECT DISTINCT lcf.lead_id,
                            TRIM((lcf.values_json -> 0) ->> 'value')
                        FROM lead_custom_field_values lcf
                        WHERE lcf.lead_id = ANY(%s)
                          AND lower(lcf.field_name) IN ('origem', 'origem-new')
                          AND TRIM(COALESCE((lcf.values_json -> 0) ->> 'value', '')) != ''
                    """, (ainda_sem,))
                    for lead_id_val, origem_kommo in kcur.fetchall():
                        if origem_kommo and lead_id_val not in lead_to_origem_kommo:
                            lead_to_origem_kommo[lead_id_val] = origem_kommo

        kcur.close()
        kconn.close()

        # Filtra apenas os sem origem e classifica o motivo
        # Leads com origem no n8n OU no campo Kommo → já têm origem, não listamos aqui
        result = []
        for rgm in rgm_list:
            kdata   = kommo_map.get(rgm)
            lead_id = kdata["lead_id"] if kdata else None
            if lead_id and (lead_id in lead_ids_com_origem_n8n or lead_id in lead_to_origem_kommo):
                continue  # tem origem — não exibir nesta listagem
            # Filtro por consultor: aplica quando parâmetro informado
            if consultor:
                responsavel = kdata["responsavel"] if kdata else "—"
                if (responsavel or "").strip().lower() not in _distribuicao_consultor_aliases(consultor):
                    continue
            aluno = aluno_map.get(rgm, {"rgm": rgm, "nome": "—", "data_matricula": "—",
                                         "polo": "—", "nivel": "—"})
            if not lead_id:
                motivo = "Sem lead no Kommo"
            else:
                motivo = "Lead no Kommo sem distribuição via n8n"
            period_date = lead_id and _dist_consultor_period_date(
                lead_id,
                kdata.get("status_id") if kdata else None,
                kdata.get("created_at") if kdata else None,
                distributed_in_period,
                received_at_map,
                received_at_won_map,
                start_date,
                end_date,
            )
            origem_kommo = lead_to_origem_kommo.get(lead_id, "—") if lead_id else "—"
            result.append({
                "rgm":            aluno["rgm"],
                "nome":           aluno["nome"],
                "data_matricula": aluno["data_matricula"],
                "polo":           aluno["polo"],
                "nivel":          aluno["nivel"],
                "lead_id":        lead_id,
                "lead_criado":    kdata["lead_criado"]  if kdata else "—",
                "responsavel":    kdata["responsavel"]  if kdata else "—",
                "origem_kommo":   origem_kommo,
                "motivo":         motivo,
                "periodo":        "do_periodo" if period_date else "fora_periodo",
            })

        result.sort(key=lambda x: x["data_matricula"], reverse=True)
        return jsonify({"ok": True, "data": result, "total": len(result)})
    except Exception as e:
        logger.exception("dist-consultor-sem-origem")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/dist-consultor/filters")
def dist_consultor_filters():
    """Retorna polos e níveis disponíveis para o período (para popular os selects do frontend)."""
    start_date = request.args.get("start_date", "").strip()
    end_date   = request.args.get("end_date", "").strip()
    if not start_date or not end_date:
        return jsonify({"ok": False, "error": "start_date e end_date obrigatórios"}), 400
    try:
        conn = _pg()
        cur  = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT {_POLO_SQL} AS polo "
            "FROM comercial_rgm_atual "
            "WHERE data_matricula BETWEEN %s AND %s "
            "  AND polo IS NOT NULL AND TRIM(polo) != '' "
            "ORDER BY polo",
            (start_date, end_date),
        )
        polos = [r[0] for r in cur.fetchall() if r[0] and r[0].strip()]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "polos": polos, "niveis": ["Graduação", "Pós-Graduação"]})
    except Exception as e:
        logger.warning("dist_consultor_filters: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/dist-consultor/detalhe-consultor")
def dist_consultor_detalhe():
    """Retorna lista detalhada de leads (RGM) de um consultor no período,
    indicando se cada um é do_periodo (verde) ou fora_periodo (laranja).

    Parâmetros: consultor (nome), start_date, end_date
    """
    consultor, _acl_info = _dist_consultor_acl(request.args.get("consultor"))
    if consultor == "__no_access__":
        return jsonify({"ok": True, "consultor": None, "do_periodo": [], "fora_periodo": []})
    start_date = request.args.get("start_date", "").strip()
    end_date   = request.args.get("end_date", "").strip()

    if not consultor or not start_date or not end_date:
        return jsonify({"ok": False, "error": "consultor, start_date e end_date obrigatórios"}), 400

    try:
        import datetime as _dt
        _dt.date.fromisoformat(start_date)
        _dt.date.fromisoformat(end_date)
    except ValueError:
        return jsonify({"ok": False, "error": "Datas inválidas. Use YYYY-MM-DD"}), 400

    try:
        # 1. RGMs do período (ERP)
        conn = _pg()
        cur  = conn.cursor()
        cur.execute(
            "SELECT rgm, nome, data_matricula FROM comercial_rgm_atual "
            "WHERE data_matricula BETWEEN %s AND %s ORDER BY data_matricula DESC",
            (start_date, end_date),
        )
        erp_rows = {_normalize_rgm(r[0]): {"nome": r[1], "data_matricula": str(r[2])}
                    for r in cur.fetchall() if _normalize_rgm(r[0])}
        cur.close(); conn.close()

        rgm_list = list(erp_rows.keys())
        if not rgm_list:
            return jsonify({"ok": True, "consultor": consultor, "do_periodo": [], "fora_periodo": []})

        # 2. Kommo: lead_id + consultor responsável + created_at por RGM
        kconn = _pg_kommo()
        kcur  = kconn.cursor()
        kcur.execute("""
            SELECT DISTINCT ON (v.rgm)
                v.rgm,
                v.lead_id,
                COALESCE(u.name, 'N/A')       AS consultor_kommo,
                l.responsible_user_id          AS uid,
                l.created_at                   AS lead_created_at,
                l.status_id                    AS status_id,
                l.closed_at                    AS closed_at
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE v.rgm = ANY(%s)
            ORDER BY v.rgm,
                     CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END,
                     l.id DESC
        """, (rgm_list,))
        lead_rows = kcur.fetchall()

        # Filtra pelo consultor solicitado (usando _KNOWN_USERS e fallbacks)
        uid_to_dist_name = {}
        dist_name_map    = {}
        lead_ids_all = [r[1] for r in lead_rows if r[1]]
        if lead_ids_all:
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead, TRIM(consultor)
                FROM distribuicao_por_consultor
                WHERE id_lead = ANY(%s)
                ORDER BY id_lead, "timestamp" DESC
            """, (lead_ids_all,))
            dist_name_map = {r[0]: r[1] for r in kcur.fetchall()}

        for _r in lead_rows:
            _lid, _uid = _r[1], _r[3]
            if _lid and _uid and _lid in dist_name_map:
                uid_to_dist_name[_uid] = dist_name_map[_lid]

        # Distribuídos no período
        if lead_ids_all:
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead,
                    ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date AS dist_date
                FROM distribuicao_por_consultor
                WHERE ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date
                      BETWEEN %s::date AND %s::date
                  AND id_lead = ANY(%s)
                ORDER BY id_lead, "timestamp" DESC
            """, (start_date, end_date, lead_ids_all))
            dist_in_period = {r[0]: r[1] for r in kcur.fetchall()}
        else:
            dist_in_period = {}

        # Data da última distribuição de cada lead
        if lead_ids_all:
            kcur.execute("""
                SELECT DISTINCT ON (id_lead) id_lead,
                    ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date AS dist_date,
                    TRIM(consultor) AS consultor
                FROM distribuicao_por_consultor
                WHERE id_lead = ANY(%s)
                ORDER BY id_lead, "timestamp" DESC
            """, (lead_ids_all,))
            last_dist = {r[0]: {"data": str(r[1]), "consultor": r[2]} for r in kcur.fetchall()}
        else:
            last_dist = {}

        # Data em que o responsável ATUAL recebeu cada lead (lead_responsible_history)
        received_at_map = {}
        if lead_ids_all:
            kcur.execute("""
                SELECT DISTINCT ON (lrh.lead_id)
                    lrh.lead_id,
                    (lrh.changed_at AT TIME ZONE 'America/Sao_Paulo')::date AS received_date
                FROM lead_responsible_history lrh
                JOIN leads l ON l.id = lrh.lead_id
                WHERE lrh.lead_id = ANY(%s)
                  AND lrh.to_user_id = l.responsible_user_id
                ORDER BY lrh.lead_id, lrh.changed_at DESC
            """, (lead_ids_all,))
            received_at_map = {r[0]: str(r[1]) for r in kcur.fetchall()}

        # Ganho (142): última transferência para o responsável atual ATÉ o fechamento
        # (evita usar evento posterior ao ganho; aproxima "quem fechou pegou o lead quando")
        received_at_won_map = {}
        if lead_ids_all:
            kcur.execute("""
                SELECT DISTINCT ON (lrh.lead_id)
                    lrh.lead_id,
                    (lrh.changed_at AT TIME ZONE 'America/Sao_Paulo')::date AS received_date
                FROM lead_responsible_history lrh
                JOIN leads l ON l.id = lrh.lead_id
                WHERE lrh.lead_id = ANY(%s)
                  AND l.status_id = 142
                  AND l.closed_at IS NOT NULL
                  AND l.closed_at > 0
                  AND lrh.to_user_id = l.responsible_user_id
                  AND lrh.changed_at <= to_timestamp(l.closed_at)
                ORDER BY lrh.lead_id, lrh.changed_at DESC
            """, (lead_ids_all,))
            received_at_won_map = {r[0]: str(r[1]) for r in kcur.fetchall()}

        # Última vez que o n8n distribuiu este lead para o MESMO consultor do modal
        # (nome exato do parâmetro; ajuda quando lead_responsible_history ainda está vazio)
        n8n_received_map = {}
        if lead_ids_all:
            _n8n_names = _distribuicao_consultor_aliases(consultor)
            if _n8n_names:
                kcur.execute("""
                    SELECT DISTINCT ON (id_lead) id_lead,
                        ("timestamp" AT TIME ZONE 'America/Sao_Paulo')::date AS dist_date
                    FROM distribuicao_por_consultor
                    WHERE id_lead = ANY(%s)
                      AND lower(trim(consultor)) = ANY(%s)
                    ORDER BY id_lead, "timestamp" DESC
                """, (lead_ids_all, _n8n_names))
                n8n_received_map = {r[0]: str(r[1]) for r in kcur.fetchall()}

        kcur.close(); kconn.close()

        # 3. Monta resultado — filtra só os leads atribuídos ao consultor solicitado
        import datetime as _dt3
        do_periodo   = []
        fora_periodo = []

        for rgm, lead_id, kommo_name, uid, created_at_raw, status_id, _closed_at in lead_rows:
            if not lead_id:
                continue

            key = _dist_consultor_owner_key(
                uid, lead_id, dist_name_map, uid_to_dist_name, kommo_name, status_id,
            )

            if key != consultor:
                continue

            # Converte created_at (Unix timestamp ou datetime)
            criado_str = _kommo_date_iso(created_at_raw)

            erp = erp_rows.get(rgm, {})
            ld  = last_dist.get(lead_id, {})

            data_matricula = erp.get("data_matricula", "")
            _h_recv = (received_at_won_map.get(lead_id) if status_id == 142 else None) \
                or received_at_map.get(lead_id)
            _cand_dates = []
            for _src in (_h_recv, n8n_received_map.get(lead_id)):
                if not _src:
                    continue
                try:
                    _cand_dates.append(_dt3.date.fromisoformat(str(_src)))
                except Exception:
                    pass
            if _cand_dates:
                data_recebimento = str(max(_cand_dates))
            else:
                data_recebimento = criado_str

            # Calcula dias entre recebimento e matrícula
            dias_ate_matricula = None
            if data_recebimento and data_matricula:
                try:
                    d_receb = _dt3.date.fromisoformat(str(data_recebimento))
                    d_mat   = _dt3.date.fromisoformat(str(data_matricula))
                    dias_ate_matricula = (d_mat - d_receb).days
                except Exception:
                    pass

            item = {
                "rgm":               rgm,
                "nome":              erp.get("nome", ""),
                "data_matricula":    data_matricula,
                "lead_id":           lead_id,
                "lead_criado":       criado_str,
                "data_recebimento":  data_recebimento,
                "dias_ate_matricula": dias_ate_matricula,
                "ultima_dist":       ld.get("data"),
                "dist_consultor":    ld.get("consultor"),
            }

            period_date = _dist_consultor_period_date(
                lead_id, status_id, created_at_raw, dist_in_period,
                received_at_map, received_at_won_map, start_date, end_date,
            )
            if period_date:
                item["dist_no_periodo"] = str(period_date)
                do_periodo.append(item)
            else:
                fora_periodo.append(item)

        return jsonify({
            "ok":          True,
            "consultor":   consultor,
            "do_periodo":  sorted(do_periodo,   key=lambda x: x["data_matricula"], reverse=True),
            "fora_periodo": sorted(fora_periodo, key=lambda x: x["data_matricula"], reverse=True),
        })

    except Exception as e:
        logger.exception("dist-consultor-detalhe")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Distribuição por Consultor — total real de leads no Kommo + gap ───────────
@comercial_rgm_bp.route("/api/dist-consultor/total-kommo")
def dist_consultor_total_kommo():
    """Conta leads do Kommo no período (uniao com distribuicao_por_consultor)
    e devolve a lista de leads que estao no Kommo mas NAO foram distribuidos
    pelo n8n. Uma unica query.
    """
    start_date = (request.args.get("start_date") or "").strip()
    end_date   = (request.args.get("end_date")   or "").strip()
    if not start_date or not end_date:
        return jsonify({"ok": False, "error": "start_date e end_date obrigatórios"}), 400
    try:
        import datetime as _dt
        _dt.date.fromisoformat(start_date)
        _dt.date.fromisoformat(end_date)
    except ValueError:
        return jsonify({"ok": False, "error": "Datas inválidas. Use YYYY-MM-DD"}), 400

    try:
        kconn = _pg_kommo()
        kcur  = kconn.cursor()
        kcur.execute(
            """
            WITH dist AS (
                SELECT DISTINCT id_lead AS id
                FROM distribuicao_por_consultor
                WHERE (timestamp AT TIME ZONE 'America/Sao_Paulo')::date
                      BETWEEN %s::date AND %s::date
            ),
            kommo AS (
                SELECT id, name, pipeline_id, status_id, created_at
                FROM leads
                WHERE (to_timestamp(created_at) AT TIME ZONE 'America/Sao_Paulo')::date
                      BETWEEN %s::date AND %s::date
            ),
            so_kommo AS (
                SELECT k.id, k.name, k.pipeline_id, k.status_id, k.created_at
                FROM kommo k
                WHERE NOT EXISTS (SELECT 1 FROM dist d WHERE d.id = k.id)
            )
            SELECT
                (SELECT COUNT(*) FROM dist)  AS total_distribuidos,
                (SELECT COUNT(*) FROM kommo) AS total_kommo,
                (SELECT COUNT(*) FROM (SELECT id FROM dist UNION SELECT id FROM kommo) u) AS total_uniao,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'id',           sk.id,
                            'name',         sk.name,
                            'pipeline_id',  sk.pipeline_id,
                            'status_id',    sk.status_id,
                            'created_at',   to_char(
                                to_timestamp(sk.created_at) AT TIME ZONE 'America/Sao_Paulo',
                                'YYYY-MM-DD"T"HH24:MI:SS'
                            )
                        )
                        ORDER BY sk.created_at DESC
                    ) FILTER (WHERE sk.id IS NOT NULL),
                    '[]'::json
                ) AS so_kommo_leads
            FROM so_kommo sk
            """,
            (start_date, end_date, start_date, end_date),
        )
        row = kcur.fetchone()
        kcur.close()
        kconn.close()

        total_distribuidos, total_kommo, total_uniao, so_kommo_leads = row
        return jsonify({
            "ok": True,
            "total_distribuidos": int(total_distribuidos or 0),
            "total_kommo":        int(total_kommo or 0),
            "total":              int(total_uniao or 0),
            "so_kommo":           so_kommo_leads or [],
        })
    except Exception as e:
        logger.exception("dist-consultor-total-kommo")
        return jsonify({"ok": False, "error": str(e)}), 500
