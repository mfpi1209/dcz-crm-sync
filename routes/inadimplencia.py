"""
Inadimplência — blueprint de taxa e evolução temporal.
Prefixo: /api/inadimplencia
"""
import re
import time
from datetime import datetime, timezone, timedelta, date as date_cls

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

from db import get_conn
from helpers import BRT

# ---------------------------------------------------------------------------
# Helpers de competência (YYYY-MM ↔ label PT-BR)
# ---------------------------------------------------------------------------

_MESES_PT = [
    "", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]


def _competencia_label(competencia: str) -> str:
    """Converte 'YYYY-MM' em 'Mês/YYYY' em PT-BR."""
    try:
        ano, mes = competencia.split("-")
        return f"{_MESES_PT[int(mes)]}/{ano}"
    except Exception:
        return competencia


def _parse_vencimento_competencia(venc_str):
    """Parseia 'DD/MM/YYYY' e retorna 'YYYY-MM', ou None."""
    if not venc_str:
        return None
    try:
        parts = venc_str.split("/")
        if len(parts) == 3:
            d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
            if 1 <= m <= 12 and 2000 <= y <= 2100:
                return f"{y:04d}-{m:02d}"
    except (ValueError, TypeError):
        pass
    return None


inadimplencia_bp = Blueprint("inadimplencia", __name__)

# ---------------------------------------------------------------------------
# Regexes para extração de data do filename (compilados uma vez)
# Tentados nesta ordem: mais específico → menos específico
# ---------------------------------------------------------------------------

# 1. DD.MM.YYYY — separador qualquer de {. - / _}, ano 4 dígitos
_RE_DMY4 = re.compile(r'(\d{2})[.\-/_](\d{2})[.\-/_](\d{4})')
# 2. YYYY-MM-DD — ISO
_RE_ISO  = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
# 3. DD.MM.YY — ano 2 dígitos (→ 2000+), não seguido de dígito
_RE_DMY2 = re.compile(r'(\d{2})[.\-/_](\d{2})[.\-/_](\d{2})(?!\d)')
# 4. DD.MM — sem ano (usa ano do uploaded_at)
_RE_DM   = re.compile(r'(\d{2})[.\-/_](\d{2})')


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _to_iso_brt(dt):
    """Converte datetime para string ISO-8601 em BRT (UTC-3)."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRT).isoformat()
    return str(dt)


def _snap_to_brt_date(uploaded_at):
    """Extrai a date em BRT a partir de um datetime."""
    if uploaded_at is None:
        return None
    if isinstance(uploaded_at, datetime):
        if uploaded_at.tzinfo is None:
            uploaded_at = uploaded_at.replace(tzinfo=timezone.utc)
        return uploaded_at.astimezone(BRT).date()
    return None


def _normalize_ua(ua):
    """Normaliza uploaded_at para datetime aware, usando sentinel para None."""
    if ua is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if isinstance(ua, datetime) and ua.tzinfo is None:
        return ua.replace(tzinfo=timezone.utc)
    return ua


def _extract_date_from_filename(filename, uploaded_at):
    """
    Extrai a data efetiva do nome do arquivo (case-insensitive não é necessário
    porque os padrões são todos numéricos).

    Ordem de tentativa (mais específico → menos específico):
    1. DD.MM.YYYY  (separador . - / _, ano 4 dígitos)
    2. YYYY-MM-DD  (ISO)
    3. DD.MM.YY    (ano 2 dígitos → 2000 + YY)
    4. DD.MM       (sem ano → usa uploaded_at.year em BRT)

    Validações por tentativa:
    - Dia ∈ [1..31], mês ∈ [1..12]
    - Se a data construída for futura (> hoje + 1 dia) → interrompe e usa fallback
    - Se date() lançar ValueError (ex.: 31/02) → tenta próximo padrão

    Fallback: uploaded_at convertido para date em BRT via _snap_to_brt_date.

    Exemplos que funcionam:
      "inad 28.05.xlsx"                              → 2026-05-28 (ano do upload)
      "inad 28-05-2026.xlsx"                         → 2026-05-28
      "inad_28_05.xlsx"                              → 2026-05-28
      "Relação de Alunos Inadimplentes 28.05.2026.xlsx" → 2026-05-28
      "inad.xlsx"                                    → fallback uploaded_at
      "inadimplentes 31.13.xlsx"                     → mês inválido → fallback
    """
    today = date_cls.today()
    fallback = _snap_to_brt_date(uploaded_at)

    def _build(day, month, year):
        """
        Tenta construir um date. Retorna (date|None, stop: bool).
        stop=True → data futura, caller deve usar fallback imediatamente.
        """
        if not (1 <= day <= 31 and 1 <= month <= 12):
            return None, False
        try:
            d = date_cls(year, month, day)
        except ValueError:
            return None, False
        if d > today + timedelta(days=1):
            return None, True
        return d, False

    if not filename:
        return fallback

    # 1. DD.MM.YYYY
    m = _RE_DMY4.search(filename)
    if m:
        d, stop = _build(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if stop:
            return fallback
        if d:
            return d

    # 2. YYYY-MM-DD (ISO)
    m = _RE_ISO.search(filename)
    if m:
        d, stop = _build(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        if stop:
            return fallback
        if d:
            return d

    # 3. DD.MM.YY (2-digit year → 2000+)
    m = _RE_DMY2.search(filename)
    if m:
        year = 2000 + int(m.group(3))
        d, stop = _build(int(m.group(1)), int(m.group(2)), year)
        if stop:
            return fallback
        if d:
            return d

    # 4. DD.MM (usa ano do uploaded_at em BRT)
    m = _RE_DM.search(filename)
    if m:
        brt_date = _snap_to_brt_date(uploaded_at)
        year = brt_date.year if brt_date else today.year
        d, stop = _build(int(m.group(1)), int(m.group(2)), year)
        if stop:
            return fallback
        if d:
            return d

    return fallback


def _ultimas_n_competencias_de_hoje(n: int) -> list:
    """
    Retorna as últimas N competências (YYYY-MM) a partir do mês atual.
    Ex: hoje=2026-06, n=3 -> ['2026-04', '2026-05', '2026-06']
    """
    if n is None or n <= 0:
        return []
    hoje = datetime.now()
    y, m = hoje.year, hoje.month
    out = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return sorted(out)


def _parse_recent_months(req_val):
    """
    Converte o param 'recent_months' em int ou None (None = sem filtro = tudo).
    Default = 3. Valores válidos: 'all', inteiros positivos.
    """
    if req_val is None or req_val == "":
        return 3
    s = str(req_val).strip().lower()
    if s in ("all", "tudo", "todos"):
        return None
    try:
        v = int(s)
        return v if v > 0 else 3
    except (ValueError, TypeError):
        return 3


_MEMO_TTL_S = 60
_EM_CURSO_MEMO: dict = {}
_CICLO_MEMO: dict = {}


def _memo_get(store: dict, key):
    hit = store.get(key)
    if not hit:
        return None
    val, ts = hit
    if time.time() - ts > _MEMO_TTL_S:
        return None
    return val


def _memo_set(store: dict, key, val) -> None:
    store[key] = (val, time.time())


def _get_total_em_curso(conn, target_date=None, _cache=None, ciclo=None):
    """
    Retorna (count_em_curso: int, uploaded_at: datetime|None) do snapshot de
    matriculados vigente na `target_date` (uploaded_at::date <= target_date,
    mais recente). Se nao houver snapshot anterior, faz fallback para o mais
    antigo disponivel. target_date None => hoje.

    Conta apenas linhas com situacao = 'EM CURSO' E nivel Graduacao. Quando
    `ciclo` e informado, restringe tambem ao ciclo correspondente (ex.: '2026/1').

    _cache: dict opcional para memoizar resultados por chave (target_date, ciclo).
    """
    key = (target_date or date_cls.today(), ciclo)
    if _cache is not None and key in _cache:
        return _cache[key]
    memoed = _memo_get(_EM_CURSO_MEMO, key)
    if memoed is not None:
        if _cache is not None:
            _cache[key] = memoed
        return memoed

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, uploaded_at FROM xl_snapshots
            WHERE tipo = 'matriculados'
              AND (%s::date IS NULL OR uploaded_at::date <= %s::date)
            ORDER BY uploaded_at DESC LIMIT 1
        """, (target_date, target_date))
        snap = cur.fetchone()
        if not snap:
            cur.execute("""
                SELECT id, uploaded_at FROM xl_snapshots
                WHERE tipo = 'matriculados'
                ORDER BY uploaded_at ASC LIMIT 1
            """)
            snap = cur.fetchone()
        if not snap:
            result = (0, None)
        else:
            snap_id, source_date = snap
            ciclo_clause = "AND data->>'ciclo' = %s" if ciclo else ""
            params = [snap_id]
            if ciclo:
                params.append(ciclo)
            cur.execute(f"""
                SELECT COUNT(*) FROM xl_rows
                WHERE snapshot_id = %s
                  AND data->>'situacao' = 'EM CURSO'
                  AND NOT (
                        (COALESCE(data->>'nivel','') != ''
                           AND data->>'nivel' ~* 'p[oó]s')
                     OR (COALESCE(data->>'nivel','') = ''
                           AND data->>'negocio' ~* 'p[oó]s')
                     OR (COALESCE(data->>'nivel','') = ''
                           AND COALESCE(data->>'negocio','') !~* 'p[oó]s'
                           AND data->>'curso' ~* '(mba|especializa[cç][aã]o|p[oó]s.gradua|lato.sensu|stricto)')
                  )
                  {ciclo_clause}
            """, params)
            result = (int(cur.fetchone()[0]), source_date)

    if _cache is not None:
        _cache[key] = result
    _memo_set(_EM_CURSO_MEMO, key, result)
    return result


def _ref_inicio_competencia(nivel):
    """Dia 10 do mês da competência (YYYY-MM) — mesma base do comparativo roxo."""
    try:
        y, m = (nivel or "").split("-")
        return date_cls(int(y), int(m), 10)
    except (ValueError, TypeError):
        return None


def _get_ciclo_da_competencia(conn, nivel, _cache=None):
    """
    Retorna o ciclo predominante (ex.: '2026/1') de uma competencia de
    inadimplencia, inferido cruzando os RGMs do snapshot mais recente dessa
    competencia com a coluna 'ciclo' da matriculados mais recente. Retorna
    None quando nao for possivel determinar (sem RGMs cruzaveis).
    """
    if not nivel:
        return None
    if _cache is not None and nivel in _cache:
        return _cache[nivel]
    memoed = _memo_get(_CICLO_MEMO, nivel)
    if memoed is not None:
        if _cache is not None:
            _cache[nivel] = memoed
        return memoed
    with conn.cursor() as cur:
        cur.execute("""
            WITH inad_snap AS (
                SELECT id FROM xl_snapshots
                WHERE tipo='inadimplentes' AND nivel=%s
                ORDER BY uploaded_at DESC LIMIT 1
            ),
            matric_snap AS (
                SELECT id FROM xl_snapshots
                WHERE tipo='matriculados'
                ORDER BY uploaded_at DESC LIMIT 1
            ),
            inad_rgms AS (
                SELECT DISTINCT r.data->>'rgm_digits' AS rgm
                FROM xl_rows r, inad_snap s
                WHERE r.snapshot_id = s.id
                  AND COALESCE(r.data->>'rgm_digits','') <> ''
            )
            SELECT m.data->>'ciclo' AS ciclo, COUNT(*) AS n
            FROM xl_rows m, matric_snap ms
            WHERE m.snapshot_id = ms.id
              AND m.data->>'rgm_digits' IN (SELECT rgm FROM inad_rgms)
              AND COALESCE(m.data->>'ciclo','') <> ''
            GROUP BY ciclo
            ORDER BY n DESC LIMIT 1
        """, (nivel,))
        row = cur.fetchone()
    ciclo = row[0] if row else None
    if _cache is not None:
        _cache[nivel] = ciclo
    _memo_set(_CICLO_MEMO, nivel, ciclo)
    return ciclo


def _get_dedupe_snapshots(conn, em_curso_total, nivel=None, date_a=None, date_b=None, nivel_in=None):
    """
    Busca todos os snapshots de inadimplentes, calcula effective_date a partir
    do filename (com fallback para uploaded_at::date BRT), agrega inadimplentes
    filtrados em uma única query (GROUP BY snapshot_id) e deduplica por data.

    Regra de dedupe: por effective_date, mantém o snapshot com MENOR inadimplentes
    (após filtros empresa ≠ TECNICO + título MENSALIDADE).
    Empate de inadimplentes: mantém o de uploaded_at mais recente.

    Filtros opcionais:
      nivel    — filtra por xl_snapshots.nivel exato (string)
      nivel_in — filtra por lista de níveis (usa ANY(%s)); ignorado se nivel passado
      date_a/date_b — uploaded_at range

    Retorna lista de dicts ordenada por effective_date DESC, cada item com:
      {snapshot_id, effective_date, uploaded_at, filename, row_count,
       inadimplentes, em_curso, taxa_pct}
    """
    where_extra = []
    params = []
    if nivel:
        where_extra.append("AND nivel = %s")
        params.append(nivel)
    elif nivel_in is not None:
        where_extra.append("AND nivel = ANY(%s)")
        params.append(list(nivel_in))
    if date_a:
        where_extra.append("AND uploaded_at::date >= %s::date")
        params.append(date_a)
    if date_b:
        where_extra.append("AND uploaded_at::date <= %s::date")
        params.append(date_b)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT id, uploaded_at, filename, row_count, nivel
            FROM xl_snapshots
            WHERE tipo = 'inadimplentes'
            {' '.join(where_extra)}
            ORDER BY uploaded_at DESC
        """, params)
        snaps = cur.fetchall()

    if not snaps:
        return []

    snap_ids = [s["id"] for s in snaps]
    placeholders = ",".join(["%s"] * len(snap_ids))
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT snapshot_id,
                   COUNT(*) AS total_rows,
                   COUNT(DISTINCT data->>'rgm_digits') FILTER (
                     WHERE COALESCE(data->>'empresa','') != 'CRUZEIRO DO SUL - TECNICO EAD'
                       AND EXISTS (
                         SELECT 1 FROM jsonb_array_elements(data->'titulos') t
                         WHERE UPPER(t->>'descricao') = 'MENSALIDADE'
                       )
                       AND COALESCE(data->>'rgm_digits','') != ''
                   ) AS old_inad,
                   BOOL_OR((data->'titulos') IS NOT NULL) AS is_old_format
            FROM xl_rows
            WHERE snapshot_id IN ({placeholders})
            GROUP BY snapshot_id
        """, snap_ids)
        inad_map = {}
        for row in cur.fetchall():
            snap_id, total_rows, old_inad, is_old = row
            inad = int(old_inad or 0) if is_old else int(total_rows or 0)
            inad_map[snap_id] = inad

    # Caches por request: evitam recomputar a base/ciclo de uma mesma
    # combinacao (data, ciclo) ou competencia.
    em_curso_cache: dict = {}
    ciclo_cache: dict = {}

    date_groups: dict = {}
    for s in snaps:
        inad = inad_map.get(s["id"], 0)
        eff_date = _extract_date_from_filename(s["filename"], s["uploaded_at"])
        # Ciclo correspondente a competencia desse snapshot (ex.: '2026/1')
        ciclo = _get_ciclo_da_competencia(conn, s["nivel"], ciclo_cache)
        # Base do começo do mês da competência (dia 10), não a do dia do upload
        ref = _ref_inicio_competencia(s["nivel"]) or eff_date
        em_curso_dia, _ = _get_total_em_curso(conn, ref, em_curso_cache, ciclo=ciclo)
        if em_curso_dia <= 0:
            em_curso_dia = em_curso_total
        taxa = round(inad / em_curso_dia * 100, 2) if em_curso_dia > 0 else 0.0
        item = {
            "snapshot_id":    s["id"],
            "effective_date": eff_date,
            "uploaded_at":    s["uploaded_at"],
            "filename":       s["filename"],
            "row_count":      s["row_count"],
            "inadimplentes":  inad,
            "em_curso":       em_curso_dia,
            "taxa_pct":       taxa,
            "nivel":          s["nivel"],
        }
        # Dedupe por (effective_date, nivel) — assim snapshots de competências
        # diferentes uploadados no mesmo dia não colidem.
        group_key = (eff_date, s["nivel"])
        existing = date_groups.get(group_key)
        if existing is None:
            date_groups[group_key] = item
        else:
            curr_ua = _normalize_ua(item["uploaded_at"])
            prev_ua = _normalize_ua(existing["uploaded_at"])
            if inad < existing["inadimplentes"] or (
                inad == existing["inadimplentes"] and curr_ua > prev_ua
            ):
                date_groups[group_key] = item

    return sorted(
        date_groups.values(),
        key=lambda x: x["effective_date"] or date_cls.min,
        reverse=True,
    )


def _find_snap_for_date(snapshots, date_str):
    """
    Procura no slice já deduplicado o snapshot com effective_date == date_str.
    Se não encontrar, retorna o snapshot com maior effective_date < date_str.

    Retorna (item|None, is_fallback, fallback_date_str, dias_diferenca).
    """
    try:
        target = date_cls.fromisoformat(date_str)
    except (ValueError, TypeError):
        return None, False, None, 0

    best_prior = None
    for snap in snapshots:
        ed = snap["effective_date"]
        if ed is None:
            continue
        if ed == target:
            return snap, False, None, 0
        if ed < target:
            if best_prior is None or ed > best_prior["effective_date"]:
                best_prior = snap

    if best_prior:
        fb_date = best_prior["effective_date"]
        fb_str = fb_date.isoformat() if fb_date else None
        dias = (target - fb_date).days if fb_date else 0
        return best_prior, True, fb_str, dias

    return None, False, None, 0


def _build_snap_response_from_item(item, requested_date, is_fallback, fallback_date, dias):
    """Monta dict de resposta a partir de um item do _get_dedupe_snapshots."""
    eff_date = item["effective_date"]
    return {
        "snapshot_id":    item["snapshot_id"],
        "snapshot_date":  eff_date.isoformat() if eff_date else None,
        "effective_date": eff_date.isoformat() if eff_date else None,
        "uploaded_at":    _to_iso_brt(item["uploaded_at"]),
        "filename":       item["filename"],
        "inadimplentes":  item["inadimplentes"],
        "em_curso":       item["em_curso"],
        "taxa_pct":       item["taxa_pct"],
        "is_fallback":    is_fallback,
        "requested_date": requested_date,
        "fallback_date":  fallback_date,
        "dias_diferenca": dias,
    }


def _empty_side(date_str, total_em_curso):
    """Resposta vazia para um lado da comparação (sem snapshot)."""
    return {
        "snapshot_id":    None,
        "snapshot_date":  None,
        "effective_date": None,
        "uploaded_at":    None,
        "filename":       None,
        "inadimplentes":  0,
        "em_curso":       total_em_curso,
        "taxa_pct":       0.0,
        "is_fallback":    False,
        "requested_date": date_str,
        "fallback_date":  None,
        "dias_diferenca": 0,
    }


# ---------------------------------------------------------------------------
# Endpoint 1: GET /api/inadimplencia/list
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/list")
def api_inadimplencia_list():
    """
    Lista todos os snapshots de inadimplentes com taxa calculada.
    Snapshots descartados pela dedupe por effective_date NÃO aparecem.
    Resultado ordenado por effective_date DESC.
    """
    competencia = request.args.get("competencia", "").strip() or None
    date_a = request.args.get("date_a", "").strip() or None
    date_b = request.args.get("date_b", "").strip() or None
    recent_months = _parse_recent_months(request.args.get("recent_months"))
    conn = get_conn()
    try:
        total_em_curso, source_date = _get_total_em_curso(conn)
        nivel_in = None
        if not competencia and recent_months is not None:
            nivel_in = _ultimas_n_competencias_de_hoje(recent_months)
        snapshots = _get_dedupe_snapshots(conn, total_em_curso, nivel=competencia, date_a=date_a, date_b=date_b, nivel_in=nivel_in)

        result = []
        for s in snapshots:
            eff_date = s["effective_date"]
            nivel = s.get("nivel")
            is_comp = bool(nivel and re.match(r'^\d{4}-\d{2}$', nivel))
            result.append({
                "id":              s["snapshot_id"],
                "snapshot_id":     s["snapshot_id"],
                "effective_date":  eff_date.isoformat() if eff_date else None,
                "uploaded_at":     _to_iso_brt(s["uploaded_at"]),
                "filename":        s["filename"],
                "row_count_total": s["row_count"],
                "inadimplentes":   s["inadimplentes"],
                "em_curso":        s["em_curso"],
                "taxa_pct":        s["taxa_pct"],
                "nivel":           nivel,
                "competencia_label": _competencia_label(nivel) if is_comp else None,
            })

        return jsonify({
            "snapshots": result,
            "total_em_curso_atual": total_em_curso,
            "total_em_curso_source_date": _to_iso_brt(source_date),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 2: GET /api/inadimplencia/atual?date=YYYY-MM-DD
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/atual")
def api_inadimplencia_atual():
    """
    Retorna dados do snapshot de inadimplentes mais próximo de uma data.
    Busca por effective_date (extraída do filename com fallback para uploaded_at::date BRT).
    Se date omitido, usa o snapshot com maior effective_date.
    """
    date_str = request.args.get("date", "").strip()
    competencia = request.args.get("competencia", "").strip() or None
    date_a = request.args.get("date_a", "").strip() or None
    date_b = request.args.get("date_b", "").strip() or None
    recent_months = _parse_recent_months(request.args.get("recent_months"))
    conn = get_conn()
    try:
        total_em_curso, _ = _get_total_em_curso(conn)
        nivel_in = None
        if not competencia and recent_months is not None:
            nivel_in = _ultimas_n_competencias_de_hoje(recent_months)
        snapshots = _get_dedupe_snapshots(conn, total_em_curso, nivel=competencia, date_a=date_a, date_b=date_b, nivel_in=nivel_in)

        if not snapshots:
            return jsonify({"error": "Nenhum snapshot de inadimplentes encontrado."}), 404

        if not date_str:
            item = snapshots[0]
            eff_date = item["effective_date"]
            resp = _build_snap_response_from_item(
                item,
                requested_date=eff_date.isoformat() if eff_date else None,
                is_fallback=False,
                fallback_date=None,
                dias=0,
            )
        else:
            item, is_fallback, fallback_date, dias = _find_snap_for_date(snapshots, date_str)
            if not item:
                return jsonify({"error": "Nenhum snapshot encontrado para a data ou antes dela."}), 404
            resp = _build_snap_response_from_item(
                item,
                requested_date=date_str,
                is_fallback=is_fallback,
                fallback_date=fallback_date,
                dias=dias,
            )

        return jsonify(resp)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 3: GET /api/inadimplencia/comparar?date_a=&date_b=
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/comparar")
def api_inadimplencia_comparar():
    """
    Compara dois snapshots de inadimplentes. Defaults: date_b=hoje, date_a=7 dias atrás.
    Usa lógica de fallback para datas sem snapshot, comparando por effective_date.
    """
    today = date_cls.today()
    date_b_str = request.args.get("date_b", today.isoformat()).strip()
    date_a_str = request.args.get("date_a", (today - timedelta(days=7)).isoformat()).strip()

    conn = get_conn()
    try:
        total_em_curso, _ = _get_total_em_curso(conn)
        snapshots = _get_dedupe_snapshots(conn, total_em_curso)

        def _get_side(date_str):
            item, is_fallback, fallback_date, dias = _find_snap_for_date(snapshots, date_str)
            if not item:
                return _empty_side(date_str, total_em_curso)
            return _build_snap_response_from_item(
                item,
                requested_date=date_str,
                is_fallback=is_fallback,
                fallback_date=fallback_date,
                dias=dias,
            )

        a = _get_side(date_a_str)
        b = _get_side(date_b_str)

        delta_taxa = round((b["taxa_pct"] or 0.0) - (a["taxa_pct"] or 0.0), 2)
        delta_inad = (b["inadimplentes"] or 0) - (a["inadimplentes"] or 0)

        return jsonify({
            "a": a,
            "b": b,
            "delta_taxa_pct": delta_taxa,
            "delta_inadimplentes": delta_inad,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 4: GET /api/inadimplencia/evolucao?days=30
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/evolucao")
def api_inadimplencia_evolucao():
    """
    Série temporal de inadimplência. days aceita: 7 | 30 | 90 | all (default 30).
    Retorna 1 ponto por effective_date (já deduplicado), ordenado ASC.
    Para days=N, filtra snapshots com effective_date >= hoje - N dias.
    Quando date_a ou date_b estão presentes, o range já delimita e o corte por days não se aplica.
    Aceita também: competencia (filtra por nivel), date_a, date_b.
    """
    days_param = request.args.get("days", "30").strip()
    competencia = request.args.get("competencia", "").strip() or None
    date_a = request.args.get("date_a", "").strip() or None
    date_b = request.args.get("date_b", "").strip() or None
    recent_months = _parse_recent_months(request.args.get("recent_months"))
    conn = get_conn()
    try:
        total_em_curso, _ = _get_total_em_curso(conn)
        nivel_in = None
        if not competencia and recent_months is not None:
            nivel_in = _ultimas_n_competencias_de_hoje(recent_months)
        snapshots = _get_dedupe_snapshots(conn, total_em_curso, nivel=competencia, date_a=date_a, date_b=date_b, nivel_in=nivel_in)

        if not snapshots:
            return jsonify({"points": [], "em_curso_constant": total_em_curso})

        # 7d/30d/90d cortam a série; se houver data final em cima, o corte
        # é a partir dela (não de "hoje"), para os dois filtros se combinarem.
        if days_param != "all":
            try:
                days = int(days_param)
                if days <= 0:
                    days = 30
            except ValueError:
                days = 30
            try:
                end = date_cls.fromisoformat(date_b) if date_b else date_cls.today()
            except ValueError:
                end = date_cls.today()
            cutoff = end - timedelta(days=days)
            snapshots = [
                s for s in snapshots
                if s["effective_date"] and s["effective_date"] >= cutoff
            ]

        # Dedupe por effective_date — mantém a competência mais recente
        # (nivel DESC). Evita múltiplos pontos no mesmo dia quando o usuário
        # subiu várias competências de uma vez.
        by_date: dict = {}
        for s in snapshots:
            eff = s["effective_date"]
            if eff is None:
                continue
            cur = by_date.get(eff)
            if cur is None or (s.get("nivel") or "") > (cur.get("nivel") or ""):
                by_date[eff] = s
        snapshots_asc = sorted(by_date.values(), key=lambda x: x["effective_date"])

        points = []
        for s in snapshots_asc:
            eff_date = s["effective_date"]
            points.append({
                "date":           eff_date.isoformat() if eff_date else None,
                "effective_date": eff_date.isoformat() if eff_date else None,
                "uploaded_at":    _to_iso_brt(s["uploaded_at"]),
                "snapshot_id":    s["snapshot_id"],
                "inadimplentes":  s["inadimplentes"],
                "em_curso":       s["em_curso"],
                "taxa_pct":       s["taxa_pct"],
            })

        return jsonify({"points": points, "em_curso_constant": total_em_curso})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 4b: GET /api/inadimplencia/evolucao-por-mes
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/evolucao-por-mes")
def api_inadimplencia_evolucao_por_mes():
    """
    Retorna uma serie por competencia (mes/ano) com pontos { day, taxa_pct, ... }.
    Respeita competencia, recent_months e date_a / date_b.
    """
    competencia = request.args.get("competencia", "").strip() or None
    date_a = request.args.get("date_a", "").strip() or None
    date_b = request.args.get("date_b", "").strip() or None
    recent_months = _parse_recent_months(request.args.get("recent_months"))
    conn = get_conn()
    try:
        total_em_curso, _ = _get_total_em_curso(conn)
        nivel_in = None
        if not competencia and recent_months is not None:
            nivel_in = _ultimas_n_competencias_de_hoje(recent_months)
        snapshots = _get_dedupe_snapshots(
            conn, total_em_curso, nivel=competencia, date_a=date_a, date_b=date_b, nivel_in=nivel_in
        )

        # Agrupa por nivel; dentro de cada nivel, mantem 1 ponto por dia do mes
        # (o snapshot com effective_date mais recente naquele dia)
        grupos: dict = {}
        for s in snapshots:
            nivel = s.get("nivel")
            eff = s.get("effective_date")
            if not nivel or not eff:
                continue
            g = grupos.setdefault(nivel, {})
            dia = eff.day
            cur = g.get(dia)
            if cur is None or eff > cur["effective_date"]:
                g[dia] = s

        # Recalcula em_curso/taxa de cada ponto usando a base de matriculados
        # vigente no DIA 10 do mes da competencia, filtrada pelo ciclo
        # correspondente (regra desse grafico).
        em_curso_cache: dict = {}
        ciclo_cache: dict = {}
        for nivel, g in grupos.items():
            try:
                y, m = nivel.split('-')
                ref = date_cls(int(y), int(m), 10)
            except (ValueError, TypeError):
                ref = None
            ciclo = _get_ciclo_da_competencia(conn, nivel, ciclo_cache)
            em_curso_ref, _ = _get_total_em_curso(conn, ref, em_curso_cache, ciclo=ciclo)
            if not em_curso_ref or em_curso_ref <= 0:
                continue
            for snap in g.values():
                inad = snap.get("inadimplentes") or 0
                snap["em_curso"] = em_curso_ref
                snap["taxa_pct"] = round(inad / em_curso_ref * 100, 2)

        competencias = []
        for nivel in sorted(grupos.keys()):
            pontos = sorted(grupos[nivel].values(), key=lambda x: x["effective_date"])
            competencias.append({
                "nivel": nivel,
                "label": _competencia_label(nivel),
                "points": [{
                    "day":           p["effective_date"].day,
                    "date":          p["effective_date"].isoformat(),
                    "taxa_pct":      p["taxa_pct"],
                    "inadimplentes": p["inadimplentes"],
                    "em_curso":      p["em_curso"],
                } for p in pontos],
            })

        return jsonify({"competencias": competencias, "em_curso_constant": total_em_curso})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 5: GET /api/inadimplencia/competencias
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/competencias")
def api_inadimplencia_competencias():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT nivel
                FROM xl_snapshots
                WHERE tipo = 'inadimplentes' AND nivel ~ '^[0-9]{4}-[0-9]{2}$'
                ORDER BY nivel DESC
            """)
            rows = cur.fetchall()
        competencias = [{"value": r[0], "label": _competencia_label(r[0])} for r in rows]
        return jsonify({"competencias": competencias})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 6: GET /api/inadimplencia/comparar-periodo
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/comparar-periodo")
def api_inadimplencia_comparar_periodo():
    competencia = request.args.get("competencia", "").strip() or None
    date_a = request.args.get("date_a", "").strip() or None
    date_b = request.args.get("date_b", "").strip() or None

    if not competencia:
        return jsonify({"error": "Parâmetro 'competencia' é obrigatório"}), 400

    conn = get_conn()
    try:
        em_curso, _ = _get_total_em_curso(conn)
        snapshots = _get_dedupe_snapshots(conn, em_curso, nivel=competencia, date_a=date_a, date_b=date_b)

        if len(snapshots) < 2:
            return jsonify({"insuficiente": True, "snapshots_count": len(snapshots)})

        ordered = sorted(snapshots, key=lambda s: s["uploaded_at"])
        primeiro = ordered[0]
        ultimo = ordered[-1]
        variacao = round(ultimo["taxa_pct"] - primeiro["taxa_pct"], 2)

        return jsonify({
            "competencia": competencia,
            "competencia_label": _competencia_label(competencia),
            "snapshots_count": len(snapshots),
            "primeiro": {
                "uploaded_at": _to_iso_brt(primeiro["uploaded_at"]),
                "inadimplentes": primeiro["inadimplentes"],
                "taxa_pct": primeiro["taxa_pct"],
            },
            "ultimo": {
                "uploaded_at": _to_iso_brt(ultimo["uploaded_at"]),
                "inadimplentes": ultimo["inadimplentes"],
                "taxa_pct": ultimo["taxa_pct"],
            },
            "variacao_pp": variacao,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 7: GET /api/inadimplencia/reincidencia
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/reincidencia")
def api_inadimplencia_reincidencia():
    date_a = request.args.get("date_a", "").strip() or None
    date_b = request.args.get("date_b", "").strip() or None

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql = """
                WITH latest_per_comp AS (
                    SELECT DISTINCT ON (nivel) id, nivel, uploaded_at
                    FROM xl_snapshots
                    WHERE tipo = 'inadimplentes' AND nivel ~ '^[0-9]{4}-[0-9]{2}$'
                      AND (%(date_a)s IS NULL OR uploaded_at::date >= %(date_a)s::date)
                      AND (%(date_b)s IS NULL OR uploaded_at::date <= %(date_b)s::date)
                      AND uploaded_at::date > (nivel || '-25')::date
                    ORDER BY nivel, uploaded_at DESC
                ),
                rgm_comp AS (
                    SELECT DISTINCT r.data->>'rgm_digits' AS rgm, l.nivel
                    FROM xl_rows r
                    JOIN latest_per_comp l ON l.id = r.snapshot_id
                    WHERE COALESCE(r.data->>'rgm_digits', '') != ''
                ),
                counts AS (
                    SELECT rgm, COUNT(DISTINCT nivel) AS n_comps
                    FROM rgm_comp GROUP BY rgm
                )
                SELECT
                    COUNT(*) FILTER (WHERE n_comps = 1) AS b1,
                    COUNT(*) FILTER (WHERE n_comps = 2) AS b2,
                    COUNT(*) FILTER (WHERE n_comps = 3) AS b3,
                    COUNT(*) FILTER (WHERE n_comps >= 4) AS b4_plus,
                    COUNT(*) AS total
                FROM counts
                WHERE n_comps >= 1
            """
            cur.execute(sql, {"date_a": date_a, "date_b": date_b})
            row = cur.fetchone() or {}

            cur.execute("""
                SELECT DISTINCT ON (nivel) id AS snapshot_id, nivel, uploaded_at
                FROM xl_snapshots
                WHERE tipo = 'inadimplentes' AND nivel ~ '^[0-9]{4}-[0-9]{2}$'
                  AND (%(date_a)s IS NULL OR uploaded_at::date >= %(date_a)s::date)
                  AND (%(date_b)s IS NULL OR uploaded_at::date <= %(date_b)s::date)
                  AND uploaded_at::date > (nivel || '-25')::date
                ORDER BY nivel, uploaded_at DESC
            """, {"date_a": date_a, "date_b": date_b})
            snaps = cur.fetchall()

        snapshots_usados = [
            {"snapshot_id": s["snapshot_id"], "nivel": s["nivel"], "uploaded_at": _to_iso_brt(s["uploaded_at"])}
            for s in snaps
        ]
        competencias_usadas = sorted({s["nivel"] for s in snaps})

        return jsonify({
            "buckets": {
                "1": int(row.get("b1") or 0),
                "2": int(row.get("b2") or 0),
                "3": int(row.get("b3") or 0),
                "4_plus": int(row.get("b4_plus") or 0),
            },
            "competencias_usadas": competencias_usadas,
            "snapshots_usados": snapshots_usados,
            "rgms_analisados": int(row.get("total") or 0),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoint 8: DELETE /api/inadimplencia/snapshot/<snap_id>
# ---------------------------------------------------------------------------

@inadimplencia_bp.route("/api/inadimplencia/snapshot/<int:snap_id>", methods=["DELETE"])
def api_inadimplencia_delete_snapshot(snap_id):
    """Apaga um snapshot de inadimplentes (e suas xl_rows)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, tipo, filename FROM xl_snapshots WHERE id = %s", (snap_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Snapshot não encontrado"}), 404
            if row[1] != "inadimplentes":
                return jsonify({"error": "Snapshot não é do tipo inadimplentes"}), 400

            cur.execute("DELETE FROM xl_rows WHERE snapshot_id = %s", (snap_id,))
            rows_deleted = cur.rowcount
            cur.execute("DELETE FROM xl_snapshots WHERE id = %s", (snap_id,))
        conn.commit()
        return jsonify({"ok": True, "snapshot_id": snap_id, "filename": row[2], "xl_rows_deleted": rows_deleted})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
