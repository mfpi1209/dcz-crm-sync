"""
Inadimplência — blueprint de taxa e evolução temporal.
Prefixo: /api/inadimplencia
"""
import re
from datetime import datetime, timezone, timedelta, date as date_cls

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

from db import get_conn
from helpers import BRT

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


def _get_total_em_curso(conn):
    """
    Retorna (count_em_curso: int, uploaded_at: datetime|None) do snapshot mais
    recente de matriculados. Conta apenas linhas com situacao = 'EM CURSO' E
    nível Graduação (a tabela de inadimplência só contempla graduação, então
    o denominador da taxa precisa ser consistente).

    Classificação de nível espelha a usada em routes/dashboard.py:
    Pós-Graduação quando nivel/negocio/curso indicam pós; Graduação caso contrário.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, uploaded_at FROM xl_snapshots
            WHERE tipo = 'matriculados'
            ORDER BY uploaded_at DESC LIMIT 1
        """)
        snap = cur.fetchone()
        if not snap:
            return 0, None
        snap_id, source_date = snap
        cur.execute("""
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
        """, (snap_id,))
        count = cur.fetchone()[0]
    return int(count), source_date


def _get_dedupe_snapshots(conn, em_curso_total):
    """
    Busca todos os snapshots de inadimplentes, calcula effective_date a partir
    do filename (com fallback para uploaded_at::date BRT), agrega inadimplentes
    filtrados em uma única query (GROUP BY snapshot_id) e deduplica por data.

    Regra de dedupe: por effective_date, mantém o snapshot com MENOR inadimplentes
    (após filtros empresa ≠ TECNICO + título MENSALIDADE).
    Empate de inadimplentes: mantém o de uploaded_at mais recente.

    Retorna lista de dicts ordenada por effective_date DESC, cada item com:
      {snapshot_id, effective_date, uploaded_at, filename, row_count,
       inadimplentes, em_curso, taxa_pct}
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, uploaded_at, filename, row_count
            FROM xl_snapshots
            WHERE tipo = 'inadimplentes'
            ORDER BY uploaded_at DESC
        """)
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

    date_groups: dict = {}
    for s in snaps:
        inad = inad_map.get(s["id"], 0)
        taxa = round(inad / em_curso_total * 100, 2) if em_curso_total > 0 else 0.0
        eff_date = _extract_date_from_filename(s["filename"], s["uploaded_at"])
        item = {
            "snapshot_id":    s["id"],
            "effective_date": eff_date,
            "uploaded_at":    s["uploaded_at"],
            "filename":       s["filename"],
            "row_count":      s["row_count"],
            "inadimplentes":  inad,
            "em_curso":       em_curso_total,
            "taxa_pct":       taxa,
        }
        existing = date_groups.get(eff_date)
        if existing is None:
            date_groups[eff_date] = item
        else:
            curr_ua = _normalize_ua(item["uploaded_at"])
            prev_ua = _normalize_ua(existing["uploaded_at"])
            if inad < existing["inadimplentes"] or (
                inad == existing["inadimplentes"] and curr_ua > prev_ua
            ):
                date_groups[eff_date] = item

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
    conn = get_conn()
    try:
        total_em_curso, source_date = _get_total_em_curso(conn)
        snapshots = _get_dedupe_snapshots(conn, total_em_curso)

        result = []
        for s in snapshots:
            eff_date = s["effective_date"]
            result.append({
                "id":             s["snapshot_id"],
                "effective_date": eff_date.isoformat() if eff_date else None,
                "uploaded_at":    _to_iso_brt(s["uploaded_at"]),
                "filename":       s["filename"],
                "row_count_total": s["row_count"],
                "inadimplentes":  s["inadimplentes"],
                "em_curso":       s["em_curso"],
                "taxa_pct":       s["taxa_pct"],
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
    conn = get_conn()
    try:
        total_em_curso, _ = _get_total_em_curso(conn)
        snapshots = _get_dedupe_snapshots(conn, total_em_curso)

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
    """
    days_param = request.args.get("days", "30").strip()
    conn = get_conn()
    try:
        total_em_curso, _ = _get_total_em_curso(conn)
        snapshots = _get_dedupe_snapshots(conn, total_em_curso)

        if not snapshots:
            return jsonify({"points": [], "em_curso_constant": total_em_curso})

        if days_param != "all":
            try:
                days = int(days_param)
                if days <= 0:
                    days = 30
            except ValueError:
                days = 30
            cutoff = date_cls.today() - timedelta(days=days)
            snapshots = [
                s for s in snapshots
                if s["effective_date"] and s["effective_date"] >= cutoff
            ]

        snapshots_asc = sorted(
            snapshots,
            key=lambda x: x["effective_date"] or date_cls.min,
        )

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
