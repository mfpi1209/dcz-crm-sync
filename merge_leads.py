"""
DataCrazy CRM — Merge de leads duplicados.

Lê o relatório enriquecido (duplicatas_enriquecido.csv) e consolida
negócios que estão em leads diferentes para o mesmo RGM.

Ações por merge:
  1. DELETE /businesses/{biz_id} -> deleta negócio duplicado do lead a remover
  2. PATCH  /leads/{lead_id}     -> atualiza telefone se necessário
  3. DELETE /leads/{lead_id}     -> remove lead vazio (sem negócios)

Lógica: o mesmo RGM já existe no lead mantido, então o negócio duplicado
no lead a remover é deletado. Quando o lead fica vazio, é deletado também.

Fases:
  1 — MANTER + CANDIDATO A MERGE (sem atividade)
  2 — in_process vs lost (manter in_process)
  3 — Ambos mesmo status (manter mais recente)
  4 — 7+ leads por RGM (somente relatório)

Uso:
    python merge_leads.py --dry-run
    python merge_leads.py --test
    python merge_leads.py --execute --fase 1
    python merge_leads.py --execute --fase 2 --limit 50
    python merge_leads.py --execute --rate 120
"""

import sys
import io
import os
import csv
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

BRT = timezone(timedelta(hours=-3))

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv(Path(__file__).parent / ".env")

API_BASE = "https://api.g1.datacrazy.io/api/v1"
API_TOKEN = os.getenv("DATACRAZY_API_TOKEN", "")

REPORTS_DIR = Path(__file__).parent / "reports"
INPUT_FILE = REPORTS_DIR / "duplicatas_enriquecido.csv"

DEFAULT_RATE = 60
MULTI_LEAD_THRESHOLD = 7

RGM_FIELD_ID = "2ac4e30f-cfd7-435f-b688-fbce27f76c38"

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)


def get_conn():
    return psycopg2.connect(**DB_DSN)


def get_business_ids_for_lead(conn, lead_id):
    """Busca IDs de todos os negócios de um lead no banco local."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM businesses WHERE data->>'leadId' = %s", (lead_id,))
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return ids


def get_biz_ids_by_rgm_on_lead(conn, lead_id, rgm):
    """Busca IDs dos negócios com um RGM específico em um lead."""
    cur = conn.cursor()
    cur.execute("""
        SELECT b.id FROM businesses b
        CROSS JOIN LATERAL jsonb_array_elements(b.data->'additionalFields') elem
        WHERE b.data->>'leadId' = %s
          AND elem->'additionalField'->>'id' = %s
          AND elem->>'value' = %s
    """, (lead_id, RGM_FIELD_ID, rgm))
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return ids


class _BRTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=BRT)
        return dt.strftime(datefmt or "%H:%M:%S")


logging.basicConfig(level=logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(_BRTFormatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"))
logging.root.handlers = [_handler]
log = logging.getLogger("merge")


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

class ApiClient:
    def __init__(self, rate_limit=DEFAULT_RATE):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {API_TOKEN}"
        self.s.headers["Content-Type"] = "application/json"
        self.rate_limit = rate_limit
        self.base_delay = 60.0 / rate_limit
        self._remaining = rate_limit
        self._reset = 0
        self._last_req = 0.0
        self.total_calls = 0

    def _throttle(self):
        if self._remaining <= 5 and self._reset > 0:
            wait = self._reset + 1
            log.warning("Rate-limit crítico (%d restantes) — pausando %ds",
                        self._remaining, wait)
            time.sleep(wait)
            return
        elapsed = time.monotonic() - self._last_req
        if elapsed < self.base_delay:
            time.sleep(self.base_delay - elapsed)

    def _read_headers(self, r):
        self._remaining = int(r.headers.get("X-RateLimit-Remaining", self._remaining))
        self._reset = int(r.headers.get("X-RateLimit-Reset", 0))

    def _request(self, method, url, json_body=None):
        for attempt in range(4):
            self._throttle()
            self._last_req = time.monotonic()
            self.total_calls += 1
            try:
                r = self.s.request(method, url, json=json_body, timeout=30)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ReadTimeout) as e:
                wait = 5 * (2 ** attempt)
                log.warning("Conexão falhou (%s) — retry em %ds (%d/4)",
                            type(e).__name__, wait, attempt + 1)
                time.sleep(wait)
                continue

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 30))
                log.warning("429 — Retry-After %ds (%d/4)", retry, attempt + 1)
                time.sleep(retry + 1)
                continue

            self._read_headers(r)
            if r.status_code >= 400:
                return {"ok": False, "status": r.status_code, "body": r.text[:500]}
            try:
                body = r.json()
            except Exception:
                body = {}
            return {"ok": True, "status": r.status_code, "body": body}

        return {"ok": False, "status": 0, "body": "Falha após 4 tentativas"}

    def delete_business(self, biz_id):
        """Deleta um negócio."""
        return self._request("DELETE", f"{API_BASE}/businesses/{biz_id}")

    def patch_lead_phone(self, lead_id, phone_number):
        """Atualiza telefone do lead."""
        return self._request("PATCH", f"{API_BASE}/leads/{lead_id}",
                             {"phone": str(phone_number)})

    def delete_lead(self, lead_id):
        """Deleta um lead (só funciona se não tiver negócios)."""
        return self._request("DELETE", f"{API_BASE}/leads/{lead_id}")


# ---------------------------------------------------------------------------
# Leitura e parsing do CSV
# ---------------------------------------------------------------------------

def parse_date(s):
    """Converte '25/02/2026 16:40' para datetime ou None."""
    if not s or not s.strip():
        return None
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y %H:%M").replace(tzinfo=BRT)
    except ValueError:
        return None


def load_report():
    """Lê o CSV enriquecido e agrupa por RGM."""
    if not INPUT_FILE.exists():
        log.error("Arquivo não encontrado: %s", INPUT_FILE)
        log.error("Rode o enriquecimento primeiro.")
        return {}

    rows = []
    with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for r in reader:
            rows.append(r)
    log.info("  %d linhas lidas do relatório", len(rows))

    by_rgm = defaultdict(list)
    for r in rows:
        rgm = (r.get("RGM") or "").strip()
        if not rgm:
            continue
        by_rgm[rgm].append({
            "rgm": rgm,
            "lead_id": r.get("lead_id", "").strip(),
            "lead_nome": r.get("lead_nome", ""),
            "lead_cpf": r.get("lead_cpf", ""),
            "lead_telefone": r.get("lead_telefone", "").strip().replace("\n", ""),
            "lead_email": r.get("lead_email", ""),
            "curso": r.get("curso", ""),
            "situacao": r.get("situacao", ""),
            "status_crm": r.get("status_crm", ""),
            "score": int(r.get("score") or 0),
            "tem_conversa": r.get("tem_conversa", "") == "Sim",
            "ultima_msg": parse_date(r.get("ultima_msg", "")),
            "ultima_msg_recebida": parse_date(r.get("ultima_msg_recebida", "")),
            "ultima_msg_enviada": parse_date(r.get("ultima_msg_enviada", "")),
            "conversa_finalizada": r.get("conversa_finalizada", "") == "Sim",
            "recomendacao": r.get("recomendacao", "").strip(),
        })

    log.info("  %d RGMs únicos", len(by_rgm))
    return by_rgm


# ---------------------------------------------------------------------------
# Regras de decisão
# ---------------------------------------------------------------------------

def _latest_activity(entry):
    """Retorna a data de atividade mais recente (msg recebida > msg enviada > msg geral)."""
    return entry["ultima_msg_recebida"] or entry["ultima_msg_enviada"] or entry["ultima_msg"]


def _best_phone_lead(entries):
    """Retorna o entry com a conversa recebida mais recente (telefone de último contato)."""
    with_recv = [e for e in entries if e["ultima_msg_recebida"]]
    if not with_recv:
        return None
    return max(with_recv, key=lambda e: e["ultima_msg_recebida"])


def decide_merge(rgm, entries):
    """
    Decide qual lead manter e quais remover.

    Retorna dict:
      fase:        int (1-4)
      manter:      entry do lead a manter
      remover:     list de entries dos leads a remover
      phone_update: str ou None (telefone a atualizar no lead mantido)
      reason:      str explicando a decisão
    """
    n = len(entries)

    if n >= MULTI_LEAD_THRESHOLD:
        return {"fase": 4, "manter": None, "remover": [], "phone_update": None,
                "reason": f"Multi-lead ({n} leads) — revisão manual"}

    has_manter = [e for e in entries if "MANTER" in e["recomendacao"]]
    has_merge = [e for e in entries if "CANDIDATO A MERGE" in e["recomendacao"]]
    has_avaliar = [e for e in entries if "AVALIAR" in e["recomendacao"]]
    blank_rec = [e for e in entries if not e["recomendacao"]]

    manter = None
    remover = []
    fase = 3
    reason = ""

    # Fase 1: MANTER + CANDIDATO A MERGE (sem atividade)
    if has_manter and has_merge and not has_avaliar and not blank_rec:
        manter = has_manter[0]
        remover = has_merge
        fase = 1
        reason = "MANTER + CANDIDATO A MERGE (sem atividade)"

    # Fase 2: in_process vs lost
    elif len(entries) <= 6:
        in_process = [e for e in entries if e["status_crm"] == "in_process"]
        lost = [e for e in entries if e["status_crm"] == "lost"]

        if in_process and lost and not (len(in_process) > 1 and len(lost) > 1):
            if len(in_process) == 1:
                manter = in_process[0]
            else:
                manter = max(in_process, key=lambda e: _latest_activity(e) or datetime.min.replace(tzinfo=BRT))
            remover = [e for e in entries if e["lead_id"] != manter["lead_id"]]
            fase = 2
            reason = f"in_process vs lost ({len(in_process)} ip, {len(lost)} lost)"
        else:
            # Fase 3: ambos mesmo status
            sorted_entries = sorted(entries,
                                    key=lambda e: _latest_activity(e) or datetime.min.replace(tzinfo=BRT),
                                    reverse=True)
            manter = sorted_entries[0]
            remover = sorted_entries[1:]
            fase = 3
            if all(e["status_crm"] == "in_process" for e in entries):
                reason = f"Ambos in_process — manter mais recente"
            elif all(e["status_crm"] == "lost" for e in entries):
                reason = f"Ambos lost — manter mais recente"
            else:
                reason = f"Status misto — manter mais recente"

    if not manter:
        sorted_entries = sorted(entries,
                                key=lambda e: _latest_activity(e) or datetime.min.replace(tzinfo=BRT),
                                reverse=True)
        manter = sorted_entries[0]
        remover = sorted_entries[1:]
        reason = reason or "Fallback — manter mais recente"

    # Regra do telefone: manter o telefone do último contato recebido
    phone_update = None
    best_phone = _best_phone_lead(entries)
    if best_phone and best_phone["lead_id"] != manter["lead_id"]:
        phone_from = best_phone["lead_telefone"]
        if phone_from and phone_from != manter.get("lead_telefone", ""):
            phone_update = phone_from

    return {
        "fase": fase,
        "manter": manter,
        "remover": remover,
        "phone_update": phone_update,
        "reason": reason,
    }


def classify_all(by_rgm):
    """Classifica todos os RGMs em fases."""
    plans = []
    for rgm, entries in sorted(by_rgm.items()):
        plan = decide_merge(rgm, entries)
        plan["rgm"] = rgm
        plan["total_leads"] = len(entries)
        plans.append(plan)
    return plans


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def dry_run_summary(plans, fase_filter=None):
    by_fase = defaultdict(list)
    for p in plans:
        by_fase[p["fase"]].append(p)

    log.info("=" * 60)
    log.info("MERGE DE LEADS — DRY-RUN")
    log.info("=" * 60)

    total_merges = 0
    total_phone = 0

    for fase in sorted(by_fase.keys()):
        items = by_fase[fase]
        if fase_filter and fase != fase_filter:
            continue

        merges = sum(len(p["remover"]) for p in items)
        phones = sum(1 for p in items if p["phone_update"])
        total_merges += merges
        total_phone += phones

        log.info("")
        label = {1: "MANTER + CANDIDATO A MERGE", 2: "in_process vs lost",
                 3: "Ambos mesmo status", 4: "Multi-lead (somente relatório)"}.get(fase, "?")
        log.info("  FASE %d — %s", fase, label)
        log.info("    RGMs: %d", len(items))
        log.info("    Leads a remover: %d", merges)
        log.info("    Telefones a atualizar: %d", phones)

        if fase < 4:
            for p in items[:5]:
                log.info("      ex: RGM %s → manter %s, remover %d lead(s)%s",
                         p["rgm"], p["manter"]["lead_nome"][:30] if p["manter"] else "?",
                         len(p["remover"]),
                         f", tel→{p['phone_update']}" if p["phone_update"] else "")
            if len(items) > 5:
                log.info("      ... e mais %d RGMs", len(items) - 5)
        else:
            log.info("    (Sem ação automática — relatório manual)")
            for p in items[:10]:
                log.info("      RGM %s: %d leads", p["rgm"], p["total_leads"])
            if len(items) > 10:
                log.info("      ... e mais %d RGMs", len(items) - 10)

    api_calls = total_merges * 2 + total_phone
    log.info("")
    log.info("  RESUMO")
    log.info("    Total RGMs processáveis: %d", sum(len(by_fase[f]) for f in [1, 2, 3] if not fase_filter or f == fase_filter))
    log.info("    Total merges (deletar biz duplicado + deletar lead): %d", total_merges)
    log.info("    Total atualizações de telefone: %d", total_phone)
    log.info("    API calls estimadas: ~%d (delete biz + delete lead + patch phone)", api_calls)
    log.info("=" * 60)

    # Relatório detalhado fase 4
    fase4 = by_fase.get(4, [])
    if fase4:
        out = REPORTS_DIR / "merge_multi_lead_manual.csv"
        REPORTS_DIR.mkdir(exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["RGM", "total_leads", "lead_id", "lead_nome", "lead_telefone",
                         "status_crm", "tem_conversa", "ultima_msg", "recomendacao"])
            for p in fase4:
                for e in sorted(by_rgm_entries(p), key=lambda x: x.get("ultima_msg") or datetime.min.replace(tzinfo=BRT), reverse=True):
                    w.writerow([p["rgm"], p["total_leads"], e["lead_id"], e["lead_nome"],
                                e["lead_telefone"], e["status_crm"],
                                "Sim" if e["tem_conversa"] else "Não",
                                e["ultima_msg"].strftime("%d/%m/%Y %H:%M") if e["ultima_msg"] else "",
                                e["recomendacao"]])
        log.info("  Relatório multi-lead: %s", out)


def by_rgm_entries(plan):
    """Recombina manter + remover num único list."""
    result = []
    if plan["manter"]:
        result.append(plan["manter"])
    result.extend(plan["remover"])
    return result


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------

def execute_one(api, plan, csv_writer, conn):
    """Executa merge de um RGM.

    Lógica: o negócio com este RGM já existe no lead mantido,
    então DELETAMOS o negócio duplicado do lead a remover.
    Se o lead ficar sem nenhum negócio, deletamos o lead também.
    """
    rgm = plan["rgm"]
    manter = plan["manter"]
    if not manter:
        return 0, 0

    manter_id = manter["lead_id"]
    manter_nome = manter["lead_nome"]
    ok_count = 0
    err_count = 0

    if plan["phone_update"]:
        log.info("  [RGM %s] PATCH telefone → %s no lead %s",
                 rgm, plan["phone_update"], manter_nome)
        r = api.patch_lead_phone(manter_id, plan["phone_update"])
        status = "OK" if r["ok"] else "ERRO"
        csv_writer.writerow([
            datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
            "PATCH_PHONE", manter_id, manter_nome, rgm,
            plan["phone_update"], "", status,
            r.get("status", ""), plan["reason"],
        ])
        if not r["ok"]:
            log.warning("    ERRO ao atualizar telefone: %s", r.get("body", ""))
        else:
            log.info("    OK — telefone atualizado")

    for rem in plan["remover"]:
        rem_id = rem["lead_id"]
        rem_nome = rem["lead_nome"]

        # 1) Buscar negócios com este RGM no lead a remover
        dup_biz_ids = get_biz_ids_by_rgm_on_lead(conn, rem_id, rgm)

        if not dup_biz_ids:
            log.info("  [RGM %s] Nenhum negócio com este RGM no lead %s — pulando",
                     rgm, rem_nome)
            continue

        log.info("  [RGM %s] %d negócio(s) duplicado(s) no lead %s — deletando",
                 rgm, len(dup_biz_ids), rem_nome)

        # 2) Deletar cada negócio duplicado
        del_failed = False
        for biz_id in dup_biz_ids:
            log.info("    DELETE biz %s (RGM %s, lead %s)", biz_id[:12], rgm, rem_nome)
            r = api.delete_business(biz_id)
            status = "OK" if r["ok"] else "ERRO"
            csv_writer.writerow([
                datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
                "DELETE_BIZ", biz_id, rem_nome, rgm,
                manter_id, manter_nome, status,
                r.get("status", ""), plan["reason"],
            ])
            if r["ok"]:
                log.info("    OK — negócio deletado")
                try:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM businesses WHERE id = %s", (biz_id,))
                    conn.commit()
                except Exception:
                    conn.rollback()
            else:
                log.warning("    ERRO ao deletar negócio: %s", r.get("body", "")[:200])
                err_count += 1
                del_failed = True

        if del_failed:
            log.warning("    Falha ao deletar algum negócio — não tentará deletar lead")
            continue

        # 3) Verificar se o lead ficou completamente vazio
        remaining = get_business_ids_for_lead(conn, rem_id)
        if remaining:
            log.info("    Lead %s ainda tem %d outro(s) negócio(s) — mantendo lead",
                     rem_nome, len(remaining))
            csv_writer.writerow([
                datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
                "KEEP_LEAD", rem_id, rem_nome, rgm,
                "", f"Ainda tem {len(remaining)} negócios", "SKIP",
                "", plan["reason"],
            ])
            ok_count += 1
            continue

        # 4) Lead vazio — deletar
        log.info("    Lead %s sem negócios restantes — deletando", rem_nome)
        r = api.delete_lead(rem_id)
        status = "OK" if r["ok"] else "ERRO"
        csv_writer.writerow([
            datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
            "DELETE_LEAD", rem_id, rem_nome, rgm,
            manter_id, manter_nome, status,
            r.get("status", ""), plan["reason"],
        ])
        if r["ok"]:
            ok_count += 1
            log.info("    OK — lead deletado")
        else:
            log.warning("    ERRO ao deletar lead: %s", r.get("body", ""))
            err_count += 1

    return ok_count, err_count


def execute(api, plans, mode, limit=None):
    """Executa merges."""
    REPORTS_DIR.mkdir(exist_ok=True)
    log_path = REPORTS_DIR / "merge_execucao.csv"

    conn = get_conn()
    try:
        with open(log_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            if f.tell() == 0:
                w.writerow(["timestamp", "acao", "id", "nome", "rgm",
                             "destino_id", "destino_nome", "resultado",
                             "http_status", "razao"])

            if mode == "--test":
                actionable = [p for p in plans if p["fase"] < 4 and p["remover"]]
                if not actionable:
                    log.info("Nenhum merge para testar.")
                    return
                plan = actionable[0]
                log.info("TESTE: RGM %s (fase %d) — %s", plan["rgm"], plan["fase"], plan["reason"])
                log.info("  Manter: %s (%s)", plan["manter"]["lead_nome"], plan["manter"]["lead_id"][:12])
                for r in plan["remover"]:
                    log.info("  Remover: %s (%s)", r["lead_nome"], r["lead_id"][:12])
                ok, err = execute_one(api, plan, w, conn)
                log.info("Resultado teste: %d OK, %d erros. API calls: %d", ok, err, api.total_calls)
                return

            actionable = [p for p in plans if p["fase"] < 4 and p["remover"]]
            if limit:
                actionable = actionable[:limit]

            total_ok = 0
            total_err = 0
            log.info("Executando merge de %d RGMs...", len(actionable))

            for i, plan in enumerate(actionable, 1):
                log.info("[%d/%d] RGM %s (fase %d) — %s",
                         i, len(actionable), plan["rgm"], plan["fase"], plan["reason"])
                ok, err = execute_one(api, plan, w, conn)
                total_ok += ok
                total_err += err

                if i % 20 == 0:
                    log.info("  Progresso: %d/%d | OK: %d | Erros: %d | API calls: %d",
                             i, len(actionable), total_ok, total_err, api.total_calls)
    finally:
        conn.close()

    log.info("=" * 60)
    log.info("CONCLUÍDO: %d OK, %d erros. Total API calls: %d", total_ok, total_err, api.total_calls)
    log.info("Log: %s", log_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    mode = "--dry-run"
    fase_filter = None
    limit = None
    rate_limit = DEFAULT_RATE

    for arg in sys.argv[1:]:
        if arg in ("--test", "--dry-run", "--execute"):
            mode = arg

    for i, arg in enumerate(sys.argv):
        if arg == "--fase" and i + 1 < len(sys.argv):
            fase_filter = int(sys.argv[i + 1])
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
        if arg == "--rate" and i + 1 < len(sys.argv):
            rate_limit = int(sys.argv[i + 1])

    log.info("=" * 50)
    log.info("MERGE DE LEADS DUPLICADOS — %s", mode.upper())
    if fase_filter:
        log.info("Fase: %d", fase_filter)
    if rate_limit != DEFAULT_RATE:
        log.info("Rate-limit: %d req/min", rate_limit)
    log.info("=" * 50)

    by_rgm = load_report()
    if not by_rgm:
        return

    plans = classify_all(by_rgm)

    if fase_filter:
        plans = [p for p in plans if p["fase"] == fase_filter]
        log.info("  %d RGMs na fase %d", len(plans), fase_filter)

    if mode == "--dry-run":
        dry_run_summary(plans, fase_filter)
        return

    api = ApiClient(rate_limit)

    if limit:
        log.info("Limitado a %d RGMs", limit)

    execute(api, plans, mode, limit)


if __name__ == "__main__":
    main()
