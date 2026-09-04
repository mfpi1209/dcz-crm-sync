"""Correcao automatica e permanente da aba Conversao (banco `disparos`).

Problema (ver AGENTS.md 2026-07-20): o webhook do n8n grava as respostas dos
disparos (a) sem `master_key`, (b) com categoria fixa `processos-caa` e (c)
so captura ~5% dos inbounds. Consequencia: rematricula (e outras) aparecem com
0 respondidos no painel de Conversao.

Enquanto o n8n nao e' corrigido na raiz, este job mantem o painel FIEL sozinho,
rodando numa janela movel (default 2 dias), de forma idempotente:

  Fase 1 — corrige o que o webhook capturou:
     - master_key := 'RGM:'||rgm quando NULL
     - category   := categoria do disparo mais recente da pessoa em 72h
                     (match por master_key efetiva OU datacrazy_lead_id)
     - remove duplicata quando a re-etiquetagem colide com o unique
       (external_id, category, dia)

  Fase 2 — backfill das respostas REAIS via DataCrazy (o que o webhook perdeu):
     - cruza destinatarios do disparo x conversas (contact.externalId =
       datacrazy_lead_id); responder = lastReceivedMessageDate dentro de
       [disparo, disparo+72h]
     - insere so quem ainda nao existe (dedup por master_key+category+dia),
       marcado origem_ativacao='datacrazy_backfill' (reversivel)

Ambas as fases sao seguras: aditivas/idempotentes e restritas a janela recente.
Reverter backfill: DELETE FROM activation_responses WHERE origem_ativacao='datacrazy_backfill';

Toggle: CONVERSAO_BACKFILL_ENABLED=0 desliga. CONVERSAO_BACKFILL_INTERVAL_MIN
controla o intervalo (default 20). CONVERSAO_BACKFILL_WINDOW_DAYS a janela (2).
"""
import os
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_meta = {"last_run_at": None, "last_ok": None, "last_error": None,
         "last_fixed": 0, "last_deleted": 0, "last_backfilled": 0}

_DATACRAZY_BASE = "https://api.g1.datacrazy.io/api/v1"
_WINDOW_H = 72  # janela de correlacao resposta<->disparo (mesma do painel)


def _disparos_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
        dbname=os.environ.get("DISPAROS_DB_NAME", "disparos"), connect_timeout=20)


def _parse_dc(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


# ── Fase 1 — corrige respostas ja capturadas ────────────────────────────────

def _fase1_corrige_capturadas(cur, since_iso):
    cur.execute("""
    WITH resp AS (
        SELECT id, received_at, category AS cat_atual, master_key AS mk_atual,
               external_id,
               COALESCE(NULLIF(master_key,''),
                        CASE WHEN NULLIF(rgm,'') IS NOT NULL THEN 'RGM:'||rgm END) AS mk_eff,
               NULLIF(datacrazy_lead_id,'') AS dc0
        FROM activation_responses
        WHERE received_at >= %s
    )
    SELECT r.id, r.external_id, (r.received_at at time zone 'UTC')::date, r.cat_atual,
           r.mk_atual, r.mk_eff,
       (SELECT d.category FROM activation_dispatch_events d
          WHERE ((r.mk_eff IS NOT NULL AND d.master_key = r.mk_eff)
              OR (r.dc0 IS NOT NULL AND d.datacrazy_lead_id = r.dc0))
            AND d.created_at <= r.received_at
            AND d.created_at >= r.received_at - interval '72 hours'
          ORDER BY d.created_at DESC LIMIT 1) AS cat_correta
    FROM resp r
    """, [since_iso])

    to_update, to_delete = [], []
    for _id, ext, dia, cat_old, mk_old, mk_eff, cat_correta in cur.fetchall():
        new_cat = cat_correta if cat_correta else cat_old
        new_mk = mk_eff if mk_eff else mk_old
        muda_cat = new_cat != cat_old
        muda_mk = (mk_old is None or mk_old == "") and bool(new_mk)
        if not (muda_cat or muda_mk):
            continue
        if muda_cat and ext:
            cur.execute("""SELECT 1 FROM activation_responses
                WHERE external_id=%s AND category=%s
                  AND (received_at at time zone 'UTC')::date=%s AND id<>%s LIMIT 1""",
                [ext, new_cat, dia, _id])
            if cur.fetchone():
                to_delete.append(_id)
                continue
        to_update.append((_id, new_cat, new_mk))

    for _id, new_cat, new_mk in to_update:
        cur.execute("UPDATE activation_responses SET category=%s, master_key=%s WHERE id=%s",
                    [new_cat, new_mk, _id])
    for _id in to_delete:
        cur.execute("DELETE FROM activation_responses WHERE id=%s", [_id])
    return len(to_update), len(to_delete)


# ── Fase 2 — backfill via DataCrazy ─────────────────────────────────────────

def _load_conversas_datacrazy(token, cutoff):
    """Retorna {externalId: (lastReceived, convId, contactId, name)} para conversas
    com inbound desde `cutoff`."""
    H = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    conv = {}
    offset = 0
    pages = 0
    while True:
        r = requests.get(f"{_DATACRAZY_BASE}/conversations",
                         params={"sort": "-lastReceivedMessageDate", "limit": 1000, "offset": offset},
                         headers=H, timeout=60)
        if r.status_code != 200:
            logger.warning("[conv_backfill] DataCrazy HTTP %s: %s", r.status_code, r.text[:160])
            break
        data = r.json().get("data", [])
        if not data:
            break
        pages += 1
        oldest = None
        for c in data:
            lr = _parse_dc(c.get("lastReceivedMessageDate"))
            if lr and (oldest is None or lr < oldest):
                oldest = lr
            ct = c.get("contact") or {}
            ext = ct.get("externalId")
            if ext and lr and (ext not in conv or lr > conv[ext][0]):
                conv[ext] = (lr, c.get("id"), ct.get("contactId") or ct.get("phoneNumber"), c.get("name"))
        if (oldest and oldest < cutoff) or len(data) < 1000 or pages >= 40:
            break
        offset += 1000
    return conv


def _fase2_backfill_datacrazy(cur, token, since_iso, cutoff):
    # dispatches recentes por datacrazy_lead_id (mantem todos p/ achar o +recente por resposta)
    cur.execute("""
        SELECT datacrazy_lead_id, category, master_key, rgm, telefone, created_at, nome
        FROM activation_dispatch_events
        WHERE created_at >= %s
          AND datacrazy_lead_id IS NOT NULL AND datacrazy_lead_id<>''
          AND master_key IS NOT NULL
        ORDER BY created_at DESC
    """, [since_iso])
    disp_by_ext = {}
    for ext, cat, mk, rgm, tel, dt, nome in cur.fetchall():
        disp_by_ext.setdefault(ext, []).append((cat, mk, rgm, tel, dt, nome))
    if not disp_by_ext:
        return 0

    conv = _load_conversas_datacrazy(token, cutoff)
    if not conv:
        return 0

    inserted = 0
    for ext, lst in disp_by_ext.items():
        if ext not in conv:
            continue
        lr, convId, contactId, cname = conv[ext]
        # disparo mais recente da pessoa dentro de [lr-72h, lr]
        best = None
        for cat, mk, rgm, tel, dt, nome in lst:
            if dt <= lr <= dt + timedelta(hours=_WINDOW_H):
                if best is None or dt > best[4]:
                    best = (cat, mk, rgm, tel, dt, nome)
        if not best:
            continue
        cat, mk, rgm, tel, dt, nome = best
        dia = lr.astimezone(timezone.utc).date()
        cur.execute("""SELECT 1 FROM activation_responses
            WHERE category=%s AND master_key=%s
              AND (received_at at time zone 'UTC')::date=%s LIMIT 1""", [cat, mk, dia])
        if cur.fetchone():
            continue
        cur.execute("""
            INSERT INTO activation_responses
              (category, master_key, datacrazy_lead_id, telefone, rgm,
               response_kind, message_text, external_id, received_at, origem_ativacao, nome_lead)
            VALUES (%s, %s, %s, %s, %s, 'message',
                    'backfill DataCrazy (resposta real nao capturada pelo webhook)',
                    %s, %s, 'datacrazy_backfill', %s)
            ON CONFLICT (external_id, category, ((received_at at time zone 'UTC')::date))
              WHERE external_id IS NOT NULL DO NOTHING
        """, [cat, mk, ext, (contactId or tel), rgm,
              "dcbf_" + str(convId), lr, (cname or nome)])
        inserted += cur.rowcount
    return inserted


# ── Orquestracao ────────────────────────────────────────────────────────────

def run_conversao_backfill(window_days=None):
    """Roda Fase 1 + Fase 2 numa janela movel. Idempotente. Seguro para cron."""
    if os.environ.get("CONVERSAO_BACKFILL_ENABLED", "1") not in ("1", "true", "True"):
        return
    if not _lock.acquire(blocking=False):
        logger.info("[conv_backfill] ja em execucao, pulando")
        return
    token = os.environ.get("DATACRAZY_API_TOKEN", "").strip()
    win = int(window_days or os.environ.get("CONVERSAO_BACKFILL_WINDOW_DAYS", "2"))
    since_dt = datetime.now(timezone.utc) - timedelta(days=win)
    since_iso = since_dt.date().isoformat()
    cutoff = datetime.fromisoformat(since_iso + "T00:00:00+00:00") - timedelta(days=1)
    conn = None
    try:
        conn = _disparos_conn()
        conn.autocommit = False
        cur = conn.cursor()
        n_up, n_del = _fase1_corrige_capturadas(cur, since_iso)
        n_bf = 0
        if token:
            n_bf = _fase2_backfill_datacrazy(cur, token, since_iso, cutoff)
        else:
            logger.warning("[conv_backfill] DATACRAZY_API_TOKEN ausente — pulando Fase 2")
        conn.commit()
        cur.close()
        _meta.update({"last_run_at": datetime.now(timezone.utc).isoformat(),
                      "last_ok": True, "last_error": None,
                      "last_fixed": n_up, "last_deleted": n_del, "last_backfilled": n_bf})
        if n_up or n_del or n_bf:
            logger.info("[conv_backfill] fase1: %d corrigidas, %d removidas | fase2: %d backfill",
                        n_up, n_del, n_bf)
        return _meta
    except Exception as e:  # noqa: BLE001
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        _meta.update({"last_run_at": datetime.now(timezone.utc).isoformat(),
                      "last_ok": False, "last_error": str(e)})
        logger.warning("[conv_backfill] falha: %s", e)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        _lock.release()


def register_conversao_backfill_job(sched):
    """Cron: mantem a aba Conversao fiel automaticamente.

    Roda a cada CONVERSAO_BACKFILL_INTERVAL_MIN (default 20) minutos + um disparo
    no boot (~40s) para corrigir o estado atual assim que a app sobe."""
    if os.environ.get("CONVERSAO_BACKFILL_ENABLED", "1") not in ("1", "true", "True"):
        logger.info("[conv_backfill] desabilitado por env (CONVERSAO_BACKFILL_ENABLED)")
        return
    from apscheduler.triggers.interval import IntervalTrigger
    interval = int(os.environ.get("CONVERSAO_BACKFILL_INTERVAL_MIN", "20"))
    sched.add_job(
        run_conversao_backfill,
        IntervalTrigger(minutes=interval, timezone="America/Sao_Paulo"),
        id="conversao_backfill",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    sched.add_job(
        run_conversao_backfill,
        "date",
        run_date=datetime.now(timezone.utc) + timedelta(seconds=40),
        id="conversao_backfill_boot",
        replace_existing=True,
    )
    logger.info("[conv_backfill] job registrado (intervalo=%dmin)", interval)
