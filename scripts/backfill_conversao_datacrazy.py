"""Backfill de respostas da aba Conversao a partir do DataCrazy (fonte de verdade).

Contexto: o webhook do n8n grava so uma fracao das respostas (e as vezes com a
categoria errada). Este script cruza os destinatarios de um disparo com as
conversas do DataCrazy e insere em `activation_responses` os respondentes reais
que o webhook nao capturou, para o painel de Conversao refletir a taxa real.

IMPORTANTE: o DataCrazy /conversations so expoe o *ultimo* inbound
(lastReceivedMessageDate). Portanto rode este backfill LOGO APOS o disparo
(idealmente no mesmo dia). Retroativo em dias antigos e' impreciso, pois o
ultimo inbound da pessoa ja foi sobrescrito por mensagens mais novas.

Uso:
  python scripts/backfill_conversao_datacrazy.py                 # dry-run, rematricula, hoje
  python scripts/backfill_conversao_datacrazy.py --apply
  python scripts/backfill_conversao_datacrazy.py --category=financeiro --since=2026-07-20 --apply

Reversao: DELETE FROM activation_responses WHERE origem_ativacao='datacrazy_backfill';
"""
import os, sys
from datetime import datetime, timezone, timedelta
from collections import Counter
import requests, psycopg2
from dotenv import load_dotenv

load_dotenv()

APPLY = "--apply" in sys.argv
CATEGORY = "rematricula"
SINCE = datetime.now(timezone.utc).date().isoformat()
WINDOW_H = 72
for a in sys.argv:
    if a.startswith("--category="):
        CATEGORY = a.split("=", 1)[1]
    elif a.startswith("--since="):
        SINCE = a.split("=", 1)[1]
    elif a.startswith("--window="):
        WINDOW_H = int(a.split("=", 1)[1])

TOKEN = os.environ["DATACRAZY_API_TOKEN"]
BASE = "https://api.g1.datacrazy.io/api/v1"
H = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}


def parse(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def main():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"], port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
        dbname="disparos", connect_timeout=20)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        SELECT datacrazy_lead_id, min(master_key), min(rgm), min(telefone),
               min(created_at), min(nome)
        FROM activation_dispatch_events
        WHERE category=%s AND created_at >= %s
          AND datacrazy_lead_id IS NOT NULL AND datacrazy_lead_id<>''
        GROUP BY datacrazy_lead_id
    """, [CATEGORY, SINCE])
    disp = {}
    for ext, mk, rgm, tel, dt, nome in cur.fetchall():
        disp[ext] = {"mk": mk, "rgm": rgm, "tel": tel, "dt": dt, "nome": nome}
    print(f"[{CATEGORY}] destinatarios (>= {SINCE}) unicos: {len(disp)}")
    if not disp:
        print("Nenhum disparo no periodo. Nada a fazer."); return

    cutoff = datetime.fromisoformat(SINCE + "T00:00:00+00:00") - timedelta(days=1)
    conv = {}
    offset = 0; pages = 0
    while True:
        r = requests.get(f"{BASE}/conversations",
            params={"sort": "-lastReceivedMessageDate", "limit": 1000, "offset": offset},
            headers=H, timeout=60)
        if r.status_code != 200:
            print("HTTP", r.status_code, r.text[:200]); break
        data = r.json().get("data", [])
        if not data:
            break
        pages += 1; oldest = None
        for c in data:
            lr = parse(c.get("lastReceivedMessageDate"))
            if lr and (oldest is None or lr < oldest):
                oldest = lr
            ct = c.get("contact") or {}
            ext = ct.get("externalId")
            if ext and lr and (ext not in conv or lr > conv[ext][0]):
                conv[ext] = (lr, c.get("id"), ct.get("contactId") or ct.get("phoneNumber"), c.get("name"))
        if (oldest and oldest < cutoff) or len(data) < 1000 or pages >= 40:
            break
        offset += 1000
    print(f"conversas DataCrazy lidas (paginas={pages}) com externalId: {len(conv)}")

    cands = []
    for ext, d in disp.items():
        if ext not in conv or not d["mk"]:
            continue
        lr, convId, contactId, cname = conv[ext]
        if lr < d["dt"] or lr > d["dt"] + timedelta(hours=WINDOW_H):
            continue
        dia = lr.astimezone(timezone.utc).date()
        cur.execute("""SELECT 1 FROM activation_responses
            WHERE category=%s AND master_key=%s
              AND (received_at at time zone 'UTC')::date=%s LIMIT 1""", [CATEGORY, d["mk"], dia])
        if cur.fetchone():
            continue
        cands.append({"ext": ext, "mk": d["mk"], "rgm": d["rgm"],
                      "tel": contactId or d["tel"], "convId": convId,
                      "lr": lr, "nome": cname or d["nome"], "dia": dia})

    print(f"A INSERIR (novos, apos dedup): {len(cands)}")
    for dia, n in sorted(Counter(str(c['dia']) for c in cands).items()):
        print(f"  {dia}: {n}")

    if not APPLY:
        print("\n(DRY-RUN) nada inserido. Use --apply para gravar.")
        return

    n = 0
    for c in cands:
        cur.execute("""
            INSERT INTO activation_responses
              (category, master_key, datacrazy_lead_id, telefone, rgm,
               response_kind, message_text, external_id, received_at, origem_ativacao, nome_lead)
            VALUES (%s, %s, %s, %s, %s, 'message',
                    'backfill DataCrazy (resposta real nao capturada pelo webhook)',
                    %s, %s, 'datacrazy_backfill', %s)
            ON CONFLICT (external_id, category, ((received_at at time zone 'UTC')::date))
              WHERE external_id IS NOT NULL DO NOTHING
        """, [CATEGORY, c["mk"], c["ext"], c["tel"], c["rgm"],
              "dcbf_" + str(c["convId"]), c["lr"], c["nome"]])
        n += cur.rowcount
    conn.commit()
    print(f"INSERIDAS: {n} respostas (origem_ativacao='datacrazy_backfill').")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
