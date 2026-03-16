"""
Teste do merge de leads duplicados.

Uso:
  python test_merge.py --dry-run 15815745 20387845
  python test_merge.py --execute 15815745 20387845

O --dry-run busca os dois leads e mostra o preview sem executar o merge.
O --execute faz o merge real.
"""

import sys
import os
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

from dotenv import load_dotenv
load_dotenv()

from kommo_merge import (
    get_session_cookies,
    fetch_lead_full,
    fetch_contact_full,
    build_merge_payload,
    merge_leads,
    merge_lead_pair,
    _extract_cf_values,
    _get_lead_contacts,
)


def preview(keep_id, remove_id):
    """Mostra preview dos dois leads sem executar merge."""
    print(f"\n{'='*60}")
    print(f"PREVIEW: manter={keep_id}, remover={remove_id}")
    print(f"{'='*60}\n")

    keep = fetch_lead_full(keep_id)
    remove = fetch_lead_full(remove_id)

    if not keep:
        print(f"ERRO: Lead {keep_id} não encontrado")
        return False
    if not remove:
        print(f"ERRO: Lead {remove_id} não encontrado")
        return False

    _print_lead(keep, "MANTER")
    _print_lead(remove, "REMOVER")

    keep_cf = _extract_cf_values(keep)
    remove_cf = _extract_cf_values(remove)
    all_ids = sorted(set(keep_cf.keys()) | set(remove_cf.keys()))

    print(f"\n--- Custom Fields Consolidados ---")
    print(f"{'Field ID':<12} {'Manter':<40} {'Remover':<40} {'Resultado'}")
    print("-" * 130)
    for fid in all_ids:
        kv = str(keep_cf.get(fid, ""))[:38]
        rv = str(remove_cf.get(fid, ""))[:38]
        result = kv if kv.strip() else rv
        marker = " ←K" if kv.strip() else " ←R" if rv.strip() else ""
        print(f"{fid:<12} {kv:<40} {rv:<40} {result}{marker}")

    keep_contacts = _get_lead_contacts(keep)
    remove_contacts = _get_lead_contacts(remove)

    print(f"\n--- Contatos ---")
    print(f"  Manter:  {[c['id'] for c in keep_contacts]}")
    print(f"  Remover: {[c['id'] for c in remove_contacts]}")

    payload = build_merge_payload(
        keep, remove,
        keep_contacts=[fetch_contact_full(c["id"]) for c in keep_contacts] if keep_contacts else None,
        remove_contacts=[fetch_contact_full(c["id"]) for c in remove_contacts] if remove_contacts else None,
    )
    print(f"\n--- Payload ({len(payload)} pares) ---")
    for k, v in payload[:30]:
        print(f"  {k} = {str(v)[:60]}")
    if len(payload) > 30:
        print(f"  ... e mais {len(payload) - 30} pares")

    return True


def _print_lead(lead, label):
    print(f"\n--- Lead {label}: {lead['id']} ---")
    print(f"  Nome:      {lead.get('name', '?')}")
    print(f"  Status:    {lead.get('status_id')}")
    print(f"  Pipeline:  {lead.get('pipeline_id')}")
    print(f"  Resp.:     {lead.get('responsible_user_id')}")
    print(f"  Criado:    {lead.get('created_at')}")
    n_cf = len(lead.get("custom_fields_values") or [])
    print(f"  CF fields: {n_cf}")
    contacts = lead.get("_embedded", {}).get("contacts", [])
    print(f"  Contatos:  {[c['id'] for c in contacts]}")


def test_session():
    """Testa conexão com Kommo_chat para obter cookies."""
    print("\n--- Teste de sessão ---")
    print(f"  KOMMO_CHAT_URL: {os.getenv('KOMMO_CHAT_URL')}")
    print(f"  KOMMO_WEB_URL:  {os.getenv('KOMMO_WEB_URL')}")

    cookies = get_session_cookies()
    if cookies:
        print(f"  OK: {len(cookies)} cookies obtidos")
        print(f"  session_id presente: {'session_id' in cookies}")
        print(f"  csrf_token presente: {'csrf_token' in cookies}")
        return True
    else:
        print("  FALHA: não foi possível obter cookies")
        return False


def main():
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        return

    mode = "--dry-run"
    ids = []
    for a in args:
        if a.startswith("--"):
            mode = a
        else:
            ids.append(int(a))

    if mode == "--session":
        test_session()
        return

    if len(ids) < 2:
        print("Informe dois IDs de leads: keep_id remove_id")
        return

    keep_id, remove_id = ids[0], ids[1]

    if mode == "--dry-run":
        preview(keep_id, remove_id)
    elif mode == "--execute":
        if not preview(keep_id, remove_id):
            return
        print(f"\n{'='*60}")
        resp = input("Confirma merge? (s/N): ").strip().lower()
        if resp != "s":
            print("Cancelado.")
            return
        print("Executando merge...")
        result = merge_lead_pair(keep_id, remove_id)
        print(f"\nResultado: {json.dumps(result, indent=2, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
