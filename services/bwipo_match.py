"""Match + execução do Upload Comercial no CRM Bwipo.

Regras = as mesmas de match_merge_lib (gerar_acoes / executar_acoes).
Só troca a fonte: índice live do Bwipo no lugar de kommo_sync + API v4.
"""
from __future__ import annotations

import logging
import re
import unicodedata
import urllib.parse
from typing import Any, Optional

from services import bwipo_crm as crm

log = logging.getLogger("match_merge")

_OWNER_DIR: Optional[dict[str, str]] = None


def _norm_person(name: str) -> str:
    raw = unicodedata.normalize("NFKD", name or "")
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    return " ".join(raw.lower().split())


def _kommo_responsible_name(cpf: str) -> str:
    """Nome do responsável do lead no funil Kommo (mesmo CPF). Vazio se não achar."""
    digits = re.sub(r"[^0-9]", "", cpf or "")
    if len(digits) != 11 or digits in ("00000000000", "00000000009"):
        return ""
    try:
        from match_merge_lib import get_kommo_conn
        conn = get_kommo_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(u.name, '')
            FROM leads l
            JOIN lead_custom_field_values lcf ON lcf.lead_id = l.id
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE l.pipeline_id = 5481944
              AND NOT COALESCE(l.is_deleted, FALSE)
              AND lcf.field_name = 'CPF'
              AND regexp_replace(lcf.values_json->0->>'value', '[^0-9]', '', 'g') = %s
            ORDER BY CASE WHEN l.status_id IN (142, 143) THEN 1 ELSE 0 END, l.id DESC
            LIMIT 1
            """,
            (digits,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return (row[0] or "").strip() if row else ""
    except Exception as exc:
        log.warning("Responsável Kommo não resolvido para CPF %s: %s", digits, exc)
        return ""


def _owner_directory() -> dict[str, str]:
    """Nome normalizado → user id no Bwipo, a partir de negócios recentes (GET /api/users é 401)."""
    global _OWNER_DIR
    if _OWNER_DIR is not None:
        return _OWNER_DIR
    found: dict[str, str] = {}

    def _absorb(items: list) -> None:
        for deal in items:
            owner = deal.get("owner") or {}
            oid = owner.get("id") or deal.get("ownerId")
            name = owner.get("name") or ""
            key = _norm_person(name)
            if oid and key and key not in found:
                found[key] = oid

    first = crm._request("GET", "/api/deals?perPage=100&page=1")
    _absorb(first.get("items") or [])
    total = int(first.get("total") or 0)
    last_page = max(1, (total + 99) // 100)
    for page in range(max(2, last_page - 3), last_page + 1):
        payload = crm._request("GET", f"/api/deals?perPage=100&page={page}")
        _absorb(payload.get("items") or [])
    _OWNER_DIR = found
    return found


def _bwipo_owner_id_for_kommo_name(name: str) -> str:
    """Casa o nome do Kommo com um usuário do Bwipo. Sem match → Admin Sistema."""
    key = _norm_person(name)
    if not key:
        return crm.ADMIN_SISTEMA_ID
    directory = _owner_directory()
    if key in directory:
        return directory[key]
    first = key.split(" ", 1)[0]
    hits = [oid for n, oid in directory.items() if n.split(" ", 1)[0] == first]
    uniq = list(dict.fromkeys(hits))
    if len(uniq) == 1:
        return uniq[0]
    quoted = urllib.parse.quote(name)
    try:
        payload = crm._request("GET", f"/api/conversations?search={quoted}&perPage=20")
    except Exception as exc:
        log.info("Busca de responsável '%s' no Bwipo falhou (%s); NOVO fica com Admin Sistema", name, exc)
        return crm.ADMIN_SISTEMA_ID
    conv_hits = []
    for conv in payload.get("items") or []:
        assigned = conv.get("assignedTo") or {}
        if _norm_person(assigned.get("name") or "") == key and assigned.get("id"):
            conv_hits.append(assigned["id"])
    conv_uniq = list(dict.fromkeys(conv_hits))
    if len(conv_uniq) == 1:
        directory[key] = conv_uniq[0]
        return conv_uniq[0]
    log.info("Responsável Kommo '%s' sem usuário único no Bwipo; NOVO fica com Admin Sistema", name)
    return crm.ADMIN_SISTEMA_ID


def _novo_owner_id(acao: dict) -> str:
    """NOVO no Bwipo herda o responsável do lead dessa pessoa no Kommo."""
    name = _kommo_responsible_name(acao.get("cpf") or "")
    return _bwipo_owner_id_for_kommo_name(name)

_KOMMO_TO_BWIPO_UPDATE = {
    "Situação": "situacao",
    "Curso_SIAA": "curso_de_inscricao",
    "Curso": "curso_de_inscricao",
    "Modalidade_SIAA": "modalidade_curso1",
    "Modalidade": "modalidade_curso1",
    "Grau_SIAA": "grau_new",
    "Grau": "grau_new",
    "Polo": "polo",
    "CPF": "cpf",
    "Telefone Inscricao": "telefone_da_inscricao",
    "E-mail": "e_mail",
    "Email Acadêmico": "e_mail_academico",
    "Preço_SIAA": "valor_curso1",
    "Duração_SIAA": "duracao",
    "Nro. da Inscrição": "nro_da_inscricao",
    "Nome": "nome_completo",
    "CEP": "cep",
    "RG": "rg",
    "Origem": "origem_do_lead",
    "RGM": "rgm",
    "Matrícula": "data_de_matricula",
}


def _pick_best(index: dict, ids) -> Optional[dict]:
    deals = [index["deals"][i] for i in ids if i in index["deals"]]
    if not deals:
        return None
    deals.sort(
        key=lambda d: (
            0 if d["status_id"] not in (crm.SYNTHETIC_GANHO, crm.SYNTHETIC_PERDIDO) else 1,
            -(d.get("number") or 0),
        )
    )
    return deals[0]


def match_inscritos() -> dict:
    from match_merge_lib import (
        _MM_INSCRITOS_COLS_FOR_MATCH,
        get_conn,
    )

    dcz = get_conn()
    cur = dcz.cursor()
    cols_sql = ", ".join(_MM_INSCRITOS_COLS_FOR_MATCH)
    cur.execute(f"""
        SELECT {cols_sql} FROM mm_inscritos
        WHERE COALESCE(situacao_final, '') NOT IN ('Reprovado', '0',
              'Pré-Matriculado Financeiro', '')
    """)
    rows = cur.fetchall()
    cur.close()
    dcz.close()

    col_i = {c: i for i, c in enumerate(_MM_INSCRITOS_COLS_FOR_MATCH)}
    index = crm.get_lead_index(refresh=True)

    detalhes = []
    tipos: dict[str, int] = {}
    com_match = 0
    n_fechado = 0
    divergencias = {
        "atualizar_matriculado": 0, "atualizar_aprovado": 0,
        "ok": 0, "sem_situacao_kommo": 0, "lead_fechado": 0,
    }

    for row in rows:
        cpf = row[col_i["cpf"]] or ""
        tel = row[col_i["telefone"]] or ""
        tel_d = re.sub(r"[^0-9]", "", tel)

        match_tipo = None
        deal = None
        if cpf:
            deal = _pick_best(index, index["idx_cpf"].get(cpf, set()))
            if deal:
                match_tipo = "cpf"
        if deal is None and len(tel_d) >= 10:
            cand = set()
            cand |= index["idx_tel"].get(tel_d[-11:], set())
            cand |= index["idx_tel"].get(tel_d[-10:], set())
            deal = _pick_best(index, cand)
            if deal:
                match_tipo = "telefone"

        ganho_lead_id = None
        if cpf:
            for lid in index["idx_cpf"].get(cpf, set()):
                d = index["deals"].get(lid)
                if d and d["status_id"] == crm.SYNTHETIC_GANHO:
                    ganho_lead_id = lid
                    break

        cpf_ids = list(index["idx_cpf"].get(cpf, set())) if cpf else []
        tel_ids = []
        if len(tel_d) >= 10:
            tel_ids = list(
                index["idx_tel"].get(tel_d[-11:], set())
                | index["idx_tel"].get(tel_d[-10:], set())
            )
        dup_count = max(len(cpf_ids), len(tel_ids))

        tem_match = deal is not None
        lead_fechado = bool(deal and deal["lead_fechado"])
        siaa_sit = row[col_i["situacao_final"]]
        kommo_sit = deal["situacao"] if deal else None

        if tem_match:
            com_match += 1
            tipos[match_tipo] = tipos.get(match_tipo, 0) + 1
            if lead_fechado:
                n_fechado += 1
                divergencias["lead_fechado"] += 1
            elif siaa_sit == "Matriculado" and kommo_sit != "Matriculado":
                divergencias["atualizar_matriculado"] += 1
            elif siaa_sit == "Aprovado" and kommo_sit not in ("Aprovado", "Matriculado"):
                divergencias["atualizar_aprovado"] += 1
            elif kommo_sit is None:
                divergencias["sem_situacao_kommo"] += 1
            else:
                divergencias["ok"] += 1

        detalhes.append({
            "siaa_id": row[col_i["id"]],
            "nome": row[col_i["nome"]],
            "cpf": cpf,
            "telefone": tel,
            "inscricao": row[col_i["inscricao"]],
            "curso_raw": row[col_i["curso_raw"]],
            "curso_limpo": row[col_i["curso_limpo"]],
            "siaa_situacao": siaa_sit,
            "polo_normalizado": row[col_i["polo_normalizado"]],
            "email": row[col_i["email"]],
            "data_inscr": row[col_i["data_inscr"]],
            "marca_instituicao": row[col_i["marca_instituicao"]],
            "modalidade": row[col_i["modalidade"]],
            "grau_curso": row[col_i["grau_curso"]],
            "chave_preco": row[col_i["chave_preco"]],
            "preco_balcao": row[col_i["preco_balcao"]],
            "semestres": row[col_i["semestres"]],
            "trimestre_ingresso": row[col_i["trimestre_ingresso"]],
            "cep": row[col_i["cep"]],
            "rg": row[col_i["rg"]],
            "lead_id_match": deal["id"] if deal else None,
            "match_tipo": match_tipo,
            "situacao_kommo": kommo_sit,
            "lead_name": deal["name"] if deal else None,
            "lead_status_id": deal["status_id"] if deal else None,
            "lead_fase": deal["stage_name"] if deal else None,
            "lead_pipeline_id": deal["pipeline_id"] if deal else None,
            "tem_match": tem_match,
            "lead_fechado": lead_fechado,
            "ganho_lead_id": ganho_lead_id,
            "lead_closed_date": deal["closed_at"] if deal else None,
            "dup_lead_ids": cpf_ids,
            "dup_tel_lead_ids": tel_ids,
            "dup_count": dup_count,
        })

    total = len(detalhes)
    log.info("Match Inscritos x Bwipo: total=%d, match=%d, sem=%d, fechado=%d",
             total, com_match, total - com_match, n_fechado)
    return {
        "total": total, "com_match": com_match, "sem_match": total - com_match,
        "lead_fechado": n_fechado, "tipos": tipos,
        "divergencias": divergencias, "detalhes": detalhes,
    }


def match_matriculados() -> dict:
    from match_merge_lib import _MM_MATRICULADOS_COLS_FOR_MATCH, get_conn

    dcz = get_conn()
    cur = dcz.cursor()
    cols_sql = ", ".join(_MM_MATRICULADOS_COLS_FOR_MATCH)
    cur.execute(f"""
        SELECT {cols_sql} FROM mm_matriculados
        WHERE rgm IS NOT NULL AND rgm != ''
          AND UPPER(COALESCE(situacao,'')) = 'MATRICULADO'
    """)
    rows = cur.fetchall()
    cur.close()
    dcz.close()

    col_i = {c: i for i, c in enumerate(_MM_MATRICULADOS_COLS_FOR_MATCH)}
    index = crm.get_lead_index()
    detalhes = []
    tipos: dict[str, int] = {}
    com_match = 0

    for row in rows:
        rgm = (row[col_i["rgm"]] or "").strip()
        cand = {
            lid for lid in index["idx_rgm"].get(rgm.lower(), set())
            if index["deals"].get(lid, {}).get("status_id")
            not in (crm.SYNTHETIC_GANHO, crm.SYNTHETIC_PERDIDO)
        }
        deal = _pick_best(index, cand)
        tem = deal is not None
        if tem:
            com_match += 1
            tipos["rgm"] = tipos.get("rgm", 0) + 1
        detalhes.append({
            "mat_id": row[col_i["id"]],
            "nome": row[col_i["nome"]],
            "cpf": row[col_i["cpf"]],
            "rgm": rgm,
            "curso_raw": row[col_i["curso_raw"]],
            "curso_limpo": row[col_i["curso_limpo"]],
            "mat_situacao": row[col_i["situacao"]],
            "polo_aulas": row[col_i["polo_aulas"]],
            "data_matricula": row[col_i["data_matricula"]],
            "tipo_matricula": row[col_i["tipo_matricula"]],
            "lead_id_match": deal["id"] if deal else None,
            "match_tipo": "rgm" if tem else None,
            "situacao_kommo": deal["situacao"] if deal else None,
            "lead_name": deal["name"] if deal else None,
            "status_id": deal["status_id"] if deal else None,
            "lead_pipeline_id": deal["pipeline_id"] if deal else None,
            "tem_match": tem,
        })

    total = len(detalhes)
    log.info("Match Matriculados x Bwipo (RGM): total=%d, match=%d, sem=%d",
             total, com_match, total - com_match)
    return {
        "total": total, "com_match": com_match, "sem_match": total - com_match,
        "tipos": tipos, "detalhes": detalhes,
    }


def load_d1_indexes() -> dict:
    index = crm.get_lead_index()
    return {
        "idx_cpf": index["idx_cpf"],
        "idx_email": index["idx_email"],
        "idx_tel": index["idx_tel"],
        "idx_rgm": index["idx_rgm"],
        "lid_cpf": index["lid_cpf"],
        "lead_pipe": index["lead_pipe"],
        "lead_deleted": index["lead_deleted"],
        "deals": index["deals"],
    }


def aceite_presos(acoes_lead_ids: set) -> list[dict]:
    """Aceite que não é de hoje não pode ficar parado.

    Só olha quem está em Aceite. Matrícula ativa de outro dia → Ganho.
    Só Cancelado/Trancado/Transferido → Perdido. Fechamento de hoje fica.
    Janela de 14 dias e os mesmos polos/tipos do D-1, para não puxar
    veterano antigo que só conversou de novo.
    """
    from datetime import date, timedelta

    from match_merge_lib import _cpf_valido, get_conn

    hoje = date.today()
    desde = hoje - timedelta(days=14)
    index = crm.get_lead_index()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT nome, regexp_replace(cpf,'[^0-9]','','g'),
               regexp_replace(rgm,'[^0-9]','','g'),
               email, email_ad, fone_cel, curso_limpo, polo_aulas,
               situacao, data_matricula::text, tipo_matricula
        FROM mm_matriculados
        WHERE data_matricula::date >= %s
          AND data_matricula::date < %s
          AND UPPER(COALESCE(tipo_matricula, '')) IN (
              'NOVA MATRICULA', 'RECOMPRA', 'RETORNO'
          )
          AND polo_captador IN (
              '18 - UNICID - GRADUAÇÃO EAD',
              '16 - CRUZEIRO DO SUL - GRADUAÇÃO EAD',
              '41 - CRUZEIRO DO SUL - PÓS-EAD'
          )
        """,
        (desde.isoformat(), hoje.isoformat()),
    )
    by_cpf, by_rgm, by_phone = {}, {}, {}

    def _tel(v):
        d = re.sub(r"[^0-9]", "", str(v or ""))
        if d.startswith("55") and len(d) > 11:
            d = d[2:]
        return d[-11:] if len(d) >= 10 else ""

    for nome, cpf, rgm, email, email_ad, fone, curso, polo, sit, data, tipo in cur.fetchall():
        item = {
            "nome": nome, "cpf": cpf or "", "rgm": rgm or "",
            "email": (email or "").strip().lower(),
            "email_ad": (email_ad or "").strip().lower(),
            "telefone": fone or "",
            "curso": curso or "", "polo": polo or "",
            "sit": (sit or "").strip(),
            "data": (data or "")[:10],
            "tipo": tipo or "",
        }
        if _cpf_valido(item["cpf"]):
            by_cpf.setdefault(item["cpf"], []).append(item)
        if item["rgm"]:
            by_rgm.setdefault(item["rgm"], []).append(item)
        tel = _tel(fone)
        if tel:
            by_phone.setdefault(tel, []).append(item)
    cur.close()
    conn.close()

    def _unico_phone(tel):
        rows = by_phone.get(tel) or []
        cpfs = {r["cpf"] for r in rows if r["cpf"]}
        if len(cpfs) > 1:
            return []
        return rows

    acoes = []
    for lid, deal in index["deals"].items():
        if deal.get("status_id") != crm.SYNTHETIC_ACEITE:
            continue
        if lid in acoes_lead_ids:
            continue
        cpf = deal.get("cpf") or ""
        rgm = (deal.get("rgm") or "").strip()
        tel = _tel(deal.get("phone"))
        rows = []
        if rgm and rgm in by_rgm:
            rows = by_rgm[rgm]
        elif _cpf_valido(cpf):
            rows = by_cpf.get(cpf) or []
        elif tel:
            rows = _unico_phone(tel)
        if not rows:
            continue
        ativos = [r for r in rows if r["sit"].upper() == "MATRICULADO"]
        ruins = [
            r for r in rows
            if any(x in r["sit"].upper() for x in ("CANCEL", "TRANC", "TRANSFER"))
        ]
        if ativos:
            row = max(ativos, key=lambda r: r["data"])
            acao = "MATRICULADO"
            match = "aceite_preso_ganho"
        elif ruins:
            row = max(ruins, key=lambda r: r["data"])
            acao = "MOVER_PERDIDO"
            match = "aceite_preso_perdido"
        else:
            continue
        acoes.append({
            "acao": acao,
            "lead_id": lid,
            "nome": row["nome"] or deal.get("name") or "",
            "cpf": row["cpf"] or cpf,
            "rgm": row["rgm"] or rgm,
            "curso_siaa": row["curso"],
            "polo": row["polo"],
            "situacao_siaa": row["sit"] or "Matriculado",
            "situacao_kommo": deal.get("situacao") or "",
            "match_tipo": match,
            "data_matricula": row["data"],
            "tipo_matricula": row["tipo"],
            "email_ad": row["email_ad"],
            "email_pessoal": row["email"],
            "telefone": row["telefone"],
            "lead_fase": deal.get("stage_name") or "Aceite",
        })
        acoes_lead_ids.add(lid)
    return acoes


def rgm_indexes() -> tuple[dict, dict]:
    index = crm.get_lead_index()
    lead_rgm_idx = {}
    rgm_to_ganho_lids: dict[str, set] = {}
    for lid, d in index["deals"].items():
        rgm = (d.get("rgm") or "").strip()
        if not rgm:
            continue
        lead_rgm_idx[lid] = rgm
        if d["status_id"] == crm.SYNTHETIC_GANHO:
            rgm_to_ganho_lids.setdefault(rgm.lower(), set()).add(lid)
    return lead_rgm_idx, rgm_to_ganho_lids


def verificar_novos(acoes: list[dict]) -> list[dict]:
    """Equivalente a _verificar_novos_kommo_api: CPF já no CRM → não cria NOVO."""
    novos = [a for a in acoes if a.get("acao") == "NOVO" and a.get("cpf")]
    if not novos:
        return acoes
    index = crm.get_lead_index()
    blocked = set()
    for a in novos:
        cpf = a.get("cpf") or ""
        for lid in index["idx_cpf"].get(cpf, set()):
            d = index["deals"].get(lid)
            if not d:
                continue
            usable = (
                d["pipeline_id"] == crm.SYNTHETIC_PIPE_VENDAS
                and d["status_id"] != crm.SYNTHETIC_PERDIDO
            )
            if usable or not a.get("novo_matriculado"):
                if usable or d["status_id"] != crm.SYNTHETIC_PERDIDO:
                    blocked.add(id(a))
                    log.warning(
                        "NOVO bloqueado (duplicata Bwipo): CPF %s já existe #%s",
                        cpf, d.get("number"),
                    )
                    break
    if not blocked:
        return acoes
    out = []
    for a in acoes:
        if a.get("acao") == "NOVO" and id(a) in blocked:
            continue
        out.append(a)
    return out


def _acao_to_bwipo_fields(acao: dict, field_map: dict, *, include_origem: bool = False) -> dict:
    out = {}
    for kommo_name, acao_key in field_map.items():
        bwipo_name = _KOMMO_TO_BWIPO_UPDATE.get(kommo_name)
        if not bwipo_name:
            continue
        if bwipo_name == "origem_do_lead" and not include_origem:
            continue
        val = acao.get(acao_key)
        if val in (None, ""):
            continue
        # um campo Bwipo só: Curso_SIAA e Curso não podem se sobrescrever
        if bwipo_name in out and kommo_name == "Curso":
            continue
        out[bwipo_name] = val
    return out


def _stage_for_novo(acao: dict) -> str:
    if acao.get("novo_matriculado"):
        return crm.STAGES["ganho"]
    sit = (acao.get("situacao_siaa") or "").strip().lower()
    if sit == "indefinido":
        return crm.STAGES["processo_seletivo"]
    return crm.STAGES["aprovado_reprovado"]


def executar_acoes_bwipo(acoes, limit=None, log_callback=None):
    to_process = acoes[:limit] if limit else acoes
    results = {"ok": 0, "erro": 0, "skip": 0, "novo_ok": 0,
               "perdido_ok": 0, "restaurar_ok": 0}
    # Não reindexar a base inteira aqui: o loop usa GET/PUT/search por pessoa.
    # O refresh=True travava ~8 min sem OK/erro na tela.

    update_fields_map = {
        "Situação": "situacao_siaa",
        "Curso_SIAA": "curso_siaa",
        "Curso": "curso_generico",
        "Modalidade_SIAA": "modalidade",
        "Grau_SIAA": "grau",
        "Polo": "polo",
        "CPF": "cpf",
        "Telefone Inscricao": "telefone",
        "E-mail": "email_pessoal",
        "Modalidade": "modalidade",
        "Grau": "grau",
        "Preço_SIAA": "preco_siaa",
        "Duração_SIAA": "duracao_siaa",
        "Nro. da Inscrição": "nro_inscricao",
        "Nome": "nome",
        "CEP": "cep",
        "RG": "rg",
    }
    mat_map = {
        "Situação": "situacao_siaa",
        "RGM": "rgm",
        "Matrícula": "data_matricula",
        "Polo": "polo",
        "Curso_SIAA": "curso_siaa",
        "Curso": "curso_siaa",
        "E-mail": "email_pessoal",
        "Email Acadêmico": "email_ad",
        "Nro. da Inscrição": "inscricao",
        "Nome": "nome",
        "Modalidade": "modalidade",
        "Grau": "grau",
        "CPF": "cpf",
        "Telefone Inscricao": "telefone",
    }

    def _log(msg):
        log.info(msg)
        if log_callback:
            log_callback(msg)

    log.info("Iniciando %d ações no Novo CRM", len(to_process))

    for i, acao in enumerate(to_process):
        tipo = acao["acao"]
        lead_id = acao.get("lead_id")
        try:
            if tipo == "ATUALIZAR" and lead_id:
                fields = _acao_to_bwipo_fields(acao, update_fields_map)
                if fields:
                    crm.put_custom_fields(lead_id, fields)
                if not acao.get("somente_dados") and acao.get("situacao_siaa") == "Aprovado":
                    crm.move_deal(lead_id, crm.STAGES["aprovado_reprovado"])
                if not fields and acao.get("somente_dados"):
                    results["skip"] += 1
                    continue
                results["ok"] += 1
                _log(f"[{i+1}/{len(to_process)}] OK ATUALIZAR lead={lead_id} {acao.get('nome','')}")

            elif tipo == "MATRICULADO" and lead_id:
                deal = crm.get_deal(lead_id)
                ja_ganho = (deal or {}).get("stageId") == crm.STAGES["ganho"]
                fields = _acao_to_bwipo_fields(acao, mat_map)
                if "situacao" not in fields:
                    fields["situacao"] = acao.get("situacao_siaa") or "Matriculado"
                crm.put_custom_fields(lead_id, fields)
                if not ja_ganho:
                    crm.move_deal(lead_id, crm.STAGES["ganho"])
                results["ok"] += 1
                _log(f"[{i+1}/{len(to_process)}] OK MATRICULADO lead={lead_id} {acao.get('nome','')}")

            elif tipo == "MOVER_PERDIDO" and lead_id:
                crm.move_deal(
                    lead_id,
                    crm.STAGES["perdido"],
                    lost_reason=crm.LOSS_REASON_DUPLICADO,
                )
                crm.put_custom_fields(lead_id, {
                    "motivo_da_perda": crm.LOSS_REASON_DUPLICADO,
                })
                results["perdido_ok"] += 1
                results["ok"] += 1
                _log(f"[{i+1}/{len(to_process)}] OK MOVER_PERDIDO lead={lead_id} {acao.get('nome','')}")

            elif tipo == "RESTAURAR" and lead_id:
                fields = _acao_to_bwipo_fields(acao, update_fields_map)
                if fields:
                    crm.put_custom_fields(lead_id, fields)
                crm.move_deal(lead_id, crm.STAGES["aprovado_reprovado"])
                results["restaurar_ok"] += 1
                results["ok"] += 1
                _log(f"[{i+1}/{len(to_process)}] OK RESTAURAR lead={lead_id} {acao.get('nome','')}")

            elif tipo == "NOVO":
                cpf_novo = acao.get("cpf") or ""
                if len(cpf_novo) == 11:
                    found = crm.search_deals(query=cpf_novo, per_page=5)
                    items = found.get("items") or []
                    existing = None
                    for d in items:
                        if str(d.get("number")) and d.get("id"):
                            existing = d
                            break
                    if existing:
                        st_id = existing.get("stageId")
                        pipe_ok = ((existing.get("stage") or {}).get("pipeline") or {}).get("id") == crm.PIPELINE_PRINCIPAL_ID
                        not_lost = st_id != crm.STAGES["perdido"]
                        usable = pipe_ok and not_lost
                        if usable or not acao.get("novo_matriculado"):
                            results["skip"] += 1
                            _log(
                                f"[{i+1}/{len(to_process)}] SKIP NOVO {acao.get('nome','')} "
                                f"— CPF já existe no Bwipo (#{existing.get('number')})"
                            )
                            continue
                nome = acao.get("nome") or "Lead SIAA"
                contact = crm.create_contact(
                    name=nome,
                    email=acao.get("email_pessoal") or acao.get("email") or "",
                    phone=acao.get("telefone") or "",
                )
                cid = (contact or {}).get("id")
                if not cid:
                    raise RuntimeError("POST /api/contacts não devolveu id")
                deal = crm.create_deal(
                    title=f"Negócio {nome}",
                    contact_id=cid,
                    stage_id=_stage_for_novo(acao),
                    owner_id=_novo_owner_id(acao),
                )
                did = (deal or {}).get("id")
                if not did:
                    raise RuntimeError("POST /api/deals não devolveu id")
                novo_map = {**update_fields_map, "Origem": "origem"}
                if acao.get("novo_matriculado"):
                    novo_map.update({
                        "RGM": "rgm",
                        "Matrícula": "data_matricula",
                        "Email Acadêmico": "email_ad",
                    })
                    if not acao.get("origem"):
                        acao["origem"] = "SIAA"
                else:
                    acao.setdefault("origem", "SIAA")
                fields = _acao_to_bwipo_fields(acao, novo_map, include_origem=True)
                if fields:
                    crm.put_custom_fields(did, fields)
                results["novo_ok"] += 1
                results["ok"] += 1
                _log(f"[{i+1}/{len(to_process)}] OK NOVO lead={did} {nome}")

            elif tipo == "UNIFICAR":
                results["skip"] += 1
                continue
            else:
                results["skip"] += 1
                continue
        except Exception as exc:
            results["erro"] += 1
            _log(f"[{i+1}/{len(to_process)}] ERRO {tipo} lead={lead_id or 'NOVO'}: {exc}")

    crm.clear_lead_index()
    log.info(
        "Execucao Bwipo: ok=%d, erro=%d, skip=%d, novos=%d, perdido=%d, restaurar=%d",
        results["ok"], results["erro"], results["skip"],
        results["novo_ok"], results["perdido_ok"], results["restaurar_ok"],
    )
    return results
