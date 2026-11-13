"""Livro-razão do motor: por aluno e ciclo, o que recebeu, o que respondeu e quando teve baixa.

`RazaoBase` tem as regras (aplicar carga, montar alunos); as subclasses só
guardam e leem. `RazaoMemoria` serve aos testes e a ensaios sem banco;
`motor.razao_pg.RazaoPg` grava nas tabelas motor_ do dcz_sync.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional

from motor.calendario import ciclo_de, semestre_de
from motor.carga import Linha, RegistroCiclo, ciclo_vigente, evoluir_ciclo
from motor.classificador import ROTULOS_DE_EXPERIENCIA
from motor.config import MotorConfig
from motor.estados import classificar_ciclo, historico_do_semestre
from motor.modelos import Aluno, Controle, Experiencia, Fila, Frente, Lote, LoteAtivo, Mensagem
from motor.seletor import com_estado

Msg = tuple[date, str, str]  # (dia, frente, mensagem)

LOTES_ATIVOS = ("etiquetando", "enviando", "pausado")
LOTES_LIBERADOS = ("simulado", "etiquetando", "enviando", "pausado", "concluido", "cancelado")
CAMPOS_DO_LOTE = frozenset({
    "status", "motivo", "crm_campaign_id", "crm_tag_id", "crm_status", "etiquetados", "enviados", "entregues",
    "lidos", "respondidos", "falhas", "stats_em", "leituras", "leitura_pagina", "tamanho", "liberado_em", "fila",
})
# Status de destinatário no CRM → status da mensagem no livro-razão.
# PENDING só aparece aqui em campanha já concluída: o CRM não chegou a enviar.
STATUS_DO_CRM = {"SENT": "enviado", "DELIVERED": "entregue", "READ": "lido", "FAILED": "falhou", "PENDING": "nao_enviado"}
# Mensagens que não chegaram a sair: não contam no limite do aluno nem no teto do dia.
NAO_SAIRAM = ("cancelado", "nao_enviado")
ERRO_NUMERO_SEM_WHATSAPP = 131026


def primeiro_ciclo_do_semestre(ciclo: str) -> str:
    ano, mes = ciclo.split("-")
    return f"{ano}-{'01' if int(mes) <= 6 else '07'}"


def efeito_da_resposta(rotulo: str) -> Optional[tuple[str, Any]]:
    """O que uma resposta muda no aluno: (campo, valor) ou None."""
    return {
        "ja_paguei": ("declarou_pagamento", True),
        "pediu_parar": ("bloqueio_motor", "recusou_mensagens"),
        "numero_errado": ("bloqueio_motor", "numero_errado"),
        "reclamou": ("suspenso", True),
        "silenciar": ("pediu_silencio", True),
        "continuar": ("pediu_silencio", False),
    }.get(rotulo)


def montar_alunos(
    retratos: Iterable[Mapping[str, Any]],
    ciclos: Mapping[str, Mapping[str, RegistroCiclo]],
    mensagens: Mapping[str, list[Msg]],
    *,
    ciclo: str,
    hoje: date,
    cfg: MotorConfig,
) -> list[Aluno]:
    """Junta retrato, histórico do semestre e mensagens do ciclo em `Aluno` com estado calculado."""
    alunos: list[Aluno] = []
    for r in retratos:
        cid = r["contact_id"]
        regs = ciclos.get(cid, {})
        classes = {
            c: classificar_ciclo(c, reg.baixa_dia10_em, encerrado=c < ciclo, folga_du=cfg.folga_baixa_du, feriados=cfg.feriados)
            for c, reg in regs.items()
            if semestre_de(c) == semestre_de(ciclo)
        }
        vigente = regs.get(ciclo)
        msgs = mensagens.get(cid, [])
        aluno = Aluno(
            contact_id=cid,
            deal_id=r.get("deal_id") or "",
            situacao=r.get("situacao") or "",
            calouro=bool(r.get("calouro")),
            nivel=r.get("nivel") or "",
            nivel_misto=bool(r.get("nivel_misto")),
            suspenso=bool(vigente.suspenso) if vigente else False,
            dia10_aberto=bool(r.get("dia10_aberto")),
            financeira_aberta=bool(r.get("financeira_aberta")),
            tem_telefone=bool(r.get("tem_telefone", True)),
            bloqueio=r.get("bloqueio_motor") or r.get("bloqueio") or None,
            historico=historico_do_semestre(classes, ciclo),
            pediu_silencio=bool(r.get("pediu_silencio")),
            declarou_pagamento_em=vigente.declarou_pagamento_em if vigente else None,
            lembretes_sem_reacao=int(r.get("lembretes_sem_reacao") or 0),
            msgs_ciclo=len(msgs),
            frentes_ciclo=frozenset(Frente(m[1]) for m in msgs),
            mensagens_ciclo=frozenset(Mensagem(m[2]) for m in msgs),
            dias_com_mensagem=frozenset(m[0] for m in msgs),
        )
        alunos.append(com_estado(aluno, ciclo=ciclo, hoje=hoje, cfg=cfg, controle_gravado=vigente.controle if vigente else None))
    return alunos


class RazaoBase:
    # ── primitivas que a subclasse implementa ─────────────────────────────
    def _ciclo_existe(self, ciclo: str) -> bool: raise NotImplementedError
    def _registros_do_ciclo(self, ciclo: str) -> dict[str, RegistroCiclo]: raise NotImplementedError
    def _gravar_carga(self, dia, ciclo, linhas, mudancas, resumo) -> int: raise NotImplementedError
    def _retratos(self) -> list[dict]: raise NotImplementedError
    def _ciclos_desde(self, ciclo_min: str) -> dict[str, dict[str, RegistroCiclo]]: raise NotImplementedError
    def _mensagens(self, ciclo: str, simulado: bool) -> dict[str, list[Msg]]: raise NotImplementedError

    # ── regras comuns ─────────────────────────────────────────────────────
    def aplicar_carga(self, linhas: Mapping[str, Linha], *, dia: date, cfg: MotorConfig) -> dict:
        """Grava a carga completa do dia e evolui o ciclo vigente. Devolve o resumo."""
        ciclo_mes = ciclo_de(dia)
        abertos = sum(1 for l in linhas.values() if l.dia10_aberto)
        ciclo, aberto = ciclo_vigente(ciclo_mes, abertos=abertos, ja_aberto=self._ciclo_existe(ciclo_mes), cfg=cfg)
        registros = self._registros_do_ciclo(ciclo)
        mudancas: dict[str, RegistroCiclo] = {}
        contagem = {"aberto": 0, "baixa": 0, "reaberto": 0}
        for cid, linha in linhas.items():
            novo, mudou = evoluir_ciclo(registros.get(cid), linha, ciclo=ciclo, hoje=dia, cfg=cfg, pode_abrir=aberto)
            if mudou and novo is not None:
                mudancas[cid] = novo
                contagem[mudou] += 1
        resumo = {
            "ciclo": ciclo,
            "ciclo_aberto": aberto,
            "negocios": len(linhas),
            "alunos": len(linhas),
            "abertos_dia10": abertos,
            "abertos_financeira": sum(1 for l in linhas.values() if l.financeira_aberta),
            "novas_baixas": contagem["baixa"],
            "abertos_no_ciclo": contagem["aberto"],
            "reabertos": contagem["reaberto"],
        }
        resumo["carga_id"] = self._gravar_carga(dia, ciclo, linhas, mudancas, resumo)
        return resumo

    def alunos(self, *, ciclo: str, hoje: date, simulado: bool, cfg: MotorConfig) -> list[Aluno]:
        return montar_alunos(
            self._retratos(),
            self._ciclos_desde(primeiro_ciclo_do_semestre(ciclo)),
            self._mensagens(ciclo, simulado),
            ciclo=ciclo,
            hoje=hoje,
            cfg=cfg,
        )


class RazaoMemoria(RazaoBase):
    """Livro-razão em memória (testes e ensaios). Mesmo contrato do RazaoPg."""

    def __init__(self) -> None:
        self.controle = Controle()
        self.ponto: dict = {}
        self.entradas_diario: list[dict] = []
        self.leituras: list[dict] = []
        self.cargas: list[dict] = []
        self.retratos: dict[str, dict] = {}
        self.ciclos: dict[str, dict[str, RegistroCiclo]] = {}
        self.msgs: list[dict] = []
        self.planos: dict[tuple[date, bool], dict] = {}
        self.lotes_db: list[dict] = []
        self.acoes: dict[str, dict] = {}
        self.respostas: list[dict] = []

    # controle, ponto, diário, fila
    def ler_controle(self) -> Controle:
        return self.controle

    def bater_ponto(self, info: dict) -> None:
        self.ponto = dict(info)

    def diario(self, tipo: str, autor: str, texto: str, dados: Optional[dict] = None) -> None:
        self.entradas_diario.append({"tipo": tipo, "autor": autor, "texto": texto, "dados": dados})

    def registrar_fila(self, fila: Optional[Fila], decisao: str, motivo: str, erro: str = "") -> None:
        self.leituras.append({"fila": fila, "decisao": decisao, "motivo": motivo, "erro": erro})

    # carga
    def _ciclo_existe(self, ciclo: str) -> bool:
        return any(ciclo in regs for regs in self.ciclos.values())

    def _registros_do_ciclo(self, ciclo: str) -> dict[str, RegistroCiclo]:
        return {cid: regs[ciclo] for cid, regs in self.ciclos.items() if ciclo in regs}

    def _gravar_carga(self, dia, ciclo, linhas, mudancas, resumo) -> int:
        carga_id = len(self.cargas) + 1
        self.cargas.append({"id": carga_id, "dia": dia, "completa": True, **resumo})
        for cid, l in linhas.items():
            antigo = self.retratos.get(cid, {})
            self.retratos[cid] = {
                **antigo,
                "contact_id": cid, "deal_id": l.deal_id, "situacao": l.situacao, "nivel": l.nivel,
                "nivel_misto": l.nivel_misto, "calouro": l.calouro,
                "dia10_aberto": bool(l.dia10_aberto), "financeira_aberta": bool(l.financeira_aberta),
                "tem_telefone": l.tem_telefone, "bloqueio": l.bloqueio, "carga_id": carga_id,
            }
        for cid, reg in mudancas.items():
            self.ciclos.setdefault(cid, {})[ciclo] = reg
        return carga_id

    def carga_do_dia(self, dia: date) -> Optional[dict]:
        feitas = [c for c in self.cargas if c["dia"] == dia and c["completa"]]
        return feitas[-1] if feitas else None

    def _retratos(self) -> list[dict]:
        if not self.cargas:
            return []
        ultima = self.cargas[-1]["id"]
        return [r for r in self.retratos.values() if r.get("carga_id") == ultima]

    def _ciclos_desde(self, ciclo_min: str) -> dict[str, dict[str, RegistroCiclo]]:
        return {cid: {c: r for c, r in regs.items() if c >= ciclo_min} for cid, regs in self.ciclos.items()}

    def _mensagens(self, ciclo: str, simulado: bool) -> dict[str, list[Msg]]:
        out: dict[str, list[Msg]] = {}
        for m in self.msgs:
            if m["ciclo"] == ciclo and m["simulado"] == simulado and m["status"] not in NAO_SAIRAM:
                out.setdefault(m["contact_id"], []).append((m["dia"], m["frente"], m["mensagem"]))
        return out

    # plano
    def plano(self, dia: date, simulado: bool) -> Optional[dict]:
        return self.planos.get((dia, simulado))

    def salvar_plano(self, *, dia: date, simulado: bool, ciclo: str, piloto: str, resumo: dict, lotes: list[dict]) -> int:
        plano_id = len(self.planos) + 1
        self.planos[(dia, simulado)] = {
            "id": plano_id, "dia": dia, "simulado": simulado, "ciclo": ciclo, "piloto": piloto,
            "status": "proposto", "resumo": resumo, "aprovado_por": None,
        }
        for l in lotes:
            self.adicionar_lote(plano_id, dia=dia, simulado=simulado, ordem=l["ordem"], frente=l["frente"], mensagem=l["mensagem"], tamanho=l["tamanho"])
        return plano_id

    def aprovar_plano(self, dia: date, simulado: bool, usuario: str) -> bool:
        p = self.planos.get((dia, simulado))
        if not p or p["status"] != "proposto":
            return False
        p.update(status="aprovado", aprovado_por=usuario)
        return True

    def encerrar_plano(self, dia: date, simulado: bool) -> None:
        if (dia, simulado) in self.planos:
            self.planos[(dia, simulado)]["status"] = "encerrado"

    # lotes
    def adicionar_lote(self, plano_id: int, *, dia: date, simulado: bool, ordem: int, frente: str, mensagem: str, tamanho: int) -> None:
        self.lotes_db.append({
            "id": len(self.lotes_db) + 1, "plano_id": plano_id, "dia": dia, "simulado": simulado, "ordem": ordem,
            "frente": frente, "mensagem": mensagem, "variante": "A", "tamanho_previsto": tamanho, "tamanho": 0,
            "status": "planejado", "motivo": "", "liberado_em": None, "fila": None,
            "crm_campaign_id": None, "crm_tag_id": None, "crm_status": None, "etiquetados": 0, "enviados": 0,
            "entregues": 0, "lidos": 0, "respondidos": 0, "falhas": 0, "stats_em": None, "leituras": 0, "leitura_pagina": 0,
        })

    def lotes(self, dia: date, simulado: bool) -> list[dict]:
        return sorted((l for l in self.lotes_db if l["dia"] == dia and l["simulado"] == simulado), key=lambda l: l["ordem"])

    def lote(self, lote_id: int) -> dict:
        return next(l for l in self.lotes_db if l["id"] == lote_id)

    def lotes_ativos(self) -> list[dict]:
        return [l for l in self.lotes_db if not l["simulado"] and l["status"] in LOTES_ATIVOS]

    def lotes_para_leitura(self) -> list[dict]:
        return [l for l in self.lotes_db if not l["simulado"] and l["status"] == "concluido" and l["crm_campaign_id"] and l["leituras"] < 2]

    def liberar_lote(self, lote_id: int, lote: Lote, estados: Mapping[str, str], *, ciclo: str, dia: date, simulado: bool, status: str, motivo: str, fila: Optional[dict], agora: datetime) -> int:
        ja = {(m["contact_id"], m["dia"], m["simulado"]) for m in self.msgs}
        if any((cid, dia, simulado) in ja for cid in lote.contact_ids):
            raise ValueError("aluno já tem mensagem neste dia")
        for cid in lote.contact_ids:
            self.msgs.append({
                "contact_id": cid, "ciclo": ciclo, "dia": dia, "simulado": simulado, "variante": lote.variante,
                "frente": lote.frente.value, "mensagem": lote.mensagem.value, "estado": estados.get(cid, ""),
                "lote_id": lote_id, "status": "simulado" if simulado else "reservado", "erro_codigo": None,
            })
        self.lote(lote_id).update(status=status, motivo=motivo, tamanho=lote.tamanho, liberado_em=agora, fila=fila, variante=lote.variante)
        return lote.tamanho

    def marcar_lote(self, lote_id: int, status: str, motivo: str = "") -> None:
        self.lote(lote_id).update(status=status, motivo=motivo)

    def atualizar_lote(self, lote_id: int, **campos) -> None:
        assert set(campos) <= CAMPOS_DO_LOTE, set(campos) - CAMPOS_DO_LOTE
        self.lote(lote_id).update(campos)

    def contatos_do_lote(self, lote_id: int, status: str, limite: int = 1000) -> list[str]:
        return [m["contact_id"] for m in self.msgs if m["lote_id"] == lote_id and m["status"] == status][:limite]

    def marcar_mensagens(self, lote_id: int, contact_ids: Iterable[str], status: str, erro: str = "") -> None:
        alvo = set(contact_ids)
        for m in self.msgs:
            if m["lote_id"] == lote_id and m["contact_id"] in alvo:
                m.update(status=status, erro=erro or None)

    def cancelar_pendentes(self, lote_id: int) -> int:
        n = 0
        for m in self.msgs:
            if m["lote_id"] == lote_id and m["status"] in ("reservado", "etiquetado"):
                m["status"] = "cancelado"
                n += 1
        return n

    def aplicar_destinatarios(self, lote_id: int, itens: Iterable[Mapping[str, Any]]) -> int:
        por_contato = {m["contact_id"]: m for m in self.msgs if m["lote_id"] == lote_id}
        n = 0
        for i in itens:
            m = por_contato.get(i["contact_id"])
            status = STATUS_DO_CRM.get(i["status"])
            if not m or not status:
                continue
            m.update(status=status, erro_codigo=i.get("erro_codigo"), respondeu_em=i.get("respondeu_em"))
            if i.get("erro_codigo") == ERRO_NUMERO_SEM_WHATSAPP:
                self.retratos.setdefault(i["contact_id"], {"contact_id": i["contact_id"]})["bloqueio_motor"] = "erro_131026"
            n += 1
        return n

    def enviados_hoje(self, dia: date, simulado: bool) -> int:
        return sum(1 for m in self.msgs if m["dia"] == dia and m["simulado"] == simulado and m["status"] not in NAO_SAIRAM)

    def lotes_liberados_hoje(self, dia: date, simulado: bool) -> int:
        return sum(1 for l in self.lotes(dia, simulado) if l["liberado_em"])

    def ultimo_lote_liberado(self, dia: date, simulado: bool) -> Optional[LoteAtivo]:
        liberados = [l for l in self.lotes(dia, simulado) if l["liberado_em"]]
        if not liberados:
            return None
        u = max(liberados, key=lambda l: l["liberado_em"])
        return LoteAtivo(lote_id=u["id"], liberado_em=u["liberado_em"], enviados=u["enviados"], falhas=u["falhas"],
                         fila_base=(u.get("fila") or {}).get("aguardando"))

    # ações (idempotência)
    def registrar_acao(self, chave: str, tipo: str, *, lote_id: Optional[int], simulado: bool, requisicao: Optional[dict] = None) -> bool:
        if chave in self.acoes:
            return False
        self.acoes[chave] = {"tipo": tipo, "lote_id": lote_id, "simulado": simulado, "requisicao": requisicao, "status": "simulado" if simulado else "iniciada", "resposta": None}
        return True

    def acao(self, chave: str) -> Optional[dict]:
        return self.acoes.get(chave)

    def concluir_acao(self, chave: str, status: str, resposta: Optional[dict] = None) -> None:
        self.acoes[chave].update(status=status, resposta=resposta)

    # respostas
    def registrar_resposta(self, *, contact_id: str, dia: date, wa_message_id: str = "", texto_pendente: str = "", rotulo: Optional[str] = None, confianca: Optional[float] = None, origem: Optional[str] = None) -> Optional[int]:
        if wa_message_id and any(r["wa_message_id"] == wa_message_id for r in self.respostas):
            return None
        envio = next((m for m in reversed(self.msgs) if m["contact_id"] == contact_id and not m["simulado"]), None)
        self.respostas.append({
            "id": len(self.respostas) + 1, "contact_id": contact_id, "dia": dia, "wa_message_id": wa_message_id,
            "lote_id": envio["lote_id"] if envio else None, "rotulo": rotulo, "confianca": confianca, "origem": origem,
            "revisar": False, "texto_pendente": texto_pendente or None, "texto_mascarado": None,
        })
        return len(self.respostas)

    def respostas_pendentes(self, limite: int = 50) -> list[dict]:
        return [dict(r) for r in self.respostas if r["rotulo"] is None][:limite]

    def gravar_classificacao(self, resposta_id: int, *, rotulo: str, confianca: float, origem: str, revisar: bool, guardar_texto: bool, dias: int = 30) -> None:
        r = next(x for x in self.respostas if x["id"] == resposta_id)
        r.update(rotulo=rotulo, confianca=confianca, origem=origem, revisar=revisar,
                 texto_mascarado=r["texto_pendente"] if guardar_texto else None, texto_pendente=None)

    def aplicar_efeito(self, contact_id: str, rotulo: str, dia: date) -> None:
        efeito = efeito_da_resposta(rotulo)
        if not efeito:
            return
        campo, valor = efeito
        regs = self.ciclos.get(contact_id, {})
        vigente = max(regs) if regs else None
        if campo == "declarou_pagamento" and vigente:
            regs[vigente] = replace(regs[vigente], declarou_pagamento_em=dia)
        elif campo == "suspenso" and vigente:
            regs[vigente] = replace(regs[vigente], suspenso=True)
        elif campo in ("bloqueio_motor", "pediu_silencio") and contact_id in self.retratos:
            self.retratos[contact_id][campo] = valor

    def experiencia_do_dia(self, dia: date, simulado: bool) -> Experiencia:
        casos = {r["contact_id"] for r in self.respostas if r["dia"] == dia and r["rotulo"] in ROTULOS_DE_EXPERIENCIA}
        lotes = [l for l in self.lotes(dia, simulado) if l["liberado_em"]]
        entregues = sum(l["entregues"] for l in lotes) or sum(l["tamanho"] for l in lotes)
        return Experiencia(casos=len(casos), entregues=entregues)

    def limpar_textos_vencidos(self) -> int:
        return 0
