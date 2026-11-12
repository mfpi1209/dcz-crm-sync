"""Livro-razão do motor: por aluno e ciclo, o que recebeu e quando teve baixa.

`RazaoBase` tem as regras (aplicar carga, montar alunos); as subclasses só
guardam e leem. `RazaoMemoria` serve aos testes e a ensaios sem banco;
`motor.razao_pg.RazaoPg` grava nas tabelas motor_ do dcz_sync.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional

from motor.calendario import ciclo_de, semestre_de
from motor.carga import Linha, RegistroCiclo, ciclo_vigente, evoluir_ciclo
from motor.config import MotorConfig
from motor.estados import classificar_ciclo, historico_do_semestre
from motor.modelos import Aluno, Controle, Fila, Frente, Lote, LoteAtivo, Mensagem
from motor.seletor import com_estado

Msg = tuple[date, str, str]  # (dia, frente, mensagem)


def primeiro_ciclo_do_semestre(ciclo: str) -> str:
    ano, mes = ciclo.split("-")
    return f"{ano}-{'01' if int(mes) <= 6 else '07'}"


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
            dia10_aberto=bool(r.get("dia10_aberto")),
            financeira_aberta=bool(r.get("financeira_aberta")),
            tem_telefone=bool(r.get("tem_telefone", True)),
            bloqueio=r.get("bloqueio") or None,
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
                "calouro": l.calouro,
                "dia10_aberto": bool(l.dia10_aberto), "financeira_aberta": bool(l.financeira_aberta),
                "tem_telefone": l.tem_telefone, "bloqueio": l.bloqueio,
                "carga_id": carga_id,
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
            if m["ciclo"] == ciclo and m["simulado"] == simulado:
                out.setdefault(m["contact_id"], []).append((m["dia"], m["frente"], m["mensagem"]))
        return out

    # plano e lotes
    def plano(self, dia: date, simulado: bool) -> Optional[dict]:
        return self.planos.get((dia, simulado))

    def salvar_plano(self, *, dia: date, simulado: bool, ciclo: str, piloto: str, resumo: dict, lotes: list[dict]) -> int:
        plano_id = len(self.planos) + 1
        self.planos[(dia, simulado)] = {"id": plano_id, "dia": dia, "simulado": simulado, "ciclo": ciclo, "piloto": piloto, "status": "proposto", "resumo": resumo}
        for l in lotes:
            self.lotes_db.append({
                "id": len(self.lotes_db) + 1, "plano_id": plano_id, "dia": dia, "simulado": simulado,
                "ordem": l["ordem"], "frente": l["frente"], "mensagem": l["mensagem"],
                "tamanho_previsto": l["tamanho"], "tamanho": 0, "status": "planejado", "motivo": "", "liberado_em": None,
            })
        return plano_id

    def adicionar_lote(self, plano_id: int, *, dia: date, simulado: bool, ordem: int, frente: str, mensagem: str, tamanho: int) -> None:
        self.lotes_db.append({
            "id": len(self.lotes_db) + 1, "plano_id": plano_id, "dia": dia, "simulado": simulado, "ordem": ordem,
            "frente": frente, "mensagem": mensagem, "tamanho_previsto": tamanho, "tamanho": 0,
            "status": "planejado", "motivo": "", "liberado_em": None,
        })

    def lotes(self, dia: date, simulado: bool) -> list[dict]:
        return sorted((l for l in self.lotes_db if l["dia"] == dia and l["simulado"] == simulado), key=lambda l: l["ordem"])

    def liberar_lote(self, lote_id: int, lote: Lote, estados: Mapping[str, str], *, ciclo: str, dia: date, simulado: bool, status: str, motivo: str, fila: Optional[dict], agora: datetime) -> int:
        ja = {(m["contact_id"], m["dia"], m["simulado"]) for m in self.msgs}
        if any((cid, dia, simulado) in ja for cid in lote.contact_ids):
            raise ValueError("aluno já tem mensagem neste dia")
        for cid in lote.contact_ids:
            self.msgs.append({
                "contact_id": cid, "ciclo": ciclo, "dia": dia, "simulado": simulado,
                "frente": lote.frente.value, "mensagem": lote.mensagem.value,
                "estado": estados.get(cid, ""), "lote_id": lote_id, "status": "simulado" if simulado else "enviado",
            })
        self._lote(lote_id).update(status=status, motivo=motivo, tamanho=lote.tamanho, liberado_em=agora, fila=fila)
        return lote.tamanho

    def marcar_lote(self, lote_id: int, status: str, motivo: str = "") -> None:
        self._lote(lote_id).update(status=status, motivo=motivo)

    def _lote(self, lote_id: int) -> dict:
        return next(l for l in self.lotes_db if l["id"] == lote_id)

    def enviados_hoje(self, dia: date, simulado: bool) -> int:
        return sum(1 for m in self.msgs if m["dia"] == dia and m["simulado"] == simulado)

    def ultimo_lote_liberado(self, dia: date, simulado: bool) -> Optional[LoteAtivo]:
        liberados = [l for l in self.lotes(dia, simulado) if l["liberado_em"]]
        if not liberados:
            return None
        u = max(liberados, key=lambda l: l["liberado_em"])
        return LoteAtivo(lote_id=u["id"], liberado_em=u["liberado_em"], enviados=0, falhas=0)

    def registrar_acao(self, chave: str, tipo: str, *, lote_id: Optional[int], simulado: bool, requisicao: Optional[dict] = None) -> bool:
        if chave in self.acoes:
            return False
        self.acoes[chave] = {"tipo": tipo, "lote_id": lote_id, "simulado": simulado, "requisicao": requisicao}
        return True
