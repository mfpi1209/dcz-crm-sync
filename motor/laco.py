"""Laço do motor: o que acontece a cada 1 a 2 minutos.

  1. bate o ponto e lê as chaves da página (ligado, parada geral)
  2. lê a fila de atendimento e roda a fórmula do governador
  3. faz a carga do dia (base de negócios do CRM) e a refaz durante a janela
  4. monta o plano do dia às 8h30
  5. dentro da janela, pede a ordem ao piloto e libera o próximo lote

Nesta versão o motor só opera em simulação: monta os lotes, registra o que
faria e não chama nenhum endpoint que escreve no CRM. Com MOTOR_DRY_RUN=0 ele
avisa no diário e segue em simulação (o envio real depende da F0, ver AGENTS.md).
"""
from __future__ import annotations

import logging
import os
import re
import socket
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from typing import Callable, Optional

from motor import VERSAO
from motor.agenda import acoes_do_dia
from motor.calendario import agora_brt, ciclo_de, eh_dia_util, para_brt
from motor.carga import consolidar, montar_mapa, normalizar, tags_de_bloqueio
from motor.config import MotorConfig
from motor.governador import decidir, dentro_da_janela, tamanho_do_lote
from motor.loteador import lotear, lotes_que_cabem_na_janela
from motor.modelos import Decisao, Fila
from motor.piloto import LotePendente, Piloto, PilotoFormula, Retrato
from motor.seletor import contar_por_estado, selecionar
from motor.trilhos import modo_efetivo, validar_lote

logger = logging.getLogger(__name__)

RETENTATIVA_CARGA = timedelta(minutes=15)
RECARGA_NA_JANELA = timedelta(minutes=60)


class Laco:
    def __init__(
        self,
        cfg: MotorConfig,
        razao,
        crm=None,
        piloto: Optional[Piloto] = None,
        relogio: Callable[[], datetime] = agora_brt,
    ):
        self.cfg = cfg
        self.razao = razao
        self.crm = crm
        self.piloto = piloto or PilotoFormula()
        self.relogio = relogio
        self.iniciado_em = relogio()
        self._avisos: set[str] = set()
        self._ultimo_modo = ""
        self._ultima_decisao = ""
        self._carga_em: Optional[datetime] = None
        self._carga_falhou_em: Optional[datetime] = None
        self._ultimo_erro = ""

    # ── utilidades ────────────────────────────────────────────────────────
    def _uma_vez(self, chave: str, tipo: str, autor: str, texto: str, dados: Optional[dict] = None) -> None:
        """Entrada no diário que não se repete a cada tick."""
        if chave in self._avisos:
            return
        self._avisos.add(chave)
        self.razao.diario(tipo, autor, texto, dados)

    def _ponto(self, modo: str) -> None:
        self.razao.bater_ponto({
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "versao": VERSAO,
            "iniciado_em": self.iniciado_em,
            "modo": modo,
            "enabled": self.cfg.enabled,
            "dry_run": self.cfg.dry_run,
            "ultimo_erro": self._ultimo_erro or None,
            "config": self.cfg.publico(),
        })

    # ── um tick ───────────────────────────────────────────────────────────
    def tick(self) -> str:
        agora = para_brt(self.relogio())
        hoje = agora.date()
        controle = self.razao.ler_controle()
        modo = modo_efetivo(self.cfg, controle)
        if modo == "real":
            self._uma_vez(
                "real-indisponivel", "controle", "trilhos",
                "MOTOR_DRY_RUN=0, mas o envio real ainda não existe nesta versão. O motor segue em simulação.",
            )
            modo = "simulacao"
        self._ponto(modo)
        if modo != self._ultimo_modo:
            self.razao.diario("controle", "worker", f"Motor em modo: {modo}.", {"por": controle.atualizado_por, "motivo": controle.parada_motivo})
            self._ultimo_modo = modo
        if modo in ("parado", "desligado"):
            return modo

        fila, decisao, motivo = self._governar(agora)
        self._carga(agora)
        self._plano(agora)
        self._rodada(agora, fila, decisao, motivo, controle)
        return modo

    # ── fila e governador ─────────────────────────────────────────────────
    def _perto_da_janela(self, agora: datetime) -> bool:
        if not eh_dia_util(agora.date(), self.cfg.feriados):
            return False
        return self.cfg.janela_inicio_h - 1 <= agora.hour <= self.cfg.janela_fim_h

    def _fila_de_ensaio(self, agora: datetime) -> Optional[Fila]:
        """Fila hipotética "aguardando,atendentes" para ensaiar o dia sem a fila real.

        Só vale em simulação (`MOTOR_DRY_RUN=1`) e só quando a leitura real
        falha. Fica marcada como ensaio em cada leitura gravada.
        """
        bruto = (os.getenv("MOTOR_SIM_FILA") or "").strip()
        if not bruto or not self.cfg.dry_run:
            return None
        try:
            aguardando, online = (int(x) for x in bruto.split(",")[:2])
        except ValueError:
            return None
        return Fila(aguardando=max(0, aguardando), maior_espera_s=0, atendentes_online=max(0, online), lida_em=agora)

    def _governar(self, agora: datetime) -> tuple[Optional[Fila], Decisao, str]:
        fila: Optional[Fila] = None
        erro = ""
        ler = self._perto_da_janela(agora)
        if ler and self.crm is not None:
            try:
                fila = replace(self.crm.fila_agora(), lida_em=agora)
            except Exception as e:  # leitura da fila nunca derruba o laço
                erro = f"{type(e).__name__}: {e}"[:300]
        elif ler:
            erro = "CRM não configurado (EDUIT_CRM_TOKEN)"
        if ler and fila is None:
            fila = self._fila_de_ensaio(agora)
            if fila is not None:
                erro = f"fila de ensaio (MOTOR_SIM_FILA), não é leitura real. {erro}".strip()
        ativo = self.razao.ultimo_lote_liberado(agora.date(), True)
        decisao, motivo = decidir(fila, self.cfg, agora=agora, lote_ativo=ativo)
        if ler:
            self.razao.registrar_fila(fila, decisao.value, motivo, erro)
            if erro:
                self._uma_vez(f"fila-erro:{agora.date()}:{erro[:60]}", "erro", "worker", f"Não consegui ler a fila: {erro}")
        return fila, decisao, motivo

    # ── carga ─────────────────────────────────────────────────────────────
    def _carga(self, agora: datetime) -> None:
        hoje = agora.date()
        if self.crm is None or agora.hour < self.cfg.carga_hora:
            return
        if self._carga_falhou_em and agora - self._carga_falhou_em < RETENTATIVA_CARGA:
            return
        feita = self.razao.carga_do_dia(hoje)
        if feita:
            pendentes = [l for l in self.razao.lotes(hoje, True) if l["status"] == "planejado"]
            recente = self._carga_em is not None and agora - self._carga_em < RECARGA_NA_JANELA
            if recente or not pendentes or not dentro_da_janela(agora, self.cfg):
                return
        try:
            mapa = montar_mapa(self.crm.campos_do_negocio())
            bloqueios = tags_de_bloqueio()
            pipeline = (os.getenv("MOTOR_PIPELINE_ID") or "").strip()
            linhas = (normalizar(d, mapa, bloqueios) for d in self.crm.negocios(pipeline_id=pipeline))
            por_contato = consolidar(l for l in linhas if l is not None)
            if not por_contato:
                raise RuntimeError("a base de negócios veio vazia")
            resumo = self.razao.aplicar_carga(por_contato, dia=hoje, cfg=self.cfg)
        except Exception as e:
            self._carga_falhou_em = agora
            self._ultimo_erro = f"carga: {type(e).__name__}: {e}"[:300]
            self.razao.diario("erro", "worker", f"Carga falhou e nada foi alterado: {self._ultimo_erro}")
            return
        self._carga_em = agora
        self._carga_falhou_em = None
        self._ultimo_erro = ""
        self.razao.diario(
            "carga", "worker",
            f"Carga do ciclo {resumo['ciclo']}: {resumo['alunos']} alunos, {resumo['abertos_dia10']} com Dia 10 em aberto, "
            f"{resumo['novas_baixas']} baixas novas.",
            resumo,
        )

    # ── plano do dia ──────────────────────────────────────────────────────
    def _plano(self, agora: datetime) -> None:
        hoje = agora.date()
        if (agora.hour, agora.minute) < (self.cfg.plano_hora, self.cfg.plano_minuto):
            return
        acoes = acoes_do_dia(hoje, self.cfg.feriados)
        if not acoes or self.razao.plano(hoje, True):
            return
        carga = self.razao.carga_do_dia(hoje)
        if not carga:
            self._uma_vez(f"plano-sem-carga:{hoje}", "plano", "trilhos", "Plano do dia aguardando a carga de hoje.")
            return
        if not carga["ciclo_aberto"] or carga["ciclo"] != ciclo_de(hoje):
            self._uma_vez(
                f"plano-sem-ciclo:{hoje}", "plano", "trilhos",
                f"A carga do ciclo {ciclo_de(hoje)} ainda não chegou ({carga['abertos_dia10']} com Dia 10 em aberto, "
                f"mínimo {self.cfg.carga_min_abertos}). Sem plano hoje.",
            )
            return
        ciclo = carga["ciclo"]
        alunos = self.razao.alunos(ciclo=ciclo, hoje=hoje, simulado=True, cfg=self.cfg)
        selecoes = [selecionar(alunos, a, hoje=hoje, cfg=self.cfg) for a in acoes]
        plano = lotear(
            selecoes, dia=hoje, cfg=self.cfg, tamanho=self.cfg.lote_max,
            enviados_hoje=self.razao.enviados_hoje(hoje, True),
            max_lotes=lotes_que_cabem_na_janela(self.cfg),
        )
        resumo = {
            "por_estado": contar_por_estado(alunos),
            "controle": sum(1 for a in alunos if a.controle),
            "acoes": [
                {
                    "frente": s.acao.frente.value, "mensagem": s.acao.mensagem.value,
                    "elegiveis": len(s.elegiveis), "exclusoes": s.exclusoes, "fora_do_alvo": s.fora_do_alvo,
                }
                for s in selecoes
            ],
            "lotes": len(plano.lotes), "total": plano.total, "adiados": plano.adiados,
            "teto_diario": self.cfg.teto_diario, "lote_max": self.cfg.lote_max,
        }
        self.razao.salvar_plano(
            dia=hoje, simulado=True, ciclo=ciclo, piloto=self.piloto.nome, resumo=resumo,
            lotes=[{"ordem": l.ordem, "frente": l.frente.value, "mensagem": l.mensagem.value, "tamanho": l.tamanho} for l in plano.lotes],
        )
        self.razao.diario(
            "plano", self.piloto.nome,
            f"Plano simulado de {hoje:%d/%m}: {plano.total} alunos em {len(plano.lotes)} lotes"
            + (f"; {plano.adiados} ficam para o próximo dia de envio" if plano.adiados else "") + ".",
            resumo,
        )

    # ── rodada do piloto ──────────────────────────────────────────────────
    def _rodada(self, agora: datetime, fila: Optional[Fila], decisao: Decisao, motivo: str, controle) -> None:
        hoje = agora.date()
        plano = self.razao.plano(hoje, True)
        if not plano:
            return
        lotes = self.razao.lotes(hoje, True)
        pendentes = [l for l in lotes if l["status"] == "planejado"]
        if not pendentes:
            return
        if not dentro_da_janela(agora, self.cfg):
            if agora.hour >= self.cfg.janela_fim_h:
                for l in pendentes:
                    self.razao.marcar_lote(l["id"], "adiado", "fim da janela de envio")
                self.razao.diario("lote", "trilhos", f"Fim da janela: {len(pendentes)} lotes adiados para o próximo dia de envio.")
            return

        ciclo = plano["ciclo"]
        enviados = self.razao.enviados_hoje(hoje, True)
        retrato = Retrato(
            agora=agora, fila=fila, decisao_governador=decisao, motivo_governador=motivo,
            tamanho_sugerido=tamanho_do_lote(fila, self.cfg),
            pendentes=tuple(LotePendente(l["ordem"], l["frente"], l["mensagem"], l["tamanho_previsto"]) for l in pendentes),
            enviados_hoje=enviados, teto_diario=self.cfg.teto_diario, lote_max=self.cfg.lote_max,
            por_estado=dict((plano.get("resumo") or {}).get("por_estado") or {}),
        )
        ordem = self.piloto.proxima_ordem(retrato)
        # Chave sem números: "faltam 25 min", "faltam 20 min"... são a mesma decisão no diário.
        chave_decisao = f"{ordem.acao}|{re.sub(r'[0-9]+', '#', ordem.motivo)}"
        if ordem.acao != "liberar":
            if chave_decisao != self._ultima_decisao:
                self.razao.diario("fila", self.piloto.nome, f"Lotes segurados: {ordem.motivo}.")
                self._ultima_decisao = chave_decisao
            return
        self._ultima_decisao = chave_decisao
        # O piloto pode segurar quando o governador libera, nunca o contrário.
        if decisao != Decisao.LIBERAR:
            self.razao.diario("lote", "trilhos", f"Ordem de liberar recusada: governador em {decisao.value} ({motivo}).")
            return
        alvo = next((l for l in pendentes if l["ordem"] == ordem.ordem_lote), None)
        if alvo is None:
            self.razao.diario("lote", "trilhos", f"Ordem recusada: lote {ordem.ordem_lote} não está pendente.")
            return
        acao = next((a for a in acoes_do_dia(hoje, self.cfg.feriados) if a.mensagem.value == alvo["mensagem"]), None)
        if acao is None:
            self.razao.marcar_lote(alvo["id"], "recusado", "mensagem fora do calendário de hoje")
            return

        # Lista recalculada a cada lote: já sem quem teve baixa ou recebeu algo hoje.
        alunos = self.razao.alunos(ciclo=ciclo, hoje=hoje, simulado=True, cfg=self.cfg)
        tamanho = max(1, min(ordem.tamanho, self.cfg.lote_max, tamanho_do_lote(fila, self.cfg)))
        selecao = selecionar(alunos, acao, hoje=hoje, cfg=self.cfg)
        montado = lotear([selecao], dia=hoje, cfg=self.cfg, tamanho=tamanho, enviados_hoje=enviados, primeira_ordem=alvo["ordem"], max_lotes=1)
        if not montado.lotes:
            self.razao.marcar_lote(alvo["id"], "concluido", "sem elegíveis no recálculo")
            return
        lote = montado.lotes[0]
        if lote.tamanho < min(self.cfg.lote_min, len(selecao.elegiveis)):
            if chave_decisao + "|pequeno" != self._ultima_decisao:
                self.razao.diario("fila", self.piloto.nome, f"Lotes segurados: folga da equipe só comporta {lote.tamanho} (mínimo {self.cfg.lote_min}).")
                self._ultima_decisao = chave_decisao + "|pequeno"
            return
        indice = {a.contact_id: a for a in alunos}
        recusas = validar_lote(lote, alunos=indice, agora=agora, cfg=self.cfg, controle=controle, enviados_hoje=enviados)
        if recusas:
            texto = "; ".join(str(r) for r in recusas)
            self.razao.marcar_lote(alvo["id"], "recusado", texto[:500])
            self.razao.diario("lote", "trilhos", f"Lote {lote.ordem} recusado pelos trilhos: {texto}.")
            return

        # Simulação: registra o que seria feito no CRM e não chama nada que escreve.
        self.razao.registrar_acao(
            f"sim:{hoje.isoformat()}:{lote.ordem}:lancar", "lancar_lote", lote_id=alvo["id"], simulado=True,
            requisicao={
                "faria": ["definir o público do lote", "criar a campanha em rascunho", "lançar a campanha"],
                "mensagem": lote.mensagem.value, "destinatarios": lote.tamanho,
            },
        )
        self.razao.liberar_lote(
            alvo["id"], lote, {cid: indice[cid].estado.value for cid in lote.contact_ids},
            ciclo=ciclo, dia=hoje, simulado=True, status="simulado", motivo=ordem.motivo,
            fila=asdict(fila) if fila else None, agora=agora,
        )
        self.razao.diario(
            "lote", self.piloto.nome,
            f"Lote {lote.ordem} ({lote.mensagem.value}) liberado em simulação: {lote.tamanho} alunos. {ordem.motivo}.",
            {"ordem": lote.ordem, "tamanho": lote.tamanho, "frente": lote.frente.value},
        )

        # Lote saiu menor que o previsto e não há outro da mesma mensagem na fila do plano:
        # o que sobrou vira um lote novo, para não ficar para trás sem aparecer.
        sobra = len(selecao.elegiveis) - lote.tamanho
        outros = [l for l in pendentes if l["id"] != alvo["id"] and l["mensagem"] == alvo["mensagem"]]
        cabe = self.cfg.teto_diario - enviados - lote.tamanho
        if sobra > 0 and not outros and cabe > 0:
            self.razao.adicionar_lote(
                plano["id"], dia=hoje, simulado=True, ordem=max(l["ordem"] for l in lotes) + 1,
                frente=alvo["frente"], mensagem=alvo["mensagem"], tamanho=min(sobra, self.cfg.lote_max, cabe),
            )
