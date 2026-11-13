"""Laço do motor: o que acontece a cada tick (padrão: 30 segundos).

  1. bate o ponto e lê as chaves da página (ligado, parada geral)
  2. classifica as respostas que chegaram pelo webhook
  3. lê a fila de atendimento e roda a fórmula do governador
  4. faz a carga do dia (base de negócios do CRM) e a refaz durante a janela
  5. monta o plano do dia quando a carga do dia é detectada (uma pessoa aprova na página)
  6. em modo real, anda a campanha do dia: lança, abre janelas, mede, pausa, cancela
  7. em simulação, pede a ordem ao piloto e libera o próximo lote simulado

Em simulação o motor monta os lotes, registra o que faria e não chama nenhum
endpoint que escreve no CRM. Modo real exige `MOTOR_ENABLED=1`,
`MOTOR_DRY_RUN=0`, motor ligado na página e plano do dia aprovado.
"""
from __future__ import annotations

import logging
import os
import re
import socket
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Callable, Optional

from motor import VERSAO
from motor.agenda import acoes_do_dia, parcela_do_dia
from motor.calendario import agora_brt, ciclo_de, eh_dia_util, para_brt
from motor.carga import consolidar, montar_mapa, normalizar, tags_de_bloqueio
from motor.classificador import Classificador, ClassificadorRegras
from motor.config import MotorConfig
from motor.governador import decidir, dentro_da_janela, nivel_de_antes, tamanho_do_lote, teto_da_rampa, trava_de_experiencia
from motor.loteador import lotear, lotes_que_cabem_na_janela
from motor.modelos import Decisao, Fila
from motor.piloto import LotePendente, Piloto, PilotoFormula, Retrato
from motor.seletor import contar_por_estado, selecionar
from motor.trilhos import modo_efetivo, validar_lote

logger = logging.getLogger(__name__)

RETENTATIVA_CARGA = timedelta(minutes=15)
RECARGA_NA_JANELA = timedelta(minutes=30)
# Estes rótulos tiram o aluno dos envios mesmo com confiança baixa: na dúvida, não insistir.
EFEITO_SEMPRE = frozenset({"pediu_parar", "numero_errado", "reclamou"})


class Laco:
    def __init__(
        self,
        cfg: MotorConfig,
        razao,
        crm=None,
        piloto: Optional[Piloto] = None,
        relogio: Callable[[], datetime] = agora_brt,
        classificador: Optional[Classificador] = None,
        envio=None,
    ):
        self.cfg = cfg
        self.razao = razao
        self.crm = crm
        self.piloto = piloto or PilotoFormula()
        self.relogio = relogio
        self.classificador = classificador or ClassificadorRegras()
        self.envio = envio
        self.iniciado_em = relogio()
        self._avisos: set[str] = set()
        self._ultimo_modo = ""
        self._ultima_decisao = ""
        self._carga_em: Optional[datetime] = None
        self._carga_falhou_em: Optional[datetime] = None
        self._limpeza_em: Optional[datetime] = None
        self._ultimo_erro = ""
        self._lote_ativo = None

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
            "config": {**self.cfg.publico(), "classificador": self.classificador.nome, "piloto": self.piloto.nome},
        })

    # ── um tick ───────────────────────────────────────────────────────────
    def tick(self) -> str:
        agora = para_brt(self.relogio())
        controle = self.razao.ler_controle()
        modo = modo_efetivo(self.cfg, controle)
        if modo == "real" and (self.envio is None or self.crm is None or not self.crm.permitir_escrita):
            self._uma_vez(
                "real-indisponivel", "controle", "trilhos",
                "MOTOR_DRY_RUN=0, mas o envio real não está disponível neste processo. O motor segue em simulação.",
            )
            modo = "simulacao"
        self._ponto(modo)
        if modo != self._ultimo_modo:
            self.razao.diario("controle", "worker", f"Motor em modo: {modo}.", {"por": controle.atualizado_por, "motivo": controle.parada_motivo})
            self._ultimo_modo = modo
        self._classificar_respostas(agora)
        hoje = agora.date()
        if modo in ("parado", "desligado"):
            # Parada geral cancela a campanha do dia; motor desligado pausa o que estiver enviando.
            if self.envio is not None and self.crm is not None and self.crm.permitir_escrita:
                self.envio.avancar(
                    agora, plano=self.razao.plano(hoje, False), decisao=Decisao.PAUSAR, motivo=modo, fila=None,
                    parada=(controle.parada_motivo or "acionada na página") if modo == "parado" else None, desligado=modo == "desligado",
                )
            return modo

        simulado = modo != "real"
        fila, decisao, motivo = self._governar(agora, simulado)
        self._carga(agora, simulado)
        self._plano(agora, simulado)
        if simulado:
            self._rodada(agora, fila, decisao, motivo, controle)
        else:
            self.envio.avancar(
                agora, plano=self.razao.plano(hoje, False), decisao=decisao, motivo=motivo, fila=fila,
                lotes_liberados=self.razao.lotes_liberados_hoje(hoje, False), fila_base=nivel_de_antes(self._lote_ativo, agora, self.cfg),
            )
        return modo

    # ── respostas dos alunos ──────────────────────────────────────────────
    def _classificar_respostas(self, agora: datetime) -> None:
        """Classifica o texto mascarado que o webhook deixou pendente e aplica o efeito no aluno."""
        for r in self.razao.respostas_pendentes(40):
            c = self.classificador.classificar(r.get("texto_pendente") or "")
            self.razao.gravar_classificacao(
                r["id"], rotulo=c.rotulo, confianca=c.confianca, origem=c.origem, revisar=c.precisa_de_revisao,
                guardar_texto=self.cfg.guardar_texto, dias=self.cfg.texto_dias,
            )
            if not c.precisa_de_revisao or c.rotulo in EFEITO_SEMPRE:
                self.razao.aplicar_efeito(r["contact_id"], c.rotulo, r["dia"])
        if self._limpeza_em is None or agora - self._limpeza_em > timedelta(hours=1):
            self._limpeza_em = agora
            self.razao.limpar_textos_vencidos()

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

    def _governar(self, agora: datetime, simulado: bool) -> tuple[Optional[Fila], Decisao, str]:
        hoje = agora.date()
        fila: Optional[Fila] = None
        erro = ""
        em_envio = [] if simulado else self.razao.planos_com_campanha_ativa()
        ler = self._perto_da_janela(agora) or bool(em_envio)
        if ler and self.crm is not None:
            try:
                fila = self.crm.fila_agora()  # `lida_em` vem do `asOf` da resposta
            except Exception as e:  # leitura da fila nunca derruba o laço
                if getattr(e, "status", 0) == 403:
                    erro = f"sem permissão no token do motor ({getattr(e, 'permissao', '') or 'não informada'}); corrija o papel do usuário do token e reinicie o worker"
                else:
                    erro = f"{type(e).__name__}: {e}"[:300]
        elif ler:
            erro = "CRM não configurado (EDUIT_CRM_TOKEN)"
        if ler and fila is None:
            fila = self._fila_de_ensaio(agora)
            if fila is not None:
                erro = f"fila de ensaio (MOTOR_SIM_FILA), não é leitura real. {erro}".strip()
        experiencia = self.razao.experiencia_do_dia(hoje, simulado)
        self._lote_ativo = self.razao.ultimo_lote_liberado(hoje, simulado)
        decisao, motivo = decidir(
            fila, self.cfg, agora=agora, lote_ativo=self._lote_ativo,
            campanha_em_andamento=any(p["campanha"]["estado"] == "enviando" for p in em_envio), experiencia=experiencia,
        )
        if ler:
            self.razao.registrar_fila(fila, decisao.value, motivo, erro)
            if erro:
                self._uma_vez(f"fila-erro:{hoje}:{erro[:60]}", "erro", "worker", f"Não consegui ler a fila: {erro}")
        if trava_de_experiencia(experiencia, self.cfg) == "encerrar":
            self._encerrar_o_dia(hoje, simulado, motivo)
        return fila, decisao, motivo

    def _encerrar_o_dia(self, hoje, simulado: bool, motivo: str) -> None:
        pendentes = [l for l in self.razao.lotes(hoje, simulado) if l["status"] == "planejado"]
        for l in pendentes:
            self.razao.marcar_lote(l["id"], "adiado", "dia encerrado pela trava de experiência")
        self.razao.encerrar_plano(hoje, simulado)
        self._uma_vez(f"encerrado:{hoje}:{simulado}", "pausa", "trilhos", f"Dia encerrado: {motivo}. {len(pendentes)} lotes não saem hoje.")

    # ── carga ─────────────────────────────────────────────────────────────
    def _carga(self, agora: datetime, simulado: bool) -> None:
        hoje = agora.date()
        if self.crm is None or agora.hour < self.cfg.carga_hora:
            return
        if self._carga_falhou_em and agora - self._carga_falhou_em < RETENTATIVA_CARGA:
            return
        feita = self.razao.carga_do_dia(hoje)
        if feita:
            # Refaz a carga a cada 30 min enquanto ela ainda pode mudar o dia: esperando a carga da
            # equipe para fechar o plano, com lotes simulados pendentes, ou antes de lançar a campanha.
            if self._carga_em is not None and agora - self._carga_em < RECARGA_NA_JANELA:
                return
            if not dentro_da_janela(agora, self.cfg):
                return
            plano = self.razao.plano(hoje, simulado)
            esperando = plano is None and bool(self._acoes(hoje))
            pendentes = simulado and any(l["status"] == "planejado" for l in self.razao.lotes(hoje, True))
            antes_de_lancar = (not simulado) and plano is not None and plano["status"] == "aprovado" and not (plano.get("campanha") or {}).get("id")
            if not (esperando or pendentes or antes_de_lancar):
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
            if getattr(e, "status", 0) == 403:  # falta permissão no token: uma entrada só, sem insistir
                self._uma_vez(f"carga-403:{getattr(e, 'permissao', '')}", "erro", "worker",
                              f"Carga sem permissão no token do motor ({getattr(e, 'permissao', '') or 'não informada'}). Corrija o papel do usuário do token e reinicie o worker.")
                return
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
    def _acoes(self, hoje) -> list:
        return acoes_do_dia(hoje, self.cfg.feriados, self.cfg.dias_antes_vencimento)

    def _parcela(self, hoje, acao) -> float:
        return parcela_do_dia(hoje, acao.frente, self.cfg.feriados, self.cfg.dias_antes_vencimento)

    def _plano(self, agora: datetime, simulado: bool) -> None:
        hoje = agora.date()
        if (agora.hour, agora.minute) < (self.cfg.plano_hora, self.cfg.plano_minuto):
            return
        acoes = self._acoes(hoje)
        if not acoes or self.razao.plano(hoje, simulado):
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
        # A carga do campo Dia 10 é importada pela equipe ao longo do dia (entre 10h e 14h, às vezes mais
        # tarde). O plano só fecha depois dela: antes, a base ainda é a de ontem.
        mudancas = self.razao.mudancas_do_dia(hoje)
        if mudancas < self.cfg.carga_dia_min_mudancas:
            self._uma_vez(
                f"plano-sem-carga-do-dia:{hoje}", "plano", "trilhos",
                f"A carga do dia ainda não foi detectada ({mudancas} mudanças no Dia 10 hoje; mínimo {self.cfg.carga_dia_min_mudancas}). O plano fecha depois dela.",
            )
            return
        ciclo = carga["ciclo"]
        alunos = self.razao.alunos(ciclo=ciclo, hoje=hoje, simulado=simulado, cfg=self.cfg)
        selecoes = [selecionar(alunos, a, hoje=hoje, cfg=self.cfg, parcela=self._parcela(hoje, a)) for a in acoes]
        plano = lotear(
            selecoes, dia=hoje, cfg=self.cfg, tamanho=self.cfg.lote_max,
            enviados_hoje=self.razao.enviados_hoje(hoje, simulado),
            max_lotes=lotes_que_cabem_na_janela(self.cfg),
        )
        resumo = {
            "por_estado": contar_por_estado(alunos),
            "controle": sum(1 for a in alunos if a.controle),
            "acoes": [
                {
                    "frente": s.acao.frente.value, "mensagem": s.acao.mensagem.value, "parcela": round(self._parcela(hoje, s.acao), 3),
                    "elegiveis": len(s.elegiveis), "exclusoes": s.exclusoes, "fora_do_alvo": s.fora_do_alvo,
                }
                for s in selecoes
            ],
            "lotes": len(plano.lotes), "total": plano.total, "adiados": plano.adiados,
            "teto_diario": self.cfg.teto_diario, "lote_max": self.cfg.lote_max, "rampa": list(self.cfg.rampa),
        }
        self.razao.salvar_plano(
            dia=hoje, simulado=simulado, ciclo=ciclo, piloto=self.piloto.nome, resumo=resumo,
            # No envio real não há lote por lista: as janelas da campanha do dia nascem na hora.
            lotes=[{"ordem": l.ordem, "frente": l.frente.value, "mensagem": l.mensagem.value, "tamanho": l.tamanho} for l in plano.lotes] if simulado else [],
        )
        self.razao.diario(
            "plano", self.piloto.nome,
            f"Plano {'simulado' if simulado else 'REAL'} de {hoje:%d/%m}: {plano.total} alunos em {len(plano.lotes)} lotes"
            + (f"; {plano.adiados} ficam para o próximo dia de envio" if plano.adiados else "")
            + ("." if simulado else " (estimativa do motor). Aguardando aprovação, com a etiqueta do dia, na página."),
            resumo,
        )

    # ── rodada do piloto (simulação) ──────────────────────────────────────
    def _rodada(self, agora: datetime, fila: Optional[Fila], decisao: Decisao, motivo: str, controle) -> None:
        """Em simulação o motor monta lotes por aluno e registra o que faria. No envio real quem anda é `Envio`."""
        hoje = agora.date()
        plano = self.razao.plano(hoje, True)
        if not plano or plano["status"] == "encerrado":
            return
        lotes = self.razao.lotes(hoje, True)
        pendentes = [l for l in lotes if l["status"] == "planejado"]
        if not pendentes:
            return
        if not dentro_da_janela(agora, self.cfg):
            if agora.hour >= self.cfg.janela_fim_h:
                for l in pendentes:
                    self.razao.marcar_lote(l["id"], "adiado", "fim do expediente")
                self.razao.diario("lote", "trilhos", f"Fim do expediente: {len(pendentes)} lotes adiados para o próximo dia de envio.")
            return

        ciclo = plano["ciclo"]
        enviados = self.razao.enviados_hoje(hoje, True)
        liberados = self.razao.lotes_liberados_hoje(hoje, True)
        sugerido = tamanho_do_lote(fila, self.cfg, liberados, nivel_de_antes(self._lote_ativo, agora, self.cfg))
        retrato = Retrato(
            agora=agora, fila=fila, decisao_governador=decisao, motivo_governador=motivo, tamanho_sugerido=sugerido,
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
        acao = next((a for a in self._acoes(hoje) if a.mensagem.value == alvo["mensagem"]), None)
        if acao is None:
            self.razao.marcar_lote(alvo["id"], "recusado", "mensagem fora do calendário de hoje")
            return

        # Lista recalculada a cada lote: já sem quem teve baixa ou recebeu algo hoje.
        alunos = self.razao.alunos(ciclo=ciclo, hoje=hoje, simulado=True, cfg=self.cfg)
        tamanho = max(1, min(ordem.tamanho, sugerido, self.cfg.lote_max))
        selecao = selecionar(alunos, acao, hoje=hoje, cfg=self.cfg, parcela=self._parcela(hoje, acao))
        montado = lotear([selecao], dia=hoje, cfg=self.cfg, tamanho=tamanho, enviados_hoje=enviados, primeira_ordem=alvo["ordem"], max_lotes=1,
                          teto_do_lote=teto_da_rampa(liberados, self.cfg))
        if not montado.lotes:
            self.razao.marcar_lote(alvo["id"], "concluido", "sem elegíveis no recálculo")
            return
        lote = montado.lotes[0]
        indice = {a.contact_id: a for a in alunos}
        recusas = validar_lote(lote, alunos=indice, agora=agora, cfg=self.cfg, controle=controle, enviados_hoje=enviados,
                               plano_aprovado=plano["status"] == "aprovado")
        if recusas:
            texto = "; ".join(str(r) for r in recusas)
            self.razao.marcar_lote(alvo["id"], "recusado", texto[:500])
            self.razao.diario("lote", "trilhos", f"Lote {lote.ordem} recusado pelos trilhos: {texto}.")
            return

        # Simulação: registra o que seria feito no CRM e não chama nada que escreve.
        self.razao.registrar_acao(
            f"sim:{hoje.isoformat()}:{lote.ordem}:janela", "janela_de_envio", lote_id=alvo["id"], simulado=True,
            requisicao={
                "faria": ["retomar a campanha do dia", f"deixar andar cerca de {lote.tamanho} mensagens", "pausar a campanha"],
                "mensagem": lote.mensagem.value, "destinatarios": lote.tamanho,
            },
        )
        self.razao.liberar_lote(
            alvo["id"], lote, {cid: indice[cid].estado.value for cid in lote.contact_ids}, ciclo=ciclo, dia=hoje, simulado=True,
            status="simulado", motivo=ordem.motivo, fila=asdict(fila) if fila else None, agora=agora,
        )
        self.razao.diario("lote", self.piloto.nome, f"Lote {lote.ordem} ({lote.mensagem.value}) liberado em simulação: {lote.tamanho} alunos. {ordem.motivo}.",
                          {"ordem": lote.ordem, "tamanho": lote.tamanho, "frente": lote.frente.value})

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
