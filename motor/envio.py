"""Envio real: UMA campanha por dia sobre a etiqueta do dia, dosada por pausar e retomar.

Só roda com tudo junto: `MOTOR_ENABLED=1`, `MOTOR_DRY_RUN=0`, motor ligado na
página e plano do dia aprovado (com a etiqueta do dia informada na aprovação).

  público    etiqueta do dia, aplicada pela importação diária da equipe. O motor
             não cria nem aplica essa etiqueta.
  exclusões  `excludeTagIds`: "Sem lembretes de mensalidade", "Sem mensagens
             automáticas" e a etiqueta do grupo de controle do mês. O motor só
             etiqueta o grupo de controle (5%, uma vez por mês).
  janelas    o "lote" é uma janela de envio: retoma, deixa andar N segundos
             (cerca de N mensagens a 1 por segundo) e pausa. A janela seguinte
             só abre quando o governador libera.
  fim        no fim do expediente, na parada geral, na trava de experiência ou
             no teto do dia a campanha é CANCELADA. Os pendentes não recebem.

Três fatos do CRM que viram regra aqui:
  - o público é fixado no lançamento; por isso a campanha nunca atravessa o dia
    (quem pagou depois ainda receberia ao retomar)
  - a pausa pega em cerca de 5 s: cada janela conta umas 5 mensagens a mais
  - o motor confere o público ANTES de lançar, pela base de negócios, e recusa
    se a etiqueta do dia tiver gente demais que não deveria receber

Cada passo que escreve no CRM é registrado antes em `motor_acoes`. Passo que
ficou "iniciado" sem resposta não é repetido às cegas.
"""
from __future__ import annotations

import os
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Mapping, Optional

from motor.calendario import para_brt
from motor.config import MotorConfig
from motor.crm import BAIXA, CrmErro, CrmMotor
from motor.governador import dentro_da_janela, tamanho_do_lote
from motor.modelos import Aluno, Decisao, Fila, Mensagem
from motor.razao import CAMPANHA_ATIVA, CAMPANHA_FINAL
from motor.trilhos import SITUACAO_ATIVA, normalizar_nivel

MESES = ("janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro")
NOME = "{{contact.name | first}}"
INTERVALO_DE_MEDICAO = timedelta(seconds=60)
ETIQUETAS_DE_EXCLUSAO = ("Sem lembretes de mensalidade", "Sem mensagens automáticas")


def _fold(s: str) -> str:
    n = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in n if not unicodedata.combining(c)).lower().strip()


def etiqueta_de_controle(ciclo: str) -> str:
    return f"motor-controle-{ciclo}"


def componentes_do_template(mensagem: str, ciclo: str) -> dict:
    """Variáveis do corpo: {{1}} primeiro nome, {{2}} mês, {{3}} prazo. O nome é resolvido pelo CRM por destinatário."""
    mes = int(ciclo.split("-")[1])
    prazo = {Mensagem.LEMBRETE_DIA10.value: 10, Mensagem.LEMBRETE_DIA25.value: 25, Mensagem.AVISO_CALOURO.value: 25}.get(mensagem)
    textos = [NOME] + ([MESES[mes - 1], f"{prazo:02d}/{mes:02d}"] if prazo else [])
    return {"version": 1, "components": [{"type": "body", "parameters": [{"type": "text", "text": t} for t in textos]}]}


def configuracao(mensagem: str) -> Optional[dict]:
    """Canal e template da mensagem, por ambiente. None = não configurado (a campanha não sai)."""
    canal = (os.getenv("MOTOR_CANAL_ID") or "").strip()
    template = (os.getenv(f"MOTOR_TEMPLATE_{mensagem.upper()}") or "").strip()
    if not canal or not template:
        return None
    return {"canal": canal, "template": template, "idioma": (os.getenv("MOTOR_TEMPLATE_IDIOMA") or "pt_BR").strip()}


@dataclass
class Conferencia:
    """Público da etiqueta do dia contra a base de negócios, antes de lançar."""
    na_etiqueta: int = 0
    excluidos: int = 0                      # saem pelas etiquetas de exclusão ou pelo controle já etiquetado
    recebem: int = 0
    indevidos: dict = field(default_factory=dict)   # motivo → quantos receberiam sem dever

    @property
    def diferenca(self) -> float:
        return sum(self.indevidos.values()) / self.recebem if self.recebem else 0.0


def conferir_publico(alunos: Iterable[Aluno], etiqueta_dia: str, cfg: MotorConfig) -> Conferencia:
    """Conta, pela base, quem está na etiqueta do dia e quem dela não deveria receber."""
    alvo = _fold(etiqueta_dia)
    exclusoes = {_fold(e) for e in ETIQUETAS_DE_EXCLUSAO}
    conf = Conferencia()
    for a in alunos:
        if alvo not in a.etiquetas:
            continue
        conf.na_etiqueta += 1
        if a.etiquetas & exclusoes or (a.controle and a.controle_etiquetado):
            conf.excluidos += 1
            continue
        conf.recebem += 1
        motivo = None
        if a.controle:
            motivo = "controle sem etiqueta"
        elif a.bloqueio:
            motivo = "pediu para parar ou número bloqueado, sem etiqueta de exclusão"
        elif a.suspenso:
            motivo = "reclamou no ciclo"
        elif a.nivel_misto:
            motivo = "negócio em dois níveis"
        elif cfg.nivel_alvo and normalizar_nivel(a.nivel) != cfg.nivel_alvo:
            motivo = "fora da graduação"
        elif _fold(a.situacao) != SITUACAO_ATIVA:
            motivo = "fora de Em Curso"
        elif not a.dia10_aberto:
            motivo = "sem mensalidade em aberto"
        elif not a.tem_telefone:
            motivo = "sem telefone"
        if motivo:
            conf.indevidos[motivo] = conf.indevidos.get(motivo, 0) + 1
    return conf


class Envio:
    def __init__(self, cfg: MotorConfig, razao, crm: CrmMotor):
        self.cfg = cfg
        self.razao = razao
        self.crm = crm
        self._avisos: set[str] = set()
        self._leu_destinatarios_em: Optional[datetime] = None

    # ── utilidades ────────────────────────────────────────────────────────
    def _uma_vez(self, chave: str, tipo: str, texto: str) -> None:
        if chave not in self._avisos:
            self._avisos.add(chave)
            self.razao.diario(tipo, "trilhos", texto)

    def _passo(self, chave: str, tipo: str, requisicao: dict) -> tuple[str, dict]:
        """('chamar' | 'feito' | 'incerto', resposta gravada). Registra o passo antes de chamar o CRM."""
        if self.razao.registrar_acao(chave, tipo, lote_id=None, simulado=False, requisicao=requisicao):
            return "chamar", {}
        feita = self.razao.acao(chave) or {}
        if feita.get("status") == "feito":
            return "feito", feita.get("resposta") or {}
        if feita.get("status") == "falhou":  # o CRM recusou com certeza: pode tentar de novo
            self.razao.concluir_acao(chave, "iniciada", None)
            return "chamar", {}
        return "incerto", {}

    def _gravar(self, plano: Mapping, c: dict) -> None:
        self.razao.atualizar_campanha(plano["id"], c)

    def _erro(self, plano: Mapping, c: dict, texto: str) -> None:
        c.update(estado="erro", motivo=texto[:500])
        self._gravar(plano, c)
        self.razao.diario("erro", "trilhos", f"Campanha do dia parada e esperando uma pessoa: {texto}")

    def _etiquetas_do_crm(self) -> dict[str, str]:
        return {_fold(str(t.get("name") or "")): str(t.get("id")) for t in self.crm.etiquetas() if t.get("id")}

    def mensagem_do_dia(self, plano: Mapping) -> Optional[Mapping]:
        """Uma campanha por dia = um template: vale a ação com mais elegíveis no plano."""
        acoes = [a for a in ((plano.get("resumo") or {}).get("acoes") or []) if a.get("elegiveis")]
        return max(acoes, key=lambda a: a["elegiveis"]) if acoes else None

    def impedimento(self, mensagem: str) -> Optional[str]:
        if not self.crm.permitir_escrita:
            return "cliente do CRM com a escrita travada"
        if configuracao(mensagem) is None:
            return f"canal (MOTOR_CANAL_ID) ou template (MOTOR_TEMPLATE_{mensagem.upper()}) não configurado"
        return None

    # ── a cada tick ───────────────────────────────────────────────────────
    def avancar(self, agora: datetime, *, plano: Optional[Mapping], decisao: Decisao, motivo: str, fila: Optional[Fila],
                parada: Optional[str] = None, desligado: bool = False, lotes_liberados: int = 0, fila_base: Optional[int] = None) -> None:
        hoje = para_brt(agora).date()
        try:
            # Campanha nunca atravessa o dia: o público foi fixado no lançamento.
            for antigo in self.razao.planos_com_campanha_ativa():
                if antigo["dia"] < hoje:
                    self._cancelar(antigo, dict(antigo["campanha"]), "campanha de dia anterior não atravessa o dia", agora)
            if plano is None:
                return
            c = dict(plano.get("campanha") or {})
            estado = c.get("estado")
            if estado in CAMPANHA_FINAL:
                return self._ler_destinatarios(plano, c, agora)
            if estado == "erro":
                return
            if estado not in CAMPANHA_ATIVA:
                if parada or desligado or plano["status"] != "aprovado":
                    return
                return self._preparar_e_lancar(plano, c, agora, decisao, motivo, fila, lotes_liberados, fila_base)

            self._medir(plano, c, agora)
            if c["estado"] not in CAMPANHA_ATIVA:
                return
            if parada:
                return self._cancelar(plano, c, f"parada geral: {parada}".rstrip(": "), agora)
            if not dentro_da_janela(agora, self.cfg):
                return self._cancelar(plano, c, "fim do expediente", agora)
            if plano["status"] == "encerrado":
                return self._cancelar(plano, c, "dia encerrado pela trava de experiência", agora)
            if c["processados"] >= self.cfg.teto_diario:
                return self._cancelar(plano, c, f"teto do dia atingido ({self.cfg.teto_diario})", agora)
            if c["estado"] == "enviando":
                if desligado:
                    return self._pausar(plano, c, "motor desligado", agora)
                if decisao == Decisao.PAUSAR:
                    return self._pausar(plano, c, motivo, agora)
                if para_brt(agora) >= datetime.fromisoformat(c["janela_fim"]):
                    return self._pausar(plano, c, f"janela {c['janela']} concluída", agora, emergencia=False)
            elif (not desligado and decisao == Decisao.LIBERAR and c["total"] - c["processados"] > 0
                  and tamanho_do_lote(fila, self.cfg, lotes_liberados, fila_base) > 0):
                self._retomar(plano, c, agora, fila, lotes_liberados, fila_base)
        except CrmErro as e:
            if e.status == 403:
                c = dict((plano or {}).get("campanha") or {})
                if plano is not None:
                    self._sem_permissao(plano, c, e)
                return
            self.razao.diario("erro", "trilhos", f"Campanha do dia: o CRM respondeu {e.status} ({e}). Tento de novo no próximo ciclo.")

    def _sem_permissao(self, plano: Mapping, c: dict, erro: CrmErro) -> None:
        """403: registra a permissão que faltou e para, sem tentar de novo. Se estava enviando, tenta pausar."""
        if c.get("estado") == "enviando" and c.get("id"):
            try:
                self.crm.pausar_campanha(c["id"])
            except CrmErro:
                pass
        self._erro(plano, c, f"falta permissão no token do motor: {erro.permissao or 'não informada'}. "
                             "Corrija o papel do usuário do token e reinicie o worker")

    # ── grupo de controle ─────────────────────────────────────────────────
    def _etiquetar_controle(self, ciclo: str, etiquetas: dict[str, str]) -> tuple[str, int]:
        """Etiqueta no CRM o controle do mês, dentro do orçamento. Devolve (id da etiqueta, quantos faltam)."""
        nome = etiqueta_de_controle(ciclo)
        tag_id = etiquetas.get(_fold(nome), "")
        if not tag_id:
            estado, feita = self._passo(f"real:controle:{ciclo}:etiqueta", "etiqueta_controle", {"nome": nome})
            if estado == "feito":
                tag_id = str(feita.get("id") or "")
            else:  # nome único: se já existir, o CRM responde 409 e o id é buscado pelo nome
                try:
                    tag_id = str(self.crm.criar_etiqueta(nome).get("id") or "")
                except CrmErro as e:
                    if e.status != 409:
                        if 400 <= e.status < 500:
                            self.razao.concluir_acao(f"real:controle:{ciclo}:etiqueta", "falhou", {"status": e.status})
                        raise
                    tag_id = self._etiquetas_do_crm().get(_fold(nome), "")
                if tag_id:
                    self.razao.concluir_acao(f"real:controle:{ciclo}:etiqueta", "feito", {"id": tag_id})
        if not tag_id:
            return "", -1
        feitos = []
        for cid in self.razao.controle_pendente(ciclo, limite=self.crm.limite.disponiveis(BAIXA)):
            try:
                self.crm.etiquetar_contato(cid, tag_id)
            except CrmErro as e:
                if e.status in (400, 404):  # contato apagado: sai do controle etiquetado mesmo assim (não está no público)
                    feitos.append(cid)
                    continue
                self.razao.marcar_controle_etiquetado(ciclo, feitos)
                if e.status == 403:
                    raise
                break
            feitos.append(cid)
        self.razao.marcar_controle_etiquetado(ciclo, feitos)
        return tag_id, len(self.razao.controle_pendente(ciclo, limite=100000))

    # ── lançamento ────────────────────────────────────────────────────────
    def _preparar_e_lancar(self, plano: Mapping, c: dict, agora: datetime, decisao: Decisao, motivo: str,
                           fila: Optional[Fila], lotes_liberados: int, fila_base: Optional[int]) -> None:
        hoje, ciclo = plano["dia"], plano["ciclo"]
        acao = self.mensagem_do_dia(plano)
        if acao is None:
            return self._uma_vez(f"sem-publico:{hoje}", "plano", "Plano do dia sem elegíveis: nada a lançar.")
        impedimento = self.impedimento(acao["mensagem"])
        if impedimento:
            return self._uma_vez(f"impedimento:{hoje}:{impedimento}", "erro", f"Campanha do dia não pode sair: {impedimento}.")
        nome_do_dia = (plano.get("etiqueta_dia") or "").strip()
        if not nome_do_dia:
            return self._uma_vez(f"sem-etiqueta:{hoje}", "plano", "Plano aprovado sem a etiqueta do dia: informe a etiqueta na aprovação.")

        etiquetas = self._etiquetas_do_crm()
        por_id = {v: k for k, v in etiquetas.items()}
        tag_dia = nome_do_dia if nome_do_dia in por_id else etiquetas.get(_fold(nome_do_dia), "")
        if not tag_dia:
            return self._uma_vez(f"etiqueta-inexistente:{hoje}:{nome_do_dia}", "erro", f"A etiqueta do dia \"{nome_do_dia}\" não existe no CRM.")
        nome_dia = por_id[tag_dia]

        tag_controle, faltam = self._etiquetar_controle(ciclo, etiquetas)
        if faltam != 0:
            self._uma_vez(f"controle:{hoje}:{faltam > 0}", "plano", f"Etiquetando o grupo de controle do mês antes de lançar ({faltam} por fazer)." if faltam > 0
                          else "Não consegui criar a etiqueta do grupo de controle do mês.")
            return
        if decisao != Decisao.LIBERAR or tamanho_do_lote(fila, self.cfg, lotes_liberados, fila_base) <= 0:
            return

        # Conferência do público, pela base, antes de qualquer campanha existir.
        carga = self.razao.carga_do_dia(hoje) or {}
        if c.get("estado") == "recusada" and c.get("carga_id") == carga.get("id"):
            return  # já recusada com esta carga; uma carga nova reabre a conferência
        alunos = self.razao.alunos(ciclo=ciclo, hoje=hoje, simulado=False, cfg=self.cfg)
        conf = conferir_publico(alunos, nome_dia, self.cfg)
        resumo = {"na_etiqueta": conf.na_etiqueta, "excluidos": conf.excluidos, "recebem": conf.recebem, "indevidos": conf.indevidos}
        if conf.recebem == 0 or conf.diferenca > self.cfg.publico_diferenca_max:
            texto = ("a etiqueta do dia não tem contatos na base" if conf.recebem == 0 else
                     f"{sum(conf.indevidos.values())} de {conf.recebem} na etiqueta do dia não deveriam receber "
                     f"({conf.diferenca:.1%}, limite {self.cfg.publico_diferenca_max:.0%}): "
                     + "; ".join(f"{m} {n}" for m, n in sorted(conf.indevidos.items())))
            c = {"estado": "recusada", "motivo": texto, "carga_id": carga.get("id"), "conferencia": resumo}
            self._gravar(plano, c)
            self.razao.diario("plano", "trilhos", f"Lançamento RECUSADO: {texto}. Uma carga nova refaz a conferência.")
            return

        cfg_msg = configuracao(acao["mensagem"])
        excluir = [etiquetas[_fold(e)] for e in ETIQUETAS_DE_EXCLUSAO if _fold(e) in etiquetas] + [tag_controle]
        filtros = {"tagIds": [tag_dia], "excludeTagIds": excluir}
        base = {"mensagem": acao["mensagem"], "frente": acao["frente"], "etiqueta_id": tag_dia, "excluir": excluir, "conferencia": resumo, "esperado": conf.recebem}
        chave = f"real:plano:{plano['id']}"
        estado, feita = self._passo(f"{chave}:campanha", "campanha", {"filtros": filtros, "template": cfg_msg["template"], "esperado": conf.recebem})
        if estado == "incerto":
            return self._erro(plano, base, "a criação da campanha ficou sem resposta do CRM; confira as campanhas antes de continuar")
        if estado == "chamar":
            try:
                campanha = self.crm.criar_campanha(
                    nome=f"Motor {hoje:%d/%m} ({acao['mensagem']})", canal_id=cfg_msg["canal"], template=cfg_msg["template"], idioma=cfg_msg["idioma"],
                    componentes=componentes_do_template(acao["mensagem"], ciclo), tag_ids=[tag_dia], excluir_tag_ids=excluir, velocidade=self.cfg.velocidade,
                )
            except CrmErro as e:
                if 400 <= e.status < 500:
                    self.razao.concluir_acao(f"{chave}:campanha", "falhou", {"status": e.status, "code": e.codigo})
                if e.codigo == "AUDIENCE_FILTERS_INVALID":
                    return self._erro(plano, base, "o CRM recusou o filtro de exclusão (excludeTagIds ainda não está na API). "
                                                   "Sem a exclusão o controle e quem pediu para parar receberiam: nada foi lançado")
                if e.status == 429 or e.status >= 500 or e.status == 403:
                    raise
                return self._erro(plano, base, f"o CRM recusou a campanha ({e.status}: {e})")
            feita = {"id": str(campanha.get("id") or "")}
            self.razao.concluir_acao(f"{chave}:campanha", "feito", feita)
        campanha_id = str(feita.get("id") or "")
        if not campanha_id:
            return self._erro(plano, base, "o CRM não devolveu o id da campanha")
        base["id"] = campanha_id

        rascunho = self.crm.campanha(campanha_id)
        estado, _ = self._passo(f"{chave}:lancar", "lancar", {"campanha": campanha_id})
        pode = estado == "chamar" or (estado == "incerto" and rascunho.get("status") == "DRAFT")
        if pode:
            # Confere o rascunho antes de lançar: público e exclusões têm de ser os pedidos.
            if rascunho.get("status") != "DRAFT" or (rascunho.get("filters") or {}) != filtros:
                self.crm.cancelar_campanha(campanha_id)
                return self._erro(plano, base, "rascunho da campanha diferente do esperado; cancelado sem enviar")
            try:
                resposta = self.crm.lancar_campanha(campanha_id)
            except CrmErro as e:
                if 400 <= e.status < 500:
                    self.razao.concluir_acao(f"{chave}:lancar", "falhou", {"status": e.status})
                if e.status == 429 or e.status >= 500 or e.status == 403:
                    raise
                return self._erro(plano, base, f"o CRM recusou o lançamento ({e.status}: {e})")
            self.razao.concluir_acao(f"{chave}:lancar", "feito", {"status": resposta.get("status")})

        base.update(estado="enviando", lancada_em=agora.isoformat(), total=0, processados=0, entregues=0, lidos=0, respondidos=0, falhas=0,
                    janela=0, leituras=0, leitura_pagina=0)
        self.razao.diario("lote", "trilhos", f"Campanha do dia LANÇADA: etiqueta \"{nome_dia}\", {conf.recebem} destinatários esperados, "
                                              f"{conf.excluidos} excluídos por etiqueta, {self.cfg.velocidade} mensagem por segundo.", resumo)
        self._abrir_janela(plano, base, agora, fila, lotes_liberados, fila_base)

    # ── janelas ───────────────────────────────────────────────────────────
    def _abrir_janela(self, plano: Mapping, c: dict, agora: datetime, fila: Optional[Fila], lotes_liberados: int, fila_base: Optional[int]) -> None:
        tamanho = tamanho_do_lote(fila, self.cfg, lotes_liberados, fila_base)
        tamanho = max(1, min(tamanho, self.cfg.teto_diario - c["processados"]))
        # A pausa pega em cerca de 5 s: a janela fecha esse tanto antes.
        duracao = max(1.0, tamanho / self.cfg.velocidade - self.cfg.pausa_atraso_s)
        c.update(janela=c["janela"] + 1, janela_tamanho=tamanho, janela_inicio=para_brt(agora).isoformat(),
                 janela_fim=(para_brt(agora) + timedelta(seconds=duracao)).isoformat(),
                 antes={"processados": c["processados"], "entregues": c["entregues"]}, estado="enviando", motivo="")
        self.razao.adicionar_lote(plano["id"], dia=plano["dia"], simulado=False, ordem=c["janela"], frente=c["frente"], mensagem=c["mensagem"], tamanho=tamanho)
        lote = self.razao.lotes(plano["dia"], False)[-1]
        c["lote_id"] = lote["id"]
        self.razao.atualizar_lote(lote["id"], status="enviando", liberado_em=agora, crm_campaign_id=c["id"],
                                  fila={"aguardando": fila.aguardando} if fila else None)
        self._gravar(plano, c)
        self.razao.diario("lote", "trilhos", f"Janela {c['janela']} aberta: cerca de {tamanho} mensagens.", {"janela": c["janela"], "tamanho": tamanho})

    def _fechar_janela(self, c: dict, status: str, motivo: str = "") -> None:
        if c.get("lote_id"):
            self.razao.atualizar_lote(c["lote_id"], status=status, motivo=motivo[:500])

    def _retomar(self, plano: Mapping, c: dict, agora: datetime, fila: Optional[Fila], lotes_liberados: int, fila_base: Optional[int]) -> None:
        try:
            self.crm.retomar_campanha(c["id"])
        except CrmErro as e:
            if e.status != 409:
                raise
            return self._medir(plano, c, agora, forcar=True)  # já não está pausada: a medição diz o que houve
        self._abrir_janela(plano, c, agora, fila, lotes_liberados, fila_base)

    def _pausar(self, plano: Mapping, c: dict, motivo: str, agora: datetime, emergencia: bool = True) -> None:
        try:
            self.crm.pausar_campanha(c["id"])
        except CrmErro as e:
            if e.status != 409:  # 409 = já não está enviando (terminou ou já pausada)
                raise
        c.update(estado="pausada", motivo=motivo[:500])
        self._medir(plano, c, agora, forcar=True)
        if c["estado"] == "pausada":
            self._fechar_janela(c, "concluido" if not emergencia else "pausado", "" if not emergencia else motivo)
            self._gravar(plano, c)
            if emergencia:
                self.razao.diario("pausa", "trilhos", f"Campanha do dia PAUSADA na janela {c['janela']}: {motivo}.")

    def _cancelar(self, plano: Mapping, c: dict, motivo: str, agora: datetime, contar: bool = True) -> None:
        if c.get("id"):
            if contar:
                self._contar(c, agora)  # antes de cancelar: depois o CRM marca os pendentes como FAILED
            try:
                self.crm.cancelar_campanha(c["id"])
            except CrmErro as e:
                if e.status != 409:  # 409 = já terminou ou já cancelada
                    raise
        sem_receber = max(0, int(c.get("total") or 0) - int(c.get("processados") or 0))
        c.update(estado="cancelada", motivo=motivo[:500], nao_enviados=sem_receber, encerrada_em=agora.isoformat())
        self._fechar_janela(c, "cancelado", motivo)
        self._gravar(plano, c)
        self.razao.diario("pausa", "trilhos", f"Campanha do dia CANCELADA: {motivo}. {sem_receber} destinatários ficaram sem receber.")

    # ── medição ───────────────────────────────────────────────────────────
    def _contar(self, c: dict, agora: datetime) -> int:
        """Conta os destinatários por status e atualiza a campanha e a janela em curso. Devolve o total."""
        st = self.crm.campanha_stats(c["id"])
        cont = self.crm.campanha_contagem(c["id"])
        falhas = cont.get("FAILED", 0)
        total = int(st.get("totalRecipients") or 0)
        entregues = cont.get("DELIVERED", 0) + cont.get("READ", 0)
        processados = cont.get("SENT", 0) + entregues + falhas
        c.update(total=total, processados=processados, entregues=entregues, lidos=cont.get("READ", 0),
                 falhas=falhas, respondidos=int(st.get("repliedCount") or 0),
                 crm_status=str(st.get("status") or ""), medido_em=para_brt(agora).isoformat())
        antes = c.get("antes") or {}
        if c.get("lote_id"):
            self.razao.atualizar_lote(c["lote_id"], tamanho=max(0, processados - antes.get("processados", 0)),
                                      entregues=max(0, entregues - antes.get("entregues", 0)), enviados=processados, falhas=c["falhas"],
                                      lidos=c["lidos"], respondidos=c["respondidos"])
        return total

    def _medir(self, plano: Mapping, c: dict, agora: datetime, forcar: bool = False) -> None:
        """Mede a campanha pelos destinatários em cada status (a cada 1 min, ou na hora de pausar)."""
        if not forcar and c.get("medido_em") and para_brt(agora) - datetime.fromisoformat(c["medido_em"]) < INTERVALO_DE_MEDICAO:
            return
        total = self._contar(c, agora)
        processados, falhas = c["processados"], c["falhas"]
        # Trilho: o público real não pode passar do que a conferência previu.
        folga = max(10, int(c.get("esperado", 0) * self.cfg.publico_diferenca_max))
        if total > int(c.get("esperado") or 0) + folga:
            self._cancelar(plano, c, f"campanha com {total} destinatários; a conferência previa {c.get('esperado')}", agora, contar=False)
            c["estado"] = "erro"
            self._gravar(plano, c)
            self.razao.diario("erro", "trilhos", "Campanha do dia em erro: público maior que o conferido. Esperando uma pessoa.")
            return
        if c["crm_status"] == "COMPLETED":
            c.update(estado="concluida", nao_enviados=max(0, total - processados), encerrada_em=agora.isoformat())
            self._fechar_janela(c, "concluido")
            self.razao.diario("lote", "trilhos", f"Campanha do dia concluída: {processados} processados, {falhas} falhas"
                              + (f", {c['nao_enviados']} não enviados pelo CRM" if c["nao_enviados"] else "") + ".")
        elif c["crm_status"] in ("CANCELLED", "FAILED"):
            c.update(estado="cancelada", motivo=f"campanha {c['crm_status']} no CRM", nao_enviados=max(0, total - processados), encerrada_em=agora.isoformat())
            self._fechar_janela(c, "cancelado", c["motivo"])
        self._gravar(plano, c)

    def _ler_destinatarios(self, plano: Mapping, c: dict, agora: datetime) -> None:
        """Preenche o livro-razão por aluno com o que o CRM diz de cada destinatário.

        Duas leituras: quando a campanha termina e 24 h depois do lançamento. Uma
        página por vez, no máximo uma a cada 10 s. Nome e telefone não são guardados.
        """
        feitas = int(c.get("leituras") or 0)
        if feitas >= 2 or not c.get("id"):
            return
        if feitas == 1 and para_brt(agora) - datetime.fromisoformat(c["lancada_em"]) < timedelta(hours=24):
            return
        if self._leu_destinatarios_em and para_brt(agora) - para_brt(self._leu_destinatarios_em) < timedelta(seconds=10):
            return
        pagina = int(c.get("leitura_pagina") or 0) + 1
        itens, total_paginas = self.crm.campanha_destinatarios_pagina(c["id"], pagina)
        self._leu_destinatarios_em = agora
        self.razao.registrar_destinatarios(ciclo=plano["ciclo"], dia=plano["dia"], frente=c["frente"], mensagem=c["mensagem"], itens=itens)
        if pagina >= total_paginas or not itens:
            c.update(leituras=feitas + 1, leitura_pagina=0)
        else:
            c["leitura_pagina"] = pagina
        self._gravar(plano, c)
