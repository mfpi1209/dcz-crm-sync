"""Envio real de um lote pelo CRM: etiqueta de lote → campanha → lançar → acompanhar.

Só roda com tudo junto: `MOTOR_ENABLED=1`, `MOTOR_DRY_RUN=0`, motor ligado na
página e plano do dia aprovado. Sem isso o cliente do CRM nem aceita escrita.

Etapas de um lote (coluna `status` de `motor_lotes`):
  etiquetando  cria a etiqueta do lote e etiqueta os contatos, dentro do
               orçamento de chamadas (cerca de 50 por minuto), em vários ticks
  enviando     campanha TEMPLATE criada com `filters.tagIds`, conferida e
               lançada a 1 mensagem por segundo; resultado lido a cada minuto
  pausado      pausa de emergência (fila 60 acima do nível do lote, falha acima
               de 6%, trava de experiência, fim do expediente); retoma quando o
               governador libera
  concluido / cancelado / erro

Cada passo que escreve no CRM é registrado antes em `motor_acoes`, com chave
própria. Passo que ficou "iniciado" sem resposta (queda no meio) não é
repetido às cegas: o lote vai para `erro` e pede uma pessoa.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Mapping, Optional

from motor.calendario import para_brt
from motor.config import MotorConfig
from motor.crm import BAIXA, CrmErro, CrmMotor
from motor.modelos import Lote, Mensagem

MESES = ("janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro")
NOME = "{{contact.name | first}}"
INTERVALO_STATS = timedelta(seconds=60)
LEITURAS = (timedelta(hours=2), timedelta(hours=24))


def componentes_do_template(mensagem: str, ciclo: str) -> dict:
    """Variáveis do corpo: {{1}} primeiro nome, {{2}} mês, {{3}} prazo. O nome é resolvido pelo CRM por destinatário."""
    mes = int(ciclo.split("-")[1])
    prazo = {Mensagem.LEMBRETE_DIA10.value: 10, Mensagem.LEMBRETE_DIA25.value: 25, Mensagem.AVISO_CALOURO.value: 25}.get(mensagem)
    textos = [NOME] + ([MESES[mes - 1], f"{prazo:02d}/{mes:02d}"] if prazo else [])
    return {"version": 1, "components": [{"type": "body", "parameters": [{"type": "text", "text": t} for t in textos]}]}


def configuracao(mensagem: str) -> Optional[dict]:
    """Canal e template da mensagem, por ambiente. None = não configurado (o lote não sai)."""
    canal = (os.getenv("MOTOR_CANAL_ID") or "").strip()
    template = (os.getenv(f"MOTOR_TEMPLATE_{mensagem.upper()}") or "").strip()
    if not canal or not template:
        return None
    return {"canal": canal, "template": template, "idioma": (os.getenv("MOTOR_TEMPLATE_IDIOMA") or "pt_BR").strip()}


class Envio:
    def __init__(self, cfg: MotorConfig, razao, crm: CrmMotor):
        self.cfg = cfg
        self.razao = razao
        self.crm = crm
        self._leu_destinatarios_em: Optional[datetime] = None
        self._aguardando: Optional[int] = None

    # ── começo de um lote ─────────────────────────────────────────────────
    def impedimento(self, mensagem: str) -> Optional[str]:
        if not self.crm.permitir_escrita:
            return "cliente do CRM com a escrita travada"
        if configuracao(mensagem) is None:
            return f"canal (MOTOR_CANAL_ID) ou template (MOTOR_TEMPLATE_{mensagem.upper()}) não configurado"
        return None

    def iniciar(self, alvo: Mapping, lote: Lote, estados: Mapping[str, str], *, ciclo: str, agora: datetime, fila: Optional[dict], motivo: str) -> None:
        """Reserva os alunos do lote no livro-razão. A etiquetagem começa no tick seguinte."""
        self.razao.liberar_lote(
            alvo["id"], lote, estados, ciclo=ciclo, dia=para_brt(agora).date(), simulado=False,
            status="etiquetando", motivo=motivo, fila=fila, agora=agora,
        )
        self.razao.registrar_acao(f"real:{alvo['id']}:reservar", "reservar_lote", lote_id=alvo["id"], simulado=False,
                                  requisicao={"mensagem": lote.mensagem.value, "destinatarios": lote.tamanho, "ciclo": ciclo})

    # ── a cada tick ───────────────────────────────────────────────────────
    def avancar(self, agora: datetime, *, pausar: Optional[str] = None, parada: Optional[str] = None, retomar: bool = False,
                fim_da_janela: bool = False, aguardando: Optional[int] = None) -> None:
        self._aguardando = aguardando
        hoje = para_brt(agora).date()
        for lote in self.razao.lotes_ativos():
            try:
                if parada:
                    self._cancelar(lote, f"parada geral: {parada}".rstrip(": "), "cancelado")
                elif lote["dia"] < hoje:
                    self._cancelar(lote, "lote de dia anterior não é retomado: a lista é refeita", "cancelado")
                elif lote["status"] == "etiquetando":
                    if fim_da_janela:
                        self._cancelar(lote, "a janela fechou antes de o lote ser lançado", "cancelado")
                    elif not pausar:
                        self._etiquetar(lote, agora)
                elif lote["status"] == "enviando":
                    if pausar:
                        self._pausar(lote, pausar)
                    self._ler_resultado(lote, agora)
                elif lote["status"] == "pausado":
                    if retomar:
                        self._retomar(lote)
                    self._ler_resultado(lote, agora)
            except CrmErro as e:
                if e.status == 403:
                    self._sem_permissao(lote, e)
                    continue
                self.razao.diario("erro", "trilhos", f"Lote {lote['ordem']}: o CRM respondeu {e.status} ({e}). Tento de novo no próximo ciclo.")
        if not parada:
            self._ler_destinatarios(agora)

    def _sem_permissao(self, lote: Mapping, erro: CrmErro) -> None:
        """403: o token não tem a permissão. Registra qual faltou e para o lote, sem tentar de novo."""
        if lote["status"] == "enviando" and lote["crm_campaign_id"]:
            try:  # sem conseguir acompanhar, o mais seguro é pausar o que está saindo
                self.crm.pausar_campanha(lote["crm_campaign_id"])
            except CrmErro:
                pass
        self._erro(lote, f"falta permissão no token do motor: {erro.permissao or 'não informada'}. "
                         "Corrija o papel do usuário do token e reinicie o worker")

    def _erro(self, lote: Mapping, texto: str) -> None:
        self.razao.atualizar_lote(lote["id"], status="erro", motivo=texto[:500])
        self.razao.diario("erro", "trilhos", f"Lote {lote['ordem']} parado e esperando uma pessoa: {texto}")

    def _uma_vez(self, lote: Mapping, passo: str, requisicao: dict) -> Optional[dict]:
        """Registra o passo antes de chamar o CRM.

        Devolve None se pode chamar agora, a resposta gravada se já foi feito,
        e levanta `_Incerto` se ficou iniciado sem resposta (queda no meio ou
        erro de rede: o CRM pode ter executado).
        """
        chave = f"real:{lote['id']}:{passo}"
        if self.razao.registrar_acao(chave, passo, lote_id=lote["id"], simulado=False, requisicao=requisicao):
            return None
        feita = self.razao.acao(chave) or {}
        if feita.get("status") == "feito":
            return feita.get("resposta") or {}
        if feita.get("status") == "falhou":  # o CRM recusou com certeza: pode tentar de novo
            self.razao.concluir_acao(chave, "iniciada", None)
            return None
        raise _Incerto(passo)

    def _concluir(self, lote: Mapping, passo: str, resposta: dict) -> None:
        self.razao.concluir_acao(f"real:{lote['id']}:{passo}", "feito", resposta)

    def _recusado(self, lote: Mapping, passo: str, erro: CrmErro) -> None:
        """Resposta 4xx: o CRM não executou. Libera o passo para nova tentativa."""
        if 400 <= erro.status < 500:
            self.razao.concluir_acao(f"real:{lote['id']}:{passo}", "falhou", {"status": erro.status})

    def _etiquetar(self, lote: Mapping, agora: datetime) -> None:
        tag_id = lote["crm_tag_id"]
        if not tag_id:
            nome = f"motor-{lote['dia']:%Y%m%d}-l{lote['ordem']}-{lote['mensagem']}"
            try:
                feita = self._uma_vez(lote, "etiqueta", {"nome": nome})
            except _Incerto:
                feita = None  # o nome é único: se já existir, o CRM responde 409 e o id é buscado pelo nome
            if feita is None:
                try:
                    tag_id = str(self.crm.criar_etiqueta(nome).get("id") or "")
                except CrmErro as e:
                    if e.status != 409:
                        self._recusado(lote, "etiqueta", e)
                        raise
                    tag_id = next((str(t.get("id")) for t in self.crm.etiquetas() if t.get("name") == nome), "")
                if not tag_id:
                    return self._erro(lote, "o CRM não devolveu o id da etiqueta do lote")
                self._concluir(lote, "etiqueta", {"id": tag_id})
            else:
                tag_id = str(feita.get("id") or "")
            self.razao.atualizar_lote(lote["id"], crm_tag_id=tag_id)

        # Etiqueta os contatos dentro do orçamento de prioridade baixa (cerca de 50 por minuto).
        cabem = self.crm.limite.disponiveis(BAIXA)
        pendentes = self.razao.contatos_do_lote(lote["id"], "reservado", limite=max(0, cabem))
        feitos, falharam = [], []
        for cid in pendentes:
            try:
                self.crm.etiquetar_contato(cid, tag_id)
                feitos.append(cid)
            except CrmErro as e:
                if e.status in (400, 404):  # contato apagado ou inválido: sai do lote, o resto segue
                    falharam.append(cid)
                    continue
                if e.status == 403:  # sem permissão para etiquetar: não adianta seguir
                    self.razao.marcar_mensagens(lote["id"], feitos, "etiquetado")
                    raise
                break
        self.razao.marcar_mensagens(lote["id"], feitos, "etiquetado")
        self.razao.marcar_mensagens(lote["id"], falharam, "falhou", "contato recusado ao etiquetar")
        if feitos or falharam:
            self.razao.atualizar_lote(lote["id"], etiquetados=int(lote["etiquetados"] or 0) + len(feitos))
        if self.razao.contatos_do_lote(lote["id"], "reservado", limite=1):
            return  # continua no próximo tick
        self._lancar(lote, tag_id, agora)

    def _lancar(self, lote: Mapping, tag_id: str, agora: datetime) -> None:
        etiquetados = self.razao.contatos_do_lote(lote["id"], "etiquetado", limite=5000)
        if not etiquetados:
            return self._erro(lote, "nenhum contato etiquetado")
        cfg_msg = configuracao(lote["mensagem"])
        if cfg_msg is None:
            return self._erro(lote, "canal ou template deixou de estar configurado")
        ciclo = f"{lote['dia'].year:04d}-{lote['dia'].month:02d}"
        passo = "campanha"
        try:
            feita = self._uma_vez(lote, "campanha", {"etiqueta": tag_id, "template": cfg_msg["template"], "destinatarios": len(etiquetados)})
            if feita is None:
                campanha = self.crm.criar_campanha(
                    nome=f"Motor {lote['dia']:%d/%m} lote {lote['ordem']} ({lote['mensagem']})",
                    canal_id=cfg_msg["canal"], template=cfg_msg["template"], idioma=cfg_msg["idioma"],
                    componentes=componentes_do_template(lote["mensagem"], ciclo), tag_ids=[tag_id], velocidade=self.cfg.velocidade,
                )
                feita = {"id": str(campanha.get("id") or "")}
                self._concluir(lote, "campanha", feita)
            campanha_id = str(feita.get("id") or "")
            if not campanha_id:
                return self._erro(lote, "o CRM não devolveu o id da campanha")
            self.razao.atualizar_lote(lote["id"], crm_campaign_id=campanha_id)

            passo = "lancar"
            rascunho = self.crm.campanha(campanha_id)
            try:
                pode_lancar = self._uma_vez(lote, "lancar", {"campanha": campanha_id}) is None
            except _Incerto:
                # Lançamento sem resposta gravada: o status da campanha no CRM diz se chegou a sair.
                pode_lancar = rascunho.get("status") == "DRAFT"
            if pode_lancar:
                # Confere o rascunho antes de lançar: público tem de ser só a etiqueta do lote.
                if rascunho.get("status") != "DRAFT" or (rascunho.get("filters") or {}) != {"tagIds": [tag_id]}:
                    self.crm.cancelar_campanha(campanha_id)
                    return self._erro(lote, "rascunho da campanha diferente do esperado; cancelado sem enviar")
                resposta = self.crm.lancar_campanha(campanha_id)
                self._concluir(lote, "lancar", {"status": resposta.get("status")})
        except _Incerto:
            return self._erro(lote, "a criação da campanha ficou sem resposta do CRM; confira as campanhas antes de continuar")
        except CrmErro as e:
            self._recusado(lote, passo, e)
            if e.status == 429 or e.status >= 500 or e.status == 403:
                raise  # 429/5xx: tenta de novo no próximo tick; 403: quem chamou registra a permissão e para o lote
            return self._erro(lote, f"o CRM recusou o passo '{passo}' ({e.status}: {e})")
        self.razao.marcar_mensagens(lote["id"], etiquetados, "enviado")
        # A vida mínima do lote conta do lançamento, não da reserva.
        # O nível da fila que vale para o governador é o do lançamento (a etiquetagem leva minutos).
        campos = {"fila": {"aguardando": self._aguardando}} if self._aguardando is not None else {}
        self.razao.atualizar_lote(lote["id"], status="enviando", crm_status="PROCESSING", tamanho=len(etiquetados), liberado_em=agora, **campos)
        self.razao.diario("lote", "trilhos", f"Lote {lote['ordem']} ({lote['mensagem']}) LANÇADO: {len(etiquetados)} alunos, {self.cfg.velocidade} mensagem por segundo.",
                          {"ordem": lote["ordem"], "tamanho": len(etiquetados), "campanha": campanha_id})

    def _pausar(self, lote: Mapping, motivo: str) -> None:
        try:
            self.crm.pausar_campanha(lote["crm_campaign_id"])
        except CrmErro as e:
            if e.status != 409:  # 409 = já não está enviando (terminou ou já pausada)
                raise
            return
        self.razao.atualizar_lote(lote["id"], status="pausado", motivo=motivo[:500], crm_status="PAUSED")
        self.razao.diario("pausa", "trilhos", f"Lote {lote['ordem']} PAUSADO: {motivo}.")

    def _retomar(self, lote: Mapping) -> None:
        try:
            self.crm.retomar_campanha(lote["crm_campaign_id"])
        except CrmErro as e:
            if e.status != 409:
                raise
        self.razao.atualizar_lote(lote["id"], status="enviando", motivo="", crm_status="SENDING")
        self.razao.diario("pausa", "trilhos", f"Lote {lote['ordem']} retomado: condições voltaram ao normal.")

    def _cancelar(self, lote: Mapping, motivo: str, status: str) -> None:
        if lote["crm_campaign_id"]:
            try:
                self.crm.cancelar_campanha(lote["crm_campaign_id"])
            except CrmErro as e:
                if e.status != 409:  # 409 = já terminou ou já cancelada
                    raise
        pendentes = self.razao.cancelar_pendentes(lote["id"])
        self.razao.atualizar_lote(lote["id"], status=status, motivo=motivo[:500], crm_status="CANCELLED" if lote["crm_campaign_id"] else None)
        self.razao.diario("pausa", "trilhos", f"Lote {lote['ordem']} CANCELADO: {motivo} ({pendentes} alunos não chegaram a ser enviados).")

    def _ler_resultado(self, lote: Mapping, agora: datetime) -> None:
        """Mede o lote pelos destinatários em cada status (a cada 1 min por campanha ativa).

        O `stats` do CRM dá o status da campanha, o total e os respondidos; o
        que foi enviado, entregue, lido e falhou vem da contagem por status.
        """
        if lote["stats_em"] and para_brt(agora) - para_brt(lote["stats_em"]) < INTERVALO_STATS:
            return
        st = self.crm.campanha_stats(lote["crm_campaign_id"])
        cont = self.crm.campanha_contagem(lote["crm_campaign_id"])
        total = int(st.get("totalRecipients") or 0)
        falhas = cont.get("FAILED", 0)
        processados = cont.get("SENT", 0) + cont.get("DELIVERED", 0) + cont.get("READ", 0) + falhas
        self.razao.atualizar_lote(
            lote["id"], enviados=processados, entregues=cont.get("DELIVERED", 0) + cont.get("READ", 0), lidos=cont.get("READ", 0),
            respondidos=int(st.get("repliedCount") or 0), falhas=falhas, crm_status=str(st.get("status") or ""), stats_em=agora,
        )
        # Trilho: a campanha nunca pode ter mais gente que o lote (etiqueta casando onde não devia).
        if total > int(lote["tamanho"] or 0):
            self._cancelar(lote, f"campanha com {total} destinatários para um lote de {lote['tamanho']}", "erro")
            return
        status = str(st.get("status") or "")
        if status == "COMPLETED":
            # Campanha concluída com destinatário ainda pendente: o CRM não enviou para ele.
            sem_envio = max(0, total - processados)
            self.razao.atualizar_lote(lote["id"], status="concluido", motivo=f"{sem_envio} não enviados pelo CRM" if sem_envio else "")
            self.razao.diario("lote", "trilhos", f"Lote {lote['ordem']} concluído: {processados} processados, {falhas} falhas"
                              + (f", {sem_envio} não enviados" if sem_envio else "") + ".")
        elif status in ("CANCELLED", "FAILED"):
            self.razao.atualizar_lote(lote["id"], status="cancelado", motivo=f"campanha {status} no CRM")

    def _ler_destinatarios(self, agora: datetime) -> None:
        """Leitura por aluno 2 h e 24 h depois do lançamento: uma página por vez, no máximo uma a cada 10 s."""
        if self._leu_destinatarios_em and para_brt(agora) - para_brt(self._leu_destinatarios_em) < timedelta(seconds=10):
            return
        for lote in self.razao.lotes_para_leitura():
            feitas = int(lote["leituras"] or 0)
            if not lote["liberado_em"] or para_brt(agora) - para_brt(lote["liberado_em"]) < LEITURAS[feitas]:
                continue
            pagina = int(lote["leitura_pagina"] or 0) + 1
            itens, total_paginas = self.crm.campanha_destinatarios_pagina(lote["crm_campaign_id"], pagina)
            self._leu_destinatarios_em = agora
            self.razao.aplicar_destinatarios(lote["id"], itens)
            if pagina >= total_paginas or not itens:
                self.razao.atualizar_lote(lote["id"], leituras=feitas + 1, leitura_pagina=0)
            else:
                self.razao.atualizar_lote(lote["id"], leitura_pagina=pagina)
            return


class _Incerto(Exception):
    """Passo registrado como iniciado e sem resposta gravada: não se repete sem uma pessoa conferir."""
