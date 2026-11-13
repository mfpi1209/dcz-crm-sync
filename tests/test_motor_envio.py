"""Rotas de integração do CRM e envio real, contra um CRM de mentira. Nenhum POST sai para a rede."""

from __future__ import annotations

import json
import os
import re
import unittest
import urllib.parse
from dataclasses import replace
from datetime import date, datetime, timedelta
from unittest import mock

from motor.calendario import BRT
from motor.config import MotorConfig
from motor.crm import ALTA, BAIXA, CrmErro, CrmMotor, EscritaBloqueada, LimiteDeTaxa, limpar_destinatario
from motor.envio import Envio, componentes_do_template, configuracao
from motor.laco import Laco
from motor.modelos import Controle
from motor.razao import RazaoMemoria
from motor.sorteio import no_controle
from tests.test_motor_laco import Relogio, Transporte, _deal, _fila_json

DIA = date(2026, 11, 24)
AMBIENTE = {"MOTOR_CANAL_ID": "canal1", "MOTOR_TEMPLATE_LEMBRETE_DIA25": "motor_lembrete_dia25_a"}


class CrmFalso(Transporte):
    """CRM de mentira com etiquetas e campanhas. Guarda tudo o que recebeu."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.tags: dict[str, str] = {}            # nome → id
        self.etiquetados: dict[str, set] = {}     # tag id → contatos
        self.campanhas: dict[str, dict] = {}
        self.corpos: list[tuple[str, str, dict]] = []
        self.horas: list[datetime] = []
        self.conclui_sozinha = True               # a campanha termina na primeira leitura de resultado
        self.publico_a_mais = 0                   # simula etiqueta casando onde não devia
        self.contatos_invalidos: set[str] = set()
        self.falhas = 0
        self.presos = 0                           # destinatários que ficam PENDING numa campanha concluída
        self.negar: dict[tuple[str, str], str] = {}  # (método, começo do caminho) → permissão que falta ao token

    def __call__(self, metodo, caminho, corpo=None):
        if self.agora:
            self.horas.append(self.agora())
        url = urllib.parse.urlparse(caminho)
        partes = url.path.strip("/").split("/")
        for (m, comeco), falta in self.negar.items():
            if metodo == m and url.path.startswith(comeco):
                self.chamadas.append((metodo, caminho))
                return 403, {}, {"message": "Acesso negado.", "required": falta}
        if metodo != "GET":
            self.chamadas.append((metodo, caminho))
            self.corpos.append((metodo, url.path, corpo))
        if url.path == "/api/tags" and metodo == "POST":
            if corpo["name"] in self.tags:
                return 409, {}, {"message": "Etiqueta já existe."}
            self.tags[corpo["name"]] = f"tag{len(self.tags) + 1}"
            return 201, {}, {"id": self.tags[corpo["name"]], "name": corpo["name"]}
        if url.path == "/api/tags" and metodo == "GET":
            self.chamadas.append((metodo, caminho))
            return 200, {}, [{"id": i, "name": n} for n, i in self.tags.items()]
        if partes[:2] == ["api", "contacts"] and partes[-1] == "tags" and metodo == "POST":
            if partes[2] in self.contatos_invalidos:
                return 404, {}, {"message": "Contato não encontrado."}
            self.etiquetados.setdefault(corpo["tagId"], set()).add(partes[2])
            return 200, {}, {"ok": True}
        if partes[:3] == ["api", "integrations", "campaigns"]:
            return self._campanha(metodo, partes[3:], corpo, urllib.parse.parse_qs(url.query), caminho)
        return super().__call__(metodo, caminho, corpo)

    def _campanha(self, metodo, resto, corpo, q, caminho):
        if metodo == "GET":
            self.chamadas.append((metodo, caminho))
        if not resto and metodo == "POST":
            cid = f"camp{len(self.campanhas) + 1}"
            self.campanhas[cid] = {"id": cid, "status": "DRAFT", **corpo}
            return 201, {}, {"campaign": self.campanhas[cid]}
        c = self.campanhas.get(resto[0])
        if c is None:
            return 404, {}, {"message": "Campanha não encontrada."}
        publico = sorted(self.etiquetados.get(c["filters"]["tagIds"][0], set()))
        if len(resto) == 1:
            return 200, {}, {"campaign": c}
        acao = resto[1]
        transicoes = {"launch": ({"DRAFT"}, "PROCESSING"), "pause": ({"PROCESSING", "SENDING"}, "PAUSED"),
                      "resume": ({"PAUSED"}, "SENDING"), "cancel": ({"DRAFT", "PROCESSING", "SENDING", "PAUSED"}, "CANCELLED")}
        if acao in transicoes and metodo == "POST":
            de, para = transicoes[acao]
            if c["status"] not in de:
                return 409, {}, {"message": "Status não permite."}
            c["status"] = para
            return 200, {}, {"message": "ok", "status": para}
        if acao == "stats":
            if self.conclui_sozinha and c["status"] in ("PROCESSING", "SENDING"):
                c["status"] = "COMPLETED"
            # Como em produção: o stats diz 0 pendentes mesmo havendo destinatário PENDING.
            return 200, {}, {"status": c["status"], "totalRecipients": len(publico) + self.publico_a_mais, "pendingCount": 0,
                             "deliveryRate": 100, "repliedCount": 0}
        if acao == "recipients":
            por_contato = self._status_dos_destinatarios(c, publico)
            filtro = q.get("status", [""])[0]
            escolhidos = [cid for cid in publico if not filtro or por_contato[cid] == filtro]
            pagina, por = int(q.get("page", ["1"])[0]), int(q.get("perPage", ["50"])[0])
            fatia = escolhidos[(pagina - 1) * por: pagina * por]
            itens = [{"id": f"r{cid}", "contactId": cid, "contact": {"id": cid, "name": "Fulano de Tal", "phone": "5511999990000"},
                      "status": por_contato[cid], "errorCode": 131026 if por_contato[cid] == "FAILED" else None,
                      "errorMessage": "Número 5511999990000 sem WhatsApp (code 131026)" if por_contato[cid] == "FAILED" else None,
                      "sentAt": "2026-11-24T12:10:00.000Z", "deliveredAt": "2026-11-24T12:10:05.000Z", "readAt": None, "repliedAt": None}
                     for cid in fatia]
            return 200, {}, {"items": itens, "total": len(escolhidos), "page": pagina, "perPage": por, "totalPages": -(-len(escolhidos) // por)}
        return 404, {}, {"message": "rota não prevista no teste"}

    def _status_dos_destinatarios(self, campanha, publico) -> dict:
        """Campanha concluída: `falhas` com FAILED, `presos` ainda PENDING, o resto DELIVERED. Antes disso, tudo PENDING."""
        if campanha["status"] != "COMPLETED":
            return {cid: "PENDING" for cid in publico}
        return {cid: "FAILED" if n < self.falhas else "PENDING" if n < self.falhas + self.presos else "DELIVERED" for n, cid in enumerate(publico)}



class LimiteComPrioridadeTest(unittest.TestCase):
    def test_baixa_para_na_reserva_e_alta_usa_o_resto(self) -> None:
        r = Relogio()
        limite = LimiteDeTaxa(60, 10, r.agora, r.dormir)
        for _ in range(50):
            self.assertEqual(limite.aguardar(BAIXA), 0.0)
        self.assertEqual(limite.disponiveis(BAIXA), 0)
        self.assertEqual(limite.disponiveis(ALTA), 10)
        for _ in range(10):
            self.assertEqual(limite.aguardar(ALTA), 0.0)      # fila, resultado e pausa ainda passam
        self.assertGreater(limite.aguardar(ALTA), 0.0)         # 61ª chamada do minuto espera
        self.assertGreaterEqual(r.t, 60.0)

    def test_nunca_passa_de_60_por_minuto(self) -> None:
        r = Relogio()
        limite = LimiteDeTaxa(60, 10, r.agora, r.dormir)
        marcas = []
        for i in range(400):
            limite.aguardar(ALTA if i % 7 == 0 else BAIXA)
            marcas.append(r.t)
        self.assertTrue(all(marcas[i] - marcas[i - 60] >= 60.0 for i in range(60, len(marcas))))


class ClienteDasRotasDeIntegracaoTest(unittest.TestCase):
    def _crm(self, t, escrita=True) -> CrmMotor:
        r = Relogio()
        return CrmMotor(transporte=t, permitir_escrita=escrita, relogio=r.agora, dormir=r.dormir)

    def test_campanha_usa_as_rotas_de_integracao(self) -> None:
        t = CrmFalso()
        crm = self._crm(t)
        tag = crm.criar_etiqueta("lote-1")["id"]
        crm.etiquetar_contato("c1", tag)
        camp = crm.criar_campanha(nome="n", canal_id="canal1", template="tpl", idioma="pt_BR",
                                  componentes=componentes_do_template("lembrete_dia25", "2026-11"), tag_ids=[tag], velocidade=1)
        self.assertEqual(camp["status"], "DRAFT")
        self.assertEqual(crm.campanha(camp["id"])["filters"], {"tagIds": [tag]})
        self.assertEqual(crm.lancar_campanha(camp["id"])["status"], "PROCESSING")
        self.assertEqual(crm.pausar_campanha(camp["id"])["status"], "PAUSED")
        self.assertEqual(crm.retomar_campanha(camp["id"])["status"], "SENDING")
        self.assertEqual(crm.cancelar_campanha(camp["id"])["status"], "CANCELLED")
        caminhos = [c for _, c in t.chamadas]
        self.assertIn("/api/integrations/campaigns", caminhos)
        for acao in ("launch", "pause", "resume", "cancel"):
            self.assertIn(f"/api/integrations/campaigns/camp1/{acao}", caminhos)
        _, _, corpo = next(c for c in t.corpos if c[1] == "/api/integrations/campaigns")
        self.assertEqual((corpo["type"], corpo["sendRate"], corpo["filters"], corpo["channelId"]), ("TEMPLATE", 1, {"tagIds": [tag]}, "canal1"))
        textos = [p["text"] for p in corpo["templateComponents"]["components"][0]["parameters"]]
        self.assertEqual(textos, ["{{contact.name | first}}", "novembro", "25/11"])

    def test_409_ao_pausar_campanha_terminada_vira_erro_com_status(self) -> None:
        t = CrmFalso()
        crm = self._crm(t)
        tag = crm.criar_etiqueta("x")["id"]
        camp = crm.criar_campanha(nome="n", canal_id="c", template="t", idioma="pt_BR", componentes=[], tag_ids=[tag])
        with self.assertRaises(CrmErro) as ctx:
            crm.pausar_campanha(camp["id"])  # ainda em rascunho
        self.assertEqual(ctx.exception.status, 409)

    def test_destinatarios_chegam_sem_nome_telefone_nem_texto_de_erro(self) -> None:
        t = CrmFalso()
        crm = self._crm(t)
        tag = crm.criar_etiqueta("x")["id"]
        for i in range(3):
            crm.etiquetar_contato(f"c{i}", tag)
        camp = crm.criar_campanha(nome="n", canal_id="c", template="t", idioma="pt_BR", componentes=[], tag_ids=[tag])
        t.falhas, t.presos = 1, 1
        t.campanhas[camp["id"]]["status"] = "COMPLETED"
        itens, paginas = crm.campanha_destinatarios_pagina(camp["id"], 1)
        self.assertEqual(paginas, 1)
        self.assertEqual({i["contact_id"] for i in itens}, {"c0", "c1", "c2"})
        self.assertEqual(sorted(itens[0]), ["contact_id", "entregue_em", "enviado_em", "erro_codigo", "lido_em", "respondeu_em", "status"])
        self.assertEqual((itens[0]["status"], itens[0]["erro_codigo"]), ("FAILED", 131026))
        # A medida do lote vem do total por status, com um item por chamada.
        self.assertEqual(crm.campanha_contagem(camp["id"]), {"SENT": 0, "DELIVERED": 1, "READ": 0, "FAILED": 1})
        self.assertTrue(all("perPage=1" in c for _, c in t.chamadas[-4:]))
        bruto = repr(itens)
        self.assertNotIn("Fulano", bruto)
        self.assertNotIn("5511999990000", bruto)
        self.assertEqual(limpar_destinatario({"contactId": "c9", "status": "SENT", "errorCode": "other"})["erro_codigo"], None)

    def test_em_simulacao_as_rotas_de_integracao_que_escrevem_ficam_travadas(self) -> None:
        t = CrmFalso()
        crm = self._crm(t, escrita=False)
        for chamada in (lambda: crm.criar_etiqueta("x"), lambda: crm.lancar_campanha("c"), lambda: crm.pausar_campanha("c"),
                        lambda: crm.cancelar_campanha("c"), lambda: crm.etiquetar_contato("c1", "t1")):
            with self.assertRaises(EscritaBloqueada):
                chamada()
        self.assertEqual(t.chamadas, [])

    def test_403_guarda_a_permissao_que_faltou_e_nao_chama_de_novo(self) -> None:
        t = CrmFalso()
        t.negar[("GET", "/api/integrations/campaigns")] = "campaign:view"
        crm = self._crm(t)
        with self.assertRaises(CrmErro) as ctx:
            crm.campanha_stats("camp1")
        self.assertEqual((ctx.exception.status, ctx.exception.permissao), (403, "campaign:view"))
        self.assertEqual(len(t.chamadas), 1)
        with self.assertRaises(CrmErro) as ctx:
            crm.campanha_stats("camp2")              # mesma rota, outra campanha
        self.assertEqual(ctx.exception.permissao, "campaign:view")
        self.assertEqual(len(t.chamadas), 1)            # não foi à rede de novo
        self.assertEqual(crm.negadas, {("GET", "/api/integrations/campaigns/{id}/stats"): "campaign:view"})
        crm.criar_etiqueta("x")                       # outra rota segue normal

    def test_template_e_canal_vem_do_ambiente(self) -> None:
        self.assertIsNone(configuracao("lembrete_dia25"))
        with mock.patch.dict(os.environ, AMBIENTE):
            self.assertEqual(configuracao("lembrete_dia25"), {"canal": "canal1", "template": "motor_lembrete_dia25_a", "idioma": "pt_BR"})
            self.assertIsNone(configuracao("firme"))
        self.assertEqual(len(componentes_do_template("firme", "2026-11")["components"][0]["parameters"]), 1)


class EnvioRealTest(unittest.TestCase):
    """Terça 24/11/2026 em modo real, com 700 alunos de graduação em aberto e um CRM de mentira."""

    CFG = MotorConfig(enabled=True, dry_run=False, carga_min_abertos=100, janela_fim_h=14)

    def setUp(self) -> None:
        amb = mock.patch.dict(os.environ, AMBIENTE)
        amb.start()
        self.addCleanup(amb.stop)
        self.agora = datetime(2026, 11, 24, 7, 0, tzinfo=BRT)
        self.crm_falso = CrmFalso(deals=[_deal(i) for i in range(700)], fila=_fila_json(20, 60_000, 10), agora=lambda: self.agora)
        self.razao = RazaoMemoria()
        self.razao.controle = Controle(ligado=True)
        self.montar(self.CFG)

    def montar(self, cfg, escrita=None) -> None:
        def dormir(s):
            self.agora += timedelta(seconds=s)

        escrita = (cfg.enabled and not cfg.dry_run) if escrita is None else escrita
        self.crm = CrmMotor(transporte=self.crm_falso, rpm=cfg.crm_rpm, reserva_alta=cfg.crm_reserva_alta, permitir_escrita=escrita,
                            relogio=lambda: self.agora.timestamp(), dormir=dormir)
        self.laco = Laco(cfg, self.razao, crm=self.crm, envio=Envio(cfg, self.razao, self.crm), relogio=lambda: self.agora)

    def ate(self, hora: int, minuto: int = 0, parar=None) -> None:
        fim = self.agora.replace(hour=hora, minute=minuto, second=0)
        while self.agora <= fim:
            self.laco.tick()
            if parar and parar():
                return
            self.agora += timedelta(seconds=30)

    def aprovar(self) -> None:
        self.ate(8, 31)
        self.assertTrue(self.razao.aprovar_plano(DIA, False, "marcelo"))

    def escritas(self) -> list[tuple[str, str]]:
        return [c for c in self.crm_falso.chamadas if c[0] != "GET"]

    def lote(self, ordem=1) -> dict:
        return next(l for l in self.razao.lotes(DIA, False) if l["ordem"] == ordem)

    def test_sem_plano_aprovado_nada_e_escrito_no_crm(self) -> None:
        self.ate(11, 0)
        self.assertEqual(self.escritas(), [])
        self.assertEqual(self.razao.plano(DIA, False)["status"], "proposto")
        self.assertTrue(any("não aprovado" in e["texto"] for e in self.razao.entradas_diario))
        self.assertEqual(self.razao.msgs, [])

    def test_as_quatro_chaves_precisam_estar_juntas(self) -> None:
        # Plano aprovado, mas com MOTOR_DRY_RUN=1: segue simulando, sem escrita.
        self.montar(replace(self.CFG, dry_run=True))
        self.ate(8, 31)
        self.razao.aprovar_plano(DIA, True, "marcelo")
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        self.assertTrue(self.razao.msgs and all(m["simulado"] for m in self.razao.msgs))
        # Desligado na página: nada sai, nem em modo real com plano aprovado.
        self.setUp()
        self.razao.controle = Controle(ligado=False)
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        # MOTOR_ENABLED=0 vence tudo.
        self.setUp()
        self.montar(replace(self.CFG, enabled=False), escrita=True)
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])

    def test_lote_real_etiqueta_cria_confere_lanca_e_acompanha(self) -> None:
        self.aprovar()
        self.ate(12, 30)
        l1, l2 = self.lote(1), self.lote(2)
        self.assertEqual((l1["tamanho"], l1["status"]), (300, "concluido"))        # rampa: 1º lote com 300
        self.assertEqual(l2["status"], "concluido")
        controle = {f"c{i}" for i in range(700) if no_controle(f"c{i}", "2026-11", self.CFG.controle_sal, self.CFG.controle_pct)}
        self.assertEqual(l1["tamanho"] + l2["tamanho"], 700 - len(controle))

        # Só as rotas previstas foram escritas, e na ordem certa para o lote 1.
        permitidas = re.compile(r"^/api/(tags|contacts/[^/]+/tags|integrations/campaigns(/camp\d+/launch)?)$")
        self.assertTrue(all(m == "POST" and permitidas.match(c) for m, c in self.escritas()), self.escritas()[:3])
        ordem = [c for _, c in self.escritas()]
        self.assertEqual(ordem[0], "/api/tags")
        self.assertLess(ordem.index("/api/integrations/campaigns"), ordem.index("/api/integrations/campaigns/camp1/launch"))
        self.assertEqual(ordem.index("/api/integrations/campaigns"), 301)  # etiqueta + 300 contatos antes da campanha

        # Público de cada campanha = exatamente os alunos do lote, sem ninguém do controle.
        etiquetados = set().union(*self.crm_falso.etiquetados.values())
        self.assertEqual(etiquetados, {m["contact_id"] for m in self.razao.msgs})
        self.assertFalse(etiquetados & controle)
        camp = self.crm_falso.campanhas["camp1"]
        self.assertEqual((camp["sendRate"], camp["templateName"], camp["channelId"], camp["type"]), (1, "motor_lembrete_dia25_a", "canal1", "TEMPLATE"))
        self.assertEqual(camp["filters"], {"tagIds": [l1["crm_tag_id"]]})
        self.assertEqual(len(self.crm_falso.etiquetados[l1["crm_tag_id"]]), 300)
        # "enviado" até a leitura de destinatários (2 h depois), que troca pelo status de cada um.
        self.assertTrue(all(m["status"] in ("enviado", "entregue", "lido", "falhou") and not m["simulado"] for m in self.razao.msgs))

        # O 2º lote só sai 30 minutos depois do LANÇAMENTO do 1º.
        self.assertGreaterEqual(l2["liberado_em"] - l1["liberado_em"], timedelta(minutes=30))
        # Orçamento: nunca mais de 60 chamadas em qualquer minuto, e etiquetagem a no máximo 50 por minuto.
        horas = self.crm_falso.horas
        self.assertTrue(all(horas[i] - horas[i - 60] >= timedelta(seconds=60) for i in range(60, len(horas))))
        self.assertTrue(all(a["status"] == "feito" for k, a in self.razao.acoes.items() if k.startswith("real:") and not k.endswith(":reservar")))

    def test_fila_que_sobe_60_pausa_a_campanha_e_a_melhora_retoma(self) -> None:
        self.crm_falso.conclui_sozinha = False
        self.aprovar()
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] == "enviando")
        self.assertEqual(self.lote(1)["status"], "enviando")
        self.crm_falso.fila = _fila_json(aguardando=178, online=13)
        self.agora += timedelta(seconds=30)
        self.laco.tick()
        self.assertEqual(self.lote(1)["status"], "pausado")
        self.assertIn(("POST", "/api/integrations/campaigns/camp1/pause"), self.escritas())
        self.assertEqual(self.crm_falso.campanhas["camp1"]["status"], "PAUSED")
        self.assertIn("fila subiu 158", self.lote(1)["motivo"])           # de 20, no lançamento, para 178
        self.assertEqual(self.lote(2)["status"], "planejado")             # nenhum lote novo com o anterior pausado
        self.crm_falso.fila = _fila_json(20, 60_000, 10)
        self.agora += timedelta(minutes=35)
        self.laco.tick()
        self.assertEqual(self.lote(1)["status"], "enviando")
        self.assertIn(("POST", "/api/integrations/campaigns/camp1/resume"), self.escritas())

    def test_falha_de_envio_acima_de_6_por_cento_pausa(self) -> None:
        self.crm_falso.conclui_sozinha = False
        self.aprovar()
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] == "enviando")
        self.razao.atualizar_lote(self.lote(1)["id"], enviados=200, falhas=20)   # 10% de falha em 200 processados
        self.agora += timedelta(seconds=30)
        self.laco.tick()
        self.assertEqual(self.lote(1)["status"], "pausado")
        self.assertIn("falha de envio", self.lote(1)["motivo"])

    def test_fim_da_janela_pausa_o_que_ainda_esta_enviando(self) -> None:
        self.crm_falso.conclui_sozinha = False
        self.aprovar()
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] == "enviando")
        self.agora = self.agora.replace(hour=14, minute=0, second=30)
        self.laco.tick()
        self.assertEqual(self.lote(1)["status"], "pausado")
        self.assertEqual(self.lote(2)["status"], "adiado")
        # No dia útil seguinte o lote pausado é cancelado, não retomado: a lista é refeita.
        self.agora = datetime(2026, 11, 25, 9, 0, tzinfo=BRT)
        self.laco.tick()
        self.assertEqual(self.lote(1)["status"], "cancelado")
        self.assertIn(("POST", "/api/integrations/campaigns/camp1/cancel"), self.escritas())

    def test_parada_geral_cancela_o_que_esta_em_andamento(self) -> None:
        self.aprovar()
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] == "etiquetando" and self.lote(1)["etiquetados"] > 0)
        self.razao.controle = Controle(ligado=True, parada_geral=True, parada_motivo="teste")
        self.agora += timedelta(seconds=30)
        self.laco.tick()
        self.assertEqual(self.lote(1)["status"], "cancelado")
        self.assertEqual(self.crm_falso.campanhas, {})                           # parou antes de criar a campanha
        self.assertTrue(any(m["status"] == "cancelado" for m in self.razao.msgs))
        antes = len(self.escritas())
        self.ate(12, 0)
        self.assertEqual(len(self.escritas()), antes)                             # nada mais é escrito

        # Com a campanha já enviando, a parada geral cancela a campanha no CRM.
        self.setUp()
        self.crm_falso.conclui_sozinha = False
        self.aprovar()
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] == "enviando")
        self.razao.controle = Controle(ligado=True, parada_geral=True)
        self.agora += timedelta(seconds=30)
        self.laco.tick()
        self.assertEqual(self.crm_falso.campanhas["camp1"]["status"], "CANCELLED")
        self.assertEqual(self.lote(1)["status"], "cancelado")

    def test_campanha_com_mais_gente_que_o_lote_e_cancelada(self) -> None:
        self.crm_falso.conclui_sozinha = False
        self.crm_falso.publico_a_mais = 5
        self.aprovar()
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] in ("erro", "concluido"))
        self.assertEqual(self.lote(1)["status"], "erro")
        self.assertEqual(self.crm_falso.campanhas["camp1"]["status"], "CANCELLED")
        self.assertIn("305 destinatários para um lote de 300", self.lote(1)["motivo"])

    def test_passo_sem_resposta_nao_e_repetido_as_cegas(self) -> None:
        self.aprovar()
        self.ate(12, 0, parar=lambda: any(l["status"] == "etiquetando" for l in self.razao.lotes(DIA, False)))
        # Queda no meio da criação da campanha: o passo ficou registrado como iniciado, sem resposta.
        self.razao.registrar_acao(f"real:{self.lote(1)['id']}:campanha", "campanha", lote_id=self.lote(1)["id"], simulado=False)
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] == "erro")
        self.assertEqual(self.lote(1)["status"], "erro")
        self.assertEqual(self.crm_falso.campanhas, {})
        self.assertNotIn("launch", " ".join(c for _, c in self.escritas()))

    def test_contato_recusado_ao_etiquetar_sai_do_lote_e_o_resto_segue(self) -> None:
        self.aprovar()
        self.ate(9, 1)
        alvo = self.razao.contatos_do_lote(self.lote(1)["id"], "reservado", 2)
        self.crm_falso.contatos_invalidos = set(alvo)
        self.ate(12, 0, parar=lambda: self.lote(1)["status"] == "concluido")
        self.assertEqual(self.lote(1)["tamanho"], 298)
        self.assertEqual(sum(1 for m in self.razao.msgs if m["status"] == "falhou"), 2)

    def test_token_sem_permissao_para_o_lote_registra_no_diario_e_nao_insiste(self) -> None:
        self.crm_falso.negar[("POST", "/api/integrations/campaigns")] = "campaign:create"
        self.aprovar()
        self.ate(11, 0)
        self.assertEqual(self.lote(1)["status"], "erro")
        self.assertIn("campaign:create", self.lote(1)["motivo"])
        tentativas = [c for c in self.escritas() if c[1] == "/api/integrations/campaigns"]
        self.assertEqual(len(tentativas), 1)                                       # uma tentativa só
        avisos = [e for e in self.razao.entradas_diario if "campaign:create" in e["texto"]]
        self.assertEqual(len(avisos), 1)
        self.assertEqual(self.crm_falso.campanhas, {})
        # Lote em erro segura o dia: o lote 2 não chega a ser etiquetado.
        self.assertEqual(self.lote(2)["status"], "planejado")
        self.assertEqual(len([c for c in self.escritas() if "/api/contacts/" in c[1]]), 300)
        self.assertTrue(any("lote em erro" in e["texto"] for e in self.razao.entradas_diario))

    def test_token_sem_permissao_para_etiquetar_para_no_primeiro_contato(self) -> None:
        self.crm_falso.negar[("POST", "/api/contacts/")] = "contact:edit"
        self.aprovar()
        self.ate(11, 0)
        self.assertEqual(self.lote(1)["status"], "erro")
        self.assertIn("contact:edit", self.lote(1)["motivo"])
        self.assertEqual(len([c for c in self.escritas() if "/api/contacts/" in c[1]]), 1)

    def test_token_sem_permissao_para_a_fila_avisa_uma_vez_e_nada_sai(self) -> None:
        self.crm_falso.negar[("GET", "/api/integrations/service-queue")] = "report:view"
        self.aprovar()
        self.ate(11, 0)
        self.assertEqual(len([c for c in self.crm_falso.chamadas if "service-queue" in c[1]]), 1)
        self.assertEqual(len([e for e in self.razao.entradas_diario if "report:view" in e["texto"]]), 1)
        self.assertEqual(self.escritas(), [])

    def test_sem_template_configurado_o_lote_nao_sai(self) -> None:
        with mock.patch.dict(os.environ, {"MOTOR_TEMPLATE_LEMBRETE_DIA25": ""}):
            self.aprovar()
            self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        self.assertEqual(self.lote(1)["status"], "recusado")

    def test_leitura_de_destinatarios_duas_horas_depois_sem_dado_pessoal(self) -> None:
        self.crm_falso.falhas, self.crm_falso.presos = 1, 2
        self.aprovar()
        self.ate(9, 30, parar=lambda: self.lote(1)["status"] == "concluido")
        # O lote é medido pelos destinatários por status, não pelo stats (que diz 0 pendentes).
        l1 = self.lote(1)
        self.assertEqual((l1["enviados"], l1["entregues"], l1["falhas"]), (298, 297, 1))
        self.assertEqual(l1["motivo"], "2 não enviados pelo CRM")
        lancado = self.lote(1)["liberado_em"]
        self.agora = lancado + timedelta(hours=1, minutes=59)
        self.laco.tick()
        self.assertFalse([c for c in self.crm_falso.chamadas if "recipients?page=" in c[1]])   # só as contagens por status até aqui
        self.agora = lancado + timedelta(hours=2, minutes=1)
        for _ in range(8):
            self.laco.tick()
            self.agora += timedelta(seconds=30)
        self.assertEqual(self.lote(1)["leituras"], 1)
        leituras = [c for c in self.crm_falso.chamadas if "/camp1/recipients?page=" in c[1]]
        self.assertEqual(len(leituras), 3)                                         # 300 destinatários = 3 páginas
        do_lote = [m["status"] for m in self.razao.msgs if m["lote_id"] == self.lote(1)["id"]]
        self.assertEqual({s: do_lote.count(s) for s in set(do_lote)}, {"entregue": 297, "falhou": 1, "nao_enviado": 2})
        bloqueados = [r for r in self.razao.retratos.values() if r.get("bloqueio_motor") == "erro_131026"]
        self.assertEqual(len(bloqueados), 1)
        # PENDING em campanha concluída = não enviado: não conta no teto do dia nem no limite do aluno.
        self.assertEqual(self.razao.enviados_hoje(DIA, False), sum(1 for m in self.razao.msgs if m["status"] not in ("nao_enviado", "cancelado")))
        tudo = json.dumps([self.razao.msgs, list(self.razao.retratos.values())], default=str)
        self.assertNotIn("Fulano", tudo)
        self.assertNotIn("5511999990000", tudo)


if __name__ == "__main__":
    unittest.main()
