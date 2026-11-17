"""Rotas de integração do CRM e envio real (campanha do dia), contra um CRM de mentira. Nenhum POST sai para a rede."""

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
from motor.crm import ALTA, BAIXA, CrmErro, CrmMotor, EscritaBloqueada, LimiteDeTaxa, limpar_destinatario, validar_filtros_de_campanha
from motor.envio import Envio, componentes_do_template, conferir_publico, configuracao, etiqueta_de_controle
from motor.laco import Laco
from motor.modelos import Aluno, Controle
from motor.razao import RazaoMemoria
from motor.sorteio import no_controle
from tests.test_motor_laco import Relogio, Transporte, _deal, _fila_json

DIA = date(2026, 11, 24)
ETIQUETA = "Lembrete 24/11"
AMBIENTE = {"MOTOR_CANAL_ID": "canal1", "MOTOR_TEMPLATE_LEMBRETE_DIA25": "motor_lembrete_dia25_a"}


class CrmFalso(Transporte):
    """CRM de mentira: etiquetas, uma campanha que envia 1 mensagem por segundo do relógio do teste, e destinatários."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.tags: dict[str, str] = {ETIQUETA: "tag-dia", "Sem lembretes de mensalidade": "tag-sem"}   # nome → id
        self.etiquetados: dict[str, set] = {}
        for d in self.deals:
            for t in (d.get("contact") or {}).get("tags") or []:
                self.etiquetados.setdefault(self.tags.setdefault(t["name"], f"tag-{len(self.tags)}"), set()).add(d["contactId"])
        self.campanhas: dict[str, dict] = {}
        self.corpos: list[tuple[str, str, dict]] = []
        self.horas: list[datetime] = []
        self.aceita_exclusao = True       # False = API sem o backend_crm1#278
        self.publico_a_mais = 0           # destinatários que a base do motor não previa
        self.falhas = 0
        self.negar: dict[tuple[str, str], str] = {}

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
            self.tags[corpo["name"]] = f"tag-{len(self.tags)}"
            return 201, {}, {"id": self.tags[corpo["name"]], "name": corpo["name"]}
        if url.path == "/api/tags" and metodo == "GET":
            self.chamadas.append((metodo, caminho))
            return 200, {}, [{"id": i, "name": n} for n, i in self.tags.items()]
        if partes[:2] == ["api", "contacts"] and partes[-1] == "tags" and metodo == "POST":
            self.etiquetados.setdefault(corpo["tagId"], set()).add(partes[2])
            return 200, {}, {"ok": True}
        if partes[:3] == ["api", "integrations", "campaigns"]:
            return self._campanha(metodo, partes[3:], corpo, urllib.parse.parse_qs(url.query), caminho)
        return super().__call__(metodo, caminho, corpo)

    def _andar(self, c) -> None:
        """A campanha envia 1 mensagem por segundo enquanto está em PROCESSING ou SENDING."""
        if c["status"] in ("PROCESSING", "SENDING") and self.agora:
            agora = self.agora()
            c["enviados"] = min(len(c["publico"]), c["enviados"] + int((agora - c["desde"]).total_seconds()))
            c["desde"] = agora
            if c["enviados"] >= len(c["publico"]):
                c["status"] = "COMPLETED"

    def _campanha(self, metodo, resto, corpo, q, caminho):
        if metodo == "GET":
            self.chamadas.append((metodo, caminho))
        if not resto and metodo == "POST":
            if "excludeTagIds" in corpo["filters"] and not self.aceita_exclusao:
                return 400, {}, {"message": "Filtro de público inválido.", "code": "AUDIENCE_FILTERS_INVALID"}
            cid = f"camp{len(self.campanhas) + 1}"
            self.campanhas[cid] = {"id": cid, "status": "DRAFT", "publico": [], "enviados": 0, "desde": None, **corpo}
            return 201, {}, {"campaign": {k: v for k, v in self.campanhas[cid].items() if k not in ("publico", "desde")}}
        c = self.campanhas.get(resto[0])
        if c is None:
            return 404, {}, {"message": "Campanha não encontrada."}
        self._andar(c)
        if len(resto) == 1:
            return 200, {}, {"campaign": {k: v for k, v in c.items() if k not in ("publico", "desde")}}
        acao = resto[1]
        transicoes = {"launch": ({"DRAFT"}, "PROCESSING"), "pause": ({"PROCESSING", "SENDING"}, "PAUSED"),
                      "resume": ({"PAUSED"}, "SENDING"), "cancel": ({"DRAFT", "PROCESSING", "SENDING", "PAUSED"}, "CANCELLED")}
        if acao in transicoes and metodo == "POST":
            de, para = transicoes[acao]
            if c["status"] not in de:
                return 409, {}, {"message": "Status não permite."}
            if acao == "launch":  # o público é fixado aqui, de uma vez
                fora = set().union(*[self.etiquetados.get(t, set()) for t in c["filters"].get("excludeTagIds", [])])
                c["publico"] = sorted(self.etiquetados.get(c["filters"]["tagIds"][0], set()) - fora) + [f"extra{i}" for i in range(self.publico_a_mais)]
            c["status"], c["desde"] = para, self.agora() if self.agora else None
            return 200, {}, {"message": "ok", "status": para}
        por_contato = self._status(c)
        if acao == "stats":   # como em produção: 0 pendentes mesmo havendo destinatário PENDING
            return 200, {}, {"status": c["status"], "totalRecipients": len(c["publico"]), "pendingCount": 0, "deliveryRate": 100, "repliedCount": 0}
        if acao == "recipients":
            filtro = q.get("status", [""])[0]
            escolhidos = [cid for cid in c["publico"] if not filtro or por_contato[cid] == filtro]
            pagina, por = int(q.get("page", ["1"])[0]), int(q.get("perPage", ["50"])[0])
            itens = []
            for cid in escolhidos[(pagina - 1) * por: pagina * por]:
                enviado = c["publico"].index(cid) < c["enviados"]
                falhou = enviado and por_contato[cid] == "FAILED"
                itens.append({"id": f"r{cid}", "contactId": cid, "contact": {"id": cid, "name": "Fulano de Tal", "phone": "5511999990000"},
                              "status": por_contato[cid], "errorCode": 131026 if falhou else None,
                              "errorMessage": "Número 5511999990000 sem WhatsApp (code 131026)" if falhou else ("Campanha cancelada." if por_contato[cid] == "FAILED" else None),
                              "sentAt": "2026-11-24T12:10:00.000Z" if enviado else None, "deliveredAt": None, "readAt": None, "repliedAt": None})
            return 200, {}, {"items": itens, "total": len(escolhidos), "page": pagina, "perPage": por, "totalPages": -(-len(escolhidos) // por)}
        return 404, {}, {"message": "rota não prevista no teste"}

    def _status(self, c) -> dict:
        """Enviados: os primeiros `falhas` com FAILED, o resto DELIVERED. Não enviados: PENDING (ou FAILED, se a campanha foi cancelada)."""
        resto = "FAILED" if c["status"] == "CANCELLED" else "PENDING"
        return {cid: ("FAILED" if n < self.falhas else "DELIVERED") if n < c["enviados"] else resto for n, cid in enumerate(c["publico"])}


class LimiteComPrioridadeTest(unittest.TestCase):
    def test_baixa_para_na_reserva_e_alta_usa_o_resto(self) -> None:
        r = Relogio()
        limite = LimiteDeTaxa(60, 10, r.agora, r.dormir)
        for _ in range(50):
            self.assertEqual(limite.aguardar(BAIXA), 0.0)
        self.assertEqual((limite.disponiveis(BAIXA), limite.disponiveis(ALTA)), (0, 10))
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

    def test_campanha_usa_as_rotas_de_integracao_com_exclusao_por_etiqueta(self) -> None:
        t = CrmFalso()
        crm = self._crm(t)
        camp = crm.criar_campanha(nome="n", canal_id="canal1", template="tpl", idioma="pt_BR", tag_ids=["tag-dia"], excluir_tag_ids=["tag-sem", "tag-ctl"],
                                  componentes=componentes_do_template("lembrete_dia25", "2026-11"), velocidade=1)
        self.assertEqual(camp["status"], "DRAFT")
        self.assertEqual(crm.campanha(camp["id"])["filters"], {"tagIds": ["tag-dia"], "excludeTagIds": ["tag-sem", "tag-ctl"]})
        self.assertEqual(crm.lancar_campanha(camp["id"])["status"], "PROCESSING")
        self.assertEqual(crm.pausar_campanha(camp["id"])["status"], "PAUSED")
        self.assertEqual(crm.retomar_campanha(camp["id"])["status"], "SENDING")
        self.assertEqual(crm.cancelar_campanha(camp["id"])["status"], "CANCELLED")
        caminhos = [c for _, c in t.chamadas]
        for acao in ("launch", "pause", "resume", "cancel"):
            self.assertIn(f"/api/integrations/campaigns/camp1/{acao}", caminhos)
        _, _, corpo = next(c for c in t.corpos if c[1] == "/api/integrations/campaigns")
        self.assertEqual((corpo["type"], corpo["sendRate"], corpo["channelId"]), ("TEMPLATE", 1, "canal1"))
        self.assertEqual([p["text"] for p in corpo["templateComponents"]["components"][0]["parameters"]], ["{{contact.name | first}}", "novembro", "25/11"])

    def test_filtro_de_campanha_so_com_etiqueta_de_publico(self) -> None:
        validar_filtros_de_campanha({"tagIds": ["a"], "excludeTagIds": ["b", "c"]})
        for ruim in ({}, {"excludeTagIds": ["b"]}, {"tagIds": ["a"], "excludeTagIds": ["a"]}, {"tagIds": ["a"], "excludeTagIds": "b"},
                     {"tagIds": ["a"], "contactIds": ["x"]}, {"tagIds": []}):
            with self.assertRaises(ValueError):
                validar_filtros_de_campanha(ruim)

    def test_destinatarios_chegam_sem_nome_telefone_nem_texto_de_erro(self) -> None:
        t = CrmFalso(deals=[_deal(i, tags=(ETIQUETA,)) for i in range(3)])
        t.falhas = 1
        crm = self._crm(t)
        camp = crm.criar_campanha(nome="n", canal_id="c", template="t", idioma="pt_BR", componentes=[], tag_ids=["tag-dia"])
        crm.lancar_campanha(camp["id"])
        t.campanhas[camp["id"]].update(enviados=2, status="COMPLETED")
        itens, paginas = crm.campanha_destinatarios_pagina(camp["id"], 1)
        self.assertEqual(paginas, 1)
        self.assertEqual(sorted(itens[0]), ["contact_id", "entregue_em", "enviado_em", "erro_codigo", "lido_em", "respondeu_em", "status"])
        self.assertEqual([(i["contact_id"], i["status"], i["erro_codigo"]) for i in itens], [("c0", "FAILED", 131026), ("c1", "DELIVERED", None), ("c2", "PENDING", None)])
        self.assertNotIn("Fulano", repr(itens))
        self.assertNotIn("5511999990000", repr(itens))
        # A medida vem do total por status, com um item por chamada.
        self.assertEqual(crm.campanha_contagem(camp["id"]), {"SENT": 0, "DELIVERED": 1, "READ": 0, "FAILED": 1})
        self.assertTrue(all("perPage=1" in c for _, c in t.chamadas[-4:]))
        self.assertIsNone(limpar_destinatario({"contactId": "c9", "status": "SENT", "errorCode": "other"})["erro_codigo"])

    def test_em_simulacao_as_rotas_que_escrevem_ficam_travadas(self) -> None:
        t = CrmFalso()
        crm = self._crm(t, escrita=False)
        for chamada in (lambda: crm.criar_etiqueta("x"), lambda: crm.lancar_campanha("c"), lambda: crm.pausar_campanha("c"),
                        lambda: crm.retomar_campanha("c"), lambda: crm.cancelar_campanha("c"), lambda: crm.etiquetar_contato("c1", "t1")):
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
        with self.assertRaises(CrmErro):
            crm.campanha_stats("camp2")                    # mesma rota, outra campanha
        self.assertEqual(len(t.chamadas), 1)               # não foi à rede de novo
        self.assertEqual(crm.negadas, {("GET", "/api/integrations/campaigns/{id}/stats"): "campaign:view"})
        crm.criar_etiqueta("x")                            # outra rota segue normal

    def test_template_e_canal_vem_do_ambiente(self) -> None:
        self.assertIsNone(configuracao("lembrete_dia25"))
        with mock.patch.dict(os.environ, AMBIENTE):
            self.assertEqual(configuracao("lembrete_dia25"), {"canal": "canal1", "template": "motor_lembrete_dia25_a", "idioma": "pt_BR"})
            self.assertIsNone(configuracao("firme"))


class ConferenciaDoPublicoTest(unittest.TestCase):
    def _a(self, cid, **kw) -> Aluno:
        base = dict(contact_id=cid, nivel="Graduação", dia10_aberto=True, etiquetas=frozenset({"lembrete 24/11"}))
        base.update(kw)
        return Aluno(**base)

    def test_conta_quem_recebe_quem_sai_por_etiqueta_e_quem_nao_deveria(self) -> None:
        alunos = [self._a(f"ok{i}") for i in range(95)] + [
            self._a("fora", etiquetas=frozenset()),                                                    # sem a etiqueta do dia
            self._a("sem", etiquetas=frozenset({"lembrete 24/11", "sem lembretes de mensalidade"})),   # excluído no CRM
            self._a("ctl", controle=True, controle_etiquetado=True),                                   # excluído no CRM
            self._a("ctl2", controle=True),                                                            # controle ainda sem etiqueta
            self._a("pos", nivel="Pós-Graduação"),
            self._a("pago", dia10_aberto=False),
            self._a("trancado", situacao="Trancado"),
            self._a("parou", bloqueio="recusou_mensagens"),
        ]
        conf = conferir_publico(alunos, ETIQUETA, MotorConfig())
        self.assertEqual((conf.na_etiqueta, conf.excluidos, conf.recebem), (102, 2, 100))
        self.assertEqual(conf.indevidos, {
            "controle sem etiqueta": 1, "fora da graduação": 1, "sem mensalidade em aberto": 1, "fora de Em Curso": 1,
            "pediu para parar ou número bloqueado, sem etiqueta de exclusão": 1,
        })
        self.assertAlmostEqual(conf.diferenca, 0.05)


class EnvioRealTest(unittest.TestCase):
    """Terça 24/11/2026 em modo real: 700 alunos de graduação em aberto com a etiqueta do dia, e um CRM de mentira."""

    CFG = MotorConfig(enabled=True, dry_run=False, carga_min_abertos=100, janela_fim_h=14)

    def setUp(self) -> None:
        amb = mock.patch.dict(os.environ, AMBIENTE)
        amb.start()
        self.addCleanup(amb.stop)
        self.agora = datetime(2026, 11, 24, 7, 0, tzinfo=BRT)
        self.crm_falso = CrmFalso(deals=[_deal(i, tags=(ETIQUETA,)) for i in range(700)], fila=_fila_json(20, 60_000, 10), agora=lambda: self.agora)
        self.razao = RazaoMemoria()
        self.razao.controle = Controle(ligado=True)
        self.montar(self.CFG)
        self.controle = {f"c{i}" for i in range(700) if no_controle(f"c{i}", "2026-11", self.CFG.controle_sal, self.CFG.controle_pct)}

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

    def aprovar(self, etiqueta=ETIQUETA) -> None:
        self.ate(8, 31)
        self.assertTrue(self.razao.aprovar_plano(DIA, False, "marcelo", etiqueta=etiqueta))

    def escritas(self) -> list[tuple[str, str]]:
        return [c for c in self.crm_falso.chamadas if c[0] != "GET"]

    def campanha(self) -> dict:
        return self.razao.plano(DIA, False)["campanha"] or {}

    def janelas(self) -> list[dict]:
        return self.razao.lotes(DIA, False)

    def enviando(self) -> bool:
        return self.campanha().get("estado") == "enviando"

    def diario(self, trecho: str) -> list[str]:
        return [e["texto"] for e in self.razao.entradas_diario if trecho in e["texto"]]

    # ── chaves ────────────────────────────────────────────────────────────
    def test_sem_plano_aprovado_nada_e_escrito_no_crm(self) -> None:
        self.ate(11, 0)
        self.assertEqual(self.escritas(), [])
        self.assertEqual(self.razao.plano(DIA, False)["status"], "proposto")
        self.assertEqual(self.janelas(), [])               # o plano real não tem lote por lista de alunos

    def test_as_quatro_chaves_precisam_estar_juntas(self) -> None:
        self.montar(replace(self.CFG, dry_run=True))       # MOTOR_DRY_RUN=1: segue simulando
        self.ate(8, 31)
        self.razao.aprovar_plano(DIA, True, "marcelo")
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        self.assertTrue(self.razao.msgs and all(m["simulado"] for m in self.razao.msgs))
        self.setUp()
        self.razao.controle = Controle(ligado=False)       # desligado na página
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        self.setUp()
        self.montar(replace(self.CFG, enabled=False), escrita=True)   # MOTOR_ENABLED=0 vence tudo
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])

    def test_aprovacao_sem_etiqueta_do_dia_ou_com_etiqueta_inexistente_nao_lanca(self) -> None:
        self.aprovar(etiqueta="")
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        self.assertTrue(self.diario("sem a etiqueta do dia"))
        self.setUp()
        self.aprovar(etiqueta="Etiqueta que não existe")
        self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        self.assertTrue(self.diario("não existe no CRM"))

    # ── o dia inteiro ─────────────────────────────────────────────────────
    def test_uma_campanha_por_dia_dosada_por_pausa_e_retomada(self) -> None:
        self.aprovar()
        self.ate(13, 0, parar=lambda: self.campanha().get("leituras") == 1)
        c = self.campanha()
        esperado = 700 - len(self.controle)
        self.assertEqual((c["estado"], c["total"], c["processados"], c["esperado"]), ("concluida", esperado, esperado, esperado))
        self.assertEqual(len(self.crm_falso.campanhas), 1)                         # UMA campanha no dia

        # O motor só etiqueta o grupo de controle, com a etiqueta do mês, antes de lançar.
        ordem = [c2 for _, c2 in self.escritas()]
        etiquetagens = [x for x in ordem if x.startswith("/api/contacts/")]
        tag_ctl = self.crm_falso.tags[etiqueta_de_controle("2026-11")]
        self.assertEqual(len(etiquetagens), len(self.controle))
        self.assertEqual(self.crm_falso.etiquetados[tag_ctl], self.controle)
        self.assertLess(max(i for i, x in enumerate(ordem) if x.startswith("/api/contacts/")), ordem.index("/api/integrations/campaigns"))
        permitidas = re.compile(r"^/api/(tags|contacts/[^/]+/tags|integrations/campaigns(/camp1/(launch|pause|resume))?)$")
        self.assertTrue(all(m == "POST" and permitidas.match(x) for m, x in self.escritas()), [x for _, x in self.escritas() if not permitidas.match(x)][:3])

        # Público = etiqueta do dia; exclusões = sem lembretes + controle do mês; 1 mensagem por segundo.
        camp = self.crm_falso.campanhas["camp1"]
        self.assertEqual(camp["filters"], {"tagIds": ["tag-dia"], "excludeTagIds": ["tag-sem", tag_ctl]})
        self.assertEqual((camp["sendRate"], camp["templateName"], camp["channelId"], camp["type"]), (1, "motor_lembrete_dia25_a", "canal1", "TEMPLATE"))
        self.assertFalse(set(camp["publico"]) & self.controle)

        # Janelas: a 1ª com cerca de 300 (rampa), a 2ª só 30 minutos depois, pela fila.
        j = self.janelas()
        self.assertEqual([x["status"] for x in j], ["concluido", "concluido"])
        self.assertTrue(295 <= j[0]["tamanho"] <= 335, j[0]["tamanho"])            # pausa a cada tick de 30 s
        self.assertEqual(j[0]["tamanho"] + j[1]["tamanho"], esperado)
        self.assertGreaterEqual(j[1]["liberado_em"] - j[0]["liberado_em"], timedelta(minutes=30))
        self.assertEqual(ordem.count("/api/integrations/campaigns/camp1/pause"), 1)
        self.assertEqual(ordem.count("/api/integrations/campaigns/camp1/resume"), 1)

        # Depois da campanha, o livro-razão por aluno é preenchido pelos destinatários, sem dado pessoal.
        reais = [m for m in self.razao.msgs if not m["simulado"]]
        self.assertEqual({m["contact_id"] for m in reais}, set(camp["publico"]))
        self.assertEqual({m["status"] for m in reais}, {"entregue"})
        tudo = json.dumps([self.razao.msgs, list(self.razao.retratos.values()), self.razao.planos[(DIA, False)]], default=str)
        self.assertNotIn("Fulano", tudo)
        self.assertNotIn("5511999990000", tudo)
        # Orçamento: nunca mais de 60 chamadas em qualquer minuto.
        horas = self.crm_falso.horas
        self.assertTrue(all(horas[i] - horas[i - 60] >= timedelta(seconds=60) for i in range(60, len(horas))))

    def test_fila_que_sobe_60_pausa_a_janela_e_a_melhora_retoma(self) -> None:
        self.aprovar()
        self.ate(12, 0, parar=self.enviando)
        self.crm_falso.fila = _fila_json(aguardando=85, online=10)         # +65 sobre os 20 do lançamento
        self.agora += timedelta(seconds=60)
        self.laco.tick()
        self.assertEqual(self.campanha()["estado"], "pausada")
        self.assertIn("fila subiu 65", self.campanha()["motivo"])
        self.assertEqual(self.janelas()[0]["status"], "pausado")
        self.assertLess(self.campanha()["processados"], 150)                # parou no meio da janela de 300
        self.crm_falso.fila = _fila_json(20, 60_000, 10)
        self.agora += timedelta(minutes=31)
        self.laco.tick()
        self.assertEqual(self.campanha()["estado"], "enviando")
        self.assertEqual(self.campanha()["janela"], 2)

    def test_fim_do_expediente_cancela_e_registra_quem_ficou_sem_receber(self) -> None:
        self.aprovar()
        self.ate(12, 0, parar=self.enviando)
        self.crm_falso.fila = _fila_json(aguardando=20, online=3)           # poucos atendentes: segura as janelas seguintes
        self.ate(14, 1)
        c = self.campanha()
        self.assertEqual(c["estado"], "cancelada")
        self.assertEqual(c["motivo"], "fim do expediente")
        self.assertEqual(c["nao_enviados"], c["total"] - c["processados"])
        self.assertGreater(c["nao_enviados"], 300)
        self.assertEqual(self.crm_falso.campanhas["camp1"]["status"], "CANCELLED")
        self.assertTrue(self.diario("destinatários ficaram sem receber"))
        # Os pendentes do cancelamento entram como "não enviado", não como falha, e não contam no dia.
        self.ate(14, 20)
        status = [m["status"] for m in self.razao.msgs if not m["simulado"]]
        self.assertEqual(status.count("nao_enviado"), c["nao_enviados"])
        self.assertEqual(status.count("entregue"), c["processados"])
        self.assertEqual(self.razao.enviados_hoje(DIA, False), c["processados"])
        # No dia seguinte nada é retomado.
        antes = len(self.escritas())
        self.agora = datetime(2026, 11, 25, 9, 0, tzinfo=BRT)
        self.laco.tick()
        self.assertEqual(len(self.escritas()), antes)

    def test_campanha_nunca_atravessa_o_dia(self) -> None:
        self.aprovar()
        self.ate(12, 0, parar=lambda: self.campanha().get("estado") == "pausada")
        # O worker ficou fora do ar até o dia seguinte, com a campanha pausada no CRM.
        self.agora = datetime(2026, 11, 25, 9, 0, tzinfo=BRT)
        self.laco.tick()
        self.assertEqual(self.razao.planos[(DIA, False)]["campanha"]["estado"], "cancelada")
        self.assertIn("não atravessa o dia", self.razao.planos[(DIA, False)]["campanha"]["motivo"])
        self.assertNotIn("/api/integrations/campaigns/camp1/resume", [x for _, x in self.escritas()])

    def test_parada_geral_cancela_a_campanha(self) -> None:
        self.aprovar()
        self.ate(12, 0, parar=self.enviando)
        self.razao.controle = Controle(ligado=True, parada_geral=True, parada_motivo="teste")
        self.agora += timedelta(seconds=30)
        self.laco.tick()
        self.assertEqual(self.crm_falso.campanhas["camp1"]["status"], "CANCELLED")
        self.assertEqual(self.campanha()["estado"], "cancelada")
        self.assertIn("parada geral", self.campanha()["motivo"])

    def test_teto_do_dia_cancela_o_que_falta(self) -> None:
        self.montar(replace(self.CFG, teto_diario=250))
        self.aprovar()
        self.ate(11, 0, parar=lambda: self.campanha().get("estado") == "cancelada")
        c = self.campanha()
        self.assertEqual(c["estado"], "cancelada")
        self.assertIn("teto do dia", c["motivo"])
        self.assertLess(c["processados"], 250 + 40)

    # ── recusas ───────────────────────────────────────────────────────────
    def test_api_sem_exclusao_por_etiqueta_recusa_o_lancamento(self) -> None:
        self.crm_falso.aceita_exclusao = False
        self.aprovar()
        self.ate(11, 0)
        self.assertEqual(self.campanha()["estado"], "erro")
        self.assertIn("excludeTagIds", self.campanha()["motivo"])
        self.assertEqual(self.crm_falso.campanhas, {})
        self.assertEqual([x for _, x in self.escritas()].count("/api/integrations/campaigns"), 1)     # uma tentativa, sem lançar sem a exclusão
        self.assertEqual(len(self.diario("excludeTagIds")), 1)

    def test_publico_que_nao_bate_com_a_base_recusa_e_uma_carga_nova_reabre(self) -> None:
        # 40 dos 700 com a etiqueta do dia são de pós-graduação (5,7%, acima do limite de 2%).
        for d in self.crm_falso.deals[:40]:
            next(c for c in d["customFields"] if c["customFieldId"] == "f_niv")["value"] = "Pós-Graduação"
        self.aprovar()
        self.ate(9, 20)
        self.assertEqual(self.campanha()["estado"], "recusada")
        self.assertIn("fora da graduação", self.campanha()["motivo"])
        self.assertEqual(self.crm_falso.campanhas, {})
        self.assertEqual(len(self.diario("Lançamento RECUSADO")), 1)
        # A equipe corrige a importação (tira a etiqueta de quem não devia): a carga seguinte refaz a conferência.
        for d in self.crm_falso.deals[:40]:
            d["contact"]["tags"] = []
        self.crm_falso.etiquetados["tag-dia"] -= {f"c{i}" for i in range(40)}
        self.ate(10, 30, parar=self.enviando)
        self.assertEqual(self.campanha()["estado"], "enviando")

    def test_publico_maior_que_o_conferido_cancela(self) -> None:
        self.crm_falso.publico_a_mais = 40
        self.aprovar()
        self.ate(9, 10, parar=lambda: self.campanha().get("estado") == "erro")
        self.assertEqual(self.campanha()["estado"], "erro")
        self.assertEqual(self.crm_falso.campanhas["camp1"]["status"], "CANCELLED")
        self.assertTrue(self.diario("a conferência previa"))

    def test_token_sem_permissao_registra_no_diario_e_nao_insiste(self) -> None:
        self.crm_falso.negar[("POST", "/api/integrations/campaigns")] = "campaign:create"
        self.aprovar()
        self.ate(11, 0)
        self.assertEqual(self.campanha()["estado"], "erro")
        self.assertIn("campaign:create", self.campanha()["motivo"])
        self.assertEqual([x for _, x in self.escritas()].count("/api/integrations/campaigns"), 1)
        self.assertEqual(len(self.diario("campaign:create")), 1)

    def test_token_sem_permissao_para_a_fila_avisa_uma_vez_e_nada_sai(self) -> None:
        self.crm_falso.negar[("GET", "/api/integrations/service-queue")] = "report:view"
        self.aprovar()
        self.ate(11, 0)
        self.assertEqual(len([c for c in self.crm_falso.chamadas if "service-queue" in c[1]]), 1)
        self.assertEqual(len(self.diario("report:view")), 1)
        self.assertEqual(self.crm_falso.campanhas, {})

    def test_passo_sem_resposta_nao_e_repetido_as_cegas(self) -> None:
        self.aprovar()
        plano = self.razao.plano(DIA, False)
        # Queda no meio da criação da campanha: o passo ficou registrado como iniciado, sem resposta.
        self.razao.registrar_acao(f"real:plano:{plano['id']}:campanha", "campanha", lote_id=None, simulado=False)
        self.ate(10, 0)
        self.assertEqual(self.campanha()["estado"], "erro")
        self.assertEqual(self.crm_falso.campanhas, {})

    def test_sem_template_configurado_nada_sai(self) -> None:
        with mock.patch.dict(os.environ, {"MOTOR_TEMPLATE_LEMBRETE_DIA25": ""}):
            self.aprovar()
            self.ate(10, 0)
        self.assertEqual(self.escritas(), [])
        self.assertTrue(self.diario("não configurado"))


class CargaDoDiaTest(unittest.TestCase):
    """O plano só fecha depois de a carga do dia aparecer (a equipe importa entre 10h e 14h)."""

    CFG = MotorConfig(enabled=True, dry_run=True, carga_min_abertos=100)

    def test_plano_espera_as_mudancas_do_dia(self) -> None:
        relogio = {"t": datetime(2026, 11, 23, 7, 0, tzinfo=BRT)}
        falso = CrmFalso(deals=[_deal(i) for i in range(600)], fila=_fila_json(20, 60_000, 10), agora=lambda: relogio["t"])
        razao = RazaoMemoria()
        razao.controle = Controle(ligado=True)
        crm = CrmMotor(transporte=falso, relogio=lambda: relogio["t"].timestamp(), dormir=lambda s: None)
        laco = Laco(self.CFG, razao, crm=crm, relogio=lambda: relogio["t"])

        def ate(dia, hora, minuto=0):
            fim = datetime(2026, 11, dia, hora, minuto, tzinfo=BRT)
            while relogio["t"] <= fim:
                laco.tick()
                relogio["t"] += timedelta(minutes=5)

        ate(23, 12)                                                    # 1º dia: a base inteira é novidade, o plano sai
        self.assertIsNotNone(razao.plano(date(2026, 11, 23), True))
        relogio["t"] = datetime(2026, 11, 24, 7, 0, tzinfo=BRT)
        ate(24, 11)                                                    # 2º dia: até as 11h nada mudou no Dia 10
        self.assertIsNone(razao.plano(DIA, True))
        self.assertTrue(any("carga do dia ainda não foi detectada" in e["texto"] for e in razao.entradas_diario))
        for d in falso.deals[:80]:                                     # 11h05: a importação da equipe dá baixa em 80 alunos
            next(c for c in d["customFields"] if c["customFieldId"] == "f_dia")["value"] = "Não"
        ate(24, 12)
        plano = razao.plano(DIA, True)
        self.assertIsNotNone(plano)
        self.assertGreaterEqual(razao.mudancas_do_dia(DIA), 80)
        self.assertLessEqual(plano["resumo"]["acoes"][0]["elegiveis"], 520)   # o plano já sem quem teve baixa


if __name__ == "__main__":
    unittest.main()
