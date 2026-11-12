"""Cliente do CRM, carga, classificador e o laço em simulação. HTTP simulado, sem rede e sem banco."""

from __future__ import annotations

import json
import os
import unittest
import urllib.parse
from dataclasses import replace
from datetime import date, datetime, timedelta
from unittest import mock

from motor.calendario import BRT
from motor.carga import (
    CargaErro,
    Linha,
    RegistroCiclo,
    ciclo_vigente,
    consolidar,
    evoluir_ciclo,
    montar_mapa,
    normalizar,
)
from motor.classificador import ClassificadorRegras, anonimizar, classificar_botao, classificar_resposta
from motor.config import MotorConfig
from motor.crm import CrmErro, CrmMotor, EscritaBloqueada, LimiteDeTaxa, interpretar_fila, validar_filtros_de_campanha
from motor.laco import Laco
from motor.modelos import Controle, Decisao, Estado
from motor.piloto import LotePendente, Ordem, PilotoFormula, Retrato
from motor.razao import RazaoMemoria
from motor.sorteio import no_controle

CAMPOS = {"dia": "f_dia", "situacaofinanceira": "f_fin", "situacao": "f_sit", "calouro": "f_cal", "nivel": "f_niv"}
MAPA = montar_mapa(CAMPOS, {"dia10": "dia", "financeira": "situacaofinanceira", "situacao": "situacao", "calouro": "calouro", "nivel": "nivel"})


def _deal(i: int, dia="Sim", fin="Não", situacao="Em Curso", calouro="Não", phone="5511999990000", tags=()):
    return {
        "id": f"d{i}", "contactId": f"c{i}",
        "contact": {"id": f"c{i}", "name": "Fulano de Tal", "phone": phone, "tags": [{"name": t} for t in tags]},
        "customFields": [
            {"customFieldId": "f_dia", "value": dia},
            {"customFieldId": "f_fin", "value": fin},
            {"customFieldId": "f_sit", "value": situacao},
            {"customFieldId": "f_cal", "value": calouro},
        ],
    }


class Relogio:
    def __init__(self, t: float = 0.0):
        self.t = t
        self.sonecas: list[float] = []

    def agora(self) -> float:
        return self.t

    def dormir(self, s: float) -> None:
        self.sonecas.append(s)
        self.t += s


class Transporte:
    """CRM de mentira: guarda cada chamada e responde pelo roteiro."""

    def __init__(self, deals=(), fila=None, respostas=None):
        self.deals = list(deals)
        self.fila = fila
        self.respostas = list(respostas or [])
        self.chamadas: list[tuple[str, str]] = []

    def __call__(self, metodo, caminho, corpo=None):
        self.chamadas.append((metodo, caminho))
        if self.respostas:
            return self.respostas.pop(0)
        url = urllib.parse.urlparse(caminho)
        q = urllib.parse.parse_qs(url.query)
        if url.path == "/api/custom-fields":
            return 200, {}, [{"id": v, "name": k} for k, v in CAMPOS.items()]
        if url.path == "/api/deals":
            pagina, por = int(q["page"][0]), int(q["perPage"][0])
            fatia = self.deals[(pagina - 1) * por: pagina * por]
            return 200, {}, {"items": fatia, "hasMore": pagina * por < len(self.deals)}
        if url.path == "/api/painel/service":
            if self.fila is None:
                return 401, {}, {"message": "Não autorizado."}
            return 200, {}, self.fila
        return 404, {}, {"message": "rota não prevista no teste"}


def _fila_json(aguardando=20, ms=120_000, online=8):
    return {"agora": {"ok": True, "data": {
        "asOf": "2026-11-24T13:00:00Z", "awaitingReply": aguardando, "inService": 3,
        "longestWait": {"ms": ms, "overSla": False, "slaMinutes": 60},
        "agents": {"online": online, "total": 13}}},
        "volume": {"ok": False, "error": "omitido"}}


def _crm(transporte, **kw) -> CrmMotor:
    r = Relogio()
    return CrmMotor(transporte=transporte, relogio=r.agora, dormir=r.dormir, **kw)


class LimiteDeTaxaTest(unittest.TestCase):
    def test_nunca_passa_do_limite_por_minuto(self) -> None:
        r = Relogio()
        limite = LimiteDeTaxa(240, r.agora, r.dormir)
        marcas = []
        for _ in range(1000):
            limite.aguardar()
            marcas.append(r.t)
        for i in range(240, len(marcas)):
            self.assertGreaterEqual(marcas[i] - marcas[i - 240], 60.0)
        # 1.000 chamadas a 240/min levam pelo menos 4 minutos.
        self.assertGreaterEqual(marcas[-1], 240.0)

    def test_abaixo_do_limite_nao_espera(self) -> None:
        r = Relogio()
        limite = LimiteDeTaxa(180, r.agora, r.dormir)
        self.assertEqual(sum(limite.aguardar() for _ in range(180)), 0.0)
        self.assertGreater(limite.aguardar(), 0.0)


class ClienteCrmTest(unittest.TestCase):
    def test_429_espera_o_retry_after_e_repete(self) -> None:
        t = Transporte(respostas=[
            (429, {"retry-after": "7"}, {"error": "rate_limit", "retryAfterSec": 7}),
            (429, {}, {"error": "rate_limit", "retryAfterSec": 3}),
            (200, {}, [{"id": "t1", "name": "x"}]),
        ])
        r = Relogio()
        crm = CrmMotor(transporte=t, relogio=r.agora, dormir=r.dormir)
        self.assertEqual(crm.etiquetas(), [{"id": "t1", "name": "x"}])
        self.assertEqual(r.sonecas, [7.0, 3.0])
        self.assertEqual(len(t.chamadas), 3)

    def test_429_sem_fim_desiste_com_erro(self) -> None:
        t = Transporte(respostas=[(429, {"retry-after": "1"}, {})] * 10)
        with self.assertRaises(CrmErro) as ctx:
            _crm(t, max_tentativas=3).etiquetas()
        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(len(t.chamadas), 3)

    def test_403_traz_a_permissao_exigida(self) -> None:
        t = Transporte(respostas=[(403, {}, {"message": "Acesso negado.", "required": "campaign:view"})])
        with self.assertRaises(CrmErro) as ctx:
            _crm(t).campanha_stats("camp1")
        self.assertEqual(ctx.exception.status, 403)
        self.assertIn("campaign:view", str(ctx.exception))
        self.assertEqual(len(t.chamadas), 1)  # 4xx não repete

    def test_em_simulacao_nenhuma_escrita_sai_para_a_rede(self) -> None:
        t = Transporte()
        crm = _crm(t)  # permitir_escrita=False é o padrão
        escritas = [
            lambda: crm.criar_etiqueta("lote"),
            lambda: crm.etiquetar_contato("c1", "t1"),
            lambda: crm.desetiquetar_contato("c1", "t1"),
            lambda: crm.criar_campanha(nome="n", canal_id="ch", template="t", idioma="pt_BR", componentes=[], tag_ids=["t1"]),
            lambda: crm.lancar_campanha("x"),
            lambda: crm.pausar_campanha("x"),
            lambda: crm.retomar_campanha("x"),
            lambda: crm.cancelar_campanha("x"),
        ]
        for escrita in escritas:
            with self.assertRaises(EscritaBloqueada):
                escrita()
        self.assertEqual(t.chamadas, [])

    def test_campanha_so_com_etiqueta_de_lote(self) -> None:
        # Filtro vazio ou com chave desconhecida faria o CRM enviar para todos os contatos.
        for ruim in ({}, {"tagIds": []}, {"tagIds": [""]}, {"contactIds": ["c1"]}, {"tagIds": ["t"], "search": "x"}, None):
            with self.assertRaises(ValueError):
                validar_filtros_de_campanha(ruim)
        validar_filtros_de_campanha({"tagIds": ["t1"]})
        t = Transporte()
        with self.assertRaises(ValueError):
            _crm(t, permitir_escrita=True).criar_campanha(nome="n", canal_id="ch", template="t", idioma="pt_BR", componentes=[], tag_ids=[])
        self.assertEqual(t.chamadas, [])

    def test_negocios_pagina_ate_o_fim(self) -> None:
        t = Transporte(deals=[_deal(i) for i in range(2500)])
        itens = list(_crm(t).negocios())
        self.assertEqual(len(itens), 2500)
        self.assertEqual(len(t.chamadas), 3)
        self.assertIn("perPage=1000", t.chamadas[0][1])
        self.assertIn("withTotal=0", t.chamadas[0][1])

    def test_fila_no_formato_do_painel(self) -> None:
        fila = _crm(Transporte(fila=_fila_json(45, 300_000, 10))).fila_agora()
        self.assertEqual((fila.aguardando, fila.maior_espera_s, fila.atendentes_online), (45, 300, 10))

    def test_fila_recusada_ou_fora_do_formato_levanta(self) -> None:
        with self.assertRaises(CrmErro) as ctx:
            _crm(Transporte(fila=None)).fila_agora()
        self.assertEqual(ctx.exception.status, 401)
        for ruim in ({}, {"agora": {"ok": False, "error": "omitido"}}, {"agora": {"ok": True, "data": {}}}):
            with self.assertRaises(CrmErro):
                interpretar_fila(ruim)


class CargaTest(unittest.TestCase):
    def test_normaliza_sem_levar_dado_pessoal(self) -> None:
        l = normalizar(_deal(1, dia="Sim", fin="Sim", calouro="Sim"), MAPA)
        self.assertEqual((l.contact_id, l.deal_id, l.situacao), ("c1", "d1", "Em Curso"))
        self.assertTrue(l.dia10_aberto and l.financeira_aberta and l.calouro and l.tem_telefone)
        self.assertNotIn("Fulano", repr(l))
        self.assertNotIn("5511999990000", repr(l))

    def test_campo_vazio_nao_vira_sim_nem_nao(self) -> None:
        l = normalizar(_deal(1, dia="", fin=None), MAPA)
        self.assertIsNone(l.dia10_aberto)
        self.assertIsNone(l.financeira_aberta)

    def test_sem_contato_sem_telefone_e_etiqueta_de_bloqueio(self) -> None:
        self.assertIsNone(normalizar({"id": "d1", "customFields": []}, MAPA))
        self.assertFalse(normalizar(_deal(1, phone=""), MAPA).tem_telefone)
        l = normalizar(_deal(1, tags=("Não Perturbe",)), MAPA, frozenset({"nao perturbe"}))
        self.assertEqual(l.bloqueio, "recusou_mensagens")

    def test_campo_obrigatorio_ausente_no_crm_aborta_a_carga(self) -> None:
        with self.assertRaises(CargaErro):
            montar_mapa({"dia": "f1"}, {"dia10": "dia", "financeira": "situacaofinanceira", "situacao": "situacao", "calouro": "calouro", "nivel": "nivel"})

    def test_contato_com_duas_matriculas_vira_um_aluno(self) -> None:
        a = Linha("c1", "d1", situacao="Cancelado", dia10_aberto=False)
        b = Linha("c1", "d2", situacao="Em Curso", dia10_aberto=True, bloqueio=None)
        c = Linha("c1", "d3", situacao="Em Curso", dia10_aberto=False, bloqueio="recusou_mensagens")
        r = consolidar([a, b, c])
        self.assertEqual(len(r), 1)
        self.assertEqual(r["c1"].deal_id, "d2")
        self.assertEqual(r["c1"].bloqueio, "recusou_mensagens")

    def test_ciclo_abre_tem_baixa_e_reabre(self) -> None:
        cfg = MotorConfig()
        kw = dict(ciclo="2026-11", cfg=cfg)
        aberto = Linha("c1", "d1", dia10_aberto=True)
        pago = Linha("c1", "d1", dia10_aberto=False)
        vazio = Linha("c1", "d1", dia10_aberto=None)

        self.assertEqual(evoluir_ciclo(None, pago, hoje=date(2026, 11, 4), pode_abrir=True, **kw), (None, None))
        self.assertEqual(evoluir_ciclo(None, aberto, hoje=date(2026, 11, 4), pode_abrir=False, **kw), (None, None))
        reg, mudou = evoluir_ciclo(None, aberto, hoje=date(2026, 11, 4), pode_abrir=True, **kw)
        self.assertEqual(mudou, "aberto")
        self.assertEqual(reg.aberto_em, date(2026, 11, 4))
        self.assertEqual(reg.controle, no_controle("c1", "2026-11", cfg.controle_sal, cfg.controle_pct))

        self.assertEqual(evoluir_ciclo(reg, aberto, hoje=date(2026, 11, 5), pode_abrir=True, **kw)[1], None)
        self.assertEqual(evoluir_ciclo(reg, vazio, hoje=date(2026, 11, 5), pode_abrir=True, **kw)[1], None)
        com_baixa, mudou = evoluir_ciclo(reg, pago, hoje=date(2026, 11, 9), pode_abrir=True, **kw)
        self.assertEqual((mudou, com_baixa.baixa_dia10_em), ("baixa", date(2026, 11, 9)))
        self.assertEqual(com_baixa.controle, reg.controle)  # o sorteio não muda no ciclo
        reaberto, mudou = evoluir_ciclo(com_baixa, aberto, hoje=date(2026, 11, 12), pode_abrir=True, **kw)
        self.assertEqual((mudou, reaberto.baixa_dia10_em), ("reaberto", None))

    def test_ciclo_do_mes_so_abre_quando_a_carga_da_faculdade_chega(self) -> None:
        cfg = MotorConfig(carga_min_abertos=8000)
        self.assertEqual(ciclo_vigente("2026-12", abertos=3000, ja_aberto=False, cfg=cfg), ("2026-11", False))
        self.assertEqual(ciclo_vigente("2026-12", abertos=20000, ja_aberto=False, cfg=cfg), ("2026-12", True))
        self.assertEqual(ciclo_vigente("2026-12", abertos=3000, ja_aberto=True, cfg=cfg), ("2026-12", True))
        self.assertEqual(ciclo_vigente("2027-01", abertos=0, ja_aberto=False, cfg=cfg), ("2026-12", False))


class ClassificadorTest(unittest.TestCase):
    def test_anonimiza_telefone_cpf_rgm_email_e_nome(self) -> None:
        texto = "Oi, sou Maria Souza, RGM 50574191, CPF 123.456.789-09, fone (11) 91747-9873, maria.souza@gmail.com. Paguei R$ 350,00 dia 10/11."
        limpo = anonimizar(texto, nomes=["Maria de Souza"])
        for vazou in ("Maria", "Souza", "50574191", "123.456.789-09", "91747", "9873", "gmail"):
            self.assertNotIn(vazou, limpo)
        self.assertIn("R$ 350,00", limpo)
        self.assertIn("10/11", limpo)
        self.assertIn("Paguei", limpo)

    def test_botao_e_tratado_em_codigo(self) -> None:
        self.assertEqual(classificar_botao("Já paguei").rotulo, "ja_paguei")
        self.assertEqual(classificar_botao("Não precisa me lembrar").rotulo, "silenciar")
        self.assertIsNone(classificar_botao("ja paguei ontem no app"))
        c = classificar_resposta("Já paguei", ClassificadorRegras())
        self.assertEqual((c.origem, c.confianca, c.precisa_de_revisao), ("botao", 1.0, False))

    def test_regras_classificam_e_ficam_abaixo_do_corte_de_confianca(self) -> None:
        r = ClassificadorRegras()
        self.assertEqual(r.classificar("ja paguei ontem").rotulo, "ja_paguei")
        self.assertEqual(r.classificar("me manda o boleto").rotulo, "como_pagar")
        self.assertEqual(r.classificar("quero trancar o curso").rotulo, "cancelar_trancar")
        self.assertEqual(r.classificar("bom dia").rotulo, "outro")
        self.assertTrue(r.classificar("ja paguei ontem").precisa_de_revisao)

    def test_classificador_recebe_o_texto_ja_anonimizado(self) -> None:
        visto = []

        class Espiao:
            nome = "espiao"

            def classificar(self, texto):
                visto.append(texto)
                return ClassificadorRegras().classificar(texto)

        classificar_resposta("Sou o Carlos, CPF 12345678909, vou pagar amanhã", Espiao(), nomes=["Carlos Lima"])
        self.assertEqual(len(visto), 1)
        self.assertNotIn("Carlos", visto[0])
        self.assertNotIn("12345678909", visto[0])


class PilotoFormulaTest(unittest.TestCase):
    def _retrato(self, **kw) -> Retrato:
        base = dict(
            agora=datetime(2026, 11, 24, 10, 0, tzinfo=BRT), fila=None, decisao_governador=Decisao.LIBERAR,
            motivo_governador="fila baixa", tamanho_sugerido=900,
            pendentes=(LotePendente(2, "dia25", "lembrete_dia25", 1200), LotePendente(1, "dia25", "lembrete_dia25", 1200)),
            enviados_hoje=0, teto_diario=7000, lote_max=1200, por_estado={},
        )
        base.update(kw)
        return Retrato(**base)

    def test_libera_o_proximo_lote_no_tamanho_que_a_equipe_absorve(self) -> None:
        o = PilotoFormula().proxima_ordem(self._retrato())
        self.assertEqual((o.acao, o.ordem_lote, o.tamanho), ("liberar", 1, 900))

    def test_espera_quando_o_governador_nao_libera(self) -> None:
        for d in (Decisao.SEGURAR, Decisao.PAUSAR):
            self.assertEqual(PilotoFormula().proxima_ordem(self._retrato(decisao_governador=d)).acao, "esperar")
        self.assertEqual(PilotoFormula().proxima_ordem(self._retrato(tamanho_sugerido=0)).acao, "esperar")

    def test_encerra_sem_pendentes_ou_no_teto(self) -> None:
        self.assertEqual(PilotoFormula().proxima_ordem(self._retrato(pendentes=())).acao, "encerrar")
        self.assertEqual(PilotoFormula().proxima_ordem(self._retrato(enviados_hoje=7000)).acao, "encerrar")
        o = PilotoFormula().proxima_ordem(self._retrato(enviados_hoje=6800))
        self.assertEqual(o.tamanho, 200)


class LacoEmSimulacaoTest(unittest.TestCase):
    """Um dia inteiro (terça 24/11/2026, lembrete do dia 25) com 3.000 alunos em aberto."""

    CFG = MotorConfig(enabled=True, dry_run=True, carga_min_abertos=100, lote_max=1200, teto_diario=7000)

    def setUp(self) -> None:
        self.agora = datetime(2026, 11, 24, 7, 0, tzinfo=BRT)
        deals = [_deal(i) for i in range(3000)]
        deals += [_deal(5000 + i, dia="Não") for i in range(50)]               # já pagaram
        deals += [_deal(6000 + i, situacao="Trancado") for i in range(20)]     # fora de Em Curso
        deals += [_deal(7000 + i, phone="") for i in range(10)]                # sem telefone
        self.transporte = Transporte(deals=deals, fila=_fila_json(20, 60_000, 10))
        self.razao = RazaoMemoria()
        self.razao.controle = Controle(ligado=True)
        self.laco = self._laco(self.CFG)

    def _laco(self, cfg) -> Laco:
        return Laco(cfg, self.razao, crm=_crm(self.transporte), piloto=PilotoFormula(), relogio=lambda: self.agora)

    def _ate(self, hora: int, minuto: int = 0, passo_min: int = 2) -> None:
        fim = self.agora.replace(hour=hora, minute=minuto)
        while self.agora <= fim:
            self.laco.tick()
            self.agora += timedelta(minutes=passo_min)

    def test_dia_inteiro_em_simulacao_nao_escreve_no_crm(self) -> None:
        self._ate(15, 0)
        metodos = {m for m, _ in self.transporte.chamadas}
        self.assertEqual(metodos, {"GET"})

        lotes = self.razao.lotes(date(2026, 11, 24), True)
        simulados = [l for l in lotes if l["status"] == "simulado"]
        self.assertGreaterEqual(len(simulados), 2)
        self.assertTrue(all(l["tamanho"] <= self.CFG.lote_max for l in simulados))
        # Lotes separados por pelo menos 30 minutos.
        horas = sorted(l["liberado_em"] for l in simulados)
        self.assertTrue(all(b - a >= timedelta(minutes=30) for a, b in zip(horas, horas[1:])))
        # Tudo dentro da janela das 9h às 14h.
        self.assertTrue(all(9 <= h.hour < 14 for h in horas))

        msgs = self.razao.msgs
        ids = [m["contact_id"] for m in msgs]
        self.assertEqual(len(ids), len(set(ids)))                      # ninguém recebe duas vezes no dia
        self.assertTrue(all(m["simulado"] and m["status"] == "simulado" for m in msgs))
        self.assertLessEqual(len(ids), self.CFG.teto_diario)

        # Ninguém do controle, já pago, trancado ou sem telefone entrou em lote.
        controle = {f"c{i}" for i in range(3000) if no_controle(f"c{i}", "2026-11", self.CFG.controle_sal, self.CFG.controle_pct)}
        self.assertGreater(len(controle), 100)
        self.assertFalse(controle & set(ids))
        self.assertFalse({i for i in ids if int(i[1:]) >= 5000})
        # Todo aluno elegível saiu: 3.000 menos o controle.
        self.assertEqual(len(ids), 3000 - len(controle))

        # Lote que sai menor que o previsto não deixa sobra escondida: vira lote novo no plano.
        self.assertFalse([l for l in lotes if l["status"] == "planejado"])
        # O diário não repete "lote anterior com menos de 30 min" a cada tick.
        segurados = [e["texto"] for e in self.razao.entradas_diario if "menos de 30 min" in e["texto"]]
        self.assertLessEqual(len(segurados), len(simulados))

        acoes = self.razao.acoes
        self.assertTrue(acoes and all(a["simulado"] for a in acoes.values()))
        tipos = [e["tipo"] for e in self.razao.entradas_diario]
        self.assertIn("carga", tipos)
        self.assertIn("plano", tipos)
        self.assertIn("lote", tipos)

    def test_desligado_nao_le_nem_planeja(self) -> None:
        self.razao.controle = Controle(ligado=False)
        self._ate(11, 0)
        self.assertEqual(self.transporte.chamadas, [])
        self.assertEqual(self.razao.planos, {})
        self.assertEqual(self.razao.ponto["modo"], "desligado")

        self.razao.controle = Controle(ligado=True)
        self.laco = self._laco(replace(self.CFG, enabled=False))  # MOTOR_ENABLED=0 vence a página
        self._ate(12, 0)
        self.assertEqual(self.transporte.chamadas, [])

    def test_parada_geral_interrompe_os_lotes(self) -> None:
        self._ate(9, 10)
        antes = len(self.razao.msgs)
        self.assertGreater(antes, 0)
        self.razao.controle = Controle(ligado=True, parada_geral=True, parada_motivo="teste")
        self._ate(13, 0)
        self.assertEqual(len(self.razao.msgs), antes)
        self.assertEqual(self.razao.ponto["modo"], "parado")

    def test_fila_acima_do_critico_segura_os_lotes(self) -> None:
        self.transporte.fila = _fila_json(aguardando=178, online=13)
        self._ate(13, 0)
        self.assertEqual(self.razao.msgs, [])
        self.assertTrue(self.razao.plano(date(2026, 11, 24), True))
        self.assertIn("pausar", {l["decisao"] for l in self.razao.leituras})

    def test_sem_leitura_da_fila_nada_e_liberado(self) -> None:
        self.transporte.fila = None  # hoje a API responde 401 ao Bearer nesta rota
        self._ate(13, 0)
        self.assertEqual(self.razao.msgs, [])
        self.assertTrue(any("Não consegui ler a fila" in e["texto"] for e in self.razao.entradas_diario))

    def test_fila_de_ensaio_so_vale_em_simulacao(self) -> None:
        self.transporte.fila = None
        with mock.patch.dict(os.environ, {"MOTOR_SIM_FILA": "20,10"}):
            self._ate(10, 0)
        self.assertGreater(len(self.razao.msgs), 0)
        self.assertTrue(all("fila de ensaio" in l["erro"] for l in self.razao.leituras))
        real = Laco(replace(self.CFG, dry_run=False), RazaoMemoria(), crm=None)
        with mock.patch.dict(os.environ, {"MOTOR_SIM_FILA": "20,10"}):
            self.assertIsNone(real._fila_de_ensaio(self.agora))

    def test_dry_run_desligado_continua_em_simulacao_nesta_versao(self) -> None:
        self.laco = self._laco(replace(self.CFG, dry_run=False))
        self._ate(10, 0)
        self.assertEqual({m for m, _ in self.transporte.chamadas}, {"GET"})
        self.assertEqual(self.razao.ponto["modo"], "simulacao")
        self.assertTrue(any("segue em simulação" in e["texto"] for e in self.razao.entradas_diario))

    def test_lote_menor_que_o_previsto_gera_lote_para_a_sobra(self) -> None:
        # 6 atendentes × 8 − 20 aguardando = 28 → lotes de 700, abaixo dos 1.200 planejados.
        self.transporte.fila = _fila_json(20, 60_000, 6)
        self._ate(15, 0)
        lotes = self.razao.lotes(date(2026, 11, 24), True)
        simulados = [l for l in lotes if l["status"] == "simulado"]
        # O último lote pode absorver um resto menor que o lote mínimo (até 99 a mais).
        self.assertTrue(all(l["tamanho"] <= 700 for l in simulados[:-1]))
        self.assertLess(simulados[-1]["tamanho"], 700 + self.CFG.lote_min)
        self.assertGreater(len(lotes), 3)  # o plano original tinha 3 lotes (1.200 + 1.200 + resto)
        controle = sum(no_controle(f"c{i}", "2026-11", self.CFG.controle_sal, self.CFG.controle_pct) for i in range(3000))
        self.assertEqual(len(self.razao.msgs), 3000 - controle)

    def test_teto_diario_adia_o_excedente(self) -> None:
        self.laco = self._laco(replace(self.CFG, teto_diario=1500))
        self._ate(15, 0)
        self.assertLessEqual(len(self.razao.msgs), 1500)
        plano = self.razao.plano(date(2026, 11, 24), True)
        self.assertGreater(plano["resumo"]["adiados"], 0)

    def test_dia_sem_acao_no_calendario_nao_gera_plano(self) -> None:
        self.agora = datetime(2026, 11, 12, 7, 0, tzinfo=BRT)
        self._ate(12, 0)
        self.assertEqual(self.razao.planos, {})

    def test_carga_com_falha_nao_altera_nada(self) -> None:
        self.transporte.deals = []
        self._ate(9, 0)
        self.assertEqual(self.razao.cargas, [])
        self.assertTrue(any("Carga falhou" in e["texto"] for e in self.razao.entradas_diario))

    def test_estado_aprendido_de_ciclos_anteriores_muda_o_publico(self) -> None:
        # c0 pagou em dia em setembro, outubro e (nada ainda) novembro → 2 ciclos em dia = Em dia;
        # c1 fechou outubro sem pagar → Primeiro atraso (fica fora do lembrete do dia 25).
        self.razao.ciclos["c0"] = {
            "2026-09": RegistroCiclo(date(2026, 9, 3), date(2026, 9, 8)),
            "2026-10": RegistroCiclo(date(2026, 10, 5), date(2026, 10, 9)),
        }
        self.razao.ciclos["c1"] = {"2026-10": RegistroCiclo(date(2026, 10, 5), None)}
        self._ate(15, 0)
        alunos = {a.contact_id: a for a in self.razao.alunos(ciclo="2026-11", hoje=date(2026, 11, 24), simulado=True, cfg=self.CFG)}
        self.assertEqual(alunos["c0"].estado, Estado.EM_DIA)
        self.assertEqual(alunos["c1"].estado, Estado.PRIMEIRO_ATRASO)
        ids = {m["contact_id"] for m in self.razao.msgs}
        self.assertNotIn("c1", ids)


if __name__ == "__main__":
    unittest.main()
