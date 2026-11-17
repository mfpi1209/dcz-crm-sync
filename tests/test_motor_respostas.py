"""Receptor de respostas, Jev, efeitos no aluno e trava de experiência. Sem rede e sem banco."""

from __future__ import annotations

import hashlib
import hmac
import json
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta

from motor.calendario import BRT
from motor.classificador import (
    JEV_URL,
    ROTULOS,
    Classificacao,
    ClassificadorJev,
    ClassificadorRegras,
    anonimizar,
    classificar_botao,
)
from motor.config import MotorConfig
from motor.crm import CrmMotor
from motor.laco import Laco
from motor.modelos import Controle
from motor.razao import RazaoMemoria, efeito_da_resposta
from motor.respostas import assinatura_valida, interpretar
from tests.test_motor_laco import Transporte, _deal, _fila_json

DIA = date(2026, 11, 24)


def _evento(conteudo="Já paguei ontem", contato="c1", **extra) -> dict:
    return {"event": "message_received", "occurredAt": "2026-11-24T13:05:00.000Z", "organizationId": "org1", "contactId": contato,
            "dealId": None, "data": {"channel": "WhatsApp", "conversationId": "conv1", "content": conteudo, "waMessageId": "wamid.1", **extra}}


class AssinaturaTest(unittest.TestCase):
    def test_confere_o_hmac_do_corpo(self) -> None:
        corpo = json.dumps(_evento()).encode("utf-8")
        boa = hmac.new(b"segredo", corpo, hashlib.sha256).hexdigest()
        self.assertTrue(assinatura_valida(corpo, "sha256=" + boa, "segredo"))
        self.assertTrue(assinatura_valida(corpo, boa.upper(), "segredo"))
        self.assertFalse(assinatura_valida(corpo + b" ", "sha256=" + boa, "segredo"))
        self.assertFalse(assinatura_valida(corpo, "sha256=" + boa, "outro"))
        self.assertFalse(assinatura_valida(corpo, "", "segredo"))
        self.assertFalse(assinatura_valida(corpo, "sha256=" + boa, ""))  # sem segredo configurado nada passa


class InterpretarTest(unittest.TestCase):
    def test_texto_livre_sai_mascarado_e_sem_classificar(self) -> None:
        r = interpretar(_evento("Sou a Maria Souza, CPF 123.456.789-09, fone 11 91747-9873, já paguei"))
        self.assertEqual((r.contact_id, r.organizacao, r.wa_message_id, r.dia), ("c1", "org1", "wamid.1", DIA))
        self.assertIsNone(r.botao)
        for vazou in ("Maria", "Souza", "123.456", "91747"):
            self.assertNotIn(vazou, r.texto_mascarado)
        self.assertIn("paguei", r.texto_mascarado)

    def test_botao_ja_vem_classificado_e_nao_guarda_texto(self) -> None:
        r = interpretar(_evento("Não quero lembretes"))
        self.assertEqual((r.botao.rotulo, r.botao.origem, r.texto_mascarado), ("pediu_parar", "botao", ""))
        self.assertEqual(interpretar(_evento("Já paguei")).botao.rotulo, "ja_paguei")

    def test_dia_e_o_de_brasilia(self) -> None:
        self.assertEqual(interpretar({**_evento(), "occurredAt": "2026-11-25T01:30:00.000Z"}).dia, DIA)

    def test_o_que_nao_e_resposta_de_aluno_e_ignorado(self) -> None:
        self.assertIsNone(interpretar({**_evento(), "event": "message_sent"}))
        self.assertIsNone(interpretar(_evento("")))
        self.assertIsNone(interpretar(_evento(contato="")))
        self.assertIsNone(interpretar("texto"))


class AnonimizarTest(unittest.TestCase):
    def test_tira_nome_em_apresentacao(self) -> None:
        self.assertNotIn("Carlos", anonimizar("Bom dia, aqui é o Carlos Lima e eu já paguei"))
        self.assertNotIn("Ana", anonimizar("Meu nome é Ana Paula"))
        self.assertIn("já paguei", anonimizar("Bom dia, aqui é o Carlos Lima e eu já paguei"))

    def test_botoes_novos(self) -> None:
        self.assertEqual(classificar_botao("Parar de receber").rotulo, "pediu_parar")
        self.assertEqual(classificar_botao("NÃO QUERO LEMBRETES").rotulo, "pediu_parar")


class JevFalso:
    def __init__(self, respostas):
        self.respostas = list(respostas)
        self.pedidos = []

    def __call__(self, url, headers, corpo):
        self.pedidos.append((url, headers, corpo))
        r = self.respostas.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _resposta(rotulo, confianca):
    return 200, {"model": "jev-1.14.0", "answers": {"tema": {"choice": rotulo, "confidence": confianca}}}


class JevTest(unittest.TestCase):
    def _jev(self, respostas, chave="chave-de-teste"):
        t = JevFalso(respostas)
        return ClassificadorJev(chave, transporte=t, dormir=lambda s: None), t

    def test_pergunta_de_escolha_com_o_texto_mascarado(self) -> None:
        jev, t = self._jev([_resposta("pediu_parar", 0.93)])
        c = jev.classificar("parem de me mandar isso")
        self.assertEqual((c.rotulo, c.confianca, c.origem, c.precisa_de_revisao), ("pediu_parar", 0.93, "jev", False))
        url, headers, corpo = t.pedidos[0]
        self.assertEqual(url, JEV_URL)
        self.assertEqual(headers["Authorization"], "Bearer chave-de-teste")
        self.assertEqual((corpo["model"], corpo["state"]), ("jev-latest", "parem de me mandar isso"))
        self.assertEqual(corpo["questions"]["tema"]["type"], "choice")
        self.assertEqual(set(corpo["questions"]["tema"]["criteria"]), set(ROTULOS))
        self.assertTrue({"pediu_parar", "reclamou", "numero_errado"} <= set(ROTULOS))

    def test_confianca_abaixo_de_07_vai_para_revisao(self) -> None:
        jev, _ = self._jev([_resposta("ja_paguei", 0.69), _resposta("ja_paguei", 0.7)])
        self.assertTrue(jev.classificar("acho que paguei").precisa_de_revisao)
        self.assertFalse(jev.classificar("paguei").precisa_de_revisao)

    def test_429_repete_e_depois_acerta(self) -> None:
        jev, t = self._jev([(429, None), (529, None), _resposta("negociar", 0.9)])
        self.assertEqual(jev.classificar("quero parcelar").rotulo, "negociar")
        self.assertEqual(len(t.pedidos), 3)

    def test_qualquer_falha_cai_nas_regras_e_vai_para_revisao(self) -> None:
        casos = [[(500, None)], [(200, {"answers": {}})], [_resposta("rotulo_inventado", 0.99)], [RuntimeError("rede caiu")], [(429, None)] * 3]
        for respostas in casos:
            jev, _ = self._jev(respostas)
            c = jev.classificar("me manda o boleto")
            self.assertEqual((c.rotulo, c.origem), ("como_pagar", "regras"))
            self.assertTrue(c.precisa_de_revisao)
            self.assertEqual(jev.falhas, 1)

    def test_sem_chave_nao_chama_ninguem(self) -> None:
        jev, t = self._jev([], chave="")
        self.assertFalse(jev.configurado)
        self.assertEqual(jev.classificar("ja paguei").origem, "regras")
        self.assertEqual(t.pedidos, [])


class Fixo:
    """Classificador de teste: devolve o que estiver no roteiro."""

    nome = "fixo"

    def __init__(self, roteiro):
        self.roteiro = roteiro
        self.vistos = []

    def classificar(self, texto):
        self.vistos.append(texto)
        return self.roteiro.get(texto) or ClassificadorRegras().classificar(texto)


class RespostasNoLacoTest(unittest.TestCase):
    CFG = MotorConfig(enabled=True, dry_run=True, carga_min_abertos=100)

    def setUp(self) -> None:
        self.agora = datetime(2026, 11, 24, 7, 0, tzinfo=BRT)
        self.transporte = Transporte(deals=[_deal(i) for i in range(1500)], fila=_fila_json(20, 60_000, 10), agora=lambda: self.agora)
        self.razao = RazaoMemoria()
        self.razao.controle = Controle(ligado=True)
        self.fixo = Fixo({})
        self.montar(self.CFG)

    def montar(self, cfg) -> None:
        crm = CrmMotor(transporte=self.transporte, rpm=60, relogio=lambda: self.agora.timestamp(), dormir=lambda s: None)
        self.laco = Laco(cfg, self.razao, crm=crm, classificador=self.fixo, relogio=lambda: self.agora)

    def ate(self, hora, minuto=0) -> None:
        fim = self.agora.replace(hour=hora, minute=minuto)
        while self.agora <= fim:
            self.laco.tick()
            self.agora += timedelta(minutes=2)

    def aluno(self, cid):
        return next(a for a in self.razao.alunos(ciclo="2026-11", hoje=DIA, simulado=True, cfg=self.CFG) if a.contact_id == cid)

    def test_texto_pendente_e_classificado_e_apagado(self) -> None:
        self.ate(7, 2)
        self.fixo.roteiro["ja paguei [numero]"] = Classificacao("ja_paguei", 0.95, "jev")
        self.razao.registrar_resposta(contact_id="c1", dia=DIA, wa_message_id="w1", texto_pendente="ja paguei [numero]")
        self.laco.tick()
        r = self.razao.respostas[0]
        self.assertEqual((r["rotulo"], r["confianca"], r["origem"], r["revisar"]), ("ja_paguei", 0.95, "jev", False))
        self.assertIsNone(r["texto_pendente"])
        self.assertIsNone(r["texto_mascarado"])          # guardar por 30 dias está desligado (D6 pendente)
        self.assertEqual(self.aluno("c1").declarou_pagamento_em, DIA)
        self.assertEqual(self.fixo.vistos, ["ja paguei [numero]"])

    def test_com_a_chave_ligada_o_texto_mascarado_fica_guardado(self) -> None:
        self.montar(replace(self.CFG, guardar_texto=True))
        self.razao.registrar_resposta(contact_id="c1", dia=DIA, texto_pendente="me manda o boleto")
        self.laco.tick()
        self.assertEqual(self.razao.respostas[0]["texto_mascarado"], "me manda o boleto")

    def test_webhook_repetido_nao_duplica(self) -> None:
        self.assertEqual(self.razao.registrar_resposta(contact_id="c1", dia=DIA, wa_message_id="w1", texto_pendente="oi"), 1)
        self.assertIsNone(self.razao.registrar_resposta(contact_id="c1", dia=DIA, wa_message_id="w1", texto_pendente="oi"))

    def test_efeitos_no_aluno(self) -> None:
        self.ate(7, 2)
        self.fixo.roteiro.update({
            "pare": Classificacao("pediu_parar", 0.4, "jev"),          # confiança baixa: mesmo assim para
            "errado": Classificacao("numero_errado", 0.9, "jev"),
            "absurdo": Classificacao("reclamou", 0.9, "jev"),
            "talvez": Classificacao("ja_paguei", 0.4, "jev"),          # confiança baixa: não vale como "já paguei"
        })
        for i, texto in enumerate(("pare", "errado", "absurdo", "talvez"), start=1):
            self.razao.registrar_resposta(contact_id=f"c{i}", dia=DIA, texto_pendente=texto)
        self.laco.tick()
        self.assertEqual(self.aluno("c1").bloqueio, "recusou_mensagens")
        self.assertEqual(self.aluno("c2").bloqueio, "numero_errado")
        self.assertTrue(self.aluno("c3").suspenso)
        self.assertIsNone(self.aluno("c4").declarou_pagamento_em)
        self.assertEqual([r["revisar"] for r in self.razao.respostas], [True, False, False, True])
        # Quem pediu para parar, reclamou ou é número errado não entra em nenhum lote do dia.
        self.ate(12, 0)
        enviados = {m["contact_id"] for m in self.razao.msgs}
        self.assertFalse({"c1", "c2", "c3"} & enviados)
        # Uma carga nova não apaga o bloqueio gravado pelo motor.
        self.assertEqual(self.aluno("c1").bloqueio, "recusou_mensagens")
        self.assertEqual(efeito_da_resposta("como_pagar"), None)

    def _parar(self, n) -> None:
        for i in range(n):
            self.razao.registrar_resposta(contact_id=f"x{i}", dia=DIA, rotulo="pediu_parar", confianca=1.0, origem="botao")

    def test_trava_de_experiencia_segura_o_proximo_lote(self) -> None:
        self.ate(9, 2)                                     # 1º lote (300) liberado às 9h
        self.assertEqual(len(self.razao.msgs), 300)
        self._parar(3)                                     # 3 casos: abaixo do mínimo de 5, nada muda
        self.ate(9, 32)
        self.assertEqual(len(self.razao.msgs), 900)        # 2º lote (600) saiu
        self._parar(8)                                     # 8 em 900 = 8,9 por 1.000: segura
        self.ate(12, 0)
        self.assertEqual(len(self.razao.msgs), 900)
        self.assertEqual(self.razao.plano(DIA, True)["status"], "proposto")
        self.assertTrue(any("trava de experiência" in e["texto"] for e in self.razao.entradas_diario))

    def test_trava_de_experiencia_encerra_o_dia(self) -> None:
        self.ate(9, 2)
        self._parar(5)                                     # 5 em 300 = 16,7 por 1.000: encerra
        self.ate(12, 0)
        self.assertEqual(len(self.razao.msgs), 300)
        self.assertEqual(self.razao.plano(DIA, True)["status"], "encerrado")
        self.assertTrue(all(l["status"] in ("simulado", "adiado") for l in self.razao.lotes(DIA, True)))
        self.assertTrue(any("Dia encerrado" in e["texto"] for e in self.razao.entradas_diario))

    def test_mesmo_aluno_reclamando_tres_vezes_conta_uma(self) -> None:
        self.ate(9, 2)
        for i in range(6):
            self.razao.registrar_resposta(contact_id="x1", dia=DIA, wa_message_id=f"w{i}", rotulo="reclamou", confianca=1.0, origem="jev")
        self.assertEqual(self.razao.experiencia_do_dia(DIA, True).casos, 1)


if __name__ == "__main__":
    unittest.main()
