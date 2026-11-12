"""Coleta do NPS: do JSON de GET /api/deals para pesquisas e respostas. HTTP simulado, sem banco."""

from __future__ import annotations

import io
import json
import unittest
import urllib.error
from datetime import datetime

from nps.coleta import (
    BRT,
    POR_PAGINA,
    ColetaErro,
    atendente_da_conversa,
    extrair,
    itens,
    ler_momento,
    montar_mapa,
    varrer,
)
from nps.crm import CrmErro, CrmLeitura

CAMPOS = [
    {"id": "f-env", "name": "nps_enviado_em", "entity": "deal"},
    {"id": "f-resp", "name": "nps_respondido_em", "entity": "deal"},
    {"id": "f-nota", "name": "nps_nota", "entity": "deal"},
    {"id": "f-res", "name": "nps_resolvido", "entity": "deal"},
    {"id": "f-mot", "name": "nps_motivo", "entity": "deal"},
    {"id": "f-conv", "name": "nps_conversa_origem", "entity": "deal"},
    {"id": "f-depto", "name": "nps_depto_origem", "entity": "deal"},
    {"id": "f-outro", "name": "nps_nota", "entity": "contact"},
]
AGORA = datetime(2026, 11, 20, 3, 20, tzinfo=BRT)


def _deal(deal_id="d1", contato="c1", **campos):
    ids = {"env": "f-env", "resp": "f-resp", "nota": "f-nota", "res": "f-res", "mot": "f-mot", "conv": "f-conv", "depto": "f-depto"}
    return {
        "id": deal_id,
        "contactId": contato,
        "customFields": [{"customFieldId": ids[k], "value": v} for k, v in campos.items()],
    }


class MapaTest(unittest.TestCase):
    def test_resolve_pelo_nome_e_ignora_campo_de_contato(self) -> None:
        mapa = montar_mapa(CAMPOS)
        self.assertEqual(mapa["nota"], "f-nota")
        self.assertEqual(len(mapa), 7)

    def test_sem_envio_ou_nota_a_coleta_nao_roda(self) -> None:
        with self.assertRaises(ColetaErro) as erro:
            montar_mapa([c for c in CAMPOS if c["name"] != "nps_enviado_em"])
        self.assertIn("nps_enviado_em", str(erro.exception))

    def test_campo_opcional_ausente_nao_impede(self) -> None:
        mapa = montar_mapa([c for c in CAMPOS if c["name"] not in ("nps_motivo", "nps_respondido_em")])
        self.assertNotIn("motivo", mapa)

    def test_envelope_da_api(self) -> None:
        self.assertEqual(len(itens({"items": CAMPOS})), len(CAMPOS))
        self.assertEqual(len(itens(CAMPOS)), len(CAMPOS))
        self.assertEqual(itens(None), [])


class ExtrairTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mapa = montar_mapa(CAMPOS)

    def test_negocio_sem_pesquisa_e_ignorado(self) -> None:
        self.assertIsNone(extrair(_deal(), self.mapa, AGORA))
        self.assertIsNone(extrair(_deal(nota="9"), self.mapa, AGORA))
        self.assertIsNone(extrair({"id": "", "contactId": "c"}, self.mapa, AGORA))

    def test_enviada_e_ainda_aguardando(self) -> None:
        p = extrair(_deal(env="2026-11-19T14:05:00-03:00", nota="aguardando", conv="conv-1", depto="SAC"), self.mapa, AGORA)
        self.assertIsNone(p.nota)
        self.assertIsNone(p.respondido_em)
        self.assertEqual((p.conversa_origem, p.depto), ("conv-1", "SAC"))

    def test_resposta_completa(self) -> None:
        p = extrair(
            _deal(env="2026-11-19T14:05:00-03:00", resp="2026-11-19T14:12:00-03:00", nota="3", res="Não",
                  mot="Liguei 2 vezes, meu RGM 12345678 e nada", conv="conv-1", depto="SAC"),
            self.mapa, AGORA,
        )
        self.assertEqual((p.nota, p.resolvido, p.respondido_estimado), (3, "nao", False))
        self.assertEqual(p.respondido_em.minute, 12)
        self.assertNotIn("12345678", p.motivo)

    def test_resposta_sem_hora_usa_a_hora_da_coleta_e_marca(self) -> None:
        p = extrair(_deal(env="2026-11-19T14:05:00-03:00", nota="10", res="Sim"), self.mapa, AGORA)
        self.assertTrue(p.respondido_estimado)
        self.assertEqual(p.respondido_em, AGORA)

    def test_hora_de_resposta_de_pesquisa_anterior_nao_vale(self) -> None:
        p = extrair(_deal(env="2026-11-19T14:05:00-03:00", resp="2026-10-02T09:00:00-03:00", nota="8"), self.mapa, AGORA)
        self.assertTrue(p.respondido_estimado)

    def test_resolvido_fora_do_padrao_fica_sem_leitura_mas_guarda_o_bruto(self) -> None:
        p = extrair(_deal(env="2026-11-19", nota="7", res="opcao_2"), self.mapa, AGORA)
        self.assertIsNone(p.resolvido)
        self.assertEqual(p.resolvido_bruto, "opcao_2")

    def test_momentos(self) -> None:
        self.assertEqual(ler_momento("2026-11-19").hour, 12)
        self.assertEqual(ler_momento("2026-11-19T17:05:00Z").astimezone(BRT).hour, 14)
        self.assertEqual(ler_momento("2026-11-19T14:05:00").tzinfo, BRT)
        for ruim in ("", None, "aguardando", "ontem"):
            self.assertIsNone(ler_momento(ruim))


class VarrerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mapa = montar_mapa(CAMPOS)

    def test_le_ate_a_pagina_incompleta_e_ignora_repetido(self) -> None:
        cheia = [_deal(f"d{i}", f"c{i}") for i in range(POR_PAGINA - 1)] + [_deal("dx", "cx", env="2026-11-19", nota="9")]
        resto = [_deal("dx", "cx", env="2026-11-19", nota="9"), _deal("dy", "cy", env="2026-11-18", nota="aguardando")]
        chamadas = []

        def buscar(pagina):
            chamadas.append(pagina)
            return {"items": cheia if pagina == 1 else resto}

        pesquisas, lidos = varrer(buscar, self.mapa, AGORA)
        self.assertEqual(chamadas, [1, 2])
        self.assertEqual(lidos, POR_PAGINA + 2)
        self.assertEqual(sorted(p.deal_id for p in pesquisas), ["dx", "dy"])

    def test_erro_em_qualquer_pagina_aborta(self) -> None:
        def buscar(pagina):
            if pagina == 2:
                raise CrmErro("CRM respondeu HTTP 500")
            return [_deal(f"d{i}", f"c{i}") for i in range(POR_PAGINA)]

        with self.assertRaises(CrmErro):
            varrer(buscar, self.mapa, AGORA)


class AtendenteTest(unittest.TestCase):
    def test_ultima_mensagem_humana_enviada(self) -> None:
        mensagens = [
            {"direction": "out", "authorType": "bot", "senderName": "Automação", "createdAt": "2026-11-19T17:00:00Z"},
            {"direction": "out", "authorType": "human", "senderName": "Ana Souza", "createdAt": "2026-11-19T16:00:00Z"},
            {"direction": "out", "authorType": "human", "senderName": "Bia Lima", "createdAt": "2026-11-19T16:30:00Z"},
            {"direction": "out", "authorType": "human", "senderName": "Nota Interna", "isPrivate": True, "createdAt": "2026-11-19T16:50:00Z"},
            {"direction": "in", "authorType": "human", "senderName": "Aluno", "createdAt": "2026-11-19T16:40:00Z"},
        ]
        self.assertEqual(atendente_da_conversa(mensagens), "Bia Lima")

    def test_conversa_sem_humano_fica_sem_atendente(self) -> None:
        self.assertEqual(atendente_da_conversa([{"direction": "out", "authorType": "bot", "senderName": "IA"}]), "")


class _Resp:
    def __init__(self, corpo):
        self._corpo = json.dumps(corpo).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def read(self):
        return self._corpo


class CrmLeituraTest(unittest.TestCase):
    def test_so_faz_get_com_bearer_e_monta_a_consulta(self) -> None:
        vistos = []

        def abrir(req, timeout=0):
            vistos.append(req)
            return _Resp({"items": []})

        crm = CrmLeitura(base="https://crm.test", token="eduit_x", rpm=120, dormir=lambda _s: None, abrir=abrir)
        crm.negocios(2, 1000, "")
        crm.mensagens("conv/1")
        self.assertEqual([r.get_method() for r in vistos], ["GET", "GET"])
        self.assertEqual(vistos[0].full_url, "https://crm.test/api/deals?page=2&perPage=1000")
        self.assertEqual(vistos[1].full_url, "https://crm.test/api/conversations/conv%2F1/messages")
        self.assertEqual(vistos[0].get_header("Authorization"), "Bearer eduit_x")
        self.assertFalse(any(hasattr(CrmLeitura, nome) for nome in ("post", "put", "delete", "patch")))

    def test_429_espera_e_repete(self) -> None:
        esperas, tentativas = [], []

        def abrir(req, timeout=0):
            tentativas.append(1)
            if len(tentativas) == 1:
                raise urllib.error.HTTPError(req.full_url, 429, "Too Many", {"Retry-After": "7"}, io.BytesIO(b""))
            return _Resp([])

        crm = CrmLeitura(base="https://crm.test", token="t", rpm=120, dormir=esperas.append, abrir=abrir)
        self.assertEqual(crm.campos(), [])
        self.assertIn(7.0, esperas)

    def test_sem_token_nao_sai_para_a_rede(self) -> None:
        crm = CrmLeitura(base="https://crm.test", token="", abrir=lambda *_a, **_k: self.fail("não devia chamar"))
        crm._token = ""
        with self.assertRaises(CrmErro):
            crm.campos()

    def test_erro_do_crm_nao_carrega_o_corpo_da_resposta(self) -> None:
        def abrir(req, timeout=0):
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, io.BytesIO(b'{"message":"segredo"}'))

        crm = CrmLeitura(base="https://crm.test", token="t", rpm=120, dormir=lambda _s: None, abrir=abrir)
        with self.assertRaises(CrmErro) as erro:
            crm.campos()
        self.assertIn("403", str(erro.exception))
        self.assertNotIn("segredo", str(erro.exception))


if __name__ == "__main__":
    unittest.main()
