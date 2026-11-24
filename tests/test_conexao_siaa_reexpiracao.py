"""Alvos confirmados de "Renovar agora"/"Liberar" e reexpiração (24/11), com a session-api simulada."""
from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

import flask

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes.conexao_siaa import conexao_siaa_bp  # noqa: E402
from tests.test_conexao_siaa_renovacao_seletiva import TOKEN, _Resp, _status  # noqa: E402

BASE = "/api/conexao-siaa/renovacao"


class AlvosConfirmadosTest(unittest.TestCase):
    def setUp(self):
        self._env = patch.dict(os.environ, {"SIAA_SESSION_URL": "https://session.test", "SIAA_SESSION_TOKEN": TOKEN})
        self._env.start()
        app = flask.Flask(__name__)
        app.secret_key = "t"
        app.register_blueprint(conexao_siaa_bp)
        self.client = app.test_client()
        with self.client.session_transaction() as s:
            s["role"] = "admin"

    def tearDown(self):
        self._env.stop()

    def _post(self, rota, corpo, resp):
        with patch("routes.conexao_siaa.requests.request", return_value=resp) as req:
            r = self.client.post(f"{BASE}/{rota}", json=corpo)
        return r, req

    def test_solicitar_repassa_modulos_confirmados_em_ordem_canonica(self):
        resp = _Resp(201, {"execucao": {"id": "rn-1", "estado": "pendente"}})
        r, req = self._post("solicitar", {"pedido_id": "a" * 32, "confirmacao": "substituir_sessoes",
                                          "modulos": ["financeiro", "academico"]}, resp)
        self.assertEqual(r.status_code, 201)
        self.assertEqual(req.call_args.kwargs["json"]["modulos"], ["academico", "financeiro"])

    def test_liberar_repassa_modulos_e_sem_campo_continua_igual(self):
        resp = _Resp(200, {"episodio": {"id": "ep-1"}, "config": {"versao": 5}, "modulos_liberados": ["financeiro"]})
        r, req = self._post("liberar", {"episodio_id": "ep-1", "versao": 4, "modulos": ["financeiro"]}, resp)
        self.assertEqual(req.call_args.kwargs["json"], {"episodio_id": "ep-1", "versao": 4, "modulos": ["financeiro"]})
        r, req = self._post("liberar", {"episodio_id": "ep-1", "versao": 4}, resp)
        self.assertEqual(req.call_args.kwargs["json"], {"episodio_id": "ep-1", "versao": 4})

    def test_modulos_invalidos_sao_recusados_sem_chamar_a_api(self):
        for ruim in ([], ["inscricoes"], ["academico", "academico"], "academico", ["academico", 1]):
            r, req = self._post("solicitar", {"pedido_id": "a" * 32, "confirmacao": "substituir_sessoes",
                                              "modulos": ruim}, _Resp(201, {}))
            self.assertEqual(r.status_code, 400, ruim)
            req.assert_not_called()

    def test_alvos_mudaram_vira_mensagem_propria(self):
        r, _ = self._post("liberar", {"episodio_id": "ep-1", "versao": 4, "modulos": ["financeiro"]},
                          _Resp(409, {"error": "alvos_mudaram", "atual": ["academico", "financeiro"]}))
        d = r.get_json()
        self.assertEqual((r.status_code, d["error"]), (409, "alvos_mudaram"))
        self.assertIn("mudaram", d["message"])

    def test_status_repassa_reexpirado_em(self):
        st = _status()
        st["episodio"]["modulos_estado"]["academico"].update(resolvido=False, reexpirado_em="2026-11-24T14:49:59Z")
        st["alvos"] = {"renovar_agora": ["academico", "financeiro"], "liberar": ["academico", "financeiro"],
                       "automatica": []}
        with patch("routes.conexao_siaa.requests.request", return_value=_Resp(200, st)):
            d = self.client.get(f"{BASE}/status").get_json()
        me = d["episodio"]["modulos_estado"]
        self.assertEqual(me["academico"]["reexpirado_em"], "2026-11-24T14:49:59Z")
        self.assertFalse(me["academico"]["resolvido"])
        self.assertNotIn("segredo", me["academico"])
        self.assertEqual(d["alvos"]["renovar_agora"], ["academico", "financeiro"])


if __name__ == "__main__":
    unittest.main()
