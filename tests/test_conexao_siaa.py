"""Blueprint Conexão SIAA com a session-api simulada (sem rede, sem banco)."""

from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import patch

import flask
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes.conexao_siaa import conexao_siaa_bp  # noqa: E402

TOKEN = "tok-secreto-teste"
COOKIE = "JSESSIONID=abc123-cookie-vazado"


class _Resp:
    def __init__(self, status, corpo):
        self.status_code = status
        self._corpo = corpo

    def json(self):
        if self._corpo is None:
            raise ValueError("sem json")
        return self._corpo


def _status_api():
    res = {"em": "2026-11-16T18:00:00Z", "classe": "disponivel", "motivo": "ping_ok", "etapa": "ping",
           "modo": "jsf", "status_http": 200, "duracao_ms": 412, "origem": "agendada",
           "cookie": COOKIE}
    mod = {"estado": "valida", "ultima_confirmacao": "2026-11-16T18:00:00Z",
           "ultima_verificacao": "2026-11-16T18:00:00Z", "proxima_execucao": "2026-11-16T18:03:00Z",
           "modo_usado": "jsf", "precisa_renovacao": False, "ultimo_resultado": res,
           "cookie_header": COOKIE}
    return {
        "gerado_em": "2026-11-16T18:00:05Z", "extra_top": COOKIE,
        "worker": {"visto_em": "2026-11-16T18:00:00Z", "iniciado_em": "x", "modo": "jsf", "intervalo_s": 180,
                   "token": TOKEN},
        "agenda": {"ativa": False, "dias": [0, 1], "inicio": "07:00", "fim": "22:00",
                   "fuso": "America/Sao_Paulo", "intervalo_s": None, "versao": 3,
                   "atualizada_em": "x", "aberta_agora": True, "proxima_mudanca": None},
        "pausa_manual": {"ativa": False, "desde": None, "motivo": None, "versao": 1},
        "disponibilidade": {"estado": "disponivel", "motivo": ""},
        "modulos": {"academico": mod, "financeiro": dict(mod, estado="expirada", precisa_renovacao=True)},
        "fora_da_agenda": ["publicacao_sonda_sincrona"],
    }


class ConexaoSiaaTest(unittest.TestCase):
    def setUp(self):
        self._env = {k: os.environ.get(k) for k in ("SIAA_SESSION_URL", "SIAA_SESSION_TOKEN")}
        os.environ["SIAA_SESSION_URL"] = "https://session.test"
        os.environ["SIAA_SESSION_TOKEN"] = TOKEN
        app = flask.Flask(__name__)
        app.secret_key = "t"
        app.register_blueprint(conexao_siaa_bp)
        self.client = app.test_client()
        self.admin()

    def tearDown(self):
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def admin(self, role="admin"):
        with self.client.session_transaction() as s:
            s["role"] = role

    def _api(self, resp=None, exc=None):
        if exc is not None:
            return patch("routes.conexao_siaa.requests.request", side_effect=exc)
        return patch("routes.conexao_siaa.requests.request", return_value=resp)

    # --- autorização -------------------------------------------------------
    def test_nao_admin_recebe_403_em_todas_as_rotas(self):
        self.admin("comercial")
        with self._api(_Resp(200, {})) as m:
            self.assertEqual(self.client.get("/api/conexao-siaa/status").status_code, 403)
            self.assertEqual(self.client.get("/api/conexao-siaa/historico").status_code, 403)
            self.assertEqual(self.client.get("/api/conexao-siaa/agenda").status_code, 403)
            self.assertEqual(self.client.put("/api/conexao-siaa/agenda", json={}).status_code, 403)
            self.assertEqual(self.client.post("/api/conexao-siaa/pausa-manual", json={}).status_code, 403)
            m.assert_not_called()

    # --- repasse e vazamento -------------------------------------------------
    def test_status_repassa_token_so_no_cabecalho_e_aplica_lista_branca(self):
        with self._api(_Resp(200, _status_api())) as m:
            r = self.client.get("/api/conexao-siaa/status")
        self.assertEqual(r.status_code, 200)
        args, kw = m.call_args
        self.assertEqual(args[0], "GET")
        self.assertEqual(args[1], "https://session.test/conexao/status")
        self.assertEqual(kw["headers"]["Authorization"], f"Bearer {TOKEN}")
        self.assertIsNotNone(kw["timeout"])
        self.assertNotIn(TOKEN, json.dumps(kw.get("params")) + json.dumps(kw.get("json")))
        bruto = r.get_data(as_text=True)
        self.assertNotIn(COOKIE, bruto)
        self.assertNotIn(TOKEN, bruto)
        self.assertNotIn("session.test", bruto)
        d = r.get_json()
        self.assertTrue(d["ok"])
        self.assertEqual(d["modulos"]["academico"]["estado"], "valida")
        self.assertEqual(d["modulos"]["financeiro"]["precisa_renovacao"], True)
        self.assertEqual(d["modulos"]["academico"]["ultimo_resultado"]["status_http"], 200)
        self.assertNotIn("extra_top", d)
        self.assertNotIn("token", d["worker"])
        self.assertNotIn("cookie_header", d["modulos"]["academico"])
        self.assertNotIn("cookie", d["modulos"]["academico"]["ultimo_resultado"])

    def test_historico_valida_e_repassa_parametros(self):
        corpo = {"itens": [{"em": "a", "modulo": "academico", "classe": "disponivel", "motivo": "ping_ok",
                            "etapa": "ping", "modo": "jsf", "status_http": 200, "duracao_ms": 1,
                            "origem": "agendada", "cookie": COOKIE}], "proximo_antes": "2026-11-16T10:00:00Z"}
        with self._api(_Resp(200, corpo)) as m:
            r = self.client.get("/api/conexao-siaa/historico?modulo=academico&limite=20&antes=2026-11-16T10:00:00Z")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[1]["params"],
                         {"modulo": "academico", "limite": 20, "antes": "2026-11-16T10:00:00Z"})
        self.assertNotIn(COOKIE, r.get_data(as_text=True))
        self.assertEqual(r.get_json()["proximo_antes"], "2026-11-16T10:00:00Z")

    def test_historico_parametros_invalidos_nao_chegam_a_api(self):
        for qs in ("modulo=outro", "limite=0", "limite=201", "limite=abc", "antes=ontem", "antes=2026-11-16"):
            with self._api(_Resp(200, {})) as m:
                r = self.client.get("/api/conexao-siaa/historico?" + qs)
            self.assertEqual(r.status_code, 400, qs)
            m.assert_not_called()

    # --- erros da session-api ---------------------------------------------------
    def test_timeout_e_erro_de_rede_viram_mensagem_sem_detalhe(self):
        with self._api(exc=requests.Timeout("https://session.test " + TOKEN)):
            r = self.client.get("/api/conexao-siaa/status")
        self.assertEqual(r.status_code, 504)
        with self._api(exc=requests.ConnectionError("https://session.test " + TOKEN)):
            r2 = self.client.get("/api/conexao-siaa/status")
        self.assertEqual(r2.status_code, 502)
        for resp in (r, r2):
            t = resp.get_data(as_text=True)
            self.assertNotIn("session.test", t)
            self.assertNotIn(TOKEN, t)

    def test_erro_5xx_e_corpo_cru_nao_vazam(self):
        with self._api(_Resp(500, {"error": "Traceback " + COOKIE})):
            r = self.client.get("/api/conexao-siaa/status")
        self.assertEqual(r.status_code, 502)
        self.assertNotIn(COOKIE, r.get_data(as_text=True))
        with self._api(_Resp(401, None)):
            self.assertEqual(self.client.get("/api/conexao-siaa/status").status_code, 502)

    def test_sem_configuracao_nao_chama_a_api(self):
        os.environ.pop("SIAA_SESSION_TOKEN")
        with self._api(_Resp(200, {})) as m:
            r = self.client.get("/api/conexao-siaa/status")
        self.assertEqual(r.status_code, 503)
        m.assert_not_called()

    # --- agenda e pausa ----------------------------------------------------------
    def _agenda_ok(self, **extra):
        d = {"ativa": True, "dias": [4, 0, 1], "inicio": "22:00", "fim": "06:00", "intervalo_s": 120, "versao": 3}
        d.update(extra)
        return d

    def test_put_agenda_envia_so_campos_validos_com_versao(self):
        resposta = {"agenda": {"ativa": True, "dias": [0, 1, 4], "inicio": "22:00", "fim": "06:00",
                               "fuso": "America/Sao_Paulo", "intervalo_s": 120, "versao": 4,
                               "atualizada_em": "x", "segredo": COOKIE}}
        with self._api(_Resp(200, resposta)) as m:
            r = self.client.put("/api/conexao-siaa/agenda", json=self._agenda_ok(cookie=COOKIE))
        self.assertEqual(r.status_code, 200)
        enviado = m.call_args[1]["json"]
        self.assertEqual(set(enviado), {"ativa", "dias", "inicio", "fim", "intervalo_s", "versao"})
        self.assertEqual(enviado["dias"], [0, 1, 4])
        self.assertEqual(enviado["versao"], 3)
        self.assertEqual(r.get_json()["agenda"]["versao"], 4)
        self.assertNotIn(COOKIE, r.get_data(as_text=True))

    def test_put_agenda_valida_no_painel(self):
        ruins = [
            self._agenda_ok(dias=[]), self._agenda_ok(dias=[0, 0]), self._agenda_ok(dias=[7]),
            self._agenda_ok(inicio="25:00"), self._agenda_ok(fim="6:00"),
            self._agenda_ok(inicio="07:00", fim="07:00"), self._agenda_ok(intervalo_s=30),
            self._agenda_ok(intervalo_s=1801), self._agenda_ok(ativa="sim"), self._agenda_ok(versao="3"),
            self._agenda_ok(versao=True),
        ]
        for corpo in ruins:
            with self._api(_Resp(200, {})) as m:
                r = self.client.put("/api/conexao-siaa/agenda", json=corpo)
            self.assertEqual(r.status_code, 400, corpo)
            m.assert_not_called()
        with self._api(_Resp(200, {"agenda": {"versao": 4}})):
            ok = self.client.put("/api/conexao-siaa/agenda", json=self._agenda_ok(intervalo_s=None))
        self.assertEqual(ok.status_code, 200)

    def test_put_agenda_desativada_aceita_sem_dias(self):
        with self._api(_Resp(200, {"agenda": {"versao": 4}})):
            r = self.client.put("/api/conexao-siaa/agenda", json=self._agenda_ok(ativa=False, dias=[]))
        self.assertEqual(r.status_code, 200)

    def test_put_agenda_desativada_sem_horario_vai_como_null(self):
        corpo = self._agenda_ok(ativa=False, dias=[], inicio=None, fim=None, versao=0)
        with self._api(_Resp(200, {"agenda": {"versao": 1, "ilegivel": False}})) as m:
            r = self.client.put("/api/conexao-siaa/agenda", json=corpo)
        self.assertEqual(r.status_code, 200)
        enviado = m.call_args[1]["json"]
        self.assertEqual((enviado["inicio"], enviado["fim"], enviado["versao"]), (None, None, 0))
        self.assertIs(r.get_json()["agenda"]["ilegivel"], False)
        with self._api(_Resp(200, {})) as m:
            r = self.client.put("/api/conexao-siaa/agenda", json=self._agenda_ok(inicio=None))
        self.assertEqual(r.status_code, 400)
        m.assert_not_called()

    def test_409_e_repassado_com_versao_atual(self):
        with self._api(_Resp(409, {"error": "versao", "atual": 7, "outro": COOKIE})):
            r = self.client.put("/api/conexao-siaa/agenda", json=self._agenda_ok())
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["atual"], 7)
        self.assertNotIn(COOKIE, r.get_data(as_text=True))
        with self._api(_Resp(409, {"error": "versao", "atual": 2})):
            r2 = self.client.post("/api/conexao-siaa/pausa-manual", json={"ativa": True, "versao": 1})
        self.assertEqual(r2.status_code, 409)

    def test_400_do_servidor_e_repassado_curto(self):
        with self._api(_Resp(400, {"error": "dias: repetidos"})):
            r = self.client.put("/api/conexao-siaa/agenda", json=self._agenda_ok())
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["error"], "dias: repetidos")

    def test_pausa_manual(self):
        with self._api(_Resp(200, {"ativa": True, "desde": "x", "motivo": "manutenção", "versao": 2, "x": COOKIE})) as m:
            r = self.client.post("/api/conexao-siaa/pausa-manual",
                                 json={"ativa": True, "motivo": " manutenção ", "versao": 1, "extra": 1})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[1]["json"], {"ativa": True, "motivo": "manutenção", "versao": 1})
        self.assertEqual(r.get_json()["pausa_manual"]["versao"], 2)
        self.assertNotIn(COOKIE, r.get_data(as_text=True))
        for corpo in ({"ativa": "x", "versao": 1}, {"ativa": True, "versao": None},
                      {"ativa": True, "motivo": "a" * 201, "versao": 1}):
            with self._api(_Resp(200, {})) as m2:
                self.assertEqual(self.client.post("/api/conexao-siaa/pausa-manual", json=corpo).status_code, 400)
                m2.assert_not_called()


if __name__ == "__main__":
    unittest.main()
