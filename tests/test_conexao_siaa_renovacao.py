"""Renovação automática no blueprint Conexão SIAA (session-api simulada, sem rede)."""

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
PEDIDO = "0123456789abcdef0123456789abcdef"
BASE = "/api/conexao-siaa/renovacao"


class _Resp:
    def __init__(self, status, corpo):
        self.status_code = status
        self._corpo = corpo

    def json(self):
        if self._corpo is None:
            raise ValueError("sem json")
        return self._corpo


def _execucao(**extra):
    mod = {"coleta": "ok", "http": 200, "publicacao": "ok", "vinculo": "vinculado",
           "sessao_validada": True, "cookie": COOKIE}
    ex = {"id": "rn-abc123", "tipo": "automatica", "estado": "concluida",
          "gatilho": {"modulos": ["financeiro"], "motivo": "sessao_expirada", "cookie": COOKIE},
          "criada_em": "2026-11-16T10:00:00Z", "inicio": "2026-11-16T10:00:05Z",
          "fim": "2026-11-16T10:03:00Z", "duracao_ms": 175000,
          "ultimo_progresso": {"etapa": "finalizando", "em": "2026-11-16T10:02:59Z", "x": COOKIE},
          "janela_fechou_durante": False, "login": "ok",
          "modulos": {"academico": dict(mod), "financeiro": dict(mod, sessao_validada=False)},
          "erro": None, "proxima_acao": "nenhuma", "cookie_header": COOKIE}
    ex.update(extra)
    return ex


def _status():
    return {"gerado_em": "2026-11-16T10:05:00Z", "estado": "monitorando",
            "motivo_estado": "Agenda aberta; sessões válidas.",
            "config": {"ativa": True, "suspensa": False, "versao": 4, "atualizada_em": "2026-11-16T09:00:00Z",
                       "token": TOKEN},
            "executor": {"visto_em": "2026-11-16T10:04:50Z", "ocupado": False, "executor_id": "abc"},
            "episodio": {"id": "ep-9f", "desde": "2026-11-16T10:00:00Z", "modulos": ["financeiro", "outro"],
                         "motivo": "sessao_expirada", "tentativas_automaticas": 1, "bloqueado": True,
                         "bloqueio_motivo": "falha_login", "liberado_em": None, "senha": "x"},
            "execucao_atual": None, "ultima_execucao": _execucao(),
            "ultima_renovacao_ok": "2026-11-16T10:03:00Z", "proxima_acao": "revisar_e_liberar",
            "extra_top": COOKIE}


class RenovacaoTest(unittest.TestCase):
    def setUp(self):
        self._env = {k: os.environ.get(k) for k in ("SIAA_SESSION_URL", "SIAA_SESSION_TOKEN")}
        os.environ["SIAA_SESSION_URL"] = "https://session.test"
        os.environ["SIAA_SESSION_TOKEN"] = TOKEN
        app = flask.Flask(__name__)
        app.secret_key = "t"
        app.register_blueprint(conexao_siaa_bp)
        self.app = app
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
            self.assertEqual(self.client.get(BASE + "/status").status_code, 403)
            self.assertEqual(self.client.get(BASE + "/execucoes").status_code, 403)
            self.assertEqual(self.client.put(BASE + "/config", json={}).status_code, 403)
            self.assertEqual(self.client.post(BASE + "/suspensao", json={}).status_code, 403)
            self.assertEqual(self.client.post(BASE + "/solicitar", json={}).status_code, 403)
            self.assertEqual(self.client.post(BASE + "/liberar", json={}).status_code, 403)
            m.assert_not_called()

    def test_sem_sessao_tambem_403(self):
        c = self.app.test_client()
        with self._api(_Resp(200, {})) as m:
            self.assertEqual(c.get(BASE + "/status").status_code, 403)
            m.assert_not_called()

    # --- status e lista branca --------------------------------------------
    def test_status_token_so_no_cabecalho_e_lista_branca(self):
        with self._api(_Resp(200, _status())) as m:
            r = self.client.get(BASE + "/status")
        self.assertEqual(r.status_code, 200)
        args, kw = m.call_args
        self.assertEqual(args[0], "GET")
        self.assertEqual(args[1], "https://session.test/renovacao/status")
        self.assertEqual(kw["headers"]["Authorization"], f"Bearer {TOKEN}")
        self.assertNotIn(TOKEN, json.dumps(kw.get("params")) + json.dumps(kw.get("json")))
        self.assertIsNotNone(kw["timeout"])
        bruto = r.get_data(as_text=True)
        for segredo in (COOKIE, TOKEN, "session.test", "cookie_header", "executor_id", "senha"):
            self.assertNotIn(segredo, bruto)
        d = r.get_json()
        self.assertEqual(d["estado"], "monitorando")
        self.assertEqual(d["config"]["versao"], 4)
        self.assertNotIn("token", d["config"])
        self.assertNotIn("extra_top", d)
        self.assertEqual(d["episodio"]["modulos"], ["financeiro"])
        ex = d["ultima_execucao"]
        self.assertEqual(ex["modulos"]["academico"]["publicacao"], "ok")
        self.assertIs(ex["modulos"]["financeiro"]["sessao_validada"], False)
        self.assertNotIn("cookie", ex["modulos"]["academico"])
        self.assertNotIn("cookie", ex["gatilho"])
        self.assertNotIn("x", ex["ultimo_progresso"])
        self.assertIsNone(d["execucao_atual"])

    def test_valores_com_cara_de_cookie_em_campos_de_lista_fixa_sao_descartados(self):
        st = _status()
        st["ultima_execucao"] = _execucao(erro=COOKIE, estado=COOKIE, login=COOKIE,
                                          proxima_acao=COOKIE, id=COOKIE)
        st["ultima_execucao"]["modulos"]["academico"]["vinculo"] = COOKIE
        st["ultima_execucao"]["modulos"]["academico"]["publicacao"] = COOKIE
        with self._api(_Resp(200, st)):
            r = self.client.get(BASE + "/status")
        self.assertNotIn("abc123", r.get_data(as_text=True))
        ex = r.get_json()["ultima_execucao"]
        self.assertIsNone(ex["erro"])
        self.assertIsNone(ex["id"])
        self.assertIsNone(ex["modulos"]["academico"]["vinculo"])

    def test_episodio_nulo_continua_nulo(self):
        st = _status()
        st["episodio"] = None
        with self._api(_Resp(200, st)):
            d = self.client.get(BASE + "/status").get_json()
        self.assertIsNone(d["episodio"])

    # --- execuções ---------------------------------------------------------
    def test_execucoes_repassa_parametros_e_filtra(self):
        corpo = {"itens": [_execucao()], "proximo_antes": "2026-11-16T09:00:00Z"}
        with self._api(_Resp(200, corpo)) as m:
            r = self.client.get(BASE + "/execucoes?limite=100&antes=2026-11-16T10:00:00Z")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[0][1], "https://session.test/renovacao/execucoes")
        self.assertEqual(m.call_args[1]["params"], {"limite": 100, "antes": "2026-11-16T10:00:00Z"})
        self.assertNotIn(COOKIE, r.get_data(as_text=True))
        self.assertEqual(r.get_json()["proximo_antes"], "2026-11-16T09:00:00Z")
        self.assertEqual(len(r.get_json()["itens"]), 1)

    def test_execucoes_parametros_invalidos_nao_chegam_a_api(self):
        for qs in ("limite=0", "limite=101", "limite=abc", "antes=ontem", "antes=2026-11-16"):
            with self._api(_Resp(200, {})) as m:
                r = self.client.get(BASE + "/execucoes?" + qs)
            self.assertEqual(r.status_code, 400, qs)
            m.assert_not_called()

    # --- config / suspensão ------------------------------------------------
    def test_config_envia_so_campos_validos(self):
        with self._api(_Resp(200, {"config": {"ativa": True, "suspensa": False, "versao": 5,
                                              "atualizada_em": "2026-11-16T10:00:00Z", "token": TOKEN}})) as m:
            r = self.client.put(BASE + "/config", json={"ativa": True, "versao": 4, "extra": COOKIE})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[0][0], "PUT")
        self.assertEqual(m.call_args[1]["json"], {"ativa": True, "versao": 4})
        self.assertEqual(r.get_json()["config"]["versao"], 5)
        self.assertNotIn(TOKEN, r.get_data(as_text=True))

    def test_config_e_suspensao_validam(self):
        ruins = [{}, {"ativa": "sim", "versao": 1}, {"ativa": True}, {"ativa": True, "versao": "1"},
                 {"ativa": True, "versao": True}, {"ativa": True, "versao": -1}]
        for corpo in ruins:
            with self._api(_Resp(200, {})) as m:
                self.assertEqual(self.client.put(BASE + "/config", json=corpo).status_code, 400, corpo)
                m.assert_not_called()
        with self._api(_Resp(200, {})) as m:
            self.assertEqual(self.client.post(BASE + "/suspensao", json={"ativa": True, "versao": 1}).status_code, 400)
            self.assertEqual(self.client.post(BASE + "/suspensao", data="x").status_code, 400)
            m.assert_not_called()

    def test_suspensao(self):
        with self._api(_Resp(200, {"config": {"ativa": True, "suspensa": True, "versao": 6}})) as m:
            r = self.client.post(BASE + "/suspensao", json={"suspensa": True, "versao": 5})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[1]["json"], {"suspensa": True, "versao": 5})
        self.assertEqual(m.call_args[0][1], "https://session.test/renovacao/suspensao")
        self.assertTrue(r.get_json()["config"]["suspensa"])

    # --- solicitar ---------------------------------------------------------
    def test_solicitar_valida_pedido_e_confirmacao(self):
        ruins = [{}, {"pedido_id": PEDIDO}, {"pedido_id": PEDIDO, "confirmacao": "sim"},
                 {"confirmacao": "substituir_sessoes"},
                 {"pedido_id": "abc", "confirmacao": "substituir_sessoes"},
                 {"pedido_id": PEDIDO + "0", "confirmacao": "substituir_sessoes"},
                 {"pedido_id": "z" * 32, "confirmacao": "substituir_sessoes"},
                 {"pedido_id": 123, "confirmacao": "substituir_sessoes"}]
        for corpo in ruins:
            with self._api(_Resp(201, {})) as m:
                r = self.client.post(BASE + "/solicitar", json=corpo)
            self.assertEqual(r.status_code, 400, corpo)
            m.assert_not_called()

    def test_solicitar_201_e_idempotente_200(self):
        corpo = {"pedido_id": PEDIDO, "confirmacao": "substituir_sessoes", "extra": COOKIE}
        with self._api(_Resp(201, {"execucao": _execucao(tipo="manual", estado="pendente")})) as m:
            r = self.client.post(BASE + "/solicitar", json=corpo)
        self.assertEqual(r.status_code, 201)
        self.assertEqual(m.call_args[1]["json"], {"pedido_id": PEDIDO, "confirmacao": "substituir_sessoes"})
        self.assertEqual(m.call_args[0][1], "https://session.test/renovacao/solicitar")
        self.assertEqual(r.get_json()["execucao"]["tipo"], "manual")
        self.assertNotIn(COOKIE, r.get_data(as_text=True))
        with self._api(_Resp(200, {"execucao": _execucao(tipo="manual")})):
            self.assertEqual(self.client.post(BASE + "/solicitar", json=corpo).status_code, 200)

    def test_solicitar_409_repassa_o_motivo(self):
        corpo = {"pedido_id": PEDIDO, "confirmacao": "substituir_sessoes"}
        for codigo in ("execucao_ativa", "pausa_manual"):
            with self._api(_Resp(409, {"error": codigo, "detalhe": COOKIE})):
                r = self.client.post(BASE + "/solicitar", json=corpo)
            self.assertEqual(r.status_code, 409)
            d = r.get_json()
            self.assertEqual(d["error"], codigo)
            self.assertTrue(d["message"])
            self.assertNotIn(COOKIE, r.get_data(as_text=True))

    # --- liberar -----------------------------------------------------------
    def test_liberar(self):
        with self._api(_Resp(200, {"episodio": {"id": "ep-9f", "bloqueado": False, "tentativas_automaticas": 0,
                                                "modulos": ["financeiro"], "x": COOKIE}})) as m:
            r = self.client.post(BASE + "/liberar", json={"episodio_id": "ep-9f", "versao": 4, "z": 1})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[1]["json"], {"episodio_id": "ep-9f", "versao": 4})
        self.assertFalse(r.get_json()["episodio"]["bloqueado"])
        self.assertNotIn(COOKIE, r.get_data(as_text=True))
        for corpo in ({}, {"episodio_id": "ep 9f", "versao": 1}, {"episodio_id": "ep-9f"},
                      {"episodio_id": "ep-9f", "versao": "1"}):
            with self._api(_Resp(200, {})) as m2:
                self.assertEqual(self.client.post(BASE + "/liberar", json=corpo).status_code, 400, corpo)
                m2.assert_not_called()

    # --- 409, erros, timeout ----------------------------------------------
    def test_409_de_versao_com_atual(self):
        with self._api(_Resp(409, {"error": "versao", "atual": 9})):
            r = self.client.put(BASE + "/config", json={"ativa": True, "versao": 1})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error"], "versao")
        self.assertEqual(r.get_json()["atual"], 9)
        with self._api(_Resp(409, {"error": "versao", "atual": 9})):
            r = self.client.post(BASE + "/liberar", json={"episodio_id": "ep-1", "versao": 1})
        self.assertEqual(r.get_json()["atual"], 9)

    def test_400_da_api_vira_mensagem_generica(self):
        with self._api(_Resp(400, {"error": "detalhe interno " + COOKIE})):
            r = self.client.put(BASE + "/config", json={"ativa": True, "versao": 1})
        self.assertEqual(r.status_code, 400)
        self.assertNotIn(COOKIE, r.get_data(as_text=True))

    def test_timeout_erro_de_rede_e_5xx_sem_detalhe(self):
        for exc, http in ((requests.Timeout("lento " + COOKIE), 504),
                          (requests.ConnectionError("https://session.test " + TOKEN), 502)):
            with self._api(exc=exc):
                r = self.client.get(BASE + "/status")
            self.assertEqual(r.status_code, http)
            bruto = r.get_data(as_text=True)
            self.assertNotIn(TOKEN, bruto)
            self.assertNotIn("session.test", bruto)
            self.assertNotIn(COOKIE, bruto)
        with self._api(_Resp(500, {"trace": "boom " + COOKIE})):
            r = self.client.post(BASE + "/solicitar", json={"pedido_id": PEDIDO, "confirmacao": "substituir_sessoes"})
        self.assertEqual(r.status_code, 502)
        self.assertNotIn(COOKIE, r.get_data(as_text=True))

    def test_timeouts_curtos(self):
        with self._api(_Resp(200, _status())) as m:
            self.client.get(BASE + "/status")
        conexao, leitura = m.call_args[1]["timeout"]
        self.assertLessEqual(conexao, 5)
        self.assertLessEqual(leitura, 10)

    def test_sem_configuracao_nao_chama_a_api(self):
        os.environ.pop("SIAA_SESSION_TOKEN", None)
        with self._api(_Resp(200, {})) as m:
            self.assertEqual(self.client.get(BASE + "/status").status_code, 503)
            m.assert_not_called()

    # --- executor nunca exposto -------------------------------------------
    def test_rotas_do_executor_nao_existem_no_painel(self):
        regras = [r.rule for r in self.app.url_map.iter_rules()]
        self.assertFalse([r for r in regras if "executor" in r])
        with self._api(_Resp(200, {})) as m:
            for metodo, caminho in (("post", "/executor/sondar"), ("post", "/executor/rn-1/progresso"),
                                    ("post", "/executor/rn-1/concluir"), ("post", "/executor/rn-1/interrompida")):
                r = getattr(self.client, metodo)(BASE + caminho, json={})
                self.assertEqual(r.status_code, 404, caminho)
            m.assert_not_called()

    def test_metodos_nao_previstos_sao_recusados(self):
        with self._api(_Resp(200, {})) as m:
            self.assertEqual(self.client.post(BASE + "/status").status_code, 405)
            self.assertEqual(self.client.get(BASE + "/solicitar").status_code, 405)
            self.assertEqual(self.client.delete(BASE + "/config").status_code, 405)
            m.assert_not_called()


if __name__ == "__main__":
    unittest.main()
