"""Rotas /siaa-sessao/salvar e /siaa-sessao/testar com o módulo inscricoes (session-api simulada, sem rede)."""

from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import patch

import flask
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import routes.siaa as siaa_mod  # noqa: E402
from routes.siaa import siaa_bp  # noqa: E402

TOKEN = "tok-teste"
URL = "https://session.test"
CPF = "12345678900"
COOKIE = "JSESSIONID=jsid-ficticio; TS01abcd=ts-ficticio"
CURL = (
    "curl 'https://siaa.test/wsvcon01.jsf' -H 'Cookie: " + COOKIE + "' "
    "--data-raw 'form%3Acpf=" + CPF + "&javax.faces.ViewState=VS-FICTICIO-123'"
)
PROIBIDOS = ("jsid-ficticio", "ts-ficticio", "JSESSIONID", "TS01abcd", CPF, "VS-FICTICIO", "view_state",
             "hash_recebido", "versao_atual", "cookie")


class _Resp:
    def __init__(self, status, corpo, texto=""):
        self.status_code = status
        self._corpo = corpo
        self.text = texto

    def json(self):
        if self._corpo is None:
            raise ValueError("sem json")
        return self._corpo


def _resp_suja(**extra):
    d = {"ok": True, "module": "inscricoes", "alive": True, "status": 200, "classe": "disponivel",
         "formulario": "form", "length": 321,
         "cookie": COOKIE, "view_state": "VS-FICTICIO-123", "hash_recebido": "abc123hash", "versao_atual": 7,
         "sonda": {"alive": True, "status": 200, "classe": "disponivel", "cookie": COOKIE, "view_state": "VS"},
         "publicacao": {"gravada": True, "sonda_validou": True, "vinculo": "vinculado", "hash": "h",
                        "cookie": COOKIE},
         "detalhe": "JSESSIONID=jsid-ficticio"}
    d.update(extra)
    return d


class Base(unittest.TestCase):
    def setUp(self):
        app = flask.Flask(__name__)
        app.secret_key = "t"
        app.register_blueprint(siaa_bp)
        self.client = app.test_client()
        self._ps = [patch.object(siaa_mod, "SIAA_URL", URL), patch.object(siaa_mod, "SIAA_TOKEN", TOKEN)]
        for p in self._ps:
            p.start()
        self.admin()

    def tearDown(self):
        for p in self._ps:
            p.stop()

    def admin(self, role="admin"):
        with self.client.session_transaction() as s:
            s["role"] = role

    def _post(self, rota, resp=None, exc=None, module="inscricoes", cookie=CURL):
        alvo = patch("routes.siaa.requests.post", side_effect=exc) if exc else \
            patch("routes.siaa.requests.post", return_value=resp)
        with alvo as m:
            r = self.client.post(f"/siaa-sessao/{rota}", json={"module": module, "cookie": cookie})
        return r, m


class AutorizacaoEValidacaoTest(Base):
    def test_nao_admin_recebe_403_sem_chamar_a_api(self):
        self.admin("comercial")
        for rota in ("salvar", "testar"):
            r, m = self._post(rota, _Resp(200, {}))
            self.assertEqual(r.status_code, 403, rota)
            m.assert_not_called()

    def test_modulo_invalido_400(self):
        for rota in ("salvar", "testar"):
            for mod in ("outro", "", "INSCRICOES_X"):
                r, m = self._post(rota, _Resp(200, {}), module=mod)
                self.assertEqual(r.status_code, 400, (rota, mod))
                m.assert_not_called()

    def test_cookie_ausente_400(self):
        for rota in ("salvar", "testar"):
            r, m = self._post(rota, _Resp(200, {}), cookie="   ")
            self.assertEqual(r.status_code, 400)
            m.assert_not_called()

    def test_modulo_em_maiusculas_e_aceito(self):
        r, m = self._post("testar", _Resp(200, _resp_suja()), module="Inscricoes")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[0][0], f"{URL}/session/inscricoes/test")


class RepasseInscricoesTest(Base):
    def test_salvar_repassa_exatamente_curl(self):
        r, m = self._post("salvar", _Resp(200, _resp_suja()))
        self.assertEqual(r.status_code, 200)
        args, kw = m.call_args
        self.assertEqual(args[0], f"{URL}/session/inscricoes")
        self.assertEqual(kw["json"], {"curl": CURL.strip()})
        self.assertEqual(kw["headers"], {"Authorization": f"Bearer {TOKEN}"})
        self.assertIsNotNone(kw.get("timeout"))

    def test_testar_repassa_exatamente_curl(self):
        r, m = self._post("testar", _Resp(200, _resp_suja()))
        self.assertEqual(r.status_code, 200)
        args, kw = m.call_args
        self.assertEqual(args[0], f"{URL}/session/inscricoes/test")
        self.assertEqual(kw["json"], {"curl": CURL.strip()})
        self.assertEqual(kw["headers"]["Authorization"], f"Bearer {TOKEN}")

    def test_resposta_chega_filtrada(self):
        for rota in ("salvar", "testar"):
            r, _ = self._post(rota, _Resp(200, _resp_suja()))
            bruto = r.get_data(as_text=True)
            for p in PROIBIDOS:
                self.assertNotIn(p, bruto, (rota, p))
            d = r.get_json()
            self.assertIs(d["ok"], True)
            self.assertIs(d["alive"], True)
            self.assertEqual(d["status"], 200)
            self.assertEqual(d["classe"], "disponivel")
            self.assertEqual(d["length"], 321)
            self.assertEqual(d["publicacao"], {"gravada": True, "sonda_validou": True, "vinculo": "vinculado"})
            self.assertEqual({k: v for k, v in d["sonda"].items() if v is not None},
                             {"alive": True, "status": 200, "classe": "disponivel"})
            for k in ("cookie", "view_state", "hash_recebido", "versao_atual", "detalhe"):
                self.assertNotIn(k, d)

    def test_formularios_encontrados_so_ids_validos(self):
        corpo = {"ok": True, "alive": False, "etapa": "formulario", "motivo": "form_nao_achado",
                 "formularios_encontrados": ["form", "j_idt12:f", COOKIE, "a b", 5, "x" * 90]}
        r, _ = self._post("testar", _Resp(200, corpo))
        d = r.get_json()
        self.assertEqual(d["formularios_encontrados"], ["form", "j_idt12:f"])
        self.assertEqual(d["etapa"], "formulario")
        self.assertNotIn("jsid-ficticio", r.get_data(as_text=True))

    def test_400_passa_codigo_e_error(self):
        corpo = {"ok": False, "error": "cURL sem cookie de sessão", "codigo": "curl_sem_cookie", "cookie": COOKIE}
        for rota in ("salvar", "testar"):
            r, _ = self._post(rota, _Resp(400, corpo))
            self.assertEqual(r.status_code, 400)
            d = r.get_json()
            self.assertIs(d["ok"], False)
            self.assertEqual(d["error"], "cURL sem cookie de sessão")
            self.assertEqual(d["codigo"], "curl_sem_cookie")
            self.assertNotIn("jsid-ficticio", r.get_data(as_text=True))

    def test_resposta_nao_json_nao_vaza_corpo_cru(self):
        r, _ = self._post("salvar", _Resp(502, None, texto="erro " + COOKIE))
        self.assertEqual(r.status_code, 502)
        self.assertNotIn("jsid-ficticio", r.get_data(as_text=True))
        self.assertIs(r.get_json()["ok"], False)

    def test_erro_de_rede_502_generico(self):
        exc = requests.ConnectionError("falha https://session.test token=" + TOKEN + " " + COOKIE)
        for rota in ("salvar", "testar"):
            r, _ = self._post(rota, exc=exc)
            self.assertEqual(r.status_code, 502)
            bruto = r.get_data(as_text=True)
            for p in ("session.test", TOKEN, "jsid-ficticio", "falha https"):
                self.assertNotIn(p, bruto)
            self.assertIs(r.get_json()["ok"], False)
            self.assertTrue(r.get_json()["error"])

    def test_error_da_api_com_cookie_em_texto_nao_deveria_passar(self):
        # routes/siaa.py (_resposta_curl): `error` é texto livre (_txt, até 200 chars) e é repassado
        # sem filtro; se a session-api ecoar o cookie/cURL num erro, ele chega ao navegador.
        corpo = {"ok": False, "error": "falhou com " + COOKIE + " cpf " + CPF, "codigo": "x"}
        r, _ = self._post("testar", _Resp(400, corpo))
        bruto = r.get_data(as_text=True)
        self.assertNotIn("jsid-ficticio", bruto)
        self.assertNotIn(CPF, bruto)


class LogsTest(Base):
    def test_logs_sem_curl_cookie_ou_cpf(self):
        for rota in ("salvar", "testar"):
            with self.assertLogs("routes.siaa", level="DEBUG") as cap:
                self._post(rota, _Resp(200, _resp_suja()))
            texto = "\n".join(cap.output)
            self.assertIn("module=inscricoes", texto)
            self.assertIn("status=200", texto)
            self.assertIn(f"len={len(CURL.strip())}", texto)
            for p in ("jsid-ficticio", "ts-ficticio", CPF, "VS-FICTICIO", "curl ", "JSESSIONID", TOKEN):
                self.assertNotIn(p, texto, (rota, p))

    def test_log_de_erro_de_rede_sem_texto_da_excecao(self):
        exc = requests.ConnectionError("detalhe " + COOKIE + " " + CPF)
        for rota in ("salvar", "testar"):
            with self.assertLogs("routes.siaa", level="DEBUG") as cap:
                self._post(rota, exc=exc)
            texto = "\n".join(cap.output)
            self.assertIn("module=inscricoes", texto)
            for p in ("jsid-ficticio", CPF, "detalhe", TOKEN):
                self.assertNotIn(p, texto)


class RegressaoAcademicoTest(Base):
    def test_academico_e_financeiro_enviam_cookie(self):
        for mod in ("academico", "financeiro"):
            for rota, sufixo in (("salvar", ""), ("testar", "/test")):
                r, m = self._post(rota, _Resp(200, {"ok": True, "alive": True, "status": 200, "length": 5,
                                                    "extra": "mantido"}), module=mod, cookie=COOKIE)
                self.assertEqual(r.status_code, 200)
                args, kw = m.call_args
                self.assertEqual(args[0], f"{URL}/session/{mod}{sufixo}")
                self.assertEqual(kw["json"], {"cookie": COOKIE})
                # resposta segue como antes (sem lista branca)
                self.assertEqual(r.get_json()["extra"], "mantido")

    def test_academico_erro_de_rede_mantem_comportamento_antigo(self):
        r, _ = self._post("salvar", exc=requests.ConnectionError("boom"), module="academico", cookie=COOKIE)
        self.assertEqual(r.status_code, 502)
        self.assertIn("boom", r.get_json()["error"])


if __name__ == "__main__":
    unittest.main()
