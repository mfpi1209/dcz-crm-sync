"""Módulo inscricoes na Conexão SIAA: status, histórico e diagnóstico incluem; renovação continua sem ele."""

from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import patch

import flask

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes.conexao_siaa import conexao_siaa_bp  # noqa: E402

TOKEN = "tok-teste"
COOKIE = "JSESSIONID=jsid-ficticio; TS01abcd=ts-ficticio"
BASE = "/api/conexao-siaa"


class _Resp:
    def __init__(self, status, corpo):
        self.status_code = status
        self._corpo = corpo

    def json(self):
        return self._corpo


def _mod(**extra):
    d = {"estado": "valida", "ultima_confirmacao": "2026-11-16T18:00:00Z", "ultima_verificacao": "2026-11-16T18:00:00Z",
         "proxima_execucao": None, "modo_usado": None, "precisa_renovacao": False, "ultimo_resultado": None}
    d.update(extra)
    return d


class Base(unittest.TestCase):
    def setUp(self):
        self._env = {k: os.environ.get(k) for k in ("SIAA_SESSION_URL", "SIAA_SESSION_TOKEN")}
        os.environ["SIAA_SESSION_URL"] = "https://session.test"
        os.environ["SIAA_SESSION_TOKEN"] = TOKEN
        app = flask.Flask(__name__)
        app.secret_key = "t"
        app.register_blueprint(conexao_siaa_bp)
        self.client = app.test_client()
        with self.client.session_transaction() as s:
            s["role"] = "admin"

    def tearDown(self):
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _api(self, resp):
        return patch("routes.conexao_siaa.requests.request", return_value=resp)


class StatusTest(Base):
    def _status(self, estado_insc):
        return {"gerado_em": "2026-11-16T18:00:05Z", "extra_top": COOKIE,
                "modulos": {"academico": _mod(), "financeiro": _mod(),
                            "inscricoes": _mod(estado=estado_insc, cookie_header=COOKIE, cookie=COOKIE,
                                               view_state="VS-FICTICIO", hash_atual="h",
                                               ultimo_resultado={"em": "2026-11-16T18:00:00Z", "classe": "sessao_expirada",
                                                                 "motivo": "redirect_login", "status_http": 302,
                                                                 "cookie": COOKIE}),
                            "outro": _mod()}}

    def test_estados_de_inscricoes_passam_sem_campos_extras(self):
        for est in ("nao_configurada", "atualizacao_manual"):
            with self._api(_Resp(200, self._status(est))):
                r = self.client.get(BASE + "/status")
            self.assertEqual(r.status_code, 200)
            d = r.get_json()
            self.assertEqual(d["modulos"]["inscricoes"]["estado"], est)
            self.assertEqual(d["modulos"]["inscricoes"]["ultimo_resultado"]["classe"], "sessao_expirada")
            self.assertEqual(set(d["modulos"]), {"academico", "financeiro", "inscricoes"})
            bruto = r.get_data(as_text=True)
            for p in ("jsid-ficticio", "ts-ficticio", "VS-FICTICIO", "cookie", "view_state", "hash_atual", "extra_top"):
                self.assertNotIn(p, bruto, p)

    def test_modulo_inscricoes_ausente_nao_quebra(self):
        corpo = self._status("valida")
        del corpo["modulos"]["inscricoes"]
        with self._api(_Resp(200, corpo)):
            r = self.client.get(BASE + "/status")
        self.assertEqual(r.status_code, 200)
        # session-api antiga: o módulo aparece vazio (o painel mostra "Sem dados"), sem erro.
        self.assertIsNone(r.get_json()["modulos"]["inscricoes"]["estado"])


class HistoricoTest(Base):
    def test_historico_inscricoes_repassado(self):
        corpo = {"itens": [{"em": "2026-11-16T10:00:00Z", "modulo": "inscricoes", "classe": "sessao_expirada",
                            "motivo": "redirect_login", "etapa": "ping", "modo": "get", "status_http": 302,
                            "duracao_ms": 5, "origem": "agendada", "cookie": COOKIE}],
                 "proximo_antes": None}
        with self._api(_Resp(200, corpo)) as m:
            r = self.client.get(BASE + "/historico?modulo=inscricoes&limite=10")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[1]["params"], {"modulo": "inscricoes", "limite": 10})
        self.assertEqual(r.get_json()["itens"][0]["modulo"], "inscricoes")
        self.assertNotIn("jsid-ficticio", r.get_data(as_text=True))

    def test_modulo_invalido_recusado(self):
        for mod in ("outro", "insc", "inscricoes;x"):
            with self._api(_Resp(200, {})) as m:
                r = self.client.get(BASE + "/historico?modulo=" + mod)
            self.assertEqual(r.status_code, 400, mod)
            m.assert_not_called()


class DiagnosticoTest(Base):
    def test_diagnostico_mantem_inscricoes(self):
        corpo = {"formato": "conexao-siaa-diagnostico", "versao_formato": 1, "exportado_em": "2026-11-16T18:00:00Z",
                 "modulos": {"academico": _mod(), "financeiro": _mod(),
                             "inscricoes": _mod(estado="atualizacao_manual", cookie_header=COOKIE)},
                 "sondas": [{"em": "2026-11-16T10:00:00Z", "modulo": "inscricoes", "classe": "sessao_expirada",
                             "motivo": "redirect_login", "etapa": "ping", "modo": "get", "status_http": 302,
                             "duracao_ms": 4, "origem": "agendada", "cookie": COOKIE}]}
        with self._api(_Resp(200, corpo)):
            r = self.client.get(BASE + "/diagnostico")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.get_data(as_text=True))
        self.assertEqual(d["modulos"]["inscricoes"]["estado"], "atualizacao_manual")
        self.assertEqual(d["sondas"][0]["modulo"], "inscricoes")
        self.assertNotIn("jsid-ficticio", r.get_data(as_text=True))


class RenovacaoSemInscricoesTest(Base):
    def _exec(self):
        mod = {"coleta": "ok", "http": 200, "publicacao": "ok", "vinculo": None, "sessao_validada": True}
        return {"id": "rn-1", "tipo": "automatica", "estado": "concluida",
                "gatilho": {"modulos": ["academico", "inscricoes"], "motivo": "sessao_expirada"},
                "criada_em": "2026-11-16T10:00:00Z", "login": "ok",
                "modulos": {"academico": mod, "financeiro": mod, "inscricoes": mod}}

    def test_status_da_renovacao_descarta_inscricoes(self):
        st = {"gerado_em": "2026-11-16T10:05:00Z", "estado": "monitorando",
              "episodio": {"id": "ep-1", "desde": "2026-11-16T10:00:00Z", "modulos": ["academico", "inscricoes"],
                           "motivo": "sessao_expirada", "tentativas_automaticas": 0, "bloqueado": False},
              "ultima_execucao": self._exec()}
        with self._api(_Resp(200, st)):
            r = self.client.get(BASE + "/renovacao/status")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertEqual(d["episodio"]["modulos"], ["academico"])
        ex = d["ultima_execucao"]
        self.assertEqual(ex["gatilho"]["modulos"], ["academico"])
        self.assertEqual(set(ex["modulos"]), {"academico", "financeiro"})
        self.assertNotIn("inscricoes", r.get_data(as_text=True))

    def test_execucoes_descartam_inscricoes(self):
        with self._api(_Resp(200, {"itens": [self._exec()], "proximo_antes": None})):
            r = self.client.get(BASE + "/renovacao/execucoes")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("inscricoes", r.get_data(as_text=True))

    def test_diagnostico_renovacoes_e_episodio_descartam_inscricoes(self):
        corpo = {"formato": "conexao-siaa-diagnostico", "versao_formato": 1,
                 "renovacao": {"estado": "monitorando",
                               "episodio": {"desde": "2026-11-16T10:00:00Z", "modulos": ["academico", "inscricoes"],
                                            "motivo": "sessao_expirada"}},
                 "renovacoes": [
                     {"criada_em": "2026-11-16T10:00:00Z", "tipo": "automatica", "estado": "concluida",
                      "gatilho": {"modulos": ["academico", "inscricoes"], "motivo": "sessao_expirada"},
                      "modulos": {"academico": {"coleta": "ok"}, "inscricoes": {"coleta": "ok"}}},
                     {"criada_em": "2026-11-16T09:00:00Z", "tipo": "automatica", "estado": "concluida",
                      "gatilho": "sessao_expirada", "modulos": ["academico", "inscricoes"]}]}
        with self._api(_Resp(200, corpo)):
            r = self.client.get(BASE + "/diagnostico")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.get_data(as_text=True))
        self.assertEqual(d["renovacao"]["episodio"]["modulos"], ["academico"])
        self.assertEqual(d["renovacoes"][0]["gatilho"]["modulos"], ["academico"])
        self.assertEqual(list(d["renovacoes"][0]["modulos"]), ["academico"])
        self.assertEqual(d["renovacoes"][1]["modulos"], ["academico"])
        self.assertNotIn("inscricoes", json.dumps(d["renovacao"]) + json.dumps(d["renovacoes"]))

    def test_helpers_diretos(self):
        from routes.conexao_siaa import _d_modulos_ren, _mods
        self.assertEqual(_mods(["academico", "inscricoes", "financeiro"]), ["academico", "financeiro"])
        self.assertEqual(_mods(["inscricoes"]), [])
        self.assertEqual(_d_modulos_ren({"inscricoes": {"coleta": "ok"}}), {})
        self.assertEqual(_d_modulos_ren(["academico", "inscricoes"]), ["academico"])


if __name__ == "__main__":
    unittest.main()
