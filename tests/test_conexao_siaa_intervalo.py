"""Intervalo aleatório e exportação de diagnóstico na Conexão SIAA (session-api simulada, sem rede)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

import flask
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes.conexao_siaa import conexao_siaa_bp  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOKEN = "tok-secreto-teste"
COOKIE = "JSESSIONID=abc123-cookie-vazado"
LONGO = "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6"  # parece token
DIAG = "/api/conexao-siaa/diagnostico"
AG = "/api/conexao-siaa/agenda"


class _Resp:
    def __init__(self, status, corpo):
        self.status_code = status
        self._corpo = corpo

    def json(self):
        if self._corpo is None:
            raise ValueError("sem json")
        return self._corpo


def _sonda(**extra):
    d = {"em": "2026-11-16T10:00:00Z", "modulo": "academico", "classe": "disponivel", "motivo": "ping_ok",
         "etapa": "ping", "modo": "jsf", "status_http": 200, "duracao_ms": 300, "origem": "agendada",
         "cookie": COOKIE, "rgm": "123456"}
    d.update(extra)
    return d


def _diag_api():
    mod = {"estado": "valida", "ultima_confirmacao": "2026-11-16T10:00:00Z", "ultima_verificacao": "2026-11-16T10:00:00Z",
           "proxima_execucao": "2026-11-16T10:07:00Z", "modo_usado": "jsf", "precisa_renovacao": False,
           "ultimo_resultado": {k: v for k, v in _sonda().items() if k != "modulo"}, "cookie_header": COOKIE}
    return {
        "formato": "conexao-siaa-diagnostico", "versao_formato": 1, "exportado_em": "2026-11-16T18:00:00Z",
        "periodo": {"desde": "2026-11-15T18:00:00Z", "ate": "2026-11-16T18:00:00Z"},
        "agenda": {"ativa": True, "dias": [0, 1], "inicio": "07:00", "fim": "22:00", "fuso": "America/Sao_Paulo",
                   "intervalo_s": None, "intervalo_modo": "aleatorio", "intervalo_min_min": 5, "intervalo_max_min": 8,
                   "versao": 3, "atualizada_em": "2026-11-16T09:00:00Z", "arquivo_interno": "/srv/x", "token": TOKEN},
        "intervalos": {"modo": "aleatorio", "fixo_s": 180, "min_s": 300, "max_s": 480, "sorteado_s": 412,
                       "proxima_execucao": "2026-11-16T18:07:00Z", "seed": 99},
        "worker": {"visto_em": "2026-11-16T18:00:00Z", "iniciado_em": "2026-11-16T10:00:00Z", "modo": "jsf",
                   "executor_id": "ex-1"},
        "pausa_manual": {"ativa": True, "desde": "2026-11-16T12:00:00Z", "motivo": "texto livre do admin"},
        "disponibilidade": {"estado": "disponivel", "motivo": "agenda_aberta"},
        "modulos": {"academico": mod, "financeiro": dict(mod, estado=COOKIE)},
        "renovacao": {"config": {"ativa": True, "suspensa": False, "versao": 4, "token": TOKEN},
                      "estado": "monitorando", "motivo_estado": "Agenda aberta; sessões válidas.",
                      "ultima_renovacao_ok": "2026-11-16T10:03:00Z",
                      "episodio": {"id": "ep-1", "desde": "2026-11-16T10:00:00Z", "modulos": ["financeiro", "x"],
                                   "motivo": "sessao_expirada", "tentativas_automaticas": 1, "bloqueado": False,
                                   "bloqueio_motivo": None, "pedido_id": "0" * 32}},
        "sondas": [_sonda(), _sonda(motivo=COOKIE, classe=LONGO)],
        "renovacoes": [{"criada_em": "2026-11-16T10:00:00Z", "inicio": "2026-11-16T10:00:05Z",
                        "fim": "2026-11-16T10:03:00Z", "tipo": "automatica", "estado": "concluida",
                        "gatilho": "sessao_expirada", "duracao_ms": 175000, "login": "ok",
                        "modulos": {"academico": {"coleta": "ok", "http": 200, "publicacao": "ok", "vinculo": None,
                                                  "sessao_validada": True, "cookie": COOKIE}},
                        "erro": None, "janela_fechou_durante": False, "execucao_id": "rn-1", "publicacao_id": "p"}],
        "limites": {"max_sondas": 5000, "max_renovacoes": 500, "truncado": False},
        "extra_top": COOKIE,
    }


class Base(unittest.TestCase):
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


class IntervaloAgendaTest(Base):
    def _ag(self, **extra):
        d = {"ativa": True, "dias": [0, 1], "inicio": "07:00", "fim": "22:00", "intervalo_s": None, "versao": 3,
             "intervalo_modo": "aleatorio", "intervalo_min_min": 5, "intervalo_max_min": 8}
        d.update(extra)
        return d

    def _put(self, corpo):
        with self._api(_Resp(200, {"agenda": {"versao": 4}})) as m:
            r = self.client.put(AG, json=corpo)
        return r, m

    def test_nao_admin_403_agenda_e_exportacao(self):
        self.admin("comercial")
        with self._api(_Resp(200, {})) as m:
            self.assertEqual(self.client.put(AG, json=self._ag()).status_code, 403)
            self.assertEqual(self.client.get(DIAG).status_code, 403)
            self.assertEqual(self.client.get(DIAG + "?periodo=7d").status_code, 403)
            m.assert_not_called()

    def test_aleatorio_valido_e_repassado(self):
        r, m = self._put(self._ag(extra="x"))
        self.assertEqual(r.status_code, 200)
        env = m.call_args[1]["json"]
        self.assertEqual(set(env), {"ativa", "dias", "inicio", "fim", "intervalo_s", "versao",
                                    "intervalo_modo", "intervalo_min_min", "intervalo_max_min"})
        self.assertEqual((env["intervalo_modo"], env["intervalo_min_min"], env["intervalo_max_min"]), ("aleatorio", 5, 8))
        r, m = self._put(self._ag(intervalo_min_min=1, intervalo_max_min=30))
        self.assertEqual(r.status_code, 200)
        r, m = self._put(self._ag(intervalo_min_min=8, intervalo_max_min=8))
        self.assertEqual(r.status_code, 200)

    def test_aleatorio_invalido_nao_chega_a_api(self):
        ruins = [
            self._ag(intervalo_min_min=9, intervalo_max_min=8), self._ag(intervalo_min_min=0),
            self._ag(intervalo_max_min=31), self._ag(intervalo_min_min=None), self._ag(intervalo_max_min=None),
            self._ag(intervalo_min_min="5"), self._ag(intervalo_max_min=True), self._ag(intervalo_min_min=2.5),
            self._ag(intervalo_modo="outro"), self._ag(intervalo_modo=None),
        ]
        sem = self._ag()
        del sem["intervalo_min_min"], sem["intervalo_max_min"]
        ruins.append(sem)
        for corpo in ruins:
            r, m = self._put(corpo)
            self.assertEqual(r.status_code, 400, corpo)
            m.assert_not_called()

    def test_modo_fixo_inalterado_sem_campos_novos(self):
        corpo = {"ativa": True, "dias": [1, 0], "inicio": "22:00", "fim": "06:00", "intervalo_s": 120, "versao": 3}
        r, m = self._put(corpo)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(set(m.call_args[1]["json"]),
                         {"ativa", "dias", "inicio", "fim", "intervalo_s", "versao"})
        # campos de minutos soltos, sem modo, não são aceitos em silêncio
        r, m = self._put(dict(corpo, intervalo_min_min=5))
        self.assertEqual(r.status_code, 400)
        m.assert_not_called()

    def test_modo_fixo_explicito_guarda_par_valido_ou_null(self):
        base = {"ativa": True, "dias": [0], "inicio": "07:00", "fim": "22:00", "intervalo_s": 90, "versao": 1,
                "intervalo_modo": "fixo"}
        for mn, mx, esperado in ((5, 8, (5, 8)), (None, None, (None, None)), (9, 8, (None, None)), (0, 99, (None, None))):
            r, m = self._put(dict(base, intervalo_min_min=mn, intervalo_max_min=mx))
            self.assertEqual(r.status_code, 200)
            env = m.call_args[1]["json"]
            self.assertEqual((env["intervalo_s"], env["intervalo_modo"]), (90, "fixo"))
            self.assertEqual((env["intervalo_min_min"], env["intervalo_max_min"]), esperado)
        r, m = self._put(dict(base, intervalo_s=30))
        self.assertEqual(r.status_code, 400)

    def test_agenda_desativada_sem_horario_e_409_preservados(self):
        corpo = self._ag(ativa=False, dias=[], inicio=None, fim=None)
        r, m = self._put(corpo)
        self.assertEqual(r.status_code, 200)
        env = m.call_args[1]["json"]
        self.assertEqual((env["inicio"], env["fim"]), (None, None))
        r, m = self._put(self._ag(inicio=None))
        self.assertEqual(r.status_code, 400)
        with self._api(_Resp(409, {"error": "versao", "atual": 7})):
            r = self.client.put(AG, json=self._ag())
        self.assertEqual((r.status_code, r.get_json()["atual"]), (409, 7))

    def test_lista_branca_dos_campos_novos_em_status_e_agenda(self):
        status = {"worker": {"visto_em": "x", "intervalo_s": 412, "intervalo_modo": "aleatorio", "intervalo_min_s": 300,
                             "intervalo_max_s": 480, "intervalo_sorteado_s": 412,
                             "proxima_execucao": "2026-11-16T18:07:00Z", "token": TOKEN},
                  "agenda": {"ativa": True, "intervalo_modo": "aleatorio", "intervalo_min_min": 5,
                             "intervalo_max_min": 8, "intervalo_s": None, "versao": 3, "cookie": COOKIE},
                  "modulos": {"academico": {"proxima_execucao": "2026-11-16T18:07:00Z", "cookie": COOKIE}}}
        with self._api(_Resp(200, status)):
            d = self.client.get("/api/conexao-siaa/status").get_json()
        w = d["worker"]
        self.assertEqual((w["intervalo_modo"], w["intervalo_min_s"], w["intervalo_max_s"], w["intervalo_sorteado_s"]),
                         ("aleatorio", 300, 480, 412))
        self.assertEqual(w["proxima_execucao"], "2026-11-16T18:07:00Z")
        self.assertNotIn("token", w)
        a = d["agenda"]
        self.assertEqual((a["intervalo_modo"], a["intervalo_min_min"], a["intervalo_max_min"]), ("aleatorio", 5, 8))
        self.assertNotIn("cookie", a)
        self.assertEqual(d["modulos"]["academico"]["proxima_execucao"], "2026-11-16T18:07:00Z")
        # tipos errados e valores com cara de cookie viram null
        ruim = {"worker": {"intervalo_modo": COOKIE, "intervalo_min_s": "300", "intervalo_sorteado_s": True},
                "agenda": {"intervalo_modo": 5, "intervalo_min_min": 5.5}}
        with self._api(_Resp(200, ruim)):
            d = self.client.get("/api/conexao-siaa/status").get_json()
        self.assertIsNone(d["worker"]["intervalo_modo"])
        self.assertIsNone(d["worker"]["intervalo_min_s"])
        self.assertIsNone(d["worker"]["intervalo_sorteado_s"])
        self.assertIsNone(d["agenda"]["intervalo_modo"])
        self.assertIsNone(d["agenda"]["intervalo_min_min"])


class DiagnosticoTest(Base):
    def _get(self, qs="", resp=None):
        with self._api(resp or _Resp(200, _diag_api())) as m:
            r = self.client.get(DIAG + qs)
        return r, m

    def test_periodos_calculam_desde_ate_utc(self):
        horas = {"6h": 6, "24h": 24, "3d": 72, "7d": 168, "": 24}
        for per, h in horas.items():
            r, m = self._get("?periodo=" + per if per else "")
            self.assertEqual(r.status_code, 200, per)
            args, kw = m.call_args
            self.assertEqual((args[0], args[1]), ("GET", "https://session.test/conexao/diagnostico"))
            self.assertEqual(kw["headers"]["Authorization"], f"Bearer {TOKEN}")
            p = kw["params"]
            self.assertEqual(set(p), {"desde", "ate"})
            for v in p.values():
                self.assertRegex(v, r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
            from datetime import datetime
            f = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
            self.assertEqual((f(p["ate"]) - f(p["desde"])).total_seconds(), h * 3600, per)
        for ruim in ("1h", "30d", "abc", "24h;x"):
            r, m = self._get("?periodo=" + ruim)
            self.assertEqual(r.status_code, 400, ruim)
            m.assert_not_called()

    def test_cabecalhos_de_download(self):
        r, _ = self._get("?periodo=24h")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.headers["Content-Type"].startswith("application/json"))
        self.assertRegex(r.headers["Content-Disposition"],
                         r'^attachment; filename="conexao-siaa-diagnostico-\d{8}T\d{6}Z\.json"$')
        self.assertEqual(r.headers["Cache-Control"], "no-store")
        json.loads(r.get_data(as_text=True))

    def test_lista_branca_remove_campos_e_valores_sensiveis(self):
        r, _ = self._get()
        txt = r.get_data(as_text=True)
        for proibido in (COOKIE, TOKEN, LONGO, "session.test", "executor_id", "ex-1", "pedido_id", "publicacao_id",
                         "execucao_id", "rn-1", "ep-1", "123456", "texto livre do admin", "arquivo_interno",
                         "extra_top", "seed", "cookie", "rgm", "motivo\": \"texto"):
            self.assertNotIn(proibido, txt, proibido)
        d = json.loads(txt)
        self.assertEqual(d["formato"], "conexao-siaa-diagnostico")
        self.assertEqual(d["agenda"]["intervalo_modo"], "aleatorio")
        self.assertEqual(d["intervalos"]["sorteado_s"], 412)
        self.assertEqual(d["intervalos"]["proxima_execucao"], "2026-11-16T18:07:00Z")
        self.assertEqual(d["pausa_manual"], {"ativa": True, "desde": "2026-11-16T12:00:00Z"})
        self.assertEqual(set(d["worker"]), {"visto_em", "iniciado_em", "modo"})
        self.assertIsNone(d["modulos"]["financeiro"]["estado"])
        self.assertEqual(d["modulos"]["academico"]["ultimo_resultado"]["status_http"], 200)
        self.assertEqual(d["renovacao"]["episodio"]["modulos"], ["financeiro"])
        self.assertNotIn("id", d["renovacao"]["episodio"])
        self.assertEqual(d["renovacao"]["motivo_estado"], "Agenda aberta; sessões válidas.")
        self.assertEqual(len(d["sondas"]), 2)
        self.assertEqual((d["sondas"][1]["motivo"], d["sondas"][1]["classe"]), (None, None))
        self.assertEqual(d["renovacoes"][0]["gatilho"], "sessao_expirada")
        self.assertEqual(d["renovacoes"][0]["modulos"]["academico"]["http"], 200)
        self.assertNotIn("cookie", d["renovacoes"][0]["modulos"]["academico"])
        self.assertEqual(set(d), {"formato", "versao_formato", "exportado_em", "periodo", "agenda", "intervalos",
                                  "worker", "pausa_manual", "disponibilidade", "modulos", "renovacao", "sondas",
                                  "renovacoes", "limites"})

    def test_texto_com_cara_de_segredo_em_motivo_estado_e_descartado(self):
        for ruim in ("sessao=" + LONGO, "Bearer abc", LONGO, "x" * 200):
            api = _diag_api()
            api["renovacao"]["motivo_estado"] = ruim
            r, _ = self._get(resp=_Resp(200, api))
            self.assertIsNone(r.get_json()["renovacao"]["motivo_estado"], ruim)

    def test_formato_inesperado_e_listas_limitadas(self):
        api = _diag_api()
        api["formato"] = "outro"
        api["sondas"] = [_sonda()] * 5100
        api["renovacoes"] = [{"tipo": "manual"}] * 600
        r, _ = self._get(resp=_Resp(200, api))
        d = r.get_json()
        self.assertIsNone(d["formato"])
        self.assertEqual((len(d["sondas"]), len(d["renovacoes"])), (5000, 500))

    def test_erros_da_api(self):
        casos = [(_Resp(400, {"error": "periodo maior que 7 dias " + COOKIE}), 400), (_Resp(401, None), 502),
                 (_Resp(404, None), 502), (_Resp(500, {"error": "Traceback " + COOKIE}), 502),
                 (_Resp(200, ["nao-dict"]), 502), (_Resp(200, None), 502)]
        for resp, esperado in casos:
            r, _ = self._get(resp=resp)
            self.assertEqual(r.status_code, esperado)
            t = r.get_data(as_text=True)
            self.assertNotIn(COOKIE, t)
            self.assertNotIn("attachment", r.headers.get("Content-Disposition", ""))
        for exc, st in ((requests.Timeout("x " + TOKEN), 504), (requests.ConnectionError("https://session.test"), 502)):
            with self._api(exc=exc):
                r = self.client.get(DIAG)
            self.assertEqual(r.status_code, st)
            self.assertNotIn(TOKEN, r.get_data(as_text=True))
            self.assertNotIn("session.test", r.get_data(as_text=True))
        os.environ.pop("SIAA_SESSION_TOKEN")
        r, m = self._get()
        self.assertEqual(r.status_code, 503)
        m.assert_not_called()


@unittest.skipUnless(shutil.which("node"), "node não instalado")
class IntervaloJsTest(unittest.TestCase):
    def test_harness(self):
        r = subprocess.run(["node", os.path.join(RAIZ, "tests", "js", "conexao_siaa_intervalo_harness.js")],
                           capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
