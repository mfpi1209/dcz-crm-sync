"""Renovação seletiva por módulo: lista branca dos campos novos (session-api simulada, sem rede)."""

from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import patch

import flask

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes.conexao_siaa import conexao_siaa_bp  # noqa: E402

TOKEN = "tok-secreto-teste"
COOKIE = "JSESSIONID=abc123-cookie-vazado"
BASE = "/api/conexao-siaa/renovacao"


class _Resp:
    def __init__(self, status, corpo):
        self.status_code = status
        self._corpo = corpo

    def json(self):
        return self._corpo


def _execucao(**extra):
    ex = {"id": "rn-1", "tipo": "automatica", "estado": "falhou",
          "gatilho": {"modulos": ["academico", "financeiro"], "motivo": "sessao_expirada"},
          "criada_em": "2026-11-19T10:00:00Z", "inicio": "2026-11-19T10:00:05Z", "fim": "2026-11-19T10:03:00Z",
          "duracao_ms": 175000, "login": "ok", "erro": "coleta_falhou", "proxima_acao": "revisar_e_liberar",
          "modulos_solicitados": ["academico", "financeiro"], "resultado": "parcial",
          "modulos": {
              "academico": {"coleta": "ok", "http": 200, "publicacao": "ok", "vinculo": None,
                            "sessao_validada": True, "situacao": "ok", "cookie": COOKIE},
              "financeiro": {"coleta": "falhou", "http": None, "publicacao": "nao_executada", "vinculo": None,
                             "sessao_validada": False, "situacao": "falhou"},
          }}
    ex.update(extra)
    return ex


def _status(**extra):
    st = {"gerado_em": "2026-11-19T10:05:00Z", "estado": "bloqueada", "motivo_estado": "episodio_bloqueado",
          "config": {"ativa": True, "suspensa": False, "versao": 4},
          "executor": {"visto_em": "2026-11-19T10:04:50Z", "ocupado": False},
          "episodio": {"id": "ep-1", "desde": "2026-11-19T09:00:00Z", "modulos": ["academico", "financeiro"],
                       "motivo": "sessao_expirada", "tentativas_automaticas": 1, "bloqueado": True,
                       "bloqueio_motivo": "falha_execucao", "liberado_em": None,
                       "modulos_estado": {
                           "academico": {"tentativas_automaticas": 1, "bloqueado": False, "bloqueio_motivo": None,
                                         "resolvido": True, "resolvido_em": "2026-11-19T10:03:00Z",
                                         "liberado_em": None, "segredo": COOKIE},
                           "financeiro": {"tentativas_automaticas": 1, "bloqueado": True,
                                          "bloqueio_motivo": "falha_execucao", "resolvido": False,
                                          "resolvido_em": None, "liberado_em": None},
                           "inscricoes": {"bloqueado": True},
                       }},
          "execucao_atual": None, "ultima_execucao": _execucao(),
          "ultima_renovacao_ok": None, "proxima_acao": "revisar_e_liberar",
          "alvos": {"renovar_agora": ["financeiro"], "liberar": ["financeiro", "inscricoes", "financeiro"],
                    "automatica": [], "extra": ["academico"]}}
    st.update(extra)
    return st


class RenovacaoSeletivaTest(unittest.TestCase):
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

    def _get_status(self, st):
        with patch("routes.conexao_siaa.requests.request", return_value=_Resp(200, st)):
            r = self.client.get(BASE + "/status")
        self.assertEqual(r.status_code, 200)
        return r

    def test_campos_novos_passam_pela_lista_branca(self):
        r = self._get_status(_status())
        self.assertNotIn("abc123", r.get_data(as_text=True))
        d = r.get_json()
        self.assertEqual(d["alvos"], {"renovar_agora": ["financeiro"], "liberar": ["financeiro"], "automatica": []})
        est = d["episodio"]["modulos_estado"]
        self.assertEqual(sorted(est), ["academico", "financeiro"])
        self.assertIs(est["academico"]["resolvido"], True)
        self.assertEqual(est["academico"]["resolvido_em"], "2026-11-19T10:03:00Z")
        self.assertNotIn("segredo", est["academico"])
        self.assertIs(est["financeiro"]["bloqueado"], True)
        self.assertEqual(est["financeiro"]["bloqueio_motivo"], "falha_execucao")
        self.assertEqual(est["financeiro"]["tentativas_automaticas"], 1)
        ex = d["ultima_execucao"]
        self.assertEqual(ex["modulos_solicitados"], ["academico", "financeiro"])
        self.assertEqual(ex["resultado"], "parcial")
        self.assertEqual(ex["modulos"]["academico"]["situacao"], "ok")
        self.assertEqual(ex["modulos"]["financeiro"]["situacao"], "falhou")
        self.assertNotIn("cookie", ex["modulos"]["academico"])

    def test_tipos_invalidos_sao_descartados(self):
        st = _status()
        st["alvos"] = {"renovar_agora": "academico", "liberar": [COOKIE, 1, None], "automatica": None}
        mest = st["episodio"]["modulos_estado"]
        mest["academico"].update(tentativas_automaticas="1", bloqueado="sim", resolvido=1,
                                 resolvido_em="ontem", bloqueio_motivo=COOKIE, liberado_em="2026-11-19")
        mest["financeiro"] = "bloqueado"
        st["ultima_execucao"] = _execucao(resultado=COOKIE, modulos_solicitados=["inscricoes", COOKIE])
        st["ultima_execucao"]["modulos"]["academico"]["situacao"] = "OK Total"
        r = self._get_status(st)
        self.assertNotIn("abc123", r.get_data(as_text=True))
        d = r.get_json()
        self.assertEqual(d["alvos"], {"renovar_agora": None, "liberar": [], "automatica": None})
        est = d["episodio"]["modulos_estado"]
        self.assertEqual(list(est), ["academico"])
        self.assertEqual(est["academico"], {"tentativas_automaticas": None, "bloqueado": None,
                                            "bloqueio_motivo": None, "resolvido": None,
                                            "resolvido_em": None, "liberado_em": None})
        ex = d["ultima_execucao"]
        self.assertIsNone(ex["resultado"])
        self.assertEqual(ex["modulos_solicitados"], [])
        self.assertIsNone(ex["modulos"]["academico"]["situacao"])

    def test_alvos_e_modulos_estado_nao_dicionario_viram_null(self):
        st = _status(alvos=["academico"])
        st["episodio"]["modulos_estado"] = ["academico"]
        d = self._get_status(st).get_json()
        self.assertIsNone(d["alvos"])
        self.assertIsNone(d["episodio"]["modulos_estado"])

    def test_backend_antigo_sem_campos_novos(self):
        st = _status()
        del st["alvos"]
        del st["episodio"]["modulos_estado"]
        for k in ("modulos_solicitados", "resultado"):
            del st["ultima_execucao"][k]
        for m in ("academico", "financeiro"):
            del st["ultima_execucao"]["modulos"][m]["situacao"]
        d = self._get_status(st).get_json()
        self.assertIsNone(d["alvos"])
        self.assertIsNone(d["episodio"]["modulos_estado"])
        ex = d["ultima_execucao"]
        # Ausente continua null (não vira lista vazia): a tela trata como "os dois".
        self.assertIsNone(ex["modulos_solicitados"])
        self.assertIsNone(ex["resultado"])
        self.assertIsNone(ex["modulos"]["academico"]["situacao"])
        self.assertEqual(ex["estado"], "falhou")

    def test_execucoes_so_um_modulo_solicitado(self):
        ex = _execucao(modulos_solicitados=["financeiro"], resultado="completo", estado="concluida")
        ex["modulos"]["academico"] = {"coleta": "nao_executada", "publicacao": "nao_executada",
                                      "sessao_validada": None, "situacao": "nao_solicitado"}
        with patch("routes.conexao_siaa.requests.request",
                   return_value=_Resp(200, {"itens": [ex], "proximo_antes": None})):
            d = self.client.get(BASE + "/execucoes").get_json()
        it = d["itens"][0]
        self.assertEqual(it["modulos_solicitados"], ["financeiro"])
        self.assertEqual(it["resultado"], "completo")
        self.assertEqual(it["modulos"]["academico"]["situacao"], "nao_solicitado")

    def test_liberar_repassa_modulos_liberados_e_corpo_igual(self):
        resp = {"episodio": {"id": "ep-1", "bloqueado": False, "modulos_estado": {
                    "financeiro": {"bloqueado": False, "tentativas_automaticas": 0, "resolvido": False}}},
                "modulos_liberados": ["financeiro", "inscricoes"], "x": COOKIE}
        with patch("routes.conexao_siaa.requests.request", return_value=_Resp(200, resp)) as m:
            r = self.client.post(BASE + "/liberar", json={"episodio_id": "ep-1", "versao": 4,
                                                          "modulos": ["academico"]})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(m.call_args[1]["json"], {"episodio_id": "ep-1", "versao": 4})
        d = r.get_json()
        self.assertEqual(d["modulos_liberados"], ["financeiro"])
        self.assertEqual(d["episodio"]["modulos_estado"]["financeiro"]["tentativas_automaticas"], 0)
        self.assertNotIn("abc123", r.get_data(as_text=True))

    def test_liberar_backend_antigo_sem_modulos_liberados(self):
        with patch("routes.conexao_siaa.requests.request",
                   return_value=_Resp(200, {"episodio": {"id": "ep-1", "bloqueado": False}})):
            d = self.client.post(BASE + "/liberar", json={"episodio_id": "ep-1", "versao": 4}).get_json()
        self.assertIsNone(d["modulos_liberados"])
        self.assertIsNone(d["episodio"]["modulos_estado"])

    def test_solicitar_corpo_nao_muda(self):
        pid = "0123456789abcdef0123456789abcdef"
        with patch("routes.conexao_siaa.requests.request",
                   return_value=_Resp(201, {"execucao": _execucao(estado="pendente", resultado=None,
                                                                  modulos_solicitados=["financeiro"])})) as m:
            r = self.client.post(BASE + "/solicitar", json={"pedido_id": pid, "confirmacao": "substituir_sessoes",
                                                            "modulos": ["academico"]})
        self.assertEqual(m.call_args[1]["json"], {"pedido_id": pid, "confirmacao": "substituir_sessoes"})
        self.assertEqual(r.get_json()["execucao"]["modulos_solicitados"], ["financeiro"])

    def test_diagnostico_aceita_campos_novos(self):
        corpo = {"formato": "conexao-siaa-diagnostico", "versao_formato": 1,
                 "renovacao": {"episodio": {"desde": "2026-11-19T09:00:00Z", "modulos": ["financeiro"],
                                            "modulos_estado": {"financeiro": {"bloqueado": True,
                                                                              "bloqueio_motivo": "resultado_incerto",
                                                                              "x": COOKIE}}}},
                 "renovacoes": [_execucao()]}
        with patch("routes.conexao_siaa.requests.request", return_value=_Resp(200, corpo)):
            r = self.client.get("/api/conexao-siaa/diagnostico")
        self.assertEqual(r.status_code, 200)
        bruto = r.get_data(as_text=True)
        self.assertNotIn("abc123", bruto)
        d = json.loads(bruto)
        self.assertEqual(d["renovacao"]["episodio"]["modulos_estado"]["financeiro"]["bloqueio_motivo"],
                         "resultado_incerto")
        it = d["renovacoes"][0]
        self.assertEqual(it["resultado"], "parcial")
        self.assertEqual(it["modulos_solicitados"], ["academico", "financeiro"])
        self.assertEqual(it["modulos"]["financeiro"]["situacao"], "falhou")


if __name__ == "__main__":
    unittest.main()
