"""Caminhos reais do painel com HTTP e banco simulados."""

from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import patch

for _nome in ("psycopg2", "psycopg2.extras", "openpyxl"):
    sys.modules.setdefault(_nome, types.ModuleType(_nome))
sys.modules["psycopg2"].connect = lambda *_a, **_k: None
sys.modules["psycopg2.extras"].Json = object
sys.modules["psycopg2.extras"].RealDictCursor = object
sys.modules["openpyxl"].load_workbook = lambda *_a, **_k: None

import flask

import routes.materias_alunos as materias
import routes.siaa as siaa


class _Resp:
    def __init__(self, status: int, body: str) -> None:
        self.status_code = status
        self.text = body
        self.headers: dict[str, str] = {}

    def json(self):
        import json

        return json.loads(self.text)


class LoteMateriasTest(unittest.TestCase):
    def setUp(self) -> None:
        self._url = os.environ.get("SIAA_SESSION_URL")
        self._token = os.environ.get("SIAA_SESSION_TOKEN")
        os.environ["SIAA_SESSION_URL"] = "https://session.test"
        os.environ["SIAA_SESSION_TOKEN"] = "tok-teste"
        self._job = materias._job
        materias._job = materias._new_job(2, 0)

    def tearDown(self) -> None:
        materias._job = self._job
        self._restaurar("SIAA_SESSION_URL", self._url)
        self._restaurar("SIAA_SESSION_TOKEN", self._token)

    def _restaurar(self, chave: str, valor: str | None) -> None:
        if valor is None:
            os.environ.pop(chave, None)
        else:
            os.environ[chave] = valor

    def test_pausa_entre_alunos_impede_a_segunda_consulta(self) -> None:
        respostas = [
            _Resp(200, '{"estado":"disponivel","motivo":""}'),
            _Resp(200, '{"estado":"pausa","motivo":"indisponivel_siaa"}'),
        ]
        consultados: list[str] = []

        def buscar(_cookie: str, rgm: str) -> dict:
            consultados.append(rgm)
            return {"aluno": "Aluno", "materias": [{"sigla": "AAA"}]}

        with (
            patch("requests.get", side_effect=respostas),
            patch.object(materias, "_load_cookie_academico", return_value="JSESSIONID=ficticio"),
            patch.object(materias, "buscar_materias", buscar),
            patch.object(materias, "_upsert_materias") as gravar,
        ):
            materias._worker(["100", "200"], 0)

        self.assertEqual(consultados, ["100"])
        gravar.assert_called_once()
        self.assertEqual(materias._job["status"], "error")
        self.assertNotEqual(materias._job["status"], "session_expired")
        self.assertIn("SIAA em pausa", materias._job["ultima_msg"])
        self.assertEqual(materias._job["sucesso"], 1)


class RefreshAlunoTest(unittest.TestCase):
    def setUp(self) -> None:
        self._url = os.environ.get("SIAA_SESSION_URL")
        self._token = os.environ.get("SIAA_SESSION_TOKEN")
        self.app = flask.Flask(__name__)
        self.app.secret_key = "teste"
        self.app.register_blueprint(siaa.siaa_bp)
        self.client = self.app.test_client()
        with self.client.session_transaction() as sess:
            sess["role"] = "admin"

    def tearDown(self) -> None:
        self._restaurar("SIAA_SESSION_URL", self._url)
        self._restaurar("SIAA_SESSION_TOKEN", self._token)

    def _restaurar(self, chave: str, valor: str | None) -> None:
        if valor is None:
            os.environ.pop(chave, None)
        else:
            os.environ[chave] = valor

    def _definir(self, url: str | None, token: str | None) -> None:
        self._restaurar("SIAA_SESSION_URL", url)
        self._restaurar("SIAA_SESSION_TOKEN", token)

    def _chamar(self, disponibilidade: _Resp | None = None):
        capturas = {"parser": 0}

        def parser(*_a, **_k):
            capturas["parser"] += 1

        alvo = patch("requests.get", return_value=disponibilidade) if disponibilidade else patch("requests.get")
        with (
            alvo as pedido,
            patch.object(siaa, "supabase_configured", return_value=True),
            patch.object(siaa, "process_siaa_http_rgm", parser),
            patch.object(
                siaa,
                "fetch_latest_captura",
                return_value=({"id": 1, "capturado_em": "2026-11-08T19:27:34Z"}, None),
            ),
            patch.object(siaa, "fetch_documentos", return_value=([], None)),
            patch.object(siaa, "fetch_titulos", return_value=([], None)),
            patch.object(siaa, "build_insights", return_value={}),
        ):
            resposta = self.client.get("/api/siaa/aluno/7?refresh=1")
        return resposta, capturas["parser"], pedido

    def test_configuracao_ausente_consulta_e_parcial_preserva(self) -> None:
        self._definir(None, None)
        resposta, chamadas, pedido = self._chamar()
        self.assertEqual(chamadas, 1)
        pedido.assert_not_called()
        self.assertEqual(resposta.status_code, 200)

        self._definir("https://session.test", None)
        resposta, chamadas, pedido = self._chamar()
        self.assertEqual(chamadas, 0)
        pedido.assert_not_called()
        corpo = resposta.get_json()
        self.assertEqual(corpo["refresh_status"], "preservado")
        self.assertIn("Falha de configuracao", corpo["refresh_message"])
        self.assertNotIn("Sessao expirada", corpo["refresh_message"])
        self.assertEqual(corpo["capturado_em"], "2026-11-08T19:27:34Z")
        self.assertFalse(corpo["siaa_consultado"])

        self._definir(None, "tok-teste")
        _resposta, chamadas, _pedido = self._chamar()
        self.assertEqual(chamadas, 0)

    def test_refresh_bloqueado_nao_chama_o_parser(self) -> None:
        self._definir("https://session.test", "tok-teste")
        resposta, chamadas, pedido = self._chamar(
            _Resp(200, '{"estado":"pausa","motivo":"indisponivel_siaa"}')
        )
        self.assertEqual(chamadas, 0)
        pedido.assert_called_once()
        corpo = resposta.get_json()
        self.assertEqual(corpo["refresh_status"], "preservado")
        self.assertIn("2026-11-08T19:27:34Z", corpo["refresh_message"])
        self.assertFalse(corpo["siaa_consultado"])

        _resposta, chamadas, _pedido = self._chamar(
            _Resp(200, '{"estado":"disponivel","motivo":""}')
        )
        self.assertEqual(chamadas, 1)

        _resposta, chamadas, _pedido = self._chamar(_Resp(503, ""))
        self.assertEqual(chamadas, 0)


if __name__ == "__main__":
    unittest.main()
