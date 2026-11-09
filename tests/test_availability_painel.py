"""Contrato da pausa no painel. HTTP simulado, sem SIAA e sem Supabase."""

from __future__ import annotations

import unittest

from siaa.availability import (
    consultar_availability,
    data_da_captura,
    decidir_consulta,
    mensagem_preservada,
)
from siaa.materias_alunos_client import (
    ConsultaInconclusiva,
    SessaoExpirada,
    _exigir_sessao_ou_inconclusiva,
    _viewstate_obrigatorio,
)


class _Resp:
    def __init__(self, status: int, body: str = "{}", location: str = "") -> None:
        self.status_code = status
        self._body = body
        self.headers = {"Location": location} if location else {}
        self.text = body

    def json(self):
        import json

        return json.loads(self._body)


class DisponibilidadePainelTest(unittest.TestCase):
    def test_api_antiga_segue_a_consulta(self) -> None:
        situacao = consultar_availability("https://session.test", "tok", get=lambda *_a, **_k: _Resp(404, ""))
        self.assertEqual(situacao["estado"], "api_antiga")
        self.assertEqual(decidir_consulta(situacao), "seguir")

    def test_falha_de_comunicacao_preserva_e_nao_declara_siaa_fora(self) -> None:
        def falha(*_a, **_k):
            raise TimeoutError()

        situacao = consultar_availability("https://session.test", "tok", get=falha)
        self.assertEqual(situacao["estado"], "desconhecido")
        self.assertEqual(decidir_consulta(situacao), "preservar")
        texto = mensagem_preservada(situacao, "2026-11-08T19:27:34Z")
        self.assertIn("2026-11-08T19:27:34Z", texto)
        self.assertNotIn("SIAA em pausa", texto)
        self.assertIn("nao foi consultado", texto.lower())

    def test_pausa_informa_a_data_e_nao_consulta(self) -> None:
        situacao = {
            "estado": "pausa",
            "motivo": "indisponivel_siaa",
        }
        self.assertEqual(decidir_consulta(situacao), "preservar")
        texto = mensagem_preservada(situacao, "2026-11-08T19:27:34Z")
        self.assertIn("SIAA em pausa", texto)
        self.assertIn("2026-11-08T19:27:34Z", texto)

    def test_disponivel_e_sem_configuracao_seguem(self) -> None:
        self.assertEqual(decidir_consulta({"estado": "disponivel"}), "seguir")
        self.assertEqual(
            decidir_consulta(consultar_availability("", "", get=lambda *_a, **_k: _Resp(500))),
            "seguir",
        )

    def test_configuracao_parcial_preserva_sem_parecer_expirada(self) -> None:
        for url, token in (("https://session.test", ""), ("", "tok")):
            def proibido(*_a, **_k):
                raise AssertionError("http")

            situacao = consultar_availability(url, token, get=proibido)
            self.assertEqual(situacao["estado"], "configuracao_incompleta")
            self.assertEqual(decidir_consulta(situacao), "preservar")
            texto = mensagem_preservada(situacao, "2026-11-08T19:27:34Z")
            self.assertIn("Falha de configuracao", texto)
            self.assertNotIn("Sessao expirada", texto)
            self.assertNotIn("SIAA em pausa", texto)

    def test_data_ausente_nao_vira_zero(self) -> None:
        self.assertIsNone(data_da_captura({}))
        self.assertIn("data indisponivel", mensagem_preservada({"estado": "pausa", "motivo": "http_403"}, None))
        self.assertNotIn("SIAA em pausa", mensagem_preservada({"estado": "pausa", "motivo": "http_403"}, None))

    def test_403_e_viewstate_ausente_nao_sao_sessao_expirada(self) -> None:
        with self.assertRaises(ConsultaInconclusiva):
            _exigir_sessao_ou_inconclusiva(_Resp(403, "forbidden"))
        with self.assertRaises(ConsultaInconclusiva):
            _viewstate_obrigatorio("<div>ficha</div>")
        with self.assertRaises(SessaoExpirada):
            _exigir_sessao_ou_inconclusiva(_Resp(302, "", "https://login.example/access_denied"))


if __name__ == "__main__":
    unittest.main()
