"""Rotas da página NPS do Aluno: permissão, período e o que sai no JSON. Banco simulado.

Pulado quando Flask ou psycopg2 não estão instalados (a suíte padrão roda sem eles).
"""

from __future__ import annotations

import unittest
from datetime import timedelta
from unittest import mock

try:
    from flask import Flask

    from routes import nps_aluno
except ImportError:  # pragma: no cover
    Flask = None
    nps_aluno = None


class _Cur:
    def __init__(self, permissoes):
        self._permissoes = permissoes
        self._achou = False

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def execute(self, _sql, params=()):
        self._achou = tuple(params) in self._permissoes

    def fetchone(self):
        return (1,) if self._achou else None


class _Conn:
    def __init__(self, permissoes=()):
        self._permissoes = set(permissoes)

    def cursor(self, **_k):
        return _Cur(self._permissoes)

    def close(self):
        pass


def _linhas(notas, depto="SAC", atendente="Ana Souza", resolvido="sim", motivo=""):
    return [
        {"depto": depto, "atendente": atendente if n is not None else None, "nota": n,
         "resolvido": resolvido if n is not None else None, "resolvido_bruto": "", "motivo": motivo if n is not None else "",
         "mes": "2026-11", "semana": "2026-11-16"}
        for n in notas
    ]


@unittest.skipUnless(Flask, "Flask/psycopg2 não instalados")
class RotasNpsTest(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        app.secret_key = "teste"
        app.register_blueprint(nps_aluno.nps_aluno_bp)
        self.cliente = app.test_client()
        self.permissoes: set = set()
        self.linhas = _linhas([10] * 60 + [8] * 25 + [2] * 15 + [None] * 100) + _linhas([3] * 5, depto="Retenção", atendente="Caio Reis", resolvido="nao", motivo="ninguém resolveu")
        remendos = [
            mock.patch.object(nps_aluno, "get_conn", lambda: _Conn(self.permissoes)),
            mock.patch.object(nps_aluno.razao_pg, "tabelas_instaladas", lambda _c: True),
            mock.patch.object(nps_aluno.razao_pg, "linhas", lambda _c, de, ate, tipo="atendimento": self._registrar(de, ate)),
            mock.patch.object(nps_aluno.razao_pg, "ultima_coleta", lambda _c: None),
        ]
        for r in remendos:
            r.start()
            self.addCleanup(r.stop)

    def _registrar(self, de, ate):
        self.periodo = (de, ate)
        return list(self.linhas)

    def _entrar(self, role="user", uid=7):
        with self.cliente.session_transaction() as s:
            s.update(authenticated=True, role=role, user_id=uid, username="fulano")

    def test_sem_login_ou_sem_permissao_e_403(self) -> None:
        self.assertEqual(self.cliente.get("/api/nps/resumo").status_code, 403)
        self._entrar()
        self.assertEqual(self.cliente.get("/api/nps/resumo").status_code, 403)
        self.assertEqual(self.cliente.get("/api/nps/motivos").status_code, 403)

    def test_resumo_para_quem_tem_a_pagina_nao_traz_atendente(self) -> None:
        self._entrar()
        self.permissoes.add((7, "nps_aluno"))
        d = self.cliente.get("/api/nps/resumo?de=2026-11-01&ate=2026-11-30").get_json()
        self.assertTrue(d["ok"] and d["instalado"])
        self.assertEqual((d["geral"]["envios"], d["geral"]["respostas"]), (205, 105))
        self.assertEqual(d["geral"]["nps"], round(100 * (60 - 20) / 105, 1))
        self.assertFalse(d["ve_atendentes"])
        self.assertNotIn("por_atendente", d)
        self.assertNotIn("Ana Souza", str(d))
        self.assertEqual(sum(d["distribuicao"]), 105)
        sac, ret = d["por_depto"][0], d["por_depto"][1]
        self.assertEqual((sac["recorte"], sac["leitura"]), ("SAC", "comparavel"))
        self.assertEqual((ret["recorte"], ret["leitura"], ret["nps"]), ("Retenção", "insuficiente", None))
        de, ate = self.periodo
        self.assertEqual((de.date().isoformat(), ate.date().isoformat()), ("2026-11-01", "2026-12-01"))

    def test_visao_por_atendente_exige_a_segunda_permissao(self) -> None:
        self._entrar()
        self.permissoes.update({(7, "nps_aluno"), (7, "nps_aluno_atendentes")})
        d = self.cliente.get("/api/nps/resumo").get_json()
        nomes = {r["recorte"]: r for r in d["por_atendente"]}
        self.assertEqual(nomes["Ana Souza"]["respostas"], 100)
        self.assertEqual(nomes["Ana Souza"]["depto"], "SAC")
        self.assertIsNone(nomes["Caio Reis"]["nps"])

    def test_periodo_padrao_e_de_30_dias_e_o_maximo_e_limitado(self) -> None:
        self._entrar(role="admin")
        self.cliente.get("/api/nps/resumo")
        de, ate = self.periodo
        self.assertEqual(ate - de, timedelta(days=30))
        self.cliente.get("/api/nps/resumo?de=2020-01-01&ate=2026-11-30")
        de, ate = self.periodo
        self.assertEqual(ate - de, timedelta(days=nps_aluno.MAX_DIAS + 1))

    def test_motivos_filtram_por_faixa_e_nao_trazem_id_nem_atendente(self) -> None:
        self._entrar(role="admin")
        d = self.cliente.get("/api/nps/motivos?classe=detrator").get_json()
        self.assertEqual(d["total"], 5)
        self.assertEqual(set(d["motivos"][0]), {"nota", "depto", "resolvido", "motivo"})
        self.assertEqual(self.cliente.get("/api/nps/motivos?classe=promotor").get_json()["total"], 0)

    def test_tabelas_nao_instaladas_avisa_em_vez_de_quebrar(self) -> None:
        self._entrar(role="admin")
        with mock.patch.object(nps_aluno.razao_pg, "tabelas_instaladas", lambda _c: False):
            d = self.cliente.get("/api/nps/resumo").get_json()
        self.assertTrue(d["ok"])
        self.assertFalse(d["instalado"])

    def test_coletar_so_admin_e_so_com_a_chave_ligada(self) -> None:
        self._entrar()
        self.permissoes.add((7, "nps_aluno"))
        self.assertEqual(self.cliente.post("/api/nps/coletar").status_code, 403)
        self._entrar(role="admin")
        with mock.patch.dict("os.environ", {"NPS_COLETA_ENABLED": "0"}):
            self.assertEqual(self.cliente.post("/api/nps/coletar").status_code, 409)
        with mock.patch.dict("os.environ", {"NPS_COLETA_ENABLED": "1", "EDUIT_CRM_TOKEN": ""}):
            self.assertEqual(self.cliente.post("/api/nps/coletar").status_code, 409)

    def test_job_nao_e_registrado_com_a_coleta_desligada(self) -> None:
        agendador = mock.Mock()
        with mock.patch.dict("os.environ", {"NPS_COLETA_ENABLED": "0"}):
            nps_aluno.register_nps_job(agendador)
        agendador.add_job.assert_not_called()
        with mock.patch.dict("os.environ", {"NPS_COLETA_ENABLED": "1"}):
            nps_aluno.register_nps_job(agendador)
        agendador.add_job.assert_called_once()


if __name__ == "__main__":
    unittest.main()
