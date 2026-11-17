"""Preço da graduação no banco do site www.eduit.com.br (site_anhanguera).

Roda sem banco: `aplicar` recebe uma conexão falsa que guarda cada SQL.
"""

from __future__ import annotations

import os
import unittest
from decimal import Decimal
from unittest import mock

from services.site_eduit_precos import (
    Oferta,
    SiteEduitNaoConfigurado,
    aplicar,
    conectar,
    planejar,
)


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.n += 1
        if self.conn.falhar_em == self.conn.n:
            raise RuntimeError("falha simulada")
        self.conn.sqls.append((" ".join(sql.split()), params))

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, falhar_em=None):
        self.sqls, self.n, self.falhar_em = [], 0, falhar_em
        self.commits = self.rollbacks = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class PlanejarTest(unittest.TestCase):
    def test_casa_por_chave_e_monta_troca(self):
        of = [Oferta(92, 92, "marketing", "MarketingEADTecnólogo", Decimal("161"))]
        p = planejar(of, {"MarketingEADTecnólogo": 180})
        self.assertEqual(p["trocas"], [{"oferta_id": 92, "curso_id": 92, "chave": "MarketingEADTecnólogo", "de": 161, "para": 180}])

    def test_alias_cst_em(self):
        of = [Oferta(1, 1, "marketing", "Cst Em MarketingEADTecnólogo", Decimal("161"))]
        self.assertEqual(planejar(of, {"MarketingEADTecnólogo": 180})["trocas"][0]["para"], 180)

    def test_iguais_nao_gravam(self):
        of = [Oferta(60, 60, "geografia", "GeografiaEADBacharelado", Decimal("161.00"))]
        p = planejar(of, {"GeografiaEADBacharelado": 161})
        self.assertEqual(p["trocas"], [])
        self.assertEqual(p["iguais"], 1)

    def test_sem_chave_reportado(self):
        of = [Oferta(5, 5, "fisica-licenciatura", None, Decimal("340"))]
        self.assertEqual(planejar(of, {})["sem_chave"], ["fisica-licenciatura"])

    def test_curso_divergente_da_oferta_entra_na_troca(self):
        # oferta já no preço novo, mas cursos.preco (o que o site mostra) ficou para trás
        of = [Oferta(92, 92, "marketing", "MarketingEADTecnólogo", Decimal("180"), Decimal("161"))]
        p = planejar(of, {"MarketingEADTecnólogo": 180})
        self.assertEqual(p["iguais"], 0)
        self.assertEqual([t["para"] for t in p["trocas"]], [180])

    def test_sem_preco_reportado(self):
        of = [Oferta(9, 9, "x", "XEADBacharelado", Decimal("100"))]
        self.assertEqual(planejar(of, {})["sem_preco"], ["XEADBacharelado"])


class AplicarTest(unittest.TestCase):
    def _plano(self, n=1):
        trocas = [{"oferta_id": 92 + i, "curso_id": 92 + i, "chave": f"C{i}", "de": 161, "para": 180} for i in range(n)]
        return {"trocas": trocas, "iguais": 3, "sem_chave": ["fisica-licenciatura"], "sem_preco": []}

    def test_aplicar_grava_oferta_e_curso_e_commita(self):
        conn = _FakeConn()
        r = aplicar(conn, self._plano())
        self.assertEqual(conn.sqls, [
            ("UPDATE curso_ofertas SET valor = %s WHERE id = %s", (180, 92)),
            ("UPDATE cursos SET preco = %s WHERE id = %s", (180, 92)),
        ])
        self.assertEqual((conn.commits, conn.rollbacks), (1, 0))
        self.assertEqual(r["atualizados"], 1)
        self.assertEqual(r["iguais"], 3)
        self.assertEqual(r["erros"], 0)
        self.assertEqual(r["linhas"], [{"chave": "C0", "de": 161, "para": 180, "ok": True}])
        self.assertEqual(r["sem_chave"], ["fisica-licenciatura"])

    def test_rollback_em_erro(self):
        conn = _FakeConn(falhar_em=3)
        with self.assertRaises(RuntimeError):
            aplicar(conn, self._plano(2))
        self.assertEqual((conn.commits, conn.rollbacks), (0, 1))


class ConectarTest(unittest.TestCase):
    def test_config_ausente(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("SITE_EDUIT_DB_")}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(SiteEduitNaoConfigurado) as ctx:
                conectar()
        self.assertEqual(str(ctx.exception), "Site eduit.com.br não configurado (SITE_EDUIT_DB_*).")

    def _env(self, **extra):
        base = {k: v for k, v in os.environ.items() if not k.startswith("SITE_EDUIT_DB_")}
        base.update(extra)
        return mock.patch.dict(os.environ, base, clear=True)

    def test_config_sem_usuario_ou_senha(self):
        with self._env(SITE_EDUIT_DB_HOST="banco_banco", SITE_EDUIT_DB_USER="u"):
            with self.assertRaises(SiteEduitNaoConfigurado):
                conectar()
        with self._env(SITE_EDUIT_DB_HOST="banco_banco", SITE_EDUIT_DB_PASS="p"):
            with self.assertRaises(SiteEduitNaoConfigurado):
                conectar()

    def test_conexao_tem_timeouts_e_keepalive(self):
        fake = mock.MagicMock()
        with self._env(SITE_EDUIT_DB_HOST="banco_banco", SITE_EDUIT_DB_USER="u", SITE_EDUIT_DB_PASS="p"), \
                mock.patch.dict("sys.modules", {"psycopg2": fake}):
            conectar()
        kw = fake.connect.call_args.kwargs
        self.assertEqual(kw["connect_timeout"], 10)
        self.assertIn("statement_timeout=15000", kw["options"])
        self.assertIn("lock_timeout=5000", kw["options"])
        self.assertEqual((kw["keepalives"], kw["keepalives_idle"]), (1, 30))


if __name__ == "__main__":
    unittest.main()
