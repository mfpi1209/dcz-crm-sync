"""Livro-razão no Postgres e rotas da página, contra um banco de TESTE.

Só roda com MOTOR_TEST_DSN apontando para um banco cujo nome termina em
"_teste" (o teste limpa as tabelas motor_). Sem a variável, é pulado: a suíte
padrão continua sem banco.

    MOTOR_TEST_DSN="host=127.0.0.1 port=55432 user=motor dbname=dcz_sync_teste" \
        python -m unittest tests.test_motor_pg
"""

from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

DSN = os.getenv("MOTOR_TEST_DSN", "")

try:
    import psycopg2
except ImportError:  # suíte padrão roda sem psycopg2
    psycopg2 = None

from motor.calendario import BRT
from motor.config import MotorConfig
from motor.modelos import Controle

RAIZ = Path(__file__).resolve().parent.parent
HOJE = date(2026, 11, 24)


def _conectar():
    return psycopg2.connect(DSN)


@unittest.skipUnless(DSN and psycopg2, "defina MOTOR_TEST_DSN para rodar contra um Postgres de teste")
class RazaoPgTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        conn = _conectar()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                nome = cur.fetchone()[0]
                if not nome.endswith("_teste"):
                    raise unittest.SkipTest(f"banco '{nome}' não é de teste (o nome precisa terminar em _teste)")
                ddl = (RAIZ / "sql" / "motor_ativacoes.sql").read_text(encoding="utf-8")
                cur.execute(ddl)
                cur.execute(ddl)  # idempotente
            conn.commit()
        finally:
            conn.close()

    def setUp(self) -> None:
        from motor.razao_pg import TABELAS, RazaoPg

        conn = _conectar()
        with conn, conn.cursor() as cur:
            cur.execute("TRUNCATE " + ", ".join(TABELAS) + " RESTART IDENTITY CASCADE")
            cur.execute("INSERT INTO motor_controle (id, ligado) VALUES (1, TRUE)")
        conn.close()
        self.razao = RazaoPg(_conectar)

    def test_tabelas_instaladas_e_controle_inicial(self) -> None:
        self.assertTrue(self.razao.instalado())
        self.assertEqual(self.razao.ler_controle(), Controle(ligado=True, parada_geral=False, parada_motivo="", atualizado_por=""))

    def test_dia_inteiro_em_simulacao_gravando_no_postgres(self) -> None:
        from tests.test_motor_laco import Transporte, _crm, _deal, _fila_json
        from motor.laco import Laco
        from motor.sorteio import no_controle

        cfg = MotorConfig(enabled=True, dry_run=True, carga_min_abertos=100)
        deals = [_deal(i) for i in range(2600)] + [_deal(9000 + i, dia="Não") for i in range(40)]
        transporte = Transporte(deals=deals, fila=_fila_json(20, 60_000, 10))
        relogio = {"t": datetime(2026, 11, 24, 7, 0, tzinfo=BRT)}
        laco = Laco(cfg, self.razao, crm=_crm(transporte), relogio=lambda: relogio["t"])
        while relogio["t"].hour < 15:
            laco.tick()
            relogio["t"] += timedelta(minutes=5)

        self.assertEqual({m for m, _ in transporte.chamadas}, {"GET"})
        lotes = self.razao.lotes(HOJE, True)
        simulados = [l for l in lotes if l["status"] == "simulado"]
        self.assertGreaterEqual(len(simulados), 2)
        controle = sum(no_controle(f"c{i}", "2026-11", cfg.controle_sal, cfg.controle_pct) for i in range(2600))
        self.assertEqual(self.razao.enviados_hoje(HOJE, True), 2600 - controle)

        conn = _conectar()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT contact_id), BOOL_AND(simulado) FROM motor_mensagens")
            total, distintos, tudo_simulado = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM motor_aluno_ciclo WHERE ciclo = '2026-11' AND controle")
            controle_gravado = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM motor_mensagens m JOIN motor_aluno_ciclo c USING (contact_id) WHERE c.controle")
            controle_que_recebeu = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM motor_acoes WHERE NOT simulado")
            acoes_reais = cur.fetchone()[0]
            cur.execute("SELECT modo, dry_run FROM motor_worker")
            modo, dry_run = cur.fetchone()
        conn.close()
        self.assertEqual(total, distintos)
        self.assertTrue(tudo_simulado)
        self.assertEqual(controle_gravado, controle)
        self.assertEqual(controle_que_recebeu, 0)
        self.assertEqual(acoes_reais, 0)
        self.assertEqual((modo, dry_run), ("simulacao", True))

    def test_banco_recusa_duas_mensagens_no_mesmo_dia(self) -> None:
        from motor.modelos import Frente, Lote, Mensagem

        plano_id = self.razao.salvar_plano(
            dia=HOJE, simulado=True, ciclo="2026-11", piloto="teste", resumo={},
            lotes=[{"ordem": 1, "frente": "dia25", "mensagem": "lembrete_dia25", "tamanho": 2},
                   {"ordem": 2, "frente": "dia25", "mensagem": "lembrete_dia25", "tamanho": 1}],
        )
        self.assertTrue(plano_id)
        l1, l2 = self.razao.lotes(HOJE, True)
        kw = dict(ciclo="2026-11", dia=HOJE, simulado=True, status="simulado", motivo="", fila=None, agora=datetime(2026, 11, 24, 9, 0, tzinfo=BRT))
        self.razao.liberar_lote(l1["id"], Lote(1, Frente.DIA25, Mensagem.LEMBRETE_DIA25, ("a", "b")), {}, **kw)
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            self.razao.liberar_lote(l2["id"], Lote(2, Frente.DIA25, Mensagem.LEMBRETE_DIA25, ("b",)), {}, **kw)
        # A transação do lote recusado foi desfeita: o lote 2 continua planejado.
        self.assertEqual([l["status"] for l in self.razao.lotes(HOJE, True)], ["simulado", "planejado"])
        self.assertEqual(self.razao.enviados_hoje(HOJE, True), 2)

    def test_acao_tem_chave_de_idempotencia(self) -> None:
        self.assertTrue(self.razao.registrar_acao("sim:x:1", "lancar_lote", lote_id=None, simulado=True))
        self.assertFalse(self.razao.registrar_acao("sim:x:1", "lancar_lote", lote_id=None, simulado=True))

    def test_rotas_da_pagina(self) -> None:
        try:
            from flask import Flask
        except ImportError:
            self.skipTest("flask não instalado")
        os.environ.setdefault("DB_HOST", "127.0.0.1")
        import routes.motor_ativacoes as rota

        rota.get_conn = _conectar
        app = Flask(__name__)
        app.secret_key = "teste"
        app.register_blueprint(rota.motor_ativacoes_bp)
        cliente = app.test_client()

        self.assertEqual(cliente.get("/api/motor/estado").status_code, 403)
        self.assertEqual(cliente.post("/api/motor/controle", json={"acao": "parar"}).status_code, 403)

        with cliente.session_transaction() as s:
            s.update(authenticated=True, role="admin", user_id=1, username="marcelo")
        estado = cliente.get("/api/motor/estado").get_json()
        self.assertTrue(estado["ok"] and estado["instalado"])
        self.assertTrue(estado["controle"]["ligado"])
        self.assertIsNone(estado["worker"])
        self.assertTrue(estado["agenda"])

        self.assertEqual(cliente.post("/api/motor/controle", json={"acao": "explodir"}).status_code, 400)
        r = cliente.post("/api/motor/controle", json={"acao": "parar", "motivo": "teste de parada"})
        self.assertEqual(r.get_json(), {"ok": True, "acao": "parar"})
        self.assertEqual(self.razao.ler_controle(), Controle(True, True, "teste de parada", "marcelo"))
        cliente.post("/api/motor/controle", json={"acao": "retomar"})
        cliente.post("/api/motor/controle", json={"acao": "desligar"})
        self.assertEqual(self.razao.ler_controle(), Controle(False, False, "", "marcelo"))
        diario = cliente.get("/api/motor/diario").get_json()["itens"]
        self.assertEqual([d["autor"] for d in diario], ["usuario:marcelo"] * 3)
        self.assertIn("PARADA GERAL", diario[-1]["texto"])


if __name__ == "__main__":
    unittest.main()
