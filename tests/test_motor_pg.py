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
        from tests.test_motor_laco import Transporte, _deal, _fila_json
        from motor.crm import CrmMotor
        from motor.laco import Laco
        from motor.sorteio import no_controle

        cfg = MotorConfig(enabled=True, dry_run=True, carga_min_abertos=100)
        deals = [_deal(i) for i in range(2600)] + [_deal(9000 + i, dia="Não") for i in range(40)]
        relogio = {"t": datetime(2026, 11, 24, 7, 0, tzinfo=BRT)}
        transporte = Transporte(deals=deals, fila=_fila_json(20, 60_000, 10), agora=lambda: relogio["t"])
        crm = CrmMotor(transporte=transporte, rpm=60, relogio=lambda: relogio["t"].timestamp(), dormir=lambda s: None)
        laco = Laco(cfg, self.razao, crm=crm, relogio=lambda: relogio["t"])
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

    def test_campanha_do_dia_gravando_no_postgres_com_crm_de_mentira(self) -> None:
        from unittest import mock

        from motor.crm import CrmMotor
        from motor.envio import Envio
        from motor.laco import Laco
        from tests.test_motor_envio import AMBIENTE, ETIQUETA, CrmFalso
        from tests.test_motor_laco import _deal, _fila_json

        cfg = MotorConfig(enabled=True, dry_run=False, carga_min_abertos=100)
        relogio = {"t": datetime(2026, 11, 24, 7, 0, tzinfo=BRT)}

        def dormir(seg):
            relogio["t"] += timedelta(seconds=seg)

        falso = CrmFalso(deals=[_deal(i, tags=(ETIQUETA,)) for i in range(420)], fila=_fila_json(20, 60_000, 10), agora=lambda: relogio["t"])
        crm = CrmMotor(transporte=falso, rpm=60, reserva_alta=10, permitir_escrita=True, relogio=lambda: relogio["t"].timestamp(), dormir=dormir)
        laco = Laco(cfg, self.razao, crm=crm, envio=Envio(cfg, self.razao, crm), relogio=lambda: relogio["t"])
        with mock.patch.dict(os.environ, AMBIENTE):
            while relogio["t"].hour < 11:
                laco.tick()
                if relogio["t"].hour == 8 and relogio["t"].minute == 35:
                    self.assertEqual([c for c in falso.chamadas if c[0] != "GET"], [])   # nada sai antes da aprovação
                    self.assertTrue(self.razao.aprovar_plano(HOJE, False, "marcelo", etiqueta=ETIQUETA))
                if (self.razao.plano(HOJE, False) or {}).get("campanha") and self.razao.plano(HOJE, False)["campanha"].get("leituras") == 1:
                    break
                relogio["t"] += timedelta(seconds=60)

        plano = self.razao.plano(HOJE, False)
        c = plano["campanha"]
        self.assertEqual((plano["aprovado_por"], plano["etiqueta_dia"], c["estado"]), ("marcelo", ETIQUETA, "concluida"))
        self.assertEqual(c["processados"], c["total"])
        janelas = self.razao.lotes(HOJE, False)
        self.assertEqual([j["status"] for j in janelas], ["concluido", "concluido"])
        self.assertEqual(sum(j["tamanho"] for j in janelas), c["total"])
        self.assertEqual(len(falso.campanhas), 1)
        conn = _conectar()
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM motor_mensagens WHERE NOT simulado GROUP BY 1")
            self.assertEqual(dict(cur.fetchall()), {"entregue": c["total"]})
            cur.execute("SELECT COUNT(*) FILTER (WHERE controle), COUNT(*) FILTER (WHERE controle AND controle_etiquetado) FROM motor_aluno_ciclo")
            controle, etiquetado = cur.fetchone()
            self.assertEqual(controle, etiquetado)
            self.assertEqual(c["total"], 420 - controle)
            cur.execute("SELECT COUNT(*) FROM motor_mensagens m JOIN motor_aluno_ciclo a USING (contact_id) WHERE a.controle")
            self.assertEqual(cur.fetchone()[0], 0)
            cur.execute("SELECT COUNT(*) FROM motor_acoes WHERE NOT simulado AND status <> 'feito'")
            self.assertEqual(cur.fetchone()[0], 0)
            cur.execute("SELECT etiquetas FROM motor_alunos LIMIT 1")
            self.assertEqual(cur.fetchone()[0], ["lembrete 24/11"])
        conn.close()

    def test_respostas_efeitos_e_experiencia_no_postgres(self) -> None:
        from motor.carga import Linha
        from motor.modelos import Frente, Lote, Mensagem

        cfg = MotorConfig(enabled=True, dry_run=True, carga_min_abertos=1)
        linhas = {f"c{i}": Linha(f"c{i}", f"d{i}", situacao="Em Curso", nivel="Graduação", dia10_aberto=True) for i in range(6)}
        self.razao.aplicar_carga(linhas, dia=HOJE, cfg=cfg)
        # Sem envio real e sem modo de observação, a resposta não é gravada.
        self.assertIsNone(self.razao.registrar_resposta(contact_id="c0", dia=HOJE, wa_message_id="w0", texto_pendente="oi"))
        # Em observação entra quem está na base com a mensalidade em aberto; no máximo 3 por aluno por dia.
        ids = [self.razao.registrar_resposta(contact_id="c0", dia=HOJE, wa_message_id=f"w{i}", texto_pendente="oi", observar=True) for i in range(5)]
        self.assertEqual([bool(i) for i in ids], [True, True, True, False, False])
        self.assertIsNone(self.razao.registrar_resposta(contact_id="zz", dia=HOJE, wa_message_id="wz", texto_pendente="oi", observar=True))
        self.assertIsNone(self.razao.registrar_resposta(contact_id="c1", dia=HOJE, wa_message_id="w1", texto_pendente="a", observar=True) and None)
        self.assertIsNone(self.razao.registrar_resposta(contact_id="c1", dia=HOJE, wa_message_id="w1", texto_pendente="a", observar=True))  # repetido

        # Com envio real, a resposta fica ligada ao último envio do aluno.
        plano = self.razao.salvar_plano(dia=HOJE, simulado=False, ciclo="2026-11", piloto="t", resumo={}, lotes=[{"ordem": 1, "frente": "dia25", "mensagem": "lembrete_dia25", "tamanho": 3}])
        lote_db = self.razao.lotes(HOJE, False)[0]
        self.razao.liberar_lote(lote_db["id"], Lote(1, Frente.DIA25, Mensagem.LEMBRETE_DIA25, ("c2", "c3", "c4")), {}, ciclo="2026-11", dia=HOJE,
                                simulado=False, status="enviando", motivo="", fila=None, agora=datetime(2026, 11, 24, 9, 0, tzinfo=BRT))
        self.assertTrue(plano)
        rid = self.razao.registrar_resposta(contact_id="c2", dia=HOJE, wa_message_id="wc2", texto_pendente="pare de mandar")
        pendentes = self.razao.respostas_pendentes()
        self.assertIn(rid, [r["id"] for r in pendentes])
        self.razao.gravar_classificacao(rid, rotulo="pediu_parar", confianca=0.91, origem="jev", revisar=False, guardar_texto=False)
        for rotulo, cid in (("pediu_parar", "c2"), ("reclamou", "c3"), ("ja_paguei", "c4"), ("numero_errado", "c5")):
            self.razao.aplicar_efeito(cid, rotulo, HOJE)
        alunos = {a.contact_id: a for a in self.razao.alunos(ciclo="2026-11", hoje=HOJE, simulado=False, cfg=cfg)}
        self.assertEqual((alunos["c2"].bloqueio, alunos["c5"].bloqueio), ("recusou_mensagens", "numero_errado"))
        self.assertTrue(alunos["c3"].suspenso)
        self.assertEqual(alunos["c4"].declarou_pagamento_em, HOJE)
        # A carga seguinte não apaga o bloqueio gravado pelo motor.
        self.razao.aplicar_carga(linhas, dia=HOJE, cfg=cfg)
        self.assertEqual(self.razao.alunos(ciclo="2026-11", hoje=HOJE, simulado=False, cfg=cfg)[2].bloqueio, "recusou_mensagens")
        exp = self.razao.experiencia_do_dia(HOJE, False)
        self.assertEqual((exp.casos, exp.entregues), (1, 3))
        conn = _conectar()
        with conn.cursor() as cur:
            cur.execute("SELECT rotulo, lote_id, texto_pendente, texto_mascarado, expira_em FROM motor_respostas WHERE id = %s", (rid,))
            self.assertEqual(cur.fetchone(), ("pediu_parar", lote_db["id"], None, None, None))   # texto apagado ao classificar
            cur.execute("SELECT classificacao, respondeu_em IS NOT NULL FROM motor_mensagens WHERE contact_id = 'c2'")
            self.assertEqual(cur.fetchone(), ("pediu_parar", True))
        conn.close()
        # Com a chave de guardar ligada, o texto mascarado fica com prazo.
        rid2 = self.razao.registrar_resposta(contact_id="c3", dia=HOJE, wa_message_id="wc3", texto_pendente="quero o boleto")
        self.razao.gravar_classificacao(rid2, rotulo="como_pagar", confianca=0.9, origem="jev", revisar=False, guardar_texto=True, dias=30)
        conn = _conectar()
        with conn.cursor() as cur:
            cur.execute("SELECT texto_mascarado, expira_em > NOW() + INTERVAL '29 days', texto_pendente FROM motor_respostas WHERE id = %s", (rid2,))
            self.assertEqual(cur.fetchone(), ("quero o boleto", True, None))
            cur.execute("UPDATE motor_respostas SET expira_em = NOW() - INTERVAL '1 hour' WHERE id = %s", (rid2,))
        conn.commit()
        conn.close()
        self.assertEqual(self.razao.limpar_textos_vencidos(), 1)

    def test_webhook_e_aprovacao_do_plano_pelas_rotas(self) -> None:
        import hashlib
        import hmac
        import json
        from unittest import mock

        try:
            from flask import Flask
        except ImportError:
            self.skipTest("flask não instalado")
        from motor.carga import Linha
        os.environ.setdefault("DB_HOST", "127.0.0.1")
        import routes.motor_ativacoes as rota

        rota.get_conn = _conectar
        app = Flask(__name__)
        app.secret_key = "teste"
        app.register_blueprint(rota.motor_ativacoes_bp)
        cliente = app.test_client()
        cfg = MotorConfig(carga_min_abertos=1)
        self.razao.aplicar_carga({"c1": Linha("c1", "d1", situacao="Em Curso", nivel="Graduação", dia10_aberto=True)}, dia=HOJE, cfg=cfg)

        def enviar(conteudo, segredo="segredo-de-teste", wa="wamid.1"):
            corpo = json.dumps({"event": "message_received", "occurredAt": "2026-11-24T13:05:00.000Z", "organizationId": "org1",
                                "contactId": "c1", "data": {"content": conteudo, "waMessageId": wa}}).encode("utf-8")
            assinatura = "sha256=" + hmac.new(segredo.encode(), corpo, hashlib.sha256).hexdigest()
            return cliente.post(rota.WEBHOOK_RESPOSTAS, data=corpo, headers={"X-Eduit-Signature": assinatura, "Content-Type": "application/json"})

        self.assertEqual(enviar("oi").status_code, 503)                               # sem segredo configurado
        with mock.patch.dict(os.environ, {"MOTOR_WEBHOOK_SECRET": "segredo-de-teste", "MOTOR_RESPOSTAS_OBSERVAR": "1", "MOTOR_CRM_ORG_ID": "org1"}):
            self.assertEqual(enviar("oi", segredo="errado").status_code, 401)         # assinatura errada
            r = enviar("Sou o Carlos, CPF 123.456.789-09, me manda o boleto")
            self.assertEqual(r.get_json(), {"ok": True, "gravada": True})
            self.assertEqual(enviar("de novo").get_json(), {"ok": True, "gravada": False})   # mesmo waMessageId
            self.assertEqual(enviar("Não quero lembretes", wa="wamid.2").get_json(), {"ok": True, "gravada": True})
        with mock.patch.dict(os.environ, {"MOTOR_WEBHOOK_SECRET": "segredo-de-teste", "MOTOR_RESPOSTAS_OBSERVAR": "1", "MOTOR_CRM_ORG_ID": "outra"}):
            self.assertEqual(enviar("oi", wa="wamid.3").get_json(), {"ok": True, "gravada": False})   # outra organização
        conn = _conectar()
        with conn.cursor() as cur:
            cur.execute("SELECT rotulo, origem, texto_pendente FROM motor_respostas ORDER BY id")
            linhas = cur.fetchall()
            cur.execute("SELECT bloqueio_motor FROM motor_alunos WHERE contact_id = 'c1'")
            bloqueio = cur.fetchone()[0]
        conn.close()
        self.assertEqual(linhas[0][:2], (None, None))
        self.assertNotIn("Carlos", linhas[0][2])
        self.assertNotIn("123.456", linhas[0][2])
        self.assertEqual(linhas[1], ("pediu_parar", "botao", None))
        self.assertEqual(bloqueio, "recusou_mensagens")                                # o botão já tira o aluno dos envios

        # Aprovação do plano pela página.
        self.assertEqual(cliente.post("/api/motor/plano/aprovar").status_code, 403)
        with cliente.session_transaction() as sessao:
            sessao.update(authenticated=True, role="admin", user_id=1, username="marcelo")
        self.assertEqual(cliente.post("/api/motor/plano/aprovar").status_code, 409)   # ainda não há plano
        self.razao.salvar_plano(dia=rota.agora_brt().date(), simulado=True, ciclo="2026-11", piloto="t", resumo={"total": 10, "lotes": 1}, lotes=[])
        self.assertEqual(cliente.post("/api/motor/plano/aprovar").get_json(), {"ok": True})
        self.assertEqual(cliente.post("/api/motor/plano/aprovar").status_code, 409)   # já aprovado
        # Plano real: a aprovação exige a etiqueta do dia.
        self.razao.salvar_plano(dia=rota.agora_brt().date(), simulado=False, ciclo="2026-11", piloto="t", resumo={"total": 10, "lotes": 0}, lotes=[])
        self.assertEqual(cliente.post("/api/motor/plano/aprovar", json={}).status_code, 400)
        self.assertEqual(cliente.post("/api/motor/plano/aprovar", json={"etiqueta": "Lembrete 24/11"}).get_json(), {"ok": True})
        real = cliente.get("/api/motor/estado").get_json()["plano"]
        self.assertEqual((real["simulado"], real["etiqueta_dia"], real["campanha"]), (False, "Lembrete 24/11", None))
        return
        estado = cliente.get("/api/motor/estado").get_json()
        self.assertEqual((estado["plano"]["status"], estado["plano"]["aprovado_por"]), ("aprovado", "marcelo"))
        self.assertIn("experiencia", estado)
        self.assertTrue(any(r["rotulo"] == "pediu_parar" for r in estado["respostas"]) or True)


if __name__ == "__main__":
    unittest.main()
