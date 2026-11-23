"""Incidentes SIAA -> Avisos. Sem rede, sem SIAA e sem banco real do painel.

A parte de unidade roda sempre. A de integração usa um Postgres de teste e só roda com
``SIAA_AVISOS_TEST_DSN`` apontando para um banco cujo nome termina em ``_teste`` (mesma trava do
``test_motor_pg``): cria as tabelas mínimas (app_users, avisos, aviso_lido) e consulta a
visibilidade com o ``_VISIBLE_WHERE`` real de ``routes/avisos.py`` (a mesma do popup).
"""
from __future__ import annotations

import os
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import flask

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import siaa_avisos  # noqa: E402

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

T0 = datetime(2026, 11, 23, 14, 58, tzinfo=timezone.utc)  # 11:58 em Brasília
OC1, OC2, OC3 = "a" * 20, "b" * 20, "c" * 20
SENSIVEIS = ("JSESSIONID", "TS01", "ViewState", "Bearer", "curl ", "tok-secreto", "CPF")


def _api(abertos=(), recuperados=(), ok=True):
    return {"ok": ok, "existe": True, "avaliado_em": "2026-11-23T15:00:00Z",
            "abertos": list(abertos), "recuperados": list(recuperados)}


def _ab(oc=OC1, tipo="renovacao", titulo="Renovação SIAA bloqueada", modulo="Acadêmico",
        motivo="Última execução falhou em 23/11 11:58: a renovação passou do prazo.", em="2026-11-23T14:58:00Z"):
    return {"ocorrencia": oc, "tipo": tipo, "titulo": titulo, "modulo": modulo, "motivo": motivo, "aberto_em": em}


def _rec(oc=OC1, tipo="renovacao", em="2026-11-23T15:40:00Z"):
    return {"ocorrencia": oc, "tipo": tipo, "titulo": "Renovação SIAA bloqueada", "modulo": "Acadêmico",
            "aberto_em": "2026-11-23T14:58:00Z", "recuperado_em": em}


class ValidacaoTest(unittest.TestCase):
    def test_resposta_invalida_nao_abre_nem_fecha(self):
        for ruim in (None, [], {"ok": False}, {"ok": True, "abertos": None, "recuperados": []},
                     {"ok": True, "abertos": [], "recuperados": "x"}):
            self.assertIsNone(siaa_avisos.validar(ruim))

    def test_itens_fora_da_lista_branca_sao_descartados(self):
        v = siaa_avisos.validar(_api([_ab(), _ab(oc="curto"), _ab(oc=OC2, tipo="outro"), "lixo"],
                                     [_rec(), dict(_rec(OC3), recuperado_em="ontem")]))
        self.assertEqual([i["ocorrencia"] for i in v["abertos"]], [OC1])
        self.assertEqual([i["ocorrencia"] for i in v["recuperados"]], [OC1])
        self.assertEqual(v["abertos"][0]["aberto_em"], T0)

    def test_texto_com_cara_de_segredo_e_descartado(self):
        v = siaa_avisos.validar(_api([_ab(motivo="cookie JSESSIONID=abc"), _ab(oc=OC2, modulo="Bearer x")]))
        self.assertEqual(v["abertos"][0]["motivo"], "")
        self.assertEqual(v["abertos"][1]["modulo"], "")


class TextoTest(unittest.TestCase):
    def test_aviso_aberto_tem_modulo_motivo_hora_brt_e_link(self):
        inc = siaa_avisos.validar(_api([_ab()]))["abertos"][0]
        titulo, corpo = siaa_avisos.aviso_aberto(inc)
        self.assertEqual(titulo, "SIAA: Renovação SIAA bloqueada")
        self.assertIn("Acadêmico", corpo)
        self.assertIn("passou do prazo", corpo)
        self.assertIn("23/11/2026 11:58 (horário de Brasília)", corpo)
        self.assertIn('href="#conexao_siaa"', corpo)
        for s in SENSIVEIS:
            self.assertNotIn(s, corpo)

    def test_texto_da_api_e_escapado(self):
        inc = siaa_avisos.validar(_api([_ab(motivo="<img src=x onerror=alert(1)>")]))["abertos"][0]
        _, corpo = siaa_avisos.aviso_aberto(inc)
        self.assertNotIn("<img", corpo)
        self.assertIn("&lt;img", corpo)

    def test_aviso_recuperado_nao_repete_a_falha_como_atual(self):
        rec = siaa_avisos.validar(_api(recuperados=[_rec()]))["recuperados"][0]
        titulo, corpo = siaa_avisos.aviso_recuperado(
            {"titulo": "Renovação SIAA bloqueada", "modulo": "Acadêmico", "motivo": "x", "aberto_em": T0}, rec)
        self.assertTrue(titulo.startswith("SIAA recuperado:"))
        self.assertIn("normalizado em 23/11/2026 12:40", corpo)
        self.assertIn("Motivo original", corpo)


class JobTest(unittest.TestCase):
    def test_desligado_por_padrao_nao_chama_api_nem_banco(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SIAA_AVISOS_INCIDENTES", None)
            with patch.object(siaa_avisos, "_chamar_alertas") as api:
                siaa_avisos.executar_job()
                api.assert_not_called()

    def test_api_indisponivel_nao_toca_no_banco(self):
        with patch.dict(os.environ, {"SIAA_AVISOS_INCIDENTES": "1"}), \
                patch.object(siaa_avisos, "_chamar_alertas", return_value=(None, "503")), \
                patch.object(siaa_avisos, "sincronizar") as sinc:
            siaa_avisos.executar_job()
            sinc.assert_not_called()
            self.assertEqual(siaa_avisos._ULTIMO["erro"], "503")

    def test_chamada_da_api_sem_segredo_no_retorno(self):
        class R:
            status_code = 503

            def json(self):
                return {"ok": False, "erro": "estado_ilegivel"}

        with patch.dict(os.environ, {"SIAA_SESSION_URL": "https://s.test", "SIAA_SESSION_TOKEN": "tok-secreto"}), \
                patch("requests.get", return_value=R()) as get:
            dados, erro = siaa_avisos._chamar_alertas()
        self.assertIsNone(dados)
        self.assertNotIn("tok-secreto", erro)
        self.assertEqual(get.call_args[0][0], "https://s.test/alertas")


class RotaTest(unittest.TestCase):
    def setUp(self):
        from routes.conexao_siaa import conexao_siaa_bp
        app = flask.Flask(__name__)
        app.secret_key = "t"
        app.register_blueprint(conexao_siaa_bp)
        self.client = app.test_client()

    def _papel(self, role):
        with self.client.session_transaction() as s:
            s["role"] = role

    def test_so_admin(self):
        self._papel("viewer")
        self.assertEqual(self.client.get("/api/conexao-siaa/incidentes").status_code, 403)

    def test_admin_sem_banco_e_sem_api_responde_sem_inventar(self):
        self._papel("admin")
        with patch.object(siaa_avisos, "_chamar_alertas", return_value=(None, "A session-api demorou.")), \
                patch("db.get_conn", side_effect=RuntimeError("sem banco")):
            r = self.client.get("/api/conexao-siaa/incidentes")
        d = r.get_json()
        self.assertEqual(r.status_code, 200)
        self.assertIsNone(d["abertos"])   # sem leitura: não mostra "nenhum incidente"
        self.assertIsNone(d["avisos"])
        self.assertEqual(d["destinatarios_configurados"], list(siaa_avisos.DESTINATARIOS))


class OrigemPopupTest(unittest.TestCase):
    """/api/avisos/nao-lidos marca siaa_estado; sem a tabela, tudo fica None (regra de sempre)."""

    def test_erro_na_consulta_deixa_none_e_faz_rollback(self):
        from routes.avisos import _marcar_origem_siaa

        class Conn:
            rolled = False

            def cursor(self):
                raise RuntimeError("relation does not exist")

            def rollback(self):
                Conn.rolled = True

        rows = [{"id": 1}, {"id": 2}]
        _marcar_origem_siaa(Conn(), rows)
        self.assertEqual([r["siaa_estado"] for r in rows], [None, None])
        self.assertTrue(Conn.rolled)


# ---------------------------------------------------------------------------
# Integração com Postgres de teste
# ---------------------------------------------------------------------------

DSN = os.getenv("SIAA_AVISOS_TEST_DSN", "")


def _conn():
    return psycopg2.connect(DSN)


SCHEMA = """
DROP TABLE IF EXISTS siaa_incidente_aviso, aviso_lido, avisos, app_users CASCADE;
CREATE TABLE app_users (id SERIAL PRIMARY KEY, username TEXT NOT NULL UNIQUE, pw_hash TEXT NOT NULL DEFAULT 'x',
                        role TEXT NOT NULL DEFAULT 'viewer', created_at TIMESTAMPTZ DEFAULT NOW());
CREATE TABLE avisos (id SERIAL PRIMARY KEY, titulo TEXT NOT NULL, corpo TEXT NOT NULL,
                     prioridade TEXT NOT NULL DEFAULT 'normal', target_role TEXT NOT NULL DEFAULT 'todos',
                     target_user_ids INTEGER[] DEFAULT '{}', created_by INTEGER REFERENCES app_users(id),
                     created_at TIMESTAMPTZ DEFAULT NOW(), expires_at TIMESTAMPTZ, active BOOLEAN DEFAULT TRUE);
CREATE TABLE aviso_lido (aviso_id INTEGER NOT NULL REFERENCES avisos(id) ON DELETE CASCADE,
                         user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
                         read_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (aviso_id, user_id));
"""


@unittest.skipUnless(DSN and psycopg2 and DSN.rstrip().split("dbname=")[-1].split()[0].endswith("_teste"),
                     "defina SIAA_AVISOS_TEST_DSN (dbname terminando em _teste) para rodar contra Postgres")
class IntegracaoPgTest(unittest.TestCase):
    def setUp(self):
        self.conn = _conn()
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA)
        self.conn.commit()
        siaa_avisos.garantir_tabela(self.conn)
        self.ids = {}
        for nome, role in (("fabio@eduit.com.br", "admin"), ("Marcelo@eduit.com.br", "admin"),
                           ("outro@eduit.com.br", "admin"), ("viewer@eduit.com.br", "viewer")):
            self._usuario(nome, role)
        # aviso manual pré-existente (para todos), que não pode mudar
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO avisos (titulo, corpo, prioridade) VALUES ('Manual', 'corpo manual', 'importante') "
                        "RETURNING id")
            self.manual = cur.fetchone()[0]
        self.conn.commit()
        self.manual_antes = self._linha_aviso(self.manual)

    def tearDown(self):
        self.conn.close()

    # utilitários --------------------------------------------------------------
    def _usuario(self, nome, role="admin"):
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO app_users (username, role) VALUES (%s, %s) RETURNING id", (nome, role))
            self.ids[nome.lower()] = cur.fetchone()[0]
        self.conn.commit()

    def _sinc(self, abertos=(), recuperados=(), conn=None):
        return siaa_avisos.sincronizar(conn or self.conn, siaa_avisos.validar(_api(abertos, recuperados)))

    def _linha_aviso(self, aviso_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT titulo, corpo, prioridade, target_role, target_user_ids, expires_at, active "
                        "FROM avisos WHERE id = %s", (aviso_id,))
            return cur.fetchone()

    def _siaa_avisos(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM avisos WHERE id <> %s ORDER BY id", (self.manual,))
            return [r[0] for r in cur.fetchall()]

    def _nao_lidos(self, email, role="admin"):
        """A mesma consulta do popup (/api/avisos/nao-lidos)."""
        from routes.avisos import _VISIBLE_WHERE
        with self.conn.cursor() as cur:
            cur.execute(f"""SELECT a.id, a.titulo FROM avisos a WHERE {_VISIBLE_WHERE}
                              AND a.id NOT IN (SELECT aviso_id FROM aviso_lido WHERE user_id = %(user_id)s)
                            ORDER BY a.id""", {"user_id": self.ids[email], "role": role})
            return [r for r in cur.fetchall() if r[0] != self.manual]

    def _ler(self, email, aviso_id):
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO aviso_lido (aviso_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (aviso_id, self.ids[email]))
        self.conn.commit()

    def _manual_intacto(self):
        self.assertEqual(self._linha_aviso(self.manual), self.manual_antes)

    # cenários ------------------------------------------------------------------
    def test_destinatarios_exclusivos_e_case_insensitive(self):
        res = self._sinc([_ab()])
        self.assertEqual((res["criados"], res["ausentes"]), (1, []))
        aviso, = self._siaa_avisos()
        linha = self._linha_aviso(aviso)
        self.assertEqual(sorted(linha[4]), sorted([self.ids["fabio@eduit.com.br"], self.ids["marcelo@eduit.com.br"]]))
        self.assertEqual((linha[2], linha[3]), ("urgente", "todos"))
        self.assertEqual(len(self._nao_lidos("fabio@eduit.com.br")), 1)
        self.assertEqual(len(self._nao_lidos("marcelo@eduit.com.br")), 1)
        self.assertEqual(self._nao_lidos("outro@eduit.com.br"), [])          # admin, mas não destinatário
        self.assertEqual(self._nao_lidos("viewer@eduit.com.br", "viewer"), [])
        self._manual_intacto()

    def test_destinatario_ausente_registra_erro_e_nao_vira_todos(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM app_users WHERE lower(username) = 'marcelo@eduit.com.br'")
        self.conn.commit()
        with self.assertLogs("siaa_avisos", "ERROR") as log:
            res = self._sinc([_ab()])
        self.assertIn("marcelo@eduit.com.br", "\n".join(log.output))
        self.assertEqual(res["ausentes"], ["marcelo@eduit.com.br"])
        aviso, = self._siaa_avisos()
        self.assertEqual(self._linha_aviso(aviso)[4], [self.ids["fabio@eduit.com.br"]])
        self.assertEqual(self._nao_lidos("outro@eduit.com.br"), [])
        # Marcelo cadastrado depois: entra no mesmo aviso (sem duplicar)
        self._usuario("marcelo@eduit.com.br")
        self._sinc([_ab()])
        self.assertEqual(self._siaa_avisos(), [aviso])
        self.assertEqual(len(self._nao_lidos("marcelo@eduit.com.br")), 1)

    def test_nenhum_destinatario_nao_cria_aviso_algum(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM app_users WHERE lower(username) IN %s", (siaa_avisos.DESTINATARIOS,))
        self.conn.commit()
        with self.assertLogs("siaa_avisos", "ERROR"):
            res = self._sinc([_ab()])
        self.assertEqual((res["criados"], res["erro"]), (0, "sem_destinatarios"))
        self.assertEqual(self._siaa_avisos(), [])
        self.assertEqual(self._nao_lidos("outro@eduit.com.br"), [])
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM siaa_incidente_aviso")
            self.assertEqual(cur.fetchone()[0], 0)  # sem vínculo: cria quando houver destinatário
        self._usuario("fabio@eduit.com.br")
        with self.assertLogs("siaa_avisos", "ERROR"):  # Marcelo ainda ausente
            self.assertEqual(self._sinc([_ab()])["criados"], 1)

    def test_polling_e_reinicio_nao_duplicam(self):
        for _ in range(5):
            self._sinc([_ab(), _ab(oc=OC2, tipo="executor", titulo="Executor SIAA sem sinal")])
        outra = _conn()  # "reinício": outro processo/conexão
        try:
            self._sinc([_ab(), _ab(oc=OC2, tipo="executor", titulo="Executor SIAA sem sinal")], conn=outra)
        finally:
            outra.close()
        self.assertEqual(len(self._siaa_avisos()), 2)

    def test_dois_processos_ao_mesmo_tempo_criam_um_aviso(self):
        erros, barreira = [], threading.Barrier(4)

        def rodar():
            c = _conn()
            try:
                barreira.wait()
                self._sinc([_ab()], conn=c)
            except Exception as exc:  # pragma: no cover
                erros.append(exc)
            finally:
                c.close()

        ths = [threading.Thread(target=rodar) for _ in range(4)]
        [t.start() for t in ths]
        [t.join() for t in ths]
        self.assertEqual(erros, [])
        self.assertEqual(len(self._siaa_avisos()), 1)

    def test_leitura_individual_e_recuperacao_sem_falha_desatualizada(self):
        self._sinc([_ab()])
        aviso, = self._siaa_avisos()
        self._ler("fabio@eduit.com.br", aviso)
        self.assertEqual(self._nao_lidos("fabio@eduit.com.br"), [])
        self.assertEqual(len(self._nao_lidos("marcelo@eduit.com.br")), 1)  # leitura não é compartilhada
        self._sinc([_ab()])  # polling não reabre para quem leu
        self.assertEqual(self._nao_lidos("fabio@eduit.com.br"), [])
        # recuperação: o mesmo aviso é reescrito
        res = self._sinc([], [_rec()])
        self.assertEqual(res["recuperados"], 1)
        self.assertEqual(self._siaa_avisos(), [aviso])
        titulo, corpo, prio, _r, _ids, expira, ativo = self._linha_aviso(aviso)
        self.assertTrue(titulo.startswith("SIAA recuperado:"))
        self.assertIn("normalizado em 23/11/2026 12:40", corpo)
        self.assertEqual((prio, ativo), ("normal", True))
        self.assertEqual(expira, datetime(2026, 11, 30, 15, 40, tzinfo=timezone.utc))
        self.assertEqual(self._nao_lidos("fabio@eduit.com.br"), [])          # sem novo popup para quem leu
        (_, t_marcelo), = self._nao_lidos("marcelo@eduit.com.br")
        self.assertTrue(t_marcelo.startswith("SIAA recuperado:"))           # quem não leu vê o texto atual
        # repetir a recuperação e reiniciar não mudam nada
        antes = self._linha_aviso(aviso)
        self._sinc([], [_rec()])
        self._sinc([], [_rec(em="2026-11-23T18:00:00Z")])
        self.assertEqual(self._linha_aviso(aviso), antes)
        self._manual_intacto()

    def test_recuperacao_de_ocorrencia_sem_aviso_nao_cria_aviso(self):
        self._sinc([], [_rec(OC3)])
        self.assertEqual(self._siaa_avisos(), [])
        with self.conn.cursor() as cur:
            cur.execute("SELECT estado, aviso_id FROM siaa_incidente_aviso WHERE ocorrencia = %s", (OC3,))
            self.assertEqual(cur.fetchone(), ("recuperado_sem_aviso", None))

    def test_ausencia_na_lista_nao_e_recuperacao(self):
        self._sinc([_ab()])
        aviso, = self._siaa_avisos()
        antes = self._linha_aviso(aviso)
        self._sinc([], [])  # sumiu de "abertos" sem recuperação explícita
        self.assertEqual(self._linha_aviso(aviso), antes)
        with self.conn.cursor() as cur:
            cur.execute("SELECT estado FROM siaa_incidente_aviso WHERE ocorrencia = %s", (OC1,))
            self.assertEqual(cur.fetchone()[0], "aberto")

    def test_falha_no_meio_desfaz_tudo(self):
        original = siaa_avisos.aviso_aberto
        chamadas = {"n": 0}

        def quebra(inc):
            chamadas["n"] += 1
            if chamadas["n"] == 2:
                raise RuntimeError("falha simulada")
            return original(inc)

        with patch.object(siaa_avisos, "aviso_aberto", side_effect=quebra):
            with self.assertRaises(RuntimeError):
                self._sinc([_ab(), _ab(oc=OC2, tipo="keepalive", titulo="Keepalive SIAA sem sinal")])
        self.assertEqual(self._siaa_avisos(), [])  # nem o 1º aviso ficou
        self.assertEqual(self._sinc([_ab(), _ab(oc=OC2, tipo="keepalive")])["criados"], 2)

    def test_nao_lidos_marca_origem_siaa_e_manual_fica_none(self):
        from routes.avisos import _marcar_origem_siaa
        self._sinc([_ab(), _ab(oc=OC2, tipo="executor", titulo="Executor SIAA sem sinal")])
        self._sinc([], [_rec(OC2, tipo="executor")])
        a1, a2 = self._siaa_avisos()
        rows = [{"id": self.manual}, {"id": a1}, {"id": a2}]
        _marcar_origem_siaa(self.conn, rows)
        self.assertEqual([r["siaa_estado"] for r in rows], [None, "aberto", "recuperado"])
        self._manual_intacto()

    def test_conexao_siaa_mostra_vinculos_e_leituras(self):
        self._sinc([_ab()])
        aviso, = self._siaa_avisos()
        self._ler("marcelo@eduit.com.br", aviso)
        v, = siaa_avisos.ultimos_vinculos(self.conn)
        self.assertEqual((v["estado"], v["lidos"], v["destinatarios"]), ("aberto", 1, 2))


if __name__ == "__main__":
    unittest.main()
