"""Fase 2 (27/11): reaproveitamento de conexões (`db_pool.py`).

Duas partes:
- lógica do pool com conexões falsas (roda sempre);
- comportamento real do psycopg2 (`reset`, autocommit, SET, transação aberta)
  contra um Postgres descartável, só com `POOL_TEST_DSN` (job `testes-pg` do CI).
"""
from __future__ import annotations

import os
import threading

import psycopg2
import psycopg2.extensions
import pytest

import db_pool


class _Cur:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *a):
        if self.conn.quebrada:
            raise psycopg2.OperationalError("server closed the connection")
        self.conn.sqls.append(sql)


class _ConnFalsa:
    def __init__(self):
        self.closed = 0
        self.autocommit = False
        self.readonly = None
        self.deferrable = None
        self.isolation_level = None
        self.status = psycopg2.extensions.STATUS_READY
        self.quebrada = False
        self.falha_reset = False
        self.resets = 0
        self.sqls = []

    def cursor(self, *a, **k):
        return _Cur(self)

    def rollback(self):
        pass

    def reset(self):
        if self.falha_reset:
            raise psycopg2.InterfaceError("reset falhou")
        self.resets += 1

    def set_session(self, **k):
        self.autocommit = k.get("autocommit", self.autocommit)

    def close(self):
        self.closed = 1


@pytest.fixture
def falso(monkeypatch):
    criadas = []

    def _connect(**kwargs):
        c = _ConnFalsa()
        criadas.append(c)
        return c

    monkeypatch.setattr(db_pool.psycopg2, "connect", _connect)
    monkeypatch.setenv("DB_POOL", "1")
    db_pool.esvaziar()
    for k in db_pool._est:
        db_pool._est[k] = 0
    yield criadas
    db_pool.esvaziar()


DSN = dict(host="h", dbname="d", user="u", password="p")


def test_close_devolve_e_proximo_reaproveita(falso):
    c1 = db_pool.conectar(**DSN)
    real = c1._conn
    c1.close()
    c2 = db_pool.conectar(**DSN)
    assert c2._conn is real
    assert len(falso) == 1
    assert real.resets == 1
    e = db_pool.estatisticas()
    assert (e["abertas"], e["reusadas"]) == (1, 1)


def test_dsn_diferente_nao_mistura(falso):
    db_pool.conectar(**DSN).close()
    outra = db_pool.conectar(**{**DSN, "dbname": "kommo_sync"})
    assert len(falso) == 2
    outra.close()


def test_conexao_em_uso_nao_e_entregue_a_outro(falso):
    a = db_pool.conectar(**DSN)
    b = db_pool.conectar(**DSN)
    assert a._conn is not b._conn


def test_depois_do_close_o_proxy_parece_fechado(falso):
    c = db_pool.conectar(**DSN)
    c.close()
    assert c.closed
    with pytest.raises(psycopg2.InterfaceError):
        c.cursor()
    c.close()  # segundo close não devolve de novo
    assert len(db_pool._pilhas[db_pool._chave(DSN)]) == 1


def test_autocommit_ligado_volta_desligado(falso):
    c = db_pool.conectar(**DSN)
    c.autocommit = True          # setattr repassa para a conexão real
    assert c._conn.autocommit is True
    c.close()
    assert db_pool.conectar(**DSN).autocommit is False


def test_reset_com_erro_descarta(falso):
    c = db_pool.conectar(**DSN)
    c._conn.falha_reset = True
    c.close()
    assert c._conn.closed
    assert db_pool.estatisticas()["ociosas"] == 0
    db_pool.conectar(**DSN)
    assert len(falso) == 2


def test_conexao_fechada_por_fora_e_descartada(falso):
    c = db_pool.conectar(**DSN)
    c._conn.close()
    c.close()
    assert db_pool.estatisticas()["ociosas"] == 0


def test_teto_de_ociosas(falso, monkeypatch):
    monkeypatch.setattr(db_pool, "MAX_OCIOSAS", 2)
    conns = [db_pool.conectar(**DSN) for _ in range(4)]
    for c in conns:
        c.close()
    assert db_pool.estatisticas()["ociosas"] == 2
    assert sum(1 for c in falso if c.closed) == 2


def test_ociosa_ha_muito_tempo_recebe_ping_e_quebrada_e_trocada(falso, monkeypatch):
    monkeypatch.setattr(db_pool, "PING_S", -1)   # toda ociosa recebe ping
    c = db_pool.conectar(**DSN)
    real = c._conn
    c.close()
    real.quebrada = True
    novo = db_pool.conectar(**DSN)
    assert novo._conn is not real and real.closed


def test_ping_ok_reaproveita(falso, monkeypatch):
    monkeypatch.setattr(db_pool, "PING_S", -1)
    c = db_pool.conectar(**DSN)
    real = c._conn
    c.close()
    assert db_pool.conectar(**DSN)._conn is real
    assert "SELECT 1" in real.sqls


def test_vida_maxima_troca_a_conexao(falso, monkeypatch):
    monkeypatch.setattr(db_pool, "VIDA_S", -1)
    c = db_pool.conectar(**DSN)
    real = c._conn
    c.close()
    assert db_pool.conectar(**DSN)._conn is not real


def test_processo_filho_nao_usa_conexao_herdada(falso, monkeypatch):
    c = db_pool.conectar(**DSN)
    real = c._conn
    c.close()
    monkeypatch.setattr(db_pool, "_pid", -1)     # como se o pool fosse de outro processo
    assert db_pool.conectar(**DSN)._conn is not real


def test_esquecida_sem_close_fecha_no_gc(falso):
    c = db_pool.conectar(**DSN)
    real = c._conn
    del c
    assert real.closed
    assert db_pool.estatisticas()["ociosas"] == 0


def test_desligado_e_psycopg2_puro(falso, monkeypatch):
    monkeypatch.setenv("DB_POOL", "0")
    c = db_pool.conectar(**DSN)
    assert isinstance(c, _ConnFalsa)


def test_with_conn_mantem_o_comportamento_do_psycopg2(falso):
    chamadas = []
    real_cls = _ConnFalsa
    real_cls.__enter__ = lambda self: chamadas.append("enter") or self
    real_cls.__exit__ = lambda self, *a: chamadas.append("exit") or None
    try:
        c = db_pool.conectar(**DSN)
        with c as dentro:
            assert dentro is c
        assert chamadas == ["enter", "exit"]
        assert not c.closed       # `with conn:` não fecha, como no psycopg2
    finally:
        del real_cls.__enter__, real_cls.__exit__


def test_threads_nunca_recebem_a_mesma_conexao(falso):
    db_pool.conectar(**DSN).close()
    vistas, erros = [], []
    barreira = threading.Barrier(8)

    def trabalho():
        try:
            barreira.wait()
            for _ in range(50):
                c = db_pool.conectar(**DSN)
                vistas.append(id(c._conn))
                assert not getattr(c._conn, "em_uso", False)
                c._conn.em_uso = True
                c._conn.em_uso = False
                c.close()
        except Exception as e:  # pragma: no cover
            erros.append(e)

    ts = [threading.Thread(target=trabalho) for _ in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not erros
    assert len(falso) <= 8


def test_rotas_principais_usam_o_pool():
    import db
    import inspect
    for fn in (db.get_conn, db.get_disparos_conn, db.get_me_conn, db.get_google_ads_conn):
        assert "conectar(" in inspect.getsource(fn)


# ── Postgres de verdade (job `testes-pg` do CI) ─────────────────────────────

PG = os.getenv("POOL_TEST_DSN")
precisa_pg = pytest.mark.skipif(not PG, reason="defina POOL_TEST_DSN para rodar contra um Postgres descartável")


@pytest.fixture
def pg(monkeypatch):
    monkeypatch.setenv("DB_POOL", "1")
    db_pool.esvaziar()
    yield psycopg2.extensions.parse_dsn(PG)
    db_pool.esvaziar()


def _mesma_sessao(dsn, acao):
    c = db_pool.conectar(**dsn)
    with c.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        pid = cur.fetchone()[0]
    acao(c)
    c.close()
    c2 = db_pool.conectar(**dsn)
    with c2.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        assert cur.fetchone()[0] == pid, "o pool deveria ter reaproveitado a mesma sessão"
    return c2


@precisa_pg
def test_pg_transacao_aberta_e_desfeita(pg):
    with db_pool.conectar(**pg) as c0, c0.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS pool_t (v int)")
        cur.execute("TRUNCATE pool_t")
    c0.close()

    def suja(c):
        with c.cursor() as cur:
            cur.execute("INSERT INTO pool_t VALUES (1)")   # sem commit
    c2 = _mesma_sessao(pg, suja)
    with c2.cursor() as cur:
        cur.execute("SELECT count(*) FROM pool_t")
        assert cur.fetchone()[0] == 0
    assert c2.status == psycopg2.extensions.STATUS_READY or c2.status == psycopg2.extensions.STATUS_BEGIN
    c2.close()


@precisa_pg
def test_pg_set_de_sessao_volta_ao_padrao(pg):
    def suja(c):
        with c.cursor() as cur:
            cur.execute("SET statement_timeout = '1234ms'")
            cur.execute("SET search_path = pg_catalog")
        c.commit()
    c2 = _mesma_sessao(pg, suja)
    with c2.cursor() as cur:
        cur.execute("SHOW statement_timeout")
        assert cur.fetchone()[0] == "0"
        cur.execute("SHOW search_path")
        assert "public" in cur.fetchone()[0]
    c2.close()


@precisa_pg
def test_pg_autocommit_e_readonly_voltam(pg):
    def suja(c):
        c.autocommit = True
        c.set_session(readonly=True)
    c2 = _mesma_sessao(pg, suja)
    assert c2.autocommit is False
    with c2.cursor() as cur:
        cur.execute("SHOW transaction_read_only")
        assert cur.fetchone()[0] == "off"
    c2.rollback()
    c2.close()


@precisa_pg
def test_pg_transacao_com_erro_nao_contamina(pg):
    def suja(c):
        with c.cursor() as cur:
            try:
                cur.execute("SELECT 1/0")
            except psycopg2.DataError:
                pass                                   # deixa a transação abortada
    c2 = _mesma_sessao(pg, suja)
    with c2.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
    c2.close()


@precisa_pg
def test_pg_sessao_morta_no_servidor_e_trocada(pg, monkeypatch):
    monkeypatch.setattr(db_pool, "PING_S", -1)
    c = db_pool.conectar(**pg)
    with c.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        pid = cur.fetchone()[0]
    c.close()
    killer = psycopg2.connect(PG)
    killer.autocommit = True
    with killer.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
    killer.close()
    c2 = db_pool.conectar(**pg)
    with c2.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        assert cur.fetchone()[0] != pid
    c2.close()
