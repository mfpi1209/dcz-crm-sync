"""Fase 3 (27/11): jobs separados entre site e worker, com trava no Postgres.

Partes:
- papel do processo (tudo/site/worker/nenhum) e quem roda cada job;
- trava só nos jobs que gravam; cache por processo sem trava;
- todo `id=` de job no código está classificado;
- trava_jobs com conexão falsa (pega, pula, solta, banco fora);
- trava de verdade entre duas sessões, só com POOL_TEST_DSN (job testes-pg do CI);
- vigia da base do painel e conferência da cópia leads_rgm.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import psycopg2
import pytest

import agendador
import trava_jobs

RAIZ = Path(__file__).resolve().parents[1]


def _nada():
    return "rodou"


def _run_scheduled_sync(*_a):
    return None


def _ag(papel):
    return agendador.Agendador(timezone="America/Sao_Paulo", papel_do_processo=papel)


def _ids(sched):
    return {j.id for j in sched.get_jobs()}


TODOS = sorted(agendador.JOBS_DO_SITE | agendador.JOBS_DO_WORKER)


@pytest.mark.parametrize("papel,esperado", [
    ("tudo", set(TODOS)),
    ("site", set(agendador.JOBS_DO_SITE)),
    ("worker", set(agendador.JOBS_DO_WORKER)),
    ("nenhum", set()),
])
def test_cada_papel_roda_so_os_seus(papel, esperado):
    s = _ag(papel)
    for job_id in TODOS:
        s.add_job(_nada, "interval", minutes=5, id=job_id)
    assert _ids(s) == esperado


def test_agendamento_da_tela_config_fica_no_site_sem_trava():
    s = _ag("site")
    s.add_job(_run_scheduled_sync, "cron", hour=2, id="sync_full_0200")
    job = s.get_job("sync_full_0200")
    assert job is not None
    assert not getattr(job.func, "_com_trava", False)
    assert _ids(_ag("worker")) == set()  # e o worker não registra nada disso
    w = _ag("worker")
    w.add_job(_run_scheduled_sync, "cron", hour=2, id="sync_full_0200")
    assert _ids(w) == set()


def test_trava_so_em_quem_grava():
    s = _ag("tudo")
    for job_id in TODOS:
        s.add_job(_nada, "interval", minutes=5, id=job_id)
    for job in s.get_jobs():
        travado = getattr(job.func, "_com_trava", False)
        assert travado == (job.id not in agendador.JOBS_POR_PROCESSO), job.id


def test_cache_por_processo_e_subconjunto_do_site():
    assert agendador.JOBS_POR_PROCESSO <= agendador.JOBS_DO_SITE
    assert not (agendador.JOBS_DO_SITE & agendador.JOBS_DO_WORKER)


def test_papel_invalido_vira_tudo(monkeypatch):
    monkeypatch.setenv("AGENDADOR", "qualquer")
    assert agendador.papel() == "tudo"
    monkeypatch.delenv("AGENDADOR")
    assert agendador.papel() == "tudo"   # padrão: comportamento de antes da Fase 3


def test_todo_job_do_codigo_esta_classificado():
    """Job novo precisa entrar em JOBS_DO_SITE ou JOBS_DO_WORKER (agendador.py)."""
    achados = set()
    for arq in list(RAIZ.glob("*.py")) + list((RAIZ / "routes").glob("*.py")) + list((RAIZ / "services").glob("*.py")):
        texto = arq.read_text(encoding="utf-8", errors="ignore")
        if "add_job" not in texto:
            continue
        for ini in [m.end() for m in re.finditer(r"add_job\(", texto)]:
            bloco = texto[ini:ini + 600].split("add_job(")[0]   # até o próximo add_job
            m = re.search(r'\bid="([a-z0-9_]+)"', bloco)
            if m:
                achados.add(m.group(1))
    assert achados, "não achei nenhum add_job"
    sem_lado = achados - agendador.JOBS_DO_SITE - agendador.JOBS_DO_WORKER
    assert not sem_lado, f"jobs sem lado definido em agendador.py: {sorted(sem_lado)}"
    sobrando = (agendador.JOBS_DO_SITE | agendador.JOBS_DO_WORKER) - achados
    assert not sobrando, f"listados em agendador.py mas não existem: {sorted(sobrando)}"


def test_worker_py_forca_o_papel():
    texto = (RAIZ / "worker.py").read_text(encoding="utf-8")
    assert 'os.environ["AGENDADOR"] = "worker"' in texto
    assert texto.index('os.environ["AGENDADOR"]') < texto.index("import app")


def test_app_usa_o_agendador_e_aquece_sumidos_so_no_site():
    texto = (RAIZ / "app.py").read_text(encoding="utf-8")
    assert "scheduler = Agendador(" in texto
    assert "BackgroundScheduler(" not in texto
    assert 'if scheduler.papel in ("tudo", "site"):\n    # Cache em memória' in texto


# ── trava_jobs com conexão falsa ────────────────────────────────────────────

class _CurT:
    def __init__(self, conn):
        self.conn = conn
        self._r = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.sqls.append(sql)
        if "pg_try_advisory_lock" in sql:
            self._r = (self.conn.livre,)
        elif "pg_advisory_lock" in sql and self.conn.timeout:
            raise psycopg2.errors.LockNotAvailable("lock timeout")

    def fetchone(self):
        return self._r


class _ConnT:
    def __init__(self, livre=True, timeout=False):
        self.livre, self.timeout = livre, timeout
        self.sqls, self.closed, self.autocommit = [], 0, False

    def cursor(self):
        return _CurT(self)

    def close(self):
        self.closed = 1


@pytest.fixture
def conn_trava(monkeypatch):
    estado = {"conn": _ConnT()}
    monkeypatch.setattr(trava_jobs, "_conectar", lambda: estado["conn"])
    monkeypatch.setenv("TRAVA_JOBS", "1")
    return estado


def test_com_trava_roda_e_solta(conn_trava):
    f = trava_jobs.com_trava("job_x", lambda: 42)
    assert f() == 42
    c = conn_trava["conn"]
    assert any("pg_advisory_unlock" in s for s in c.sqls)
    assert c.closed


def test_com_trava_pula_se_outro_processo_esta_rodando(conn_trava):
    conn_trava["conn"] = _ConnT(livre=False)
    chamadas = []
    f = trava_jobs.com_trava("job_x", lambda: chamadas.append(1))
    assert f() is None
    assert chamadas == []
    assert conn_trava["conn"].closed


def test_com_trava_solta_mesmo_com_erro_no_job(conn_trava):
    def quebra():
        raise ValueError("x")
    with pytest.raises(ValueError):
        trava_jobs.com_trava("job_x", quebra)()
    assert any("pg_advisory_unlock" in s for s in conn_trava["conn"].sqls)
    assert conn_trava["conn"].closed


def test_banco_fora_roda_sem_trava_ou_pula_no_modo_estrito(monkeypatch):
    def sem_banco():
        raise psycopg2.OperationalError("down")
    monkeypatch.setattr(trava_jobs, "_conectar", sem_banco)
    assert trava_jobs.com_trava("job_x", lambda: 1)() == 1
    monkeypatch.setenv("TRAVA_JOBS_ESTRITA", "1")
    assert trava_jobs.com_trava("job_x", lambda: 1)() is None


def test_trava_desligada(monkeypatch):
    monkeypatch.setenv("TRAVA_JOBS", "0")
    monkeypatch.setattr(trava_jobs, "_conectar", lambda: pytest.fail("não devia conectar"))
    assert trava_jobs.com_trava("job_x", lambda: 7)() == 7
    with trava_jobs.trava_global("t") as pegou:
        assert pegou


def test_chave_estavel_e_distinta():
    assert trava_jobs.chave_do_job("a") == trava_jobs.chave_do_job("a")
    assert trava_jobs.chave_do_job("a") != trava_jobs.chave_do_job("b")
    assert -(2 ** 63) <= trava_jobs.chave_do_job("a") < 2 ** 63


def test_trava_global_espera_tenta_e_desiste(conn_trava):
    with trava_jobs.trava_global("t", esperar=True) as pegou:
        assert pegou
    assert any("lock_timeout" in s for s in conn_trava["conn"].sqls)
    conn_trava["conn"] = _ConnT(livre=False)
    with trava_jobs.trava_global("t", esperar=False) as pegou:
        assert not pegou
    conn_trava["conn"] = _ConnT(timeout=True)
    with trava_jobs.trava_global("t", esperar=True, espera_max_s=1) as pegou:
        assert not pegou
    assert conn_trava["conn"].closed


def test_trava_global_tem_chave_diferente_do_job():
    # Mesmo nome não pode colidir com a trava do job (sessões diferentes se bloqueariam).
    assert trava_jobs.chave_do_job("trecho:x") != trava_jobs.chave_do_job("x")


# ── vigia do painel e cópia leads_rgm ───────────────────────────────────────

class _CurV:
    def __init__(self, valor):
        self.valor = valor

    def execute(self, *a):
        pass

    def fetchone(self):
        return self.valor


class _ConnV:
    def __init__(self, valor):
        self.valor = valor

    def cursor(self):
        return _CurV(self.valor)

    def rollback(self):
        pass

    def close(self):
        pass


def test_vigia_vence_cache_so_quando_built_at_muda(monkeypatch):
    from routes import bwipo_sync as bs
    venceu = []
    monkeypatch.setattr(bs, "_expirar_caches_painel", lambda: venceu.append(1))
    monkeypatch.setattr(bs, "_painel_meta_vista", None)
    valor = {"v": ("t1",)}
    monkeypatch.setattr(bs, "get_conn", lambda: _ConnV(valor["v"]))
    assert bs.vigiar_painel_base() is False      # primeira leitura: só memoriza
    assert bs.vigiar_painel_base() is False      # igual
    valor["v"] = ("t2",)
    assert bs.vigiar_painel_base() is True       # mudou em outro processo
    assert venceu == [1]
    bs._marcar_meta_vista("t3")                  # este processo montou e já venceu
    valor["v"] = ("t3",)
    assert bs.vigiar_painel_base() is False
    assert venceu == [1]


def test_confirmar_copia_usa_a_que_existe_e_cria_se_faltar(monkeypatch):
    from services import leads_rgm
    from routes import comercial_rgm
    monkeypatch.setattr(leads_rgm, "_ultimo_ok", None)
    monkeypatch.setattr(comercial_rgm, "_pg_kommo", lambda: _ConnV((True,)))
    assert leads_rgm.confirmar_copia() is True
    assert leads_rgm.pronta()
    monkeypatch.setattr(leads_rgm, "_ultimo_ok", None)
    monkeypatch.setattr(comercial_rgm, "_pg_kommo", lambda: _ConnV(None))
    criou = []
    monkeypatch.setattr(leads_rgm, "garantir", lambda: criou.append(1) or True)
    assert leads_rgm.confirmar_copia() is True
    assert criou == [1]


# ── Postgres de verdade (job testes-pg do CI) ───────────────────────────────

PG = os.getenv("POOL_TEST_DSN")


@pytest.mark.skipif(not PG, reason="defina POOL_TEST_DSN para rodar contra um Postgres descartável")
def test_pg_dois_processos_nao_rodam_o_mesmo_job(monkeypatch):
    dsn = psycopg2.extensions.parse_dsn(PG)
    monkeypatch.setattr(trava_jobs, "_conectar", lambda: psycopg2.connect(**dsn))
    monkeypatch.setenv("TRAVA_JOBS", "1")
    outro = psycopg2.connect(**dsn)          # "outro processo" segurando a trava
    outro.autocommit = True
    with outro.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (trava_jobs.chave_do_job("job_pg"),))
    rodou = []
    f = trava_jobs.com_trava("job_pg", lambda: rodou.append(1))
    assert f() is None and rodou == []
    with trava_jobs.trava_global("trecho_pg", esperar=False) as pegou:
        assert pegou                          # chave de trecho é outra
    outro.close()                             # processo morreu: trava some sozinha
    assert f() is None and rodou == [1]
