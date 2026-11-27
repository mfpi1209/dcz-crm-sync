"""Cache da rota de alunos da home (27/11): ~30 s por chamada em produção.

A resposta só muda quando muda o último upload de Matriculados, então o cache é
por (snapshot_id, linhas do snapshot, filtros). Aqui a conta pesada é trocada por
uma falsa que conta chamadas.
"""
from __future__ import annotations

import threading
import time

import pytest
from flask import Flask, jsonify

from routes import dashboard as d


class _Conn:
    def close(self):
        pass


@pytest.fixture
def app_falso(monkeypatch):
    estado = {"versao": (10, 500), "chamadas": [], "erro": False, "demora": 0.0}

    def _sem_cache():
        from flask import request
        estado["chamadas"].append(dict(request.args))
        if estado["demora"]:
            time.sleep(estado["demora"])
        if estado["erro"]:
            return jsonify({"error": "boom"}), 500
        return jsonify({"grand_total": len(estado["chamadas"]), "ciclo": request.args.get("ciclo")})

    monkeypatch.setattr(d, "_api_dashboard_students_sem_cache", _sem_cache)
    monkeypatch.setattr(d, "get_disparos_conn", lambda: _Conn())
    monkeypatch.setattr(d, "_alunos_versao", lambda conn: estado["versao"])
    d._ALUNOS_CACHE.clear()
    d._ALUNOS_USADOS.clear()
    d._ALUNOS_CALC_LOCKS.clear()
    app = Flask(__name__)
    app.register_blueprint(d.dashboard_bp)
    yield app, estado
    d._ALUNOS_CACHE.clear()
    d._ALUNOS_USADOS.clear()


def test_segunda_chamada_vem_do_cache(app_falso):
    app, est = app_falso
    c = app.test_client()
    r1 = c.get("/api/dashboard/students?ciclo=2027/1")
    r2 = c.get("/api/dashboard/students?ciclo=2027/1")
    assert r1.status_code == r2.status_code == 200
    assert r1.headers["X-Cache"] == "miss" and r2.headers["X-Cache"] == "hit"
    assert r1.get_json() == r2.get_json()
    assert len(est["chamadas"]) == 1


def test_filtro_diferente_calcula_de_novo(app_falso):
    app, est = app_falso
    c = app.test_client()
    c.get("/api/dashboard/students?ciclo=2027/1")
    c.get("/api/dashboard/students?ciclo=2026/2")
    c.get("/api/dashboard/students?ciclo=2027/1&polo=X")
    assert len(est["chamadas"]) == 3


def test_upload_novo_ou_ainda_gravando_invalida(app_falso):
    app, est = app_falso
    c = app.test_client()
    c.get("/api/dashboard/students?ciclo=2027/1")
    est["versao"] = (10, 800)            # mesmo upload, ainda gravando linhas
    assert c.get("/api/dashboard/students?ciclo=2027/1").headers["X-Cache"] == "miss"
    est["versao"] = (11, 800)            # upload novo
    assert c.get("/api/dashboard/students?ciclo=2027/1").headers["X-Cache"] == "miss"
    assert len(est["chamadas"]) == 3
    assert all(k[0] == (11, 800) for k in d._ALUNOS_CACHE)   # versões velhas saem


def test_erro_nao_entra_no_cache(app_falso):
    app, est = app_falso
    c = app.test_client()
    est["erro"] = True
    assert c.get("/api/dashboard/students?ciclo=2027/1").status_code == 500
    est["erro"] = False
    r = c.get("/api/dashboard/students?ciclo=2027/1")
    assert r.status_code == 200 and r.headers["X-Cache"] == "miss"


def test_mesmos_filtros_ao_mesmo_tempo_fazem_uma_conta_so(app_falso):
    app, est = app_falso
    est["demora"] = 0.3
    resultados = []

    def pedir():
        with app.test_client() as c:
            resultados.append(c.get("/api/dashboard/students?ciclo=2027/1").status_code)

    ts = [threading.Thread(target=pedir) for _ in range(5)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert resultados == [200] * 5
    assert len(est["chamadas"]) == 1


def test_parametros_chegam_iguais_na_conta(app_falso):
    app, est = app_falso
    app.test_client().get("/api/dashboard/students?ciclo=2027/1&nivel=GRAD&from=2026-01-01")
    args = est["chamadas"][0]
    assert args["ciclo"] == "2027/1" and args["nivel"] == "GRAD" and args["from"] == "2026-01-01"


def test_aquecimento_recalcula_filtros_usados_quando_o_upload_muda(app_falso, monkeypatch):
    app, est = app_falso
    monkeypatch.setattr(d, "_ALUNOS_APP", app)
    c = app.test_client()
    c.get("/api/dashboard/students?ciclo=2027/1")
    assert d.aquecer_alunos_home() == 0          # já está no cache
    est["versao"] = (12, 900)                    # chegou upload novo
    assert d.aquecer_alunos_home() == 1
    n = len(est["chamadas"])
    assert c.get("/api/dashboard/students?ciclo=2027/1").headers["X-Cache"] == "hit"
    assert len(est["chamadas"]) == n


def test_sem_upload_cai_na_conta_direta(app_falso):
    app, est = app_falso
    est["versao"] = None
    r = app.test_client().get("/api/dashboard/students?ciclo=2027/1")
    assert r.status_code == 200
    assert d._ALUNOS_CACHE == {}
