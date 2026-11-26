"""Fase 2 (26/11): o workflow de deploy roda o pytest antes do build.

Leitura de texto do YAML (sem PyYAML): confere só o que garante a regra
"teste falhou = sem imagem e sem deploy".
"""

from __future__ import annotations

import re
from pathlib import Path

WF = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "deploy-easypanel.yml"
TEXTO = WF.read_text(encoding="utf-8")


def _job(nome: str) -> str:
    """Bloco do job `nome` (até o próximo job de 2 espaços de recuo ou o fim)."""
    m = re.search(rf"^  {nome}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", TEXTO, re.S | re.M)
    assert m, f"job {nome} não encontrado"
    return m.group(1)


def test_roda_em_push_e_em_pr_para_master():
    gatilhos = TEXTO.split("\nenv:", 1)[0]
    assert re.search(r"push:\n\s+branches: \[master\]", gatilhos)
    assert re.search(r"pull_request:\n\s+branches: \[master\]", gatilhos)


def test_job_de_testes_roda_pytest_na_versao_do_dockerfile():
    job = _job("testes")
    assert "python -m pytest tests" in job
    assert "pip install -r requirements.txt pytest" in job
    docker = (WF.parents[2] / "Dockerfile").read_text(encoding="utf-8")
    versao = re.search(r"FROM python:(\d+\.\d+)", docker).group(1)
    assert f'python-version: "{versao}"' in job


def test_job_de_testes_nao_usa_segredo_nem_postgres():
    job = _job("testes")
    assert "secrets." not in job
    assert "services:" not in job
    assert "_TEST_DSN" not in job


def test_build_e_deploy_dependem_dos_testes_e_nao_rodam_em_pr():
    job = _job("build-and-deploy")
    assert re.search(r"^    needs: testes$", job, re.M)
    assert "if: github.event_name != 'pull_request'" in job
    # build, push e webhook continuam dentro do job que depende dos testes
    assert "docker/build-push-action" in job
    assert "secrets.PAINEL_EDUIT_TOKEN" in job
    assert TEXTO.count("docker/build-push-action") == 1
    assert TEXTO.count("secrets.PAINEL_EDUIT_TOKEN") == 1


def test_fila_de_deploy_so_no_job_de_deploy():
    # Concorrência no nível do workflow faria um PR substituir um deploy pendente.
    cabecalho = TEXTO.split("\njobs:", 1)[0]
    assert "concurrency:" not in cabecalho
    assert "group: deploy-easypanel" in _job("build-and-deploy")
