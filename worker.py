"""Processo dos jobs agendados, fora do site (Modernização, Fase 3).

No EasyPanel: serviço com a mesma imagem do painel e o comando `python worker.py`.
Não abre porta. O site fica com `AGENDADOR=site` (ver agendador.py).

Importar o app registra e inicia os jobs: com `AGENDADOR=worker`, só os que
gravam no banco ou chamam APIs. Os que aquecem cache na memória ficam no site.
"""
import logging
import os
import signal
import threading

# Força o papel: o worker em `tudo` rodaria também os jobs do site.
os.environ["AGENDADOR"] = "worker"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger("worker")

import app  # noqa: E402  (registra e inicia o agendador)

_parar = threading.Event()


def _sinal(signum, _frame):
    log.info("sinal %s recebido; encerrando o agendador", signum)
    _parar.set()


signal.signal(signal.SIGTERM, _sinal)
signal.signal(signal.SIGINT, _sinal)

jobs = sorted(j.id for j in app.scheduler.get_jobs())
log.info("worker no ar com %s jobs: %s", len(jobs), ", ".join(jobs))
log.info("ficam no site: %s", ", ".join(sorted(set(app.scheduler.ignorados))) or "nenhum")

while not _parar.wait(60):
    pass

app.scheduler.shutdown(wait=True)
log.info("worker encerrado")
