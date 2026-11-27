"""Processo dos jobs agendados, fora do site (Modernização, Fase 3).

No EasyPanel: serviço com a mesma imagem do painel e `AGENDADOR=worker` no ambiente
(o `iniciar.sh` da imagem chama este arquivo). O site fica com `AGENDADOR=site`.

Responde `GET /health` na porta 5001 (`PORT`), só para a verificação de saúde do
EasyPanel: sem porta aberta, ele parava o container ~9 s depois de subir (27/11).

Importar o app registra e inicia os jobs: com `AGENDADOR=worker`, só os que
gravam no banco ou chamam APIs. Os que aquecem cache na memória ficam no site.
"""
import json
import logging
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Força o papel: o worker em `tudo` rodaria também os jobs do site.
os.environ["AGENDADOR"] = "worker"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger("worker")

_estado = {"fase": "iniciando", "jobs": []}


class _Saude(BaseHTTPRequestHandler):
    """Só responde a saúde do processo; nenhuma rota do painel existe aqui."""

    def do_GET(self):
        corpo = json.dumps({"ok": True, "papel": "worker", **_estado}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(corpo)))
        self.end_headers()
        self.wfile.write(corpo)

    do_HEAD = do_GET

    def log_message(self, *_a):   # sem uma linha de log por verificação
        pass


def _subir_saude() -> ThreadingHTTPServer | None:
    porta = int(os.getenv("PORT", "5001"))
    try:
        srv = ThreadingHTTPServer(("0.0.0.0", porta), _Saude)
    except OSError as e:
        log.warning("saúde: não abri a porta %s (%s); sigo sem ela", porta, e)
        return None
    threading.Thread(target=srv.serve_forever, name="worker-saude", daemon=True).start()
    log.info("saúde: respondendo em :%s/health", porta)
    return srv


# Abre a porta antes de importar o app (o import leva alguns segundos).
_servidor_saude = _subir_saude()

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
_estado.update(fase="rodando", jobs=jobs)

while not _parar.wait(60):
    pass

# Sem esperar os jobs em andamento: o Docker mata o processo ~10 s depois do SIGTERM.
# A trava some com a sessão e o próximo worker retoma na rodada seguinte.
app.scheduler.shutdown(wait=False)
if _servidor_saude is not None:
    _servidor_saude.shutdown()
log.info("worker encerrado")
