#!/bin/sh
# Comando do container (Fase 3, 27/11). O EasyPanel não deixa trocar o comando pela tela,
# então a mesma imagem escolhe pelo ambiente:
#   AGENDADOR=worker  -> jobs agendados (worker.py), sem porta
#   qualquer outro    -> site (Gunicorn), como antes
set -e
if [ "$AGENDADOR" = "worker" ]; then
    exec python worker.py
fi
exec gunicorn --bind 0.0.0.0:5001 --workers 1 --threads 4 --timeout 600 app:app
