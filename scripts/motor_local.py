"""Ambiente LOCAL de teste do Motor de Ativações: Postgres próprio, painel e worker.

    python scripts/motor_local.py preparar   # uma vez: ambiente Python, banco local e tabelas motor_
    python scripts/motor_local.py            # sobe Postgres, worker e painel em http://127.0.0.1:5057
    python scripts/motor_local.py parar      # para worker e Postgres que tenham ficado de pé
    python scripts/motor_local.py zerar      # apaga o banco local e recria vazio

Tudo fica em `.motor-local/` (fora do git). O banco é sempre o local, em
127.0.0.1: as variáveis de banco deste script valem sobre as do ambiente e
sobre qualquer `.env`. O motor sobe sempre em simulação (MOTOR_DRY_RUN=1), com
a escrita no CRM travada, e os agendamentos do painel não rodam. Segredos
opcionais (token de leitura do CRM) vão em `.motor-local/env`, não versionado.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
BASE = RAIZ / ".motor-local"
PG_DADOS = BASE / "pg"
VENV_PY = BASE / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
ARQ_ENV = BASE / "env"
PID_WORKER = BASE / "worker.pid"
PORTA_PG = os.getenv("MOTOR_LOCAL_PG_PORT", "55440")
PORTA_PAINEL = int(os.getenv("MOTOR_LOCAL_PORT", "5057"))
BANCO, USUARIO = "dcz_sync_local", "motor"
EXE = ".exe" if os.name == "nt" else ""

# Só estas chaves do arquivo local chegam ao painel e ao worker; o banco nunca vem de arquivo.
PERMITIDAS = ("EDUIT_CRM_TOKEN", "EDUIT_CRM_BASE_URL", "MOTOR_REPRISE_DIR", "MOTOR_FERIADOS", "TYPESAFE_API_KEY", "MOTOR_CRM_ORG_ID")


def _pg_bin() -> Path:
    pasta = os.getenv("MOTOR_LOCAL_PG_BIN", "")
    candidatos = [Path(pasta)] if pasta else []
    candidatos += sorted(Path("C:/Program Files/PostgreSQL").glob("*/bin"), reverse=True) if os.name == "nt" else []
    achado = shutil.which("initdb")
    if achado:
        candidatos.append(Path(achado).parent)
    for c in candidatos:
        if (c / f"initdb{EXE}").is_file():
            return c
    sys.exit("PostgreSQL não encontrado. Instale ou aponte MOTOR_LOCAL_PG_BIN para a pasta bin.")


def _pg(nome: str, *args: str, saida: Path | None = None) -> int:
    destino = open(saida or (BASE / "pg-comandos.log"), "ab")
    try:
        return subprocess.call([str(_pg_bin() / f"{nome}{EXE}"), *args], stdout=destino, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    finally:
        destino.close()


def _pg_no_ar() -> bool:
    return _pg("pg_isready", "-h", "127.0.0.1", "-p", PORTA_PG, "-t", "2") == 0


def _subir_pg() -> subprocess.Popen | None:
    if _pg_no_ar():
        return None
    if not (PG_DADOS / "PG_VERSION").is_file():
        sys.exit("Banco local ainda não existe. Rode: python scripts/motor_local.py preparar")
    log = open(BASE / "pg.log", "ab")
    proc = subprocess.Popen(
        [str(_pg_bin() / f"postgres{EXE}"), "-D", str(PG_DADOS), "-p", PORTA_PG, "-c", "listen_addresses=127.0.0.1"],
        stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
    )
    for _ in range(30):
        if _pg_no_ar():
            return proc
        time.sleep(1)
    sys.exit(f"O Postgres local não respondeu. Veja {BASE / 'pg.log'}")


def _parar_pg() -> None:
    if (PG_DADOS / "postmaster.pid").is_file():
        _pg("pg_ctl", "-D", str(PG_DADOS), "-m", "fast", "-w", "stop")


def _ambiente() -> dict:
    """Ambiente do painel e do worker: banco local, simulação, sem agendador e sem segredo herdado do banco."""
    env = dict(os.environ)
    if ARQ_ENV.is_file():
        for linha in ARQ_ENV.read_text(encoding="utf-8").splitlines():
            chave, _, valor = linha.partition("=")
            chave, valor = chave.strip(), valor.strip().strip('"').strip("'")
            if chave in PERMITIDAS and valor:
                env[chave] = valor
    env.update({
        "DB_HOST": "127.0.0.1", "DB_PORT": PORTA_PG, "DB_USER": USUARIO, "DB_PASS": "", "DB_NAME": BANCO,
        "DISPAROS_DB_NAME": BANCO, "KOMMO_PG_HOST": "127.0.0.1", "KOMMO_PG_PORT": PORTA_PG, "KOMMO_PG_USER": USUARIO,
        "KOMMO_PG_PASS": "", "KOMMO_PG_DB": BANCO,
        "MOTOR_ENABLED": "1", "MOTOR_DRY_RUN": "1", "MOTOR_LOCAL": "1", "DEV_SKIP_AUTH": "1",
        "SECRET_KEY": "motor-local", "PYTHONUNBUFFERED": "1",
    })
    env.setdefault("MOTOR_REPRISE_DIR", str(BASE / "rodadas"))
    env.pop("MOTOR_WEBHOOK_SECRET", None)
    return env


def _aplicar_sql(env: dict) -> None:
    codigo = (
        "import psycopg2, pathlib;"
        f"c = psycopg2.connect(host='127.0.0.1', port={PORTA_PG!r}, user={USUARIO!r}, dbname={BANCO!r});"
        "cur = c.cursor();"
        "cur.execute(pathlib.Path('sql/motor_ativacoes.sql').read_text(encoding='utf-8'));"
        "cur.execute('INSERT INTO motor_controle (id, ligado) VALUES (1, FALSE) ON CONFLICT (id) DO NOTHING');"
        "c.commit(); c.close()"
    )
    subprocess.check_call([str(VENV_PY), "-c", codigo], cwd=RAIZ, env=env)


def preparar() -> None:
    BASE.mkdir(exist_ok=True)
    (BASE / "rodadas").mkdir(exist_ok=True)
    if not VENV_PY.is_file():
        print("Criando o ambiente Python local (leva alguns minutos na primeira vez)...")
        subprocess.check_call([sys.executable, "-m", "venv", str(BASE / "venv")])
        subprocess.check_call([str(VENV_PY), "-m", "pip", "install", "--quiet", "--disable-pip-version-check", "-r", str(RAIZ / "requirements.txt")])
    if not (PG_DADOS / "PG_VERSION").is_file():
        print("Criando o Postgres local...")
        if _pg("initdb", "-D", str(PG_DADOS), "-U", USUARIO, "-A", "trust", "-E", "UTF8", "--locale=C") != 0:
            sys.exit(f"initdb falhou. Veja {BASE / 'pg-comandos.log'}")
    meu = _subir_pg()
    _pg("createdb", "-h", "127.0.0.1", "-p", PORTA_PG, "-U", USUARIO, BANCO)   # já existir não é erro
    _aplicar_sql(_ambiente())
    if meu is not None:
        _parar_pg()
    if not ARQ_ENV.is_file():
        shutil.copyfile(RAIZ / ".env.motor-local.example", ARQ_ENV)
    print(f"Pronto. Rodadas para a reprise: {BASE / 'rodadas'} (ou MOTOR_REPRISE_DIR em {ARQ_ENV}).")


def _parar_worker() -> None:
    if not PID_WORKER.is_file():
        return
    pid = PID_WORKER.read_text().strip()
    if pid.isdigit():
        if os.name == "nt":
            subprocess.call(["taskkill", "/PID", pid, "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            try:
                os.kill(int(pid), 15)
            except OSError:
                pass
    PID_WORKER.unlink(missing_ok=True)


def parar() -> None:
    _parar_worker()
    _parar_pg()
    print("Worker e Postgres locais parados.")


def zerar() -> None:
    parar()
    meu = _subir_pg()
    _pg("dropdb", "-h", "127.0.0.1", "-p", PORTA_PG, "-U", USUARIO, "--if-exists", BANCO)
    _pg("createdb", "-h", "127.0.0.1", "-p", PORTA_PG, "-U", USUARIO, BANCO)
    _aplicar_sql(_ambiente())
    if meu is not None:
        _parar_pg()
    print("Banco local zerado, com as tabelas motor_ recriadas.")


def subir() -> None:
    if not VENV_PY.is_file():
        sys.exit("Ambiente local ainda não preparado. Rode: python scripts/motor_local.py preparar")
    if Path(sys.prefix).resolve() != (BASE / "venv").resolve():
        # Chamado pelo Python do sistema: continua no Python do ambiente local, que tem as dependências do painel.
        sys.exit(subprocess.call([str(VENV_PY), str(Path(__file__).resolve()), "subir"], cwd=RAIZ))
    env = _ambiente()
    if env["MOTOR_DRY_RUN"] != "1" or env["DB_HOST"] != "127.0.0.1":
        sys.exit("O ambiente local só sobe em simulação e no banco local.")
    _parar_worker()
    _subir_pg()
    _aplicar_sql(env)
    log = open(BASE / "worker.log", "ab")
    worker = subprocess.Popen([str(VENV_PY), "-m", "motor.worker"], cwd=RAIZ, env=env, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    PID_WORKER.write_text(str(worker.pid))
    print(f"Motor local em simulação: http://127.0.0.1:{PORTA_PAINEL}/  (worker: {BASE / 'worker.log'})", flush=True)
    try:
        os.environ.update(env)
        os.chdir(RAIZ)
        sys.path.insert(0, str(RAIZ))
        import apscheduler.schedulers.background as agendador

        agendador.BackgroundScheduler.start = lambda self, *a, **k: None   # nenhum agendamento do painel roda no ambiente local
        import app as painel

        painel.app.run(host="127.0.0.1", port=PORTA_PAINEL, threaded=True, use_reloader=False)
    except KeyboardInterrupt:
        pass
    finally:
        _parar_worker()
        _parar_pg()


if __name__ == "__main__":
    comando = sys.argv[1] if len(sys.argv) > 1 else "subir"
    acoes = {"preparar": preparar, "subir": subir, "parar": parar, "zerar": zerar}
    if comando not in acoes:
        sys.exit(__doc__)
    acoes[comando]()
