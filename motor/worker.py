"""Worker do Motor de Ativações: `python -m motor.worker`.

Roda em serviço próprio (segundo serviço do EasyPanel, mesma imagem e mesmo
banco do painel). NÃO roda dentro do gunicorn: um deploy do painel não pode
parar o laço no meio de um envio.

Uma trava no banco (`pg_try_advisory_lock`) garante um único motor ativo. Um
segundo worker fica em espera e assume se o primeiro cair.

Sobe desligado e em simulação. Não cria tabela nenhuma: se as tabelas motor_
não existirem, espera até `python scripts/apply_motor_sql.py` ser rodado.
"""
from __future__ import annotations

import logging
import signal
import sys
import threading
from pathlib import Path

CHAVE_DA_TRAVA = 2026111201  # constante: identifica o Motor de Ativações no advisory lock
ESPERA_SEM_TRAVA_S = 30

logger = logging.getLogger("motor.worker")
_parar = threading.Event()


def _sinal(_num, _quadro) -> None:
    logger.info("Sinal de parada recebido; encerrando depois do tick atual.")
    _parar.set()


def _pegar_trava(conectar):
    """Conexão dedicada que segura a trava enquanto o processo viver (None = outro motor está ativo)."""
    conn = conectar()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (CHAVE_DA_TRAVA,))
        if cur.fetchone()[0]:
            return conn
    conn.close()
    return None


def _trava_viva(conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    from db import get_conn
    from motor import VERSAO
    from motor.config import carregar
    from motor.crm import CrmMotor
    from motor.laco import Laco
    from motor.piloto import criar_piloto
    from motor.razao_pg import RazaoPg
    from services import eduit_crm

    signal.signal(signal.SIGTERM, _sinal)
    signal.signal(signal.SIGINT, _sinal)

    cfg = carregar()
    logger.info(
        "Motor de Ativações %s: enabled=%s dry_run=%s tick=%ss janela=%sh-%sh",
        VERSAO, cfg.enabled, cfg.dry_run, cfg.tick_s, cfg.janela_inicio_h, cfg.janela_fim_h,
    )
    razao = RazaoPg(get_conn)
    # Trava de escrita sempre ligada nesta versão: nenhum POST/PUT/DELETE sai para o CRM.
    crm = CrmMotor(rpm=cfg.crm_rpm, permitir_escrita=False) if eduit_crm.configured() else None
    if crm is None:
        logger.warning("EDUIT_CRM_TOKEN não configurado: sem fila e sem carga.")
    piloto = criar_piloto()

    trava = None
    laco = None
    avisou_tabelas = False
    while not _parar.is_set():
        try:
            if trava is not None and not _trava_viva(trava):
                logger.error("Perdi a conexão da trava; paro de agir até recuperar.")
                trava, laco = None, None
            if trava is None:
                trava = _pegar_trava(get_conn)
                if trava is None:
                    logger.info("Outro motor está ativo; em espera.")
                    _parar.wait(ESPERA_SEM_TRAVA_S)
                    continue
                logger.info("Trava adquirida: este é o motor ativo.")
            if not razao.instalado():
                if not avisou_tabelas:
                    logger.warning("Tabelas motor_ não encontradas. Rode: python scripts/apply_motor_sql.py")
                    avisou_tabelas = True
                _parar.wait(ESPERA_SEM_TRAVA_S)
                continue
            if laco is None:
                laco = Laco(cfg, razao, crm=crm, piloto=piloto)
            modo = laco.tick()
            logger.debug("tick ok (%s)", modo)
        except Exception:
            # Nunca imprimir segredo: o traceback só traz tipos e mensagens das nossas camadas.
            logger.exception("Falha no tick; tento de novo no próximo.")
        _parar.wait(cfg.tick_s)

    if trava is not None:
        try:
            trava.close()
        except Exception:
            pass
    logger.info("Motor encerrado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
