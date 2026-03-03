"""
Orquestrador principal de sincronização Kommo -> SQLite.

Uso:
    python main.py                  # Sync incremental (delta)
    python main.py --full           # Força full sync de tudo
    python main.py --only leads     # Sync apenas leads
    python main.py --only contacts  # Sync apenas contatos
    python main.py --only pipelines # Sync apenas pipelines
    python main.py --only fields    # Sync apenas custom fields
    python main.py --status         # Mostra status da última sincronização

Ordem de sincronização:
    1. Pipelines e Stages (base estrutural)
    2. Custom Fields (metadados dos campos)
    3. Leads (com custom fields e contatos vinculados)
    4. Contatos (com custom fields)
"""

import sys
import os
import argparse
import logging
import time
from datetime import datetime

# Garantir que o diretório do script esteja no path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import LOG_LEVEL, LOG_FILE
from database import init_database, get_leads_count, get_contacts_count, get_sync_summary
from api_client import KommoAPIClient
from sync_pipelines import sync_pipelines
from sync_custom_fields import sync_custom_fields
from sync_leads import sync_leads
from sync_contacts import sync_contacts


def setup_logging():
    """Configura logging para console e arquivo."""
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Handler para arquivo
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))

    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    console_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Reduzir verbosidade de libs externas
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def print_status():
    """Exibe o status atual da sincronização."""
    print("\n" + "=" * 70)
    print("  KOMMO SYNC - STATUS DA SINCRONIZAÇÃO")
    print("=" * 70)

    summary = get_sync_summary()
    if not summary:
        print("\n  Nenhuma sincronização realizada ainda.\n")
        return

    for item in summary:
        print(f"\n  Entidade: {item['entity_type']}")
        print(f"    Último sync:      {item.get('last_sync_at', 'N/A')}")
        print(f"    Último full sync: {item.get('last_full_sync_at', 'N/A')}")
        print(f"    Registros:        {item.get('records_synced', 0)}")
        print(f"    Status:           {item.get('status', 'N/A')}")

    print(f"\n  Total de leads no banco:    {get_leads_count()}")
    print(f"  Total de contatos no banco: {get_contacts_count()}")
    print("=" * 70 + "\n")


def run_sync(force_full: bool = False, only: str = None):
    """
    Executa a sincronização completa ou parcial.
    
    Args:
        force_full: Se True, força full sync (ignora delta)
        only: Se definido, sincroniza apenas a entidade especificada
    """
    logger = logging.getLogger(__name__)

    start_time = time.time()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info("#" * 70)
    logger.info("#  KOMMO SYNC - INÍCIO DA SINCRONIZAÇÃO")
    logger.info("#  Data/Hora: %s", now)
    logger.info("#  Modo: %s", "FULL SYNC" if force_full else "INCREMENTAL (DELTA)")
    if only:
        logger.info("#  Entidade: %s", only)
    logger.info("#" * 70)

    # Inicializar banco
    init_database()

    # Criar cliente API
    client = KommoAPIClient()

    results = {}
    errors = []

    # === 1. Pipelines e Stages ===
    if only is None or only == "pipelines":
        try:
            results["pipelines"] = sync_pipelines(client)
        except Exception as e:
            errors.append(("pipelines", str(e)))
            logger.error("FALHA no sync de pipelines: %s", e)

    # === 2. Custom Fields ===
    if only is None or only == "fields":
        try:
            results["custom_fields"] = sync_custom_fields(client)
        except Exception as e:
            errors.append(("custom_fields", str(e)))
            logger.error("FALHA no sync de custom fields: %s", e)

    # === 3. Leads ===
    if only is None or only == "leads":
        try:
            results["leads"] = sync_leads(client, force_full=force_full)
        except Exception as e:
            errors.append(("leads", str(e)))
            logger.error("FALHA no sync de leads: %s", e)

    # === 4. Contatos ===
    if only is None or only == "contacts":
        try:
            results["contacts"] = sync_contacts(client, force_full=force_full)
        except Exception as e:
            errors.append(("contacts", str(e)))
            logger.error("FALHA no sync de contatos: %s", e)

    # === Relatório Final ===
    elapsed = time.time() - start_time
    api_stats = client.get_stats()

    logger.info("")
    logger.info("#" * 70)
    logger.info("#  KOMMO SYNC - RELATÓRIO FINAL")
    logger.info("#" * 70)

    for entity, stats in results.items():
        logger.info("  %s: %s", entity, stats)

    logger.info("")
    logger.info("  Requisições HTTP totais: %d", api_stats["total_requests"])
    logger.info("  Tempo total: %.1f segundos (%.1f minutos)", elapsed, elapsed / 60)
    logger.info("  Leads no banco: %d", get_leads_count())
    logger.info("  Contatos no banco: %d", get_contacts_count())

    if errors:
        logger.error("")
        logger.error("  ERROS ENCONTRADOS:")
        for entity, error in errors:
            logger.error("    - %s: %s", entity, error)
        logger.info("")
        logger.info("  Sincronização concluída COM ERROS.")
    else:
        logger.info("")
        logger.info("  Sincronização concluída com SUCESSO!")

    logger.info("#" * 70)

    return len(errors) == 0


def main():
    """Entry point com parse de argumentos CLI."""
    parser = argparse.ArgumentParser(
        description="Sincronizador Kommo CRM -> SQLite Local",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py                  # Sync incremental
  python main.py --full           # Full sync forçado
  python main.py --only leads     # Apenas leads
  python main.py --status         # Ver status
        """
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Forçar sincronização completa (ignora delta)"
    )
    parser.add_argument(
        "--only",
        choices=["leads", "contacts", "pipelines", "fields"],
        help="Sincronizar apenas uma entidade específica"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Exibir status da última sincronização"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    if args.status:
        init_database()
        print_status()
        return

    # Executar sincronização
    success = run_sync(force_full=args.full, only=args.only)
    
    # Mostrar status após sync
    print_status()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
