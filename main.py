import argparse
import os
import time
from datetime import datetime

from config import load_config
from exporter import (
    connect_with_retry,
    discover_tables,
    export_all_data,
    export_sequences,
    extract_ddl,
    get_dependency_order,
)
from importer import run_import, safety_check
from logger_setup import setup_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copia todas as tabelas de um PostgreSQL remoto para um local"
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Número de workers paralelos para export (padrão: 4)"
    )
    parser.add_argument(
        "--backup-dir", default="./backups",
        help="Diretório raiz dos backups (padrão: ./backups)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Mostra o que seria feito sem executar"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Pula confirmação de segurança antes do drop"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config()

    # Create timestamped backup directory
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_dir = os.path.join(args.backup_dir, f"backup_{timestamp}")
    os.makedirs(backup_dir, exist_ok=True)

    logger = setup_logger(backup_dir)
    overall_start = time.time()

    remote_schema = config["remote_schema"]
    local_schema = config["local_schema"]
    logger.info("=== POSTGRES RESTORE — INICIANDO ===")
    logger.info(f"Schema remoto: {remote_schema}, Schema local: {local_schema}")

    # --- EXPORT PHASE ---
    logger.info("Conectando ao banco remoto...")
    try:
        remote_conn = connect_with_retry(config["remote"], logger)
    except Exception as e:
        logger.error(f"Falha ao conectar ao banco remoto: {e}")
        return 1

    try:
        # Discover tables
        tables = discover_tables(remote_conn, logger, schema=remote_schema)
        if not tables:
            logger.warning(f"Nenhuma tabela encontrada no schema {remote_schema}")
            return 0

        # Dependency order
        ordered_tables = get_dependency_order(remote_conn, tables, schema=remote_schema)
        logger.info(f"Ordem de dependência calculada para {len(ordered_tables)} tabelas")

        # Extract DDL
        logger.info("Extraindo DDL...")
        ddl = extract_ddl(remote_conn, config["remote"], ordered_tables, backup_dir, logger, schema=remote_schema)

        # Export sequences
        export_sequences(remote_conn, backup_dir, logger, schema=remote_schema)
    finally:
        remote_conn.close()

    # Export data (parallel)
    logger.info(f"Exportando dados ({args.workers} workers)...")
    export_results = export_all_data(config["remote"], ordered_tables, backup_dir, logger, workers=args.workers, schema=remote_schema)

    # Check export errors
    errors = [(t, e) for t, _, e in export_results if e]
    exported = [(t, r) for t, r, e in export_results if not e]
    total_exported_rows = sum(r for _, r in exported)

    if errors:
        logger.warning(f"{len(errors)} tabelas falharam na exportação:")
        for t, e in errors:
            logger.warning(f"  {t}: {e}")

    logger.info(f"Export concluído: {len(exported)}/{len(ordered_tables)} tabelas, {total_exported_rows} linhas")

    # --- IMPORT PHASE ---
    # Safety check
    exportable_tables = [t for t, _, e in export_results if not e]
    if not safety_check(config["remote"], config["local"], exportable_tables, args.force, args.dry_run, logger):
        if args.dry_run:
            return 0
        return 1

    logger.info("Importando para banco local...")
    try:
        total_imported = run_import(config["local"], exportable_tables, backup_dir, logger, schema=local_schema, source_schema=remote_schema)
    except Exception as e:
        logger.error(f"Import falhou: {e}")
        return 1

    # --- SUMMARY ---
    overall_elapsed = time.time() - overall_start
    minutes = int(overall_elapsed // 60)
    seconds = int(overall_elapsed % 60)

    logger.info("=== RESUMO ===")
    logger.info(f"Tabelas copiadas: {len(exported)}/{len(ordered_tables)}")
    logger.info(f"Total de linhas: {total_imported}")
    logger.info(f"Tempo total: {minutes}m{seconds:02d}s")
    logger.info(f"Erros: {len(errors)}")
    logger.info(f"Backup salvo em: {backup_dir}")

    return 0 if not errors else 1


if __name__ == "__main__":
    exit(main())
