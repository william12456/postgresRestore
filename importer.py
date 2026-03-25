import os
import time

import psycopg
from psycopg import sql


def safety_check(remote_config, local_config, tables, force, dry_run, logger):
    """Verify that local != remote and get user confirmation."""
    if remote_config["host"] == local_config["host"] and remote_config["port"] == local_config["port"]:
        if remote_config["dbname"] == local_config["dbname"]:
            logger.error("ABORTADO: banco local e remoto são idênticos!")
            return False
        logger.warning(f"AVISO: host local ({local_config['host']}) é o mesmo do remoto!")

    logger.info(f"Safety check: remoto={remote_config['host']}, local={local_config['host']}")

    if dry_run:
        logger.info("=== DRY RUN ===")
        logger.info(f"Tabelas que seriam dropadas e recriadas: {', '.join(tables)}")
        return False

    if not force:
        print(f"\nAs seguintes {len(tables)} tabelas serão DROPADAS e recriadas no banco local:")
        for t in tables:
            print(f"  - {t}")
        print(f"\nBanco local: {local_config['user']}@{local_config['host']}:{local_config['port']}/{local_config['dbname']}")
        confirm = input("\nContinuar? (y/N): ").strip().lower()
        if confirm != "y":
            logger.info("Operação cancelada pelo usuário")
            return False

    return True


def run_import(local_config, tables, backup_dir, logger, schema="public"):
    """Execute transactional import: drop, create, copy data, restore sequences."""
    start = time.time()

    # Read schema SQL
    schema_path = os.path.join(backup_dir, "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    # Read sequences SQL (optional)
    seq_path = os.path.join(backup_dir, "sequences.sql")
    sequences_sql = None
    if os.path.exists(seq_path):
        with open(seq_path, "r", encoding="utf-8") as f:
            sequences_sql = f.read()

    conn = psycopg.connect(**local_config, autocommit=False)
    try:
        with conn.cursor() as cur:
            # Disable FK checks
            cur.execute("SET session_replication_role = 'replica'")
            logger.info("FK constraints desabilitadas")

            # Drop all tables
            for table in reversed(tables):
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema, table)
                ))
                logger.debug(f"Dropada: {table}")
            logger.info(f"{len(tables)} tabelas dropadas")

            # Reabilita para criação correta do schema
            cur.execute("SET session_replication_role = 'DEFAULT'")

            # Create schema (multi-statement DDL)
            conn.execute(schema_sql)
            logger.info("Schema recriado")

            # Disable FK checks again for data import
            cur.execute("SET session_replication_role = 'replica'")

            # Import data
            total_rows = 0
            for table in tables:
                csv_path = os.path.join(backup_dir, f"{table}.csv")
                if not os.path.exists(csv_path):
                    logger.warning(f"CSV não encontrado para {table}, pulando")
                    continue

                table_start = time.time()
                row_count = 0

                with cur.copy(sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER)").format(
                    sql.Identifier(schema, table)
                )) as copy:
                    with open(csv_path, "rb") as f:
                        while data := f.read(8192):
                            copy.write(data)

                # Count imported rows
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(schema, table)
                ))
                row_count = cur.fetchone()[0]
                total_rows += row_count

                table_elapsed = time.time() - table_start
                logger.info(f"Importando: {table} ({row_count} linhas) ... OK [{table_elapsed:.1f}s]")

            # Restore sequences (multi-statement)
            if sequences_sql:
                for line in sequences_sql.strip().splitlines():
                    line = line.strip()
                    if line:
                        cur.execute(line)
                logger.info("Sequences restauradas")

            # Re-enable FK checks
            cur.execute("SET session_replication_role = 'DEFAULT'")

        conn.commit()
        elapsed = time.time() - start
        logger.info(f"Import concluído com sucesso em {elapsed:.1f}s ({total_rows} linhas total)")
        return total_rows

    except Exception as e:
        conn.rollback()
        logger.error(f"ERRO no import — rollback executado: {e}")
        raise
    finally:
        conn.close()
