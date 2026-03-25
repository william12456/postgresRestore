import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg
from psycopg import sql


def connect_with_retry(conn_params, logger, max_retries=3):
    """Connect to PostgreSQL with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            conn = psycopg.connect(**conn_params)
            return conn
        except psycopg.OperationalError as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
                logger.warning(f"Conexão falhou (tentativa {attempt + 1}/{max_retries}), retentando em {wait}s: {e}")
                time.sleep(wait)
            else:
                raise


def discover_tables(conn, logger, schema="public"):
    """List all tables in the given schema."""
    with conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename", (schema,))
        tables = [row[0] for row in cur.fetchall()]
    logger.info(f"{len(tables)} tabelas encontradas no schema {schema}")
    return tables


def get_dependency_order(conn, tables, schema="public"):
    """Return tables in topological order based on foreign key dependencies.

    Returns a list where referenced tables come before dependent tables.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                tc.table_name AS dependent,
                ccu.table_name AS referenced
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
                AND tc.table_name != ccu.table_name
        """, (schema,))
        edges = cur.fetchall()

    # Build adjacency list
    deps = {t: set() for t in tables}
    for dependent, referenced in edges:
        if dependent in deps and referenced in deps:
            deps[dependent].add(referenced)

    # Topological sort (Kahn's algorithm)
    in_degree = {t: 0 for t in tables}
    for t, refs in deps.items():
        for ref in refs:
            in_degree[t] += 1  # t depends on ref

    queue = [t for t in tables if in_degree[t] == 0]
    result = []
    while queue:
        queue.sort()  # deterministic order
        node = queue.pop(0)
        result.append(node)
        for t, refs in deps.items():
            if node in refs:
                in_degree[t] -= 1
                if in_degree[t] == 0 and t not in result:
                    queue.append(t)

    # Add any remaining tables (circular deps)
    for t in tables:
        if t not in result:
            result.append(t)

    return result


def extract_ddl_pgdump(conn_params, tables, backup_dir, logger, schema="public"):
    """Extract DDL using pg_dump --schema-only for each table."""
    env = os.environ.copy()
    env["PGHOST"] = conn_params["host"]
    env["PGPORT"] = str(conn_params["port"])
    env["PGDATABASE"] = conn_params["dbname"]
    env["PGUSER"] = conn_params["user"]
    env["PGPASSWORD"] = conn_params["password"]
    env["PGSSLMODE"] = conn_params.get("sslmode", "require")

    all_ddl = []
    for table in tables:
        try:
            result = subprocess.run(
                ["pg_dump", "--schema-only", "--no-owner", "--no-privileges", "-t", f"{schema}.{table}"],
                capture_output=True, text=True, env=env, timeout=30,
            )
            if result.returncode == 0:
                all_ddl.append(f"-- Table: {table}\n{result.stdout}")
            else:
                logger.warning(f"pg_dump falhou para {table}: {result.stderr.strip()}")
                return None  # fallback to pg_catalog
        except FileNotFoundError:
            logger.warning("pg_dump não encontrado, usando fallback via pg_catalog")
            return None
        except subprocess.TimeoutExpired:
            logger.warning(f"pg_dump timeout para {table}")
            return None

    ddl = "\n".join(all_ddl)
    schema_path = os.path.join(backup_dir, "schema.sql")
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write(ddl)
    logger.info(f"DDL extraído via pg_dump ({len(tables)} tabelas)")
    return ddl


def extract_ddl_fallback(conn, tables, backup_dir, logger, schema="public"):
    """Extract DDL using pg_catalog queries (fallback when pg_dump unavailable)."""
    logger.warning("Usando fallback pg_catalog para DDL — partial indexes e expression defaults podem ser omitidos")
    all_ddl = []

    with conn.cursor() as cur:
        for table in tables:
            # Get columns
            cur.execute("""
                SELECT column_name, data_type, character_maximum_length,
                       is_nullable, column_default, udt_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            columns = cur.fetchall()

            col_defs = []
            for col_name, data_type, max_len, nullable, default, udt_name in columns:
                # Map data types
                if data_type == 'USER-DEFINED':
                    col_type = udt_name
                elif data_type == 'character varying':
                    col_type = f"varchar({max_len})" if max_len else "varchar"
                elif data_type == 'character':
                    col_type = f"char({max_len})" if max_len else "char"
                elif data_type == 'ARRAY':
                    col_type = f"{udt_name}"
                else:
                    col_type = data_type

                parts = [f'    "{col_name}" {col_type}']
                if nullable == 'NO':
                    parts.append("NOT NULL")
                if default:
                    parts.append(f"DEFAULT {default}")
                col_defs.append(" ".join(parts))

            ddl = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n'
            ddl += ",\n".join(col_defs)

            # Get primary key
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """, (schema, table))
            pk_cols = [row[0] for row in cur.fetchall()]
            if pk_cols:
                pk_quoted = ", ".join(f'"{c}"' for c in pk_cols)
                ddl += f",\n    PRIMARY KEY ({pk_quoted})"

            ddl += "\n);\n"

            # Get indexes (non-primary)
            cur.execute("""
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s
                AND indexname NOT IN (
                    SELECT constraint_name FROM information_schema.table_constraints
                    WHERE table_schema = %s AND table_name = %s AND constraint_type = 'PRIMARY KEY'
                )
            """, (schema, table, schema, table))
            for (indexdef,) in cur.fetchall():
                ddl += f"{indexdef};\n"

            all_ddl.append(f"-- Table: {table}\n{ddl}")

    # Get foreign keys separately (to add after all tables created)
    fk_ddl = []
    with conn.cursor() as cur:
        for table in tables:
            cur.execute("""
                SELECT
                    tc.constraint_name,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                    AND tc.table_schema = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
            """, (schema, table))
            for constraint_name, tbl, col, ref_tbl, ref_col in cur.fetchall():
                fk_ddl.append(
                    f'ALTER TABLE "{schema}"."{tbl}" ADD CONSTRAINT "{constraint_name}" '
                    f'FOREIGN KEY ("{col}") REFERENCES "{schema}"."{ref_tbl}" ("{ref_col}");'
                )

    if fk_ddl:
        all_ddl.append("-- Foreign Keys\n" + "\n".join(fk_ddl))

    ddl = "\n\n".join(all_ddl)
    schema_path = os.path.join(backup_dir, "schema.sql")
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write(ddl)
    logger.info(f"DDL extraído via pg_catalog fallback ({len(tables)} tabelas)")
    return ddl


def extract_ddl(conn, conn_params, tables, backup_dir, logger, schema="public"):
    """Extract DDL — try pg_dump first, fallback to pg_catalog."""
    ddl = extract_ddl_pgdump(conn_params, tables, backup_dir, logger, schema=schema)
    if ddl is None:
        ddl = extract_ddl_fallback(conn, tables, backup_dir, logger, schema=schema)
    return ddl


def export_table_data(conn_params, table, backup_dir, logger, schema="public"):
    """Export a single table's data to CSV using COPY TO STDOUT."""
    start = time.time()
    csv_path = os.path.join(backup_dir, f"{table}.csv")
    row_count = 0

    try:
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Get row count
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(schema, table)
                ))
                row_count = cur.fetchone()[0]

                # Export via COPY
                with cur.copy(sql.SQL("COPY {} TO STDOUT WITH (FORMAT CSV, HEADER, ENCODING 'UTF8')").format(
                    sql.Identifier(schema, table)
                )) as copy:
                    with open(csv_path, "wb") as f:
                        for data in copy:
                            f.write(data)

        elapsed = time.time() - start
        logger.info(f"Exportando: {table} ({row_count} linhas) ... OK [{elapsed:.1f}s]")
        return table, row_count, None
    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"Exportando: {table} ... ERRO [{elapsed:.1f}s]: {e}")
        return table, 0, str(e)


def export_all_data(conn_params, tables, backup_dir, logger, workers=4, schema="public"):
    """Export all tables in parallel using ThreadPoolExecutor."""
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(export_table_data, conn_params, table, backup_dir, logger, schema=schema): table
            for table in tables
        }
        for future in as_completed(futures):
            results.append(future.result())
    return results


def export_sequences(conn, backup_dir, logger, schema="public"):
    """Export sequence states to sequences.sql."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sequencename, last_value, is_called
            FROM pg_sequences
            WHERE schemaname = %s
        """, (schema,))
        sequences = cur.fetchall()

    if not sequences:
        logger.info("Nenhuma sequence encontrada")
        return

    seq_path = os.path.join(backup_dir, "sequences.sql")
    with open(seq_path, "w", encoding="utf-8") as f:
        for seq_name, last_value, is_called in sequences:
            if last_value is not None:
                is_called_str = "true" if is_called else "false"
                f.write(f"SELECT setval('\"{schema}\".\"{seq_name}\"', {last_value}, {is_called_str});\n")

    logger.info(f"{len(sequences)} sequences exportadas")
