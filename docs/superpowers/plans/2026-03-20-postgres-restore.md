# Postgres Restore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a CLI tool that copies all tables from a remote PostgreSQL to a local PostgreSQL via CSV files, with full DDL recreation and transactional safety.

**Architecture:** Python CLI using psycopg v3 for streaming COPY TO/FROM. Export phase runs in parallel (ThreadPoolExecutor), import phase runs sequentially in a single transaction with FK constraints disabled. Backups stored in timestamped folders.

**Tech Stack:** Python 3.10+, psycopg[binary] v3, python-dotenv, argparse

---

## File Structure

| File | Responsibility |
|------|---------------|
| `requirements.txt` | Dependencies |
| `.env.example` | Credential template |
| `.gitignore` | Ignore .env, backups/, __pycache__ |
| `config.py` | Load and validate .env, build connection params |
| `logger_setup.py` | Dual logging (file + terminal) |
| `exporter.py` | Connect to remote, discover tables, extract DDL, export CSVs, export sequences |
| `importer.py` | Safety check, transactional DROP+CREATE+COPY FROM+sequences |
| `main.py` | CLI entry point, orchestrate export→import, summary |

---

### Task 1: Project Scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `.gitignore`

- [ ] **Step 1: Create requirements.txt**

```
psycopg[binary]>=3.1
python-dotenv>=1.0
```

- [ ] **Step 2: Create .env.example**

```env
# Remote database (use individual vars OR REMOTE_DATABASE_URL)
# REMOTE_DATABASE_URL=postgresql://user:pass@host:port/db?sslmode=require
REMOTE_DB_HOST=
REMOTE_DB_PORT=5432
REMOTE_DB_NAME=
REMOTE_DB_USER=
REMOTE_DB_PASSWORD=
REMOTE_DB_SSLMODE=require

# Local database (use individual vars OR LOCAL_DATABASE_URL)
# LOCAL_DATABASE_URL=postgresql://user:pass@localhost:5432/db
LOCAL_DB_HOST=localhost
LOCAL_DB_PORT=5432
LOCAL_DB_NAME=
LOCAL_DB_USER=
LOCAL_DB_PASSWORD=
```

- [ ] **Step 3: Create .gitignore**

```
.env
backups/
__pycache__/
*.pyc
```

- [ ] **Step 4: Commit**

```bash
git add requirements.txt .env.example .gitignore
git commit -m "chore: project scaffolding with dependencies and config template"
```

---

### Task 2: Configuration Module

**Files:**
- Create: `config.py`

- [ ] **Step 1: Implement config.py**

```python
import os
import sys
from dotenv import load_dotenv


def load_config():
    """Load and validate database configuration from .env file."""
    load_dotenv()

    # Support DATABASE_URL style or individual vars
    remote_url = os.getenv("REMOTE_DATABASE_URL")
    if remote_url:
        remote = {"conninfo": remote_url}
    else:
        remote = {
            "host": os.getenv("REMOTE_DB_HOST"),
            "port": int(os.getenv("REMOTE_DB_PORT", "5432")),
            "dbname": os.getenv("REMOTE_DB_NAME"),
            "user": os.getenv("REMOTE_DB_USER"),
            "password": os.getenv("REMOTE_DB_PASSWORD"),
            "sslmode": os.getenv("REMOTE_DB_SSLMODE", "require"),
        }

    local_url = os.getenv("LOCAL_DATABASE_URL")
    if local_url:
        local = {"conninfo": local_url}
    else:
        local = {
            "host": os.getenv("LOCAL_DB_HOST", "localhost"),
            "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
            "dbname": os.getenv("LOCAL_DB_NAME"),
            "user": os.getenv("LOCAL_DB_USER"),
            "password": os.getenv("LOCAL_DB_PASSWORD"),
        }

    # Validate required remote fields
    if "conninfo" not in remote:
        missing = [k for k in ("host", "dbname", "user", "password") if not remote[k]]
        if missing:
            print(f"ERRO: Variáveis de ambiente faltando para banco remoto: {', '.join('REMOTE_DB_' + k.upper() for k in missing)}")
            sys.exit(1)

    # Validate required local fields
    if "conninfo" not in local:
        missing = [k for k in ("dbname", "user", "password") if not local[k]]
        if missing:
            print(f"ERRO: Variáveis de ambiente faltando para banco local: {', '.join('LOCAL_DB_' + k.upper() for k in missing)}")
            sys.exit(1)

    return {"remote": remote, "local": local}
```

- [ ] **Step 2: Commit**

```bash
git add config.py
git commit -m "feat: add configuration module with .env loading and validation"
```

---

### Task 3: Logger Setup

**Files:**
- Create: `logger_setup.py`

- [ ] **Step 1: Implement logger_setup.py**

```python
import logging
import os


def setup_logger(backup_dir):
    """Configure dual logging: file (in backup_dir/restore.log) + terminal."""
    logger = logging.getLogger("pg_restore")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Terminal handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    log_path = os.path.join(backup_dir, "restore.log")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
```

- [ ] **Step 2: Commit**

```bash
git add logger_setup.py
git commit -m "feat: add dual logging setup (file + terminal)"
```

---

### Task 4: Exporter — Table Discovery and Dependency Analysis

**Files:**
- Create: `exporter.py`

- [ ] **Step 1: Implement table discovery and dependency analysis**

```python
import os
import shutil
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


def discover_tables(conn, logger):
    """List all tables in the public schema."""
    with conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
        tables = [row[0] for row in cur.fetchall()]
    logger.info(f"{len(tables)} tabelas encontradas no schema public")
    return tables


def get_dependency_order(conn, tables):
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
                AND tc.table_schema = 'public'
                AND tc.table_name != ccu.table_name
        """)
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
```

- [ ] **Step 2: Commit**

```bash
git add exporter.py
git commit -m "feat: add table discovery and dependency analysis"
```

---

### Task 5: Exporter — DDL Extraction

**Files:**
- Modify: `exporter.py`

- [ ] **Step 1: Add DDL extraction functions to exporter.py**

Append to `exporter.py`:

```python
def extract_ddl_pgdump(conn_params, tables, backup_dir, logger):
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
                ["pg_dump", "--schema-only", "--no-owner", "--no-privileges", "-t", f"public.{table}"],
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


def extract_ddl_fallback(conn, tables, backup_dir, logger):
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
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table,))
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

            ddl = f'CREATE TABLE IF NOT EXISTS "public"."{table}" (\n'
            ddl += ",\n".join(col_defs)

            # Get primary key
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = 'public'
                    AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """, (table,))
            pk_cols = [row[0] for row in cur.fetchall()]
            if pk_cols:
                ddl += f',\n    PRIMARY KEY ({", ".join(f"{c}" for c in pk_cols)})'

            ddl += "\n);\n"

            # Get indexes (non-primary)
            cur.execute("""
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = 'public' AND tablename = %s
                AND indexname NOT IN (
                    SELECT constraint_name FROM information_schema.table_constraints
                    WHERE table_schema = 'public' AND table_name = %s AND constraint_type = 'PRIMARY KEY'
                )
            """, (table, table))
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
                    AND tc.table_schema = 'public'
                    AND tc.table_name = %s
            """, (table,))
            for constraint_name, tbl, col, ref_tbl, ref_col in cur.fetchall():
                fk_ddl.append(
                    f'ALTER TABLE "public"."{tbl}" ADD CONSTRAINT "{constraint_name}" '
                    f'FOREIGN KEY ("{col}") REFERENCES "public"."{ref_tbl}" ("{ref_col}");'
                )

    if fk_ddl:
        all_ddl.append("-- Foreign Keys\n" + "\n".join(fk_ddl))

    ddl = "\n\n".join(all_ddl)
    schema_path = os.path.join(backup_dir, "schema.sql")
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write(ddl)
    logger.info(f"DDL extraído via pg_catalog fallback ({len(tables)} tabelas)")
    return ddl


def extract_ddl(conn, conn_params, tables, backup_dir, logger):
    """Extract DDL — try pg_dump first, fallback to pg_catalog."""
    ddl = extract_ddl_pgdump(conn_params, tables, backup_dir, logger)
    if ddl is None:
        ddl = extract_ddl_fallback(conn, tables, backup_dir, logger)
    return ddl
```

- [ ] **Step 2: Commit**

```bash
git add exporter.py
git commit -m "feat: add DDL extraction with pg_dump + pg_catalog fallback"
```

---

### Task 6: Exporter — Data Export and Sequence State

**Files:**
- Modify: `exporter.py`

- [ ] **Step 1: Add data export and sequence functions to exporter.py**

Append to `exporter.py`:

```python
def export_table_data(conn_params, table, backup_dir, logger):
    """Export a single table's data to CSV using COPY TO STDOUT."""
    start = time.time()
    csv_path = os.path.join(backup_dir, f"{table}.csv")
    row_count = 0

    try:
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Get row count (approximate, fast)
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier("public", table)
                ))
                row_count = cur.fetchone()[0]

                # Export via COPY
                with cur.copy(sql.SQL("COPY {} TO STDOUT WITH (FORMAT CSV, HEADER, ENCODING 'UTF8')").format(
                    sql.Identifier("public", table)
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


def export_all_data(conn_params, tables, backup_dir, logger, workers=4):
    """Export all tables in parallel using ThreadPoolExecutor."""
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(export_table_data, conn_params, table, backup_dir, logger): table
            for table in tables
        }
        for future in as_completed(futures):
            results.append(future.result())
    return results


def export_sequences(conn, backup_dir, logger):
    """Export sequence states to sequences.sql."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sequencename, last_value, is_called
            FROM pg_sequences
            WHERE schemaname = 'public'
        """)
        sequences = cur.fetchall()

    if not sequences:
        logger.info("Nenhuma sequence encontrada")
        return

    seq_path = os.path.join(backup_dir, "sequences.sql")
    with open(seq_path, "w", encoding="utf-8") as f:
        for seq_name, last_value, is_called in sequences:
            if last_value is not None:
                is_called_str = "true" if is_called else "false"
                f.write(f"SELECT setval('\"public\".\"{seq_name}\"', {last_value}, {is_called_str});\n")

    logger.info(f"{len(sequences)} sequences exportadas")
```

- [ ] **Step 2: Commit**

```bash
git add exporter.py
git commit -m "feat: add parallel data export and sequence state export"
```

---

### Task 7: Importer — Safety Check and Transactional Import

**Files:**
- Create: `importer.py`

- [ ] **Step 1: Implement importer.py**

```python
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


def run_import(local_config, tables, backup_dir, logger):
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
                    sql.Identifier("public", table)
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
                    sql.Identifier("public", table)
                )) as copy:
                    with open(csv_path, "rb") as f:
                        while data := f.read(8192):
                            copy.write(data)

                # Count imported rows
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier("public", table)
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
```

- [ ] **Step 2: Commit**

```bash
git add importer.py
git commit -m "feat: add transactional importer with safety checks and rollback"
```

---

### Task 8: Main Entry Point — CLI Orchestration

**Files:**
- Create: `main.py`

- [ ] **Step 1: Implement main.py**

```python
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

    logger.info("=== POSTGRES RESTORE — INICIANDO ===")

    # --- EXPORT PHASE ---
    logger.info("Conectando ao banco remoto...")
    try:
        remote_conn = connect_with_retry(config["remote"], logger)
    except Exception as e:
        logger.error(f"Falha ao conectar ao banco remoto: {e}")
        return 1

    try:
        # Discover tables
        tables = discover_tables(remote_conn, logger)
        if not tables:
            logger.warning("Nenhuma tabela encontrada no schema public")
            return 0

        # Dependency order
        ordered_tables = get_dependency_order(remote_conn, tables)
        logger.info(f"Ordem de dependência calculada para {len(ordered_tables)} tabelas")

        # Extract DDL
        logger.info("Extraindo DDL...")
        ddl = extract_ddl(remote_conn, config["remote"], ordered_tables, backup_dir, logger)

        # Export sequences
        export_sequences(remote_conn, backup_dir, logger)
    finally:
        remote_conn.close()

    # Export data (parallel)
    logger.info(f"Exportando dados ({args.workers} workers)...")
    export_results = export_all_data(config["remote"], ordered_tables, backup_dir, logger, workers=args.workers)

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
        total_imported = run_import(config["local"], exportable_tables, backup_dir, logger)
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
```

- [ ] **Step 2: Commit**

```bash
git add main.py
git commit -m "feat: add CLI entry point with export/import orchestration and summary"
```

---

### Task 9: Install Dependencies and Test Run

- [ ] **Step 1: Create virtual environment and install dependencies**

```bash
python -m venv venv
source venv/Scripts/activate  # Windows Git Bash
pip install -r requirements.txt
```

- [ ] **Step 2: Test dry-run mode (without real database)**

```bash
python main.py --dry-run
```

Expected: should fail gracefully with a message about missing .env variables.

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "fix: adjustments from test run"
```
