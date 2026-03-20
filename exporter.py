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
