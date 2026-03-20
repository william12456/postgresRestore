# Postgres Restore — Design Spec

## Overview

Ferramenta CLI em Python que conecta a um PostgreSQL remoto, descobre todas as tabelas do schema `public`, exporta schema (DDL) e dados (CSV via `COPY`), salva em pasta local com timestamp, e restaura tudo em um PostgreSQL local — dropando e recriando as tabelas antes da importação.

## Goals

- **Performance:** usar `COPY TO/FROM` do Postgres (streaming, sem carga em memória)
- **Confiabilidade:** retry com backoff, falha parcial não interrompe execução
- **Segurança:** conexão SSL, validação de credenciais, proteção contra drop acidental
- **Organização:** backups em pastas nomeadas com data/hora
- **Visibilidade:** logging completo em arquivo + terminal

## Architecture

```
┌─────────────┐      CSV files        ┌─────────────┐
│  Postgres   │  ──── COPY TO ────►   │   Pasta     │
│   Remoto    │                       │  backup_    │
│  (SSL)      │                       │  YYYY-MM-DD │
└─────────────┘                       │  _HH-MM-SS  │
                                      └──────┬──────┘
                                             │
                                      COPY FROM
                                             │
                                      ┌──────▼──────┐
                                      │  Postgres   │
                                      │    Local    │
                                      └─────────────┘
```

### Flow

1. Conecta ao banco remoto (com SSL)
2. Lista todas as tabelas do schema `public`
3. Analisa dependências entre tabelas (foreign keys) via `pg_catalog`
4. Para cada tabela: extrai DDL via `pg_dump --schema-only` (fallback: `pg_catalog`)
5. Exporta dados via `COPY TO STDOUT` → salva como CSV (UTF-8) na pasta `backups/backup_YYYY-MM-DD_HH-MM-SS/`
6. Exporta estado das sequences (`pg_sequences`: `last_value`, `is_called`)
7. Conecta ao banco local
8. **Safety check:** verifica que host local ≠ host remoto; pede confirmação se `--force` não foi passado
9. Dentro de uma **transação única**: dropa tabelas com `CASCADE`, recria schema, importa dados via `COPY FROM`, restaura sequences
10. Commit da transação (ou rollback em caso de erro)
11. Gera log completo (arquivo `restore.log` + terminal)

### Parallelism

- `concurrent.futures.ThreadPoolExecutor` com número de workers configurável (padrão: 4)
- Cada worker usa sua própria conexão ao banco
- Paralelismo aplicado na fase de **export** (leitura do remoto)
- A fase de **import** é sequencial dentro de uma transação única, com FK constraints desabilitadas via `SET session_replication_role = 'replica'` para permitir inserção em qualquer ordem
- Ao final do import, restaura `session_replication_role = 'DEFAULT'` e reabilita constraints

## Project Structure

```
postgresRestore/
├── .env                  # credenciais (git-ignored)
├── .env.example          # template de credenciais
├── .gitignore
├── requirements.txt      # psycopg[binary], python-dotenv
├── main.py               # entry point (CLI com argparse)
├── config.py             # carrega .env e valida
├── exporter.py           # COPY TO + DDL extraction
├── importer.py           # DROP + CREATE + COPY FROM
├── logger_setup.py       # configuração de logging
└── backups/              # pasta raiz dos backups (git-ignored)
    └── backup_2026-03-20_14-30-00/
        ├── schema.sql    # DDL completo
        ├── sequences.sql # estado das sequences
        ├── tabela1.csv
        ├── tabela2.csv
        └── restore.log
```

## Configuration

### .env Variables

```env
# Remote database
REMOTE_DB_HOST=
REMOTE_DB_PORT=5432
REMOTE_DB_NAME=
REMOTE_DB_USER=
REMOTE_DB_PASSWORD=

# Local database
LOCAL_DB_HOST=localhost
LOCAL_DB_PORT=5432
LOCAL_DB_NAME=
LOCAL_DB_USER=
LOCAL_DB_PASSWORD=
```

Conexões suportam também `DATABASE_URL` style: `REMOTE_DATABASE_URL=postgresql://user:pass@host:port/db?sslmode=require`

### CLI Arguments

```
python main.py [--workers N] [--backup-dir PATH] [--dry-run] [--force]
```

- `--workers`: número de workers paralelos (padrão: 4)
- `--backup-dir`: diretório raiz dos backups (padrão: `./backups`)
- `--dry-run`: mostra o que seria feito sem executar
- `--force`: pula confirmação de segurança antes do drop

## Data Export (exporter.py)

### Table Discovery

```sql
SELECT tablename FROM pg_tables WHERE schemaname = 'public';
```

### Dependency Analysis

Consulta `pg_catalog.pg_constraint` para mapear foreign keys e determinar ordem topológica de criação/drop das tabelas.

### DDL Extraction

Estratégia primária: `pg_dump --schema-only -t public.tabela` via subprocess, passando credenciais via variáveis de ambiente (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`).

Fallback (se `pg_dump` não instalado): construir DDL via `pg_catalog` (colunas, tipos, defaults, constraints, indexes). Log de aviso quando fallback é ativo — pode não capturar partial indexes, expression defaults, ou custom types.

### Data Export

Usa `psycopg` v3 (pacote `psycopg`):

```python
with cursor.copy("COPY tablename TO STDOUT WITH (FORMAT CSV, HEADER, ENCODING 'UTF8')") as copy:
    with open("tablename.csv", "wb") as f:
        for data in copy:
            f.write(data)
```

- Streaming: não carrega tabela inteira em memória
- Encoding explícito UTF-8

### Sequence State Export

```sql
SELECT sequencename, last_value, is_called
FROM pg_sequences
WHERE schemaname = 'public';
```

Salva em `sequences.sql` com comandos `SELECT setval('seq', val, is_called)`.

## Data Import (importer.py)

### Safety Check

Antes de qualquer operação destrutiva:
1. Compara `REMOTE_DB_HOST` com `LOCAL_DB_HOST` — avisa se iguais
2. Se `--force` não passado, exibe lista de tabelas a serem dropadas e pede confirmação (`y/N`)
3. Se `--dry-run`, imprime o plano e sai

### Transactional Import

Toda a importação roda dentro de **uma transação**:

```python
conn.autocommit = False
try:
    # Desabilita FK checks
    cursor.execute("SET session_replication_role = 'replica'")

    # Drop tables (CASCADE)
    for table in reverse_topological_order:
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    # Create tables (DDL)
    cursor.execute(schema_sql)

    # Import data
    for table in tables:
        with cursor.copy(f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
            with open(f"{table}.csv", "rb") as f:
                while data := f.read(8192):
                    copy.write(data)

    # Restore sequences
    cursor.execute(sequences_sql)

    # Reabilita FK checks
    cursor.execute("SET session_replication_role = 'DEFAULT'")

    conn.commit()
except Exception:
    conn.rollback()
    raise
```

Se qualquer etapa falhar, rollback garante que o banco local não fica em estado inconsistente.

## Error Handling

| Cenário | Comportamento |
|---------|---------------|
| Tabela vazia | Exporta CSV com header apenas, importa normalmente |
| Foreign keys | Desabilitadas via `session_replication_role` durante import |
| Sequences | Exporta `last_value` + `is_called`, restaura com `setval(seq, val, is_called)` |
| Tipos especiais (JSON, arrays, bytea) | `COPY` CSV do Postgres lida nativamente |
| Falha no export de uma tabela | Loga erro, continua com próximas, reporta no final |
| Falha no import | Rollback da transação inteira, banco local preservado |
| Conexão perdida | Retry com backoff exponencial (1s, 2s, 4s — até 3 tentativas) para conexões; COPY operations não são retried |
| `pg_dump` não instalado | Fallback para DDL via `pg_catalog` (com aviso no log) |
| Host local = host remoto | Aviso + confirmação obrigatória (ou `--force`) |

## Logging

Dual output: arquivo `restore.log` na pasta do backup + terminal.

```
[2026-03-20 14:30:00] INFO  Conectando ao banco remoto (SSL)...
[2026-03-20 14:30:01] INFO  22 tabelas encontradas no schema public
[2026-03-20 14:30:01] INFO  Exportando: users (15230 linhas) ... OK [0.8s]
[2026-03-20 14:30:02] INFO  Exportando: orders (142500 linhas) ... OK [3.2s]
...
[2026-03-20 14:33:00] INFO  Importando para banco local...
[2026-03-20 14:33:00] INFO  Safety check: remoto=prod.example.com, local=localhost ✓
[2026-03-20 14:35:00] INFO  === RESUMO ===
[2026-03-20 14:35:00] INFO  Tabelas copiadas: 22/22
[2026-03-20 14:35:00] INFO  Total de linhas: 523400
[2026-03-20 14:35:00] INFO  Tempo total: 5m00s
[2026-03-20 14:35:00] INFO  Erros: 0
```

## Dependencies

```
psycopg[binary]>=3.1
python-dotenv>=1.0
```

Usa **psycopg v3** (não psycopg2) — API moderna com `cursor.copy()` context manager, melhor performance, e suporte nativo a streaming.

## Scope and Limitations (v1)

- Apenas schema `public`
- Apenas tabelas (não views, materialized views, functions, triggers, extensions)
- Sem filtro por tabela individual (copia todas)
- Sem compressão de backups antigos
- Sem cleanup automático de backups

## Tech Decisions

- **psycopg v3 (não psycopg2/asyncpg):** API moderna com `cursor.copy()` streaming, mais performático, e manutenção ativa
- **CSV com encoding UTF-8 (não binary COPY):** legibilidade dos backups, compatibilidade, performance suficiente
- **ThreadPoolExecutor (não multiprocessing):** I/O-bound workload, threads são adequadas
- **`pg_dump` para DDL com fallback `pg_catalog`:** DDL exato incluindo todos os detalhes, com fallback para independência de binários externos
- **Transação única no import:** garante consistência — tudo ou nada
- **`session_replication_role = 'replica'`:** desabilita FK checks durante import, permitindo inserção em qualquer ordem
