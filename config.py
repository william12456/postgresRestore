import os
import sys
from urllib.parse import urlparse

from dotenv import load_dotenv


def _parse_url(url):
    """Parse a DATABASE_URL into individual connection fields."""
    parsed = urlparse(url)
    params = dict(p.split("=", 1) for p in parsed.query.split("&") if "=" in p) if parsed.query else {}
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/") if parsed.path else "",
        "user": parsed.username or "",
        "password": parsed.password or "",
        "sslmode": params.get("sslmode", "require"),
        "conninfo": url,
    }


def load_config():
    """Load and validate database configuration from .env file."""
    load_dotenv()

    # Support DATABASE_URL style or individual vars
    remote_url = os.getenv("REMOTE_DATABASE_URL")
    if remote_url:
        remote = _parse_url(remote_url)
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
        local = _parse_url(local_url)
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

    remote_schema = os.getenv("REMOTE_DB_SCHEMA", "public")
    local_schema = os.getenv("LOCAL_DB_SCHEMA", "public")

    return {"remote": remote, "local": local, "remote_schema": remote_schema, "local_schema": local_schema}
