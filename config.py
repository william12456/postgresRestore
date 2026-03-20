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
