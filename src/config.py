"""
Configuracao e sessao (src/config.py)
- Le o arquivo .env (sem dependencias externas).
- Persiste a sessao Matrix (access/refresh token + device_id) em .matrix_session.json,
  para o bot reaproveitar o mesmo "dispositivo" e renovar tokens entre reinicios.
"""

import json
import os

SESSION_FILE = ".matrix_session.json"


def load_dotenv(path: str = ".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def save_env(values: dict, path: str = ".env"):
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Gerado por setup_real_matrix.py - NAO faca commit deste arquivo\n")
        for k, v in values.items():
            f.write(f"{k}={v}\n")


def load_session(homeserver: str, user: str | None) -> dict | None:
    if not os.path.exists(SESSION_FILE):
        return None
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    if data.get("homeserver") != homeserver:
        return None
    if user and data.get("user_id") and user.startswith("@") and data["user_id"] != user:
        return None
    return data


def save_session(client) -> None:
    data = {
        "homeserver": client.homeserver_url,
        "user_id": client.user_id,
        "device_id": client.device_id,
        "access_token": client.access_token,
        "refresh_token": client.refresh_token,
    }
    with open(SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def build_authenticated_client(homeserver: str, user: str, password: str, token: str):
    """
    Ordem de preferencia:
    1. Sessao salva (.matrix_session.json) -> reaproveita device e refresh token.
    2. Login por senha (pede refresh token; relogin automatico se expirar).
    3. Access token avulso (pode expirar no matrix.org, que usa MAS/OIDC).
    """
    from src.matrix_client import MatrixClient

    client = MatrixClient(homeserver_url=homeserver, user_id=user or None)
    client.on_tokens_changed = save_session
    if password:
        client._username, client._password = user, password  # permite relogin automatico

    session = load_session(homeserver, user)
    if session and session.get("access_token"):
        client.access_token = session["access_token"]
        client.refresh_token = session.get("refresh_token")
        client.device_id = session.get("device_id")
        try:
            client.whoami()
            return client, "sessao salva"
        except Exception:
            client.access_token = None

    if password:
        client.login(user, password, device_id=session.get("device_id") if session else None)
        return client, "login por senha"

    if token:
        client.access_token = token
        client.whoami()
        return client, "access token (pode expirar no matrix.org)"

    raise RuntimeError("Configure MATRIX_PASSWORD ou MATRIX_ACCESS_TOKEN no .env "
                       "(rode `python setup_real_matrix.py`).")

