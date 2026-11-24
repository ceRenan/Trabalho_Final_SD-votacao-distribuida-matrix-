"""
Cliente do Protocolo Matrix - Client-Server API v3 (src/matrix_client.py)

Implementa diretamente (sem SDK) os endpoints da especificacao Matrix usados no projeto:
- POST /_matrix/client/v3/login            (login por senha, com refresh token)
- POST /_matrix/client/v3/refresh          (renovacao de token - exigido pelo matrix.org/MAS)
- GET  /_matrix/client/v3/account/whoami
- POST /_matrix/client/v3/createRoom, /join/{room}, /rooms/{room}/invite
- GET  /_matrix/client/v3/sync             (long-polling com cursor next_batch)
- PUT  /_matrix/client/v3/rooms/{room}/send/{type}/{txnId}     (eventos de timeline)
- PUT  /_matrix/client/v3/rooms/{room}/state/{type}/{stateKey} (state events)
- GET  /_matrix/client/v3/rooms/{room}/state[/{type}/{stateKey}]

Tolerancia a falhas de rede:
- Toda requisicao passa por `_request`, que faz retry com backoff em erros de rede/5xx
  e respeita `retry_after_ms` em HTTP 429 (rate limit do homeserver).
- Como o envio de eventos usa PUT com txnId FIXO entre as tentativas, o retry e
  IDEMPOTENTE: o homeserver devolve o mesmo event_id e nao duplica o evento no DAG.
"""

import json
import time
import uuid
import logging
import urllib.parse
import requests

logger = logging.getLogger("MatrixClient")


class MatrixAuthError(Exception):
    pass


class MatrixClient:
    CUSTOM_EVENT_VOTE = "br.ufc.dc.sd.poll.vote"
    STATE_EVENT_POLL = "br.ufc.dc.sd.poll.state"

    # Enquetes nativas do Element (MSC3381). Element ainda usa o prefixo instavel.
    POLL_START_TYPES = ("org.matrix.msc3381.poll.start", "m.poll.start")
    POLL_RESPONSE_TYPES = ("org.matrix.msc3381.poll.response", "m.poll.response")
    POLL_END_TYPE = "org.matrix.msc3381.poll.end"

    def __init__(
        self,
        homeserver_url: str,
        user_id: str | None = None,
        access_token: str | None = None,
        max_retries: int = 4,
    ):
        self.homeserver_url = homeserver_url.rstrip("/")
        self.user_id = user_id
        self.access_token = access_token
        self.refresh_token: str | None = None
        self.device_id: str | None = None
        self.next_batch: str | None = None
        self.max_retries = max_retries
        self.session = requests.Session()
        # Credenciais guardadas apenas em memoria para relogin automatico se o token expirar
        self._password: str | None = None
        self._username: str | None = None
        # Callback opcional chamado quando os tokens mudam (para persistir em disco)
        self.on_tokens_changed = None

    # ------------------------------------------------------------------ infra
    def _headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    @staticmethod
    def _encode(segment: str) -> str:
        return urllib.parse.quote(segment, safe="")

    def _url(self, path: str) -> str:
        return f"{self.homeserver_url}/_matrix/client/v3{path}"

    def _request(self, method: str, path: str, *, json_body=None, params=None,
                 timeout: float = 20.0, auth_retry: bool = True) -> dict:
        """
        Executa a requisicao HTTP com:
        - retry + backoff exponencial para falhas de rede e HTTP 5xx;
        - espera de `retry_after_ms` em HTTP 429 (M_LIMIT_EXCEEDED);
        - renovacao automatica do token em HTTP 401 (M_UNKNOWN_TOKEN).
        """
        url = self._url(path)
        delay = 0.5
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.session.request(
                    method, url, json=json_body, params=params,
                    headers=self._headers(), timeout=timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                logger.warning("Falha de rede em %s %s (tentativa %d): %s", method, path, attempt + 1, exc)
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue

            if resp.status_code == 429:
                try:
                    wait_ms = resp.json().get("retry_after_ms", 1000)
                except ValueError:
                    wait_ms = 1000
                logger.warning("Rate limit (429) do homeserver. Aguardando %d ms...", wait_ms)
                time.sleep(wait_ms / 1000.0)
                continue

            if resp.status_code >= 500:
                last_exc = requests.HTTPError(f"{resp.status_code} {resp.text[:200]}", response=resp)
                logger.warning("Erro %d do servidor em %s (tentativa %d). Retry...", resp.status_code, path, attempt + 1)
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue

            if resp.status_code == 401 and auth_retry and self._try_reauthenticate():
                return self._request(method, path, json_body=json_body, params=params,
                                     timeout=timeout, auth_retry=False)

            if resp.status_code >= 400:
                try:
                    err = resp.json()
                except ValueError:
                    err = {"error": resp.text[:200]}
                raise requests.HTTPError(
                    f"HTTP {resp.status_code} em {method} {path}: {err.get('errcode', '')} {err.get('error', '')}",
                    response=resp,
                )
            return resp.json() if resp.content else {}

        raise last_exc or RuntimeError(f"Falha apos {self.max_retries + 1} tentativas: {method} {path}")

    # ------------------------------------------------------------- autenticacao
    def _set_tokens(self, data: dict):
        self.access_token = data["access_token"]
        if data.get("refresh_token"):
            self.refresh_token = data["refresh_token"]
        if data.get("device_id"):
            self.device_id = data["device_id"]
        if data.get("user_id"):
            self.user_id = data["user_id"]
        if callable(self.on_tokens_changed):
            self.on_tokens_changed(self)

    def login(self, username: str, password: str, device_id: str | None = None) -> dict:
        """
        Login por senha (m.login.password) pedindo refresh token.
        Reutilizar o mesmo device_id evita criar um 'dispositivo' novo a cada execucao.
        """
        payload = {
            "type": "m.login.password",
            "identifier": {"type": "m.id.user", "user": username},
            "password": password,
            "refresh_token": True,
            "initial_device_display_name": "Bot Apurador SD",
        }
        if device_id or self.device_id:
            payload["device_id"] = device_id or self.device_id
        self.access_token = None
        data = self._request("POST", "/login", json_body=payload, auth_retry=False)
        self._username, self._password = username, password
        self._set_tokens(data)
        return data

    def refresh(self) -> dict:
        """POST /_matrix/client/v3/refresh - troca o refresh token por um novo par de tokens."""
        if not self.refresh_token:
            raise MatrixAuthError("Sem refresh token disponivel.")
        saved = self.access_token
        self.access_token = None
        try:
            data = self._request("POST", "/refresh", json_body={"refresh_token": self.refresh_token},
                                 auth_retry=False)
        except Exception:
            self.access_token = saved
            raise
        self._set_tokens(data)
        return data

    def _try_reauthenticate(self) -> bool:
        """Chamado em HTTP 401: tenta refresh token e, se falhar, relogin com a senha em memoria."""
        if self.refresh_token:
            try:
                self.refresh()
                logger.info("Access token renovado via /refresh.")
                return True
            except Exception as exc:
                logger.warning("Falha no /refresh: %s", exc)
        if self._username and self._password:
            try:
                self.login(self._username, self._password, device_id=self.device_id)
                logger.info("Sessao restabelecida via novo login (mesmo device_id).")
                return True
            except Exception as exc:
                logger.error("Falha no relogin: %s", exc)
        return False

    def whoami(self) -> dict:
        data = self._request("GET", "/account/whoami")
        if data.get("user_id"):
            self.user_id = data["user_id"]
        if data.get("device_id"):
            self.device_id = data["device_id"]
        return data

    # ------------------------------------------------------------------ salas
    def create_room(self, name: str, invite: list[str] | None = None, topic: str = "") -> str:
        """
        Cria uma sala SEM criptografia. Quem cria vira administrador (power level 100),
        portanto o bot pode gravar State Events sem precisar ser promovido.
        """
        body = {
            "name": name,
            "topic": topic,
            "preset": "private_chat",
            "invite": invite or [],
            "is_direct": False,
        }
        return self._request("POST", "/createRoom", json_body=body)["room_id"]

    def invite_user(self, room_id: str, user_id: str) -> dict:
        return self._request("POST", f"/rooms/{self._encode(room_id)}/invite", json_body={"user_id": user_id})

    def join_room(self, room_id_or_alias: str) -> dict:
        return self._request("POST", f"/join/{self._encode(room_id_or_alias)}", json_body={})

    def sync(self, timeout_ms: int = 3000, since: str | None = None, timeline_limit: int | None = None) -> dict:
        """
        Sincronizacao incremental via HTTP long-polling.
        O cursor `next_batch` garante que cada chamada receba apenas eventos novos, em ordem.
        """
        params = {"timeout": timeout_ms}
        token = since if since is not None else self.next_batch
        if token:
            params["since"] = token
        if timeline_limit:
            params["filter"] = json.dumps({"room": {"timeline": {"limit": timeline_limit}}})
        data = self._request("GET", "/sync", params=params, timeout=(timeout_ms / 1000.0) + 20.0)
        self.next_batch = data.get("next_batch")
        return data

    # ---------------------------------------------------------------- eventos
    @staticmethod
    def new_txn_id() -> str:
        return f"txn_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

    def send_event(self, room_id: str, event_type: str, content: dict, txn_id: str | None = None) -> dict:
        """
        PUT /rooms/{roomId}/send/{eventType}/{txnId}
        O txnId e gerado UMA vez e reutilizado em todos os retries de `_request` -> idempotencia.
        """
        txn = txn_id or self.new_txn_id()
        path = f"/rooms/{self._encode(room_id)}/send/{self._encode(event_type)}/{self._encode(txn)}"
        return self._request("PUT", path, json_body=content)

    def send_text_message(self, room_id: str, body: str, formatted_body: str | None = None) -> dict:
        content = {"msgtype": "m.text", "body": body}
        if formatted_body:
            content["format"] = "org.matrix.custom.html"
            content["formatted_body"] = formatted_body
        return self.send_event(room_id, "m.room.message", content)

    def send_vote_event(self, room_id: str, poll_id: str, option_index: int,
                        voter_override: str | None = None, origin_event_id: str | None = None,
                        source: str = "custom_event", txn_id: str | None = None) -> dict:
        """Publica o evento personalizado de voto `br.ufc.dc.sd.poll.vote` no DAG da sala."""
        payload = {
            "poll_id": poll_id,
            "option_index": int(option_index),
            "client_ts": int(time.time() * 1000),
            "source": source,
        }
        if voter_override:
            payload["voter"] = voter_override
        if origin_event_id:
            payload["origin_event_id"] = origin_event_id
        return self.send_event(room_id, self.CUSTOM_EVENT_VOTE, payload, txn_id=txn_id)

    def send_native_poll_start(self, room_id: str, question: str, options: list[str]) -> dict:
        """Cria uma enquete nativa do Element (botoes clicaveis) - MSC3381."""
        fallback = question + "\n" + "\n".join(f"{i + 1}. {o}" for i, o in enumerate(options))
        content = {
            "org.matrix.msc3381.poll.start": {
                "question": {"org.matrix.msc1767.text": question},
                "kind": "org.matrix.msc3381.poll.disclosed",
                "max_selections": 1,
                "answers": [
                    {"id": str(i + 1), "org.matrix.msc1767.text": opt} for i, opt in enumerate(options)
                ],
            },
            "org.matrix.msc1767.text": fallback,
        }
        return self.send_event(room_id, self.POLL_START_TYPES[0], content)

    def send_native_poll_end(self, room_id: str, start_event_id: str, summary: str) -> dict:
        content = {
            "m.relates_to": {"rel_type": "m.reference", "event_id": start_event_id},
            "org.matrix.msc3381.poll.end": {},
            "org.matrix.msc1767.text": summary,
        }
        return self.send_event(room_id, self.POLL_END_TYPE, content)

    def put_state_event(self, room_id: str, event_type: str, state_key: str, content: dict) -> dict:
        """PUT /rooms/{roomId}/state/{eventType}/{stateKey} - grava/atualiza estado replicado."""
        path = f"/rooms/{self._encode(room_id)}/state/{self._encode(event_type)}/{self._encode(state_key)}"
        return self._request("PUT", path, json_body=content)

    def get_room_state(self, room_id: str) -> list[dict]:
        """GET /rooms/{roomId}/state - todos os state events atuais (usado no crash recovery)."""
        return self._request("GET", f"/rooms/{self._encode(room_id)}/state")

    def get_state_event(self, room_id: str, event_type: str, state_key: str = "") -> dict:
        path = f"/rooms/{self._encode(room_id)}/state/{self._encode(event_type)}/{self._encode(state_key)}"
        return self._request("GET", path)

    def can_send_state(self, room_id: str, event_type: str) -> tuple[bool, int, int]:
        """Verifica em m.room.power_levels se este usuario pode enviar o state event."""
        pl = self.get_state_event(room_id, "m.room.power_levels")
        my_level = pl.get("users", {}).get(self.user_id, pl.get("users_default", 0))
        required = pl.get("events", {}).get(event_type, pl.get("state_default", 50))
        return my_level >= required, my_level, required

