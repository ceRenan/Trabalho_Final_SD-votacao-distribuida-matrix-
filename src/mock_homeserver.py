"""
Homeserver Matrix Local (Compativel com Matrix Client-Server API v3) (src/mock_homeserver.py)
Permite executar, testar e demonstrar o sistema distribuido 100% offline/localmente
sem depender de Docker ou contas externas, implementando fielmente os endpoints da
especificacao Matrix:
- GET  /_matrix/client/versions
- POST /_matrix/client/v3/login
- POST /_matrix/client/v3/createRoom
- POST /_matrix/client/v3/join/<roomId>
- GET  /_matrix/client/v3/sync
- PUT  /_matrix/client/v3/rooms/<roomId>/send/<eventType>/<txnId>
- PUT  /_matrix/client/v3/rooms/<roomId>/state/<eventType>/<stateKey>
- PUT  /_matrix/client/v3/rooms/<roomId>/state/<eventType>
- GET  /_matrix/client/v3/rooms/<roomId>/state
"""

import time
import uuid
import threading
from flask import Flask, request, jsonify


class InMemoryMatrixServer:
    def __init__(self, server_name: str = "localhost"):
        self.server_name = server_name
        self.lock = threading.Condition()
        self.tokens: dict[str, str] = {}  # token -> user_id
        self.rooms: dict[str, dict] = {}
        self.txn_cache: dict[tuple[str, str, str], str] = {}  # (user_id, room_id, txn_id) -> event_id
        self.global_stream_id: int = 0
        # Quantas respostas de /send devem "se perder" apos o commit (teste de idempotencia)
        self.fail_after_commit: int = 0

        # Cria uma sala padrao para facilitar testes rapidos
        self.default_room_id = f"!monitoramento:{self.server_name}"
        self._ensure_room(self.default_room_id)

    def _ensure_room(self, room_id: str) -> dict:
        if room_id not in self.rooms:
            self.rooms[room_id] = {
                "room_id": room_id,
                "members": set(),
                "timeline": [],  # lista ordenada de dicts (eventos com stream_id)
                "state": {},     # (event_type, state_key) -> event dict
            }
        return self.rooms[room_id]

    def authenticate(self, auth_header: str | None) -> str:
        if not auth_header or not auth_header.startswith("Bearer "):
            return f"@anon:{self.server_name}"
        token = auth_header.removeprefix("Bearer ").strip()
        return self.tokens.get(token, f"@user:{self.server_name}")

    def create_token(self, username: str) -> tuple[str, str]:
        user_id = username if username.startswith("@") else f"@{username}:{self.server_name}"
        token = f"syt_local_{uuid.uuid4().hex[:16]}"
        self.tokens[token] = user_id
        return user_id, token

    def append_timeline_event(
        self,
        room_id: str,
        sender: str,
        event_type: str,
        content: dict,
        txn_id: str | None = None,
        state_key: str | None = None,
    ) -> str:
        with self.lock:
            room = self._ensure_room(room_id)
            room["members"].add(sender)

            if txn_id:
                cache_key = (sender, room_id, txn_id)
                if cache_key in self.txn_cache:
                    return self.txn_cache[cache_key]

            self.global_stream_id += 1
            event_id = f"$ev_{self.global_stream_id}_{uuid.uuid4().hex[:8]}:{self.server_name}"

            event = {
                "event_id": event_id,
                "type": event_type,
                "sender": sender,
                "room_id": room_id,
                "origin_server_ts": int(time.time() * 1000),
                "content": content,
                "_stream_id": self.global_stream_id,
            }
            if state_key is not None:
                event["state_key"] = state_key
                room["state"][(event_type, state_key)] = event

            room["timeline"].append(event)

            if txn_id:
                self.txn_cache[(sender, room_id, txn_id)] = event_id

            self.lock.notify_all()
            return event_id

    def get_sync_payload(self, since_token: str | None, timeout_ms: int) -> dict:
        since_id = 0
        if since_token and since_token.startswith("s"):
            try:
                since_id = int(since_token[1:])
            except ValueError:
                since_id = 0

        deadline = time.time() + (timeout_ms / 1000.0)
        with self.lock:
            while self.global_stream_id <= since_id and timeout_ms > 0:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self.lock.wait(timeout=remaining)

            current_batch = f"s{self.global_stream_id}"
            joined_rooms = {}

            for room_id, room in self.rooms.items():
                new_events = [
                    {k: v for k, v in ev.items() if k != "_stream_id"}
                    for ev in room["timeline"]
                    if ev["_stream_id"] > since_id
                ]
                state_events = [
                    {k: v for k, v in ev.items() if k != "_stream_id"}
                    for ev in room["state"].values()
                ]
                joined_rooms[room_id] = {
                    "timeline": {
                        "events": new_events,
                        "limited": False,
                        "prev_batch": f"s{since_id}",
                    },
                    "state": {
                        "events": state_events,
                    },
                }

            return {
                "next_batch": current_batch,
                "rooms": {
                    "join": joined_rooms,
                    "invite": {},
                    "leave": {},
                },
            }


def create_mock_homeserver_app(server_name: str = "localhost") -> tuple[Flask, InMemoryMatrixServer]:
    app = Flask(__name__)
    state = InMemoryMatrixServer(server_name=server_name)

    @app.get("/_matrix/client/versions")
    def get_versions():
        return jsonify({
            "versions": ["v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6"],
            "unstable_features": {},
        })

    @app.get("/_matrix/client/v3/account/whoami")
    def whoami():
        sender = state.authenticate(request.headers.get("Authorization"))
        return jsonify({"user_id": sender, "device_id": "LOCAL_MATRIX_DEVICE"})

    @app.post("/_matrix/client/v3/login")
    def login():
        data = request.get_json(silent=True) or {}
        ident = data.get("identifier", {})
        user = ident.get("user") or data.get("user") or "bot"
        user_id, token = state.create_token(user)
        return jsonify({
            "user_id": user_id,
            "access_token": token,
            "home_server": state.server_name,
            "device_id": "LOCAL_MATRIX_DEVICE",
        })

    @app.post("/_matrix/client/v3/createRoom")
    def create_room():
        sender = state.authenticate(request.headers.get("Authorization"))
        data = request.get_json(silent=True) or {}
        alias = data.get("room_alias_name", f"room_{uuid.uuid4().hex[:6]}")
        room_id = f"!{alias}:{state.server_name}"
        room = state._ensure_room(room_id)
        room["members"].add(sender)
        return jsonify({"room_id": room_id})

    @app.post("/_matrix/client/v3/join/<path:room_id_or_alias>")
    def join_room(room_id_or_alias: str):
        sender = state.authenticate(request.headers.get("Authorization"))
        room = state._ensure_room(room_id_or_alias)
        room["members"].add(sender)
        return jsonify({"room_id": room_id_or_alias})

    @app.get("/_matrix/client/v3/sync")
    def sync():
        since = request.args.get("since")
        timeout_ms = int(request.args.get("timeout", "0"))
        payload = state.get_sync_payload(since_token=since, timeout_ms=timeout_ms)
        return jsonify(payload)

    @app.put("/_matrix/client/v3/rooms/<path:room_id>/send/<path:event_type>/<path:txn_id>")
    def send_event(room_id: str, event_type: str, txn_id: str):
        sender = state.authenticate(request.headers.get("Authorization"))
        content = request.get_json(silent=True) or {}
        event_id = state.append_timeline_event(
            room_id=room_id,
            sender=sender,
            event_type=event_type,
            content=content,
            txn_id=txn_id,
        )
        # Injecao de falha para demonstracao: o evento JA FOI gravado, mas a resposta
        # "se perde" (503). O cliente fara retry com o mesmo txnId.
        if state.fail_after_commit > 0:
            state.fail_after_commit -= 1
            return jsonify({"errcode": "M_UNKNOWN", "error": "Falha simulada apos commit"}), 503
        return jsonify({"event_id": event_id})

    @app.put("/_matrix/client/v3/rooms/<path:room_id>/state/<path:event_type>/<path:state_key>")
    def put_state_with_key(room_id: str, event_type: str, state_key: str):
        sender = state.authenticate(request.headers.get("Authorization"))
        content = request.get_json(silent=True) or {}
        event_id = state.append_timeline_event(
            room_id=room_id,
            sender=sender,
            event_type=event_type,
            content=content,
            state_key=state_key,
        )
        return jsonify({"event_id": event_id})

    @app.put("/_matrix/client/v3/rooms/<path:room_id>/state/<path:event_type>")
    def put_state_empty_key(room_id: str, event_type: str):
        sender = state.authenticate(request.headers.get("Authorization"))
        content = request.get_json(silent=True) or {}
        event_id = state.append_timeline_event(
            room_id=room_id,
            sender=sender,
            event_type=event_type,
            content=content,
            state_key="",
        )
        return jsonify({"event_id": event_id})

    @app.get("/_matrix/client/v3/rooms/<path:room_id>/state")
    def get_state(room_id: str):
        room = state._ensure_room(room_id)
        events = [
            {k: v for k, v in ev.items() if k != "_stream_id"}
            for ev in room["state"].values()
        ]
        return jsonify(events)

    return app, state

