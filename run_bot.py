"""
Executa o Bot Apurador conectado a um homeserver Matrix real (matrix.org, Synapse...).
Configure antes com:  python setup_real_matrix.py
"""

import os
import sys
from src.config import load_dotenv, build_authenticated_client
from src.poll_engine import DistributedPollEngine
from src.bot import MatrixPollBot


def main():
    load_dotenv()
    homeserver = os.environ.get("MATRIX_HOMESERVER", "https://matrix.dc.ufc.br")
    user = os.environ.get("MATRIX_USER", "")
    password = os.environ.get("MATRIX_PASSWORD", "")
    token = os.environ.get("MATRIX_ACCESS_TOKEN", "")
    room_id = os.environ.get("MATRIX_ROOM_ID", "")
    policy = os.environ.get("DEFAULT_POLL_POLICY", "CHANGE_ALLOWED")
    native = os.environ.get("NATIVE_POLLS", "true").lower() in ("1", "true", "sim", "yes")

    if not room_id:
        print("[!] MATRIX_ROOM_ID nao configurado. Rode `python setup_real_matrix.py` primeiro.")
        sys.exit(1)

    print("=" * 70)
    print(" BOT APURADOR - VOTACAO DISTRIBUIDA (Matrix Client-Server API v3)")
    print("=" * 70)
    print(f"[*] Homeserver : {homeserver}")
    try:
        client, how = build_authenticated_client(homeserver, user, password, token)
    except Exception as exc:
        print(f"[!] Falha de autenticacao: {exc}")
        sys.exit(1)
    print(f"[+] Bot        : {client.user_id} (device {client.device_id}, via {how})")
    print(f"[+] Sala       : {room_id}")
    print(f"[+] Estado     : {MatrixPollBot.STATE_EVENT_POLL} | Voto: {MatrixPollBot.CUSTOM_EVENT_VOTE}")
    print("[*] No Element, digite !ajuda na sala. Ctrl+C para encerrar.\n")

    bot = MatrixPollBot(client, room_id=room_id, engine=DistributedPollEngine(policy), native_polls=native)
    try:
        bot.start(block=True)
    except KeyboardInterrupt:
        print("\n[!] Bot encerrado. O estado continua salvo nos State Events da sala.")
        bot.stop()


if __name__ == "__main__":
    main()

