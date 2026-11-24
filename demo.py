"""
Demonstracao local (sem internet) - Votacao Distribuida via Matrix (demo.py)

Sobe um homeserver Matrix local (src/mock_homeserver.py), o bot apurador e 5 eleitores,
e executa 7 cenarios que mostram os conceitos de Sistemas Distribuidos:

 1. Criacao da enquete -> State Event `br.ufc.dc.sd.poll.state`
 2. Tres caminhos de voto -> evento personalizado, botao do Element (m.poll.response), !votar
 3. Conflito do mesmo eleitor -> Last-Write-Wins
 4. Falha de rede (resposta perdida apos commit) -> retry com mesmo txnId, sem duplicar
 5. Crash do bot + voto durante a queda -> recuperacao pelos State Events + catch-up
 6. Encerramento -> voto tardio rejeitado
 7. Auditoria + inspecao do State Event no servidor

Uso:  python demo.py            (roteiro automatico)
      python demo.py -i         (roteiro + chat interativo no terminal)
"""

import json
import time
import logging
import argparse
import threading
from werkzeug.serving import make_server

from src.mock_homeserver import create_mock_homeserver_app
from src.matrix_client import MatrixClient
from src.bot import MatrixPollBot

logging.getLogger("werkzeug").setLevel(logging.ERROR)
logging.getLogger("MatrixPollBot").setLevel(logging.WARNING)
logging.getLogger("MatrixClient").setLevel(logging.ERROR)

ROOM = "!eleicao_sd:localhost"
PAUSE = 0.7


class LocalServer(threading.Thread):
    def __init__(self, port: int = 8008):
        super().__init__(daemon=True)
        self.app, self.state = create_mock_homeserver_app("localhost")
        self.server = make_server("127.0.0.1", port, self.app, threaded=True)
        self.url = f"http://127.0.0.1:{port}"

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


def banner(title: str):
    print("\n" + "=" * 78 + f"\n {title}\n" + "=" * 78)


def show_new_events(observer: MatrixClient, bot_user: str):
    """Le o /sync do observador e imprime o que trafegou na sala."""
    time.sleep(PAUSE)
    data = observer.sync(timeout_ms=300)
    events = data.get("rooms", {}).get("join", {}).get(ROOM, {}).get("timeline", {}).get("events", [])
    for ev in events:
        t, sender, c = ev.get("type"), ev.get("sender"), ev.get("content", {})
        if t == MatrixClient.CUSTOM_EVENT_VOTE:
            print(f"  [DAG] {t} de {sender}: {json.dumps(c, ensure_ascii=False)}")
        elif t in MatrixClient.POLL_START_TYPES:
            print(f"  [DAG] enquete nativa do Element criada ({ev['event_id']})")
        elif t in MatrixClient.POLL_RESPONSE_TYPES:
            print(f"  [DAG] clique no botao do Element por {sender}: {json.dumps(c, ensure_ascii=False)}")
        elif t == "m.room.message" and sender == bot_user:
            print(f"  <<< bot:")
            for line in c.get("body", "").splitlines():
                print(f"      {line}")


def new_client(url: str, name: str) -> MatrixClient:
    c = MatrixClient(url)
    c.login(name, "senha")
    c.join_room(ROOM)
    return c


def run_demo(interactive: bool):
    banner("VOTACAO E ENQUETE DISTRIBUIDA VIA PROTOCOLO MATRIX - DEMONSTRACAO")
    srv = LocalServer()
    srv.start()
    print(f"[setup] Homeserver Matrix local em {srv.url} | sala {ROOM}")

    bot_client = MatrixClient(srv.url)
    bot_client.login("bot_apurador", "senha")
    bot = MatrixPollBot(bot_client, ROOM)
    bot.start(block=False)
    print(f"[setup] Bot apurador online como {bot_client.user_id}")

    v = {n: new_client(srv.url, n) for n in ("alice", "bob", "carol", "dave", "erin")}
    observer = new_client(srv.url, "observador")
    observer.sync(timeout_ms=0)
    print("[setup] Eleitores conectados: @alice @bob @carol @dave @erin")

    # 1 ---------------------------------------------------------------
    banner("CENARIO 1 - CRIACAO DA ENQUETE (STATE EVENT)")
    cmd = '!criar_enquete "Qual algoritmo de consenso usar no cluster?" "Raft" "Paxos" "PBFT"'
    print(f">>> @alice: {cmd}")
    v["alice"].send_text_message(ROOM, cmd)
    show_new_events(observer, bot_client.user_id)

    # 2 ---------------------------------------------------------------
    banner("CENARIO 2 - TRES CAMINHOS DE VOTO, UM UNICO FLUXO DE APURACAO")
    print(">>> @alice envia evento personalizado br.ufc.dc.sd.poll.vote (opcao 1 - Raft)")
    v["alice"].send_vote_event(ROOM, "enquete_1", 0)
    time.sleep(0.3)
    native_id = bot.engine.polls["enquete_1"]["native_poll_event_id"]
    print(">>> @bob clica no botao 'Paxos' da enquete nativa do Element (m.poll.response)")
    v["bob"].send_event(ROOM, "org.matrix.msc3381.poll.response", {
        "m.relates_to": {"rel_type": "m.reference", "event_id": native_id},
        "org.matrix.msc3381.poll.response": {"answers": ["2"]},
    })
    time.sleep(0.3)
    print(">>> @carol digita no chat: !votar enquete_1 1")
    v["carol"].send_text_message(ROOM, "!votar enquete_1 1")
    show_new_events(observer, bot_client.user_id)

    # 3 ---------------------------------------------------------------
    banner("CENARIO 3 - CONFLITO: O MESMO ELEITOR VOTA DE NOVO (LAST-WRITE-WINS)")
    print(">>> @bob muda o voto para Raft com um novo evento")
    v["bob"].send_vote_event(ROOM, "enquete_1", 0)
    time.sleep(0.3)
    v["alice"].send_text_message(ROOM, "!parcial enquete_1")
    show_new_events(observer, bot_client.user_id)

    # 4 ---------------------------------------------------------------
    banner("CENARIO 4 - FALHA DE REDE: RESPOSTA PERDIDA APOS O COMMIT (IDEMPOTENCIA txnId)")
    srv.state.fail_after_commit = 1
    print(">>> @dave vota PBFT; o servidor grava o evento mas a resposta HTTP 'se perde' (503)")
    print("    O cliente repete o PUT com o MESMO txnId...")
    res = v["dave"].send_vote_event(ROOM, "enquete_1", 2, txn_id="txn_dave_unico")
    dave_events = [e for e in srv.state.rooms[ROOM]["timeline"]
                   if e["sender"] == "@dave:localhost" and e["type"] == MatrixClient.CUSTOM_EVENT_VOTE]
    print(f"    Retry retornou event_id={res['event_id']}")
    print(f"    Eventos de voto do @dave gravados no DAG: {len(dave_events)}  (sem duplicata)")
    show_new_events(observer, bot_client.user_id)

    # 5 ---------------------------------------------------------------
    banner("CENARIO 5 - CRASH DO BOT APURADOR + VOTO DURANTE A QUEDA")
    print("[!] Bot apurador derrubado (toda a memoria local perdida)")
    bot.stop()
    time.sleep(0.5)
    print(">>> @erin vota Paxos ENQUANTO o bot esta fora do ar")
    v["erin"].send_vote_event(ROOM, "enquete_1", 1)
    time.sleep(0.3)
    print("[*] Nova instancia do bot: GET /rooms/{room}/state + catch-up do historico...")
    bot2_client = MatrixClient(srv.url)
    bot2_client.login("bot_apurador", "senha")
    bot = MatrixPollBot(bot2_client, ROOM)
    bot.start(block=False)
    p = bot.engine.polls["enquete_1"]
    print(f"[+] Estado recuperado: {p['total_valid_votes']} eleitores, placar {p['tally']} "
          f"(o voto da @erin foi apurado no catch-up; votos antigos NAO foram recontados)")
    show_new_events(observer, bot2_client.user_id)

    # 6 ---------------------------------------------------------------
    banner("CENARIO 6 - ENCERRAMENTO E VOTO TARDIO")
    print(">>> @bob tenta encerrar (nao e o criador)")
    v["bob"].send_text_message(ROOM, "!encerrar enquete_1")
    show_new_events(observer, bot2_client.user_id)
    print(">>> @alice (criadora): !encerrar enquete_1")
    v["alice"].send_text_message(ROOM, "!encerrar enquete_1")
    show_new_events(observer, bot2_client.user_id)
    print(">>> @carol tenta votar depois do encerramento")
    v["carol"].send_vote_event(ROOM, "enquete_1", 2)
    show_new_events(observer, bot2_client.user_id)

    # 7 ---------------------------------------------------------------
    banner("CENARIO 7 - AUDITORIA E STATE EVENT GRAVADO NO HOMESERVER")
    v["alice"].send_text_message(ROOM, "!auditar enquete_1")
    show_new_events(observer, bot2_client.user_id)
    for st in observer.get_room_state(ROOM):
        if st["type"] == MatrixClient.STATE_EVENT_POLL:
            c = st["content"]
            print(f"\n  GET /rooms/{ROOM}/state ->")
            print(f"    type={st['type']} state_key={st['state_key']} status={c['status']} "
                  f"v{c['state_version']} tally={c['tally']} eleitores={c['total_valid_votes']}")

    if interactive:
        banner("CHAT INTERATIVO (voce e @alice). Ex.: !ajuda, !criar_enquete ..., 'sair'")
        while True:
            try:
                line = input("\n@alice > ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if line.lower() in ("sair", "exit", "quit"):
                break
            if line:
                v["alice"].send_text_message(ROOM, line)
                show_new_events(observer, bot2_client.user_id)

    bot.stop()
    srv.shutdown()
    print("\n[+] Demonstracao concluida.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--interactive", action="store_true")
    run_demo(ap.parse_args().interactive)

