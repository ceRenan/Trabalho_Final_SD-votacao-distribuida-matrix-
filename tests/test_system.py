"""
Testes automatizados (python -m unittest discover -s tests -v)
Cobrem: LWW/STRICT_ONCE, ordem total (ts, event_id), voto tardio, idempotencia por txnId
(inclusive com resposta perdida e retry), enquete nativa do Element, permissao de
encerramento, crash recovery e catch-up de votos emitidos com o bot fora do ar.
"""

import time
import unittest
import threading
import logging
from werkzeug.serving import make_server

from src.mock_homeserver import create_mock_homeserver_app
from src.matrix_client import MatrixClient
from src.poll_engine import DistributedPollEngine
from src.bot import MatrixPollBot

logging.getLogger("werkzeug").setLevel(logging.ERROR)


def wait_until(cond, timeout=4.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


class TestPollEngine(unittest.TestCase):
    def setUp(self):
        self.e = DistributedPollEngine()

    def test_lww_keeps_one_vote_per_voter(self):
        pid = self.e.create_poll("Q?", ["A", "B", "C"], "@prof:uf")["poll_id"]
        self.assertTrue(self.e.apply_vote_event(pid, "@a:uf", 0, "$1", 1000)["accepted"])
        r = self.e.apply_vote_event(pid, "@a:uf", 1, "$2", 2000)
        self.assertTrue(r["updated"])
        self.assertEqual(self.e.polls[pid]["total_valid_votes"], 1)
        self.assertEqual(self.e.polls[pid]["tally"], {"0": 0, "1": 1, "2": 0})

    def test_stale_vote_rejected_and_tie_broken_by_event_id(self):
        pid = self.e.create_poll("Q?", ["A", "B"], "@prof:uf")["poll_id"]
        self.e.apply_vote_event(pid, "@a:uf", 0, "$m", 2000)
        self.assertFalse(self.e.apply_vote_event(pid, "@a:uf", 1, "$old", 1500)["accepted"])
        # mesmo timestamp: vence o maior event_id ("$z" > "$m"), "$a" perde
        self.assertFalse(self.e.apply_vote_event(pid, "@a:uf", 1, "$a", 2000)["accepted"])
        self.assertTrue(self.e.apply_vote_event(pid, "@a:uf", 1, "$z", 2000)["accepted"])

    def test_strict_once(self):
        pid = self.e.create_poll("Q?", ["A", "B"], "@prof:uf", policy="STRICT_ONCE")["poll_id"]
        self.assertTrue(self.e.apply_vote_event(pid, "@b:uf", 0, "$1")["accepted"])
        r = self.e.apply_vote_event(pid, "@b:uf", 1, "$2")
        self.assertFalse(r["accepted"])
        self.assertEqual(self.e.polls[pid]["tally"]["0"], 1)

    def test_duplicate_vote_id_is_ignored(self):
        pid = self.e.create_poll("Q?", ["A", "B"], "@prof:uf")["poll_id"]
        self.e.apply_vote_event(pid, "@a:uf", 0, "$1")
        r = self.e.apply_vote_event(pid, "@a:uf", 0, "$1")
        self.assertTrue(r.get("duplicate"))
        self.assertEqual(len(self.e.polls[pid]["audit_trail"]), 1)

    def test_late_vote_after_close(self):
        pid = self.e.create_poll("Q?", ["A", "B"], "@prof:uf")["poll_id"]
        self.e.close_poll(pid, "@prof:uf")
        r = self.e.apply_vote_event(pid, "@c:uf", 0, "$late")
        self.assertFalse(r["accepted"])
        self.assertEqual(self.e.polls[pid]["total_valid_votes"], 0)


class TestMatrixIntegration(unittest.TestCase):
    PORT = 18009

    def setUp(self):
        self.app, self.state = create_mock_homeserver_app("test.local")
        self.http = make_server("127.0.0.1", self.PORT, self.app, threaded=True)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.PORT}"
        self.room = "!sala:test.local"
        self.bots = []

    def tearDown(self):
        for b in self.bots:
            b.stop()
        self.http.shutdown()
        self.http.server_close()

    def client(self, name):
        c = MatrixClient(self.url)
        c.login(name, "x")
        c.join_room(self.room)
        return c

    def start_bot(self):
        bot = MatrixPollBot(self.client("bot"), self.room)
        bot.start(block=False)
        self.bots.append(bot)
        return bot

    def test_txn_retry_after_lost_response_does_not_duplicate(self):
        alice = self.client("alice")
        self.state.fail_after_commit = 1
        res = alice.send_event(self.room, "br.ufc.dc.sd.poll.vote", {"poll_id": "x", "option_index": 0},
                               txn_id="fixo")
        events = [e for e in self.state.rooms[self.room]["timeline"] if e["type"] == "br.ufc.dc.sd.poll.vote"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], res["event_id"])

    def test_full_flow_native_poll_close_permission_and_crash_recovery(self):
        bot = self.start_bot()
        alice, bob, erin = self.client("alice"), self.client("bob"), self.client("erin")

        alice.send_text_message(self.room, '!criar_enquete "Consenso?" "Raft" "Paxos"')
        self.assertTrue(wait_until(lambda: "enquete_1" in bot.engine.polls
                                   and bot.engine.polls["enquete_1"]["native_poll_event_id"]))
        native = bot.engine.polls["enquete_1"]["native_poll_event_id"]

        alice.send_vote_event(self.room, "enquete_1", 0)                       # evento personalizado
        bob.send_event(self.room, "org.matrix.msc3381.poll.response", {         # botao do Element
            "m.relates_to": {"rel_type": "m.reference", "event_id": native},
            "org.matrix.msc3381.poll.response": {"answers": ["2"]}})
        self.assertTrue(wait_until(lambda: bot.engine.polls["enquete_1"]["total_valid_votes"] == 2))

        bob.send_text_message(self.room, "!encerrar enquete_1")                # bob nao e o criador
        time.sleep(0.5)
        self.assertEqual(bot.engine.polls["enquete_1"]["status"], "OPEN")

        # crash do bot; erin vota pelo chat enquanto ele esta fora
        bot.stop()
        time.sleep(0.3)
        erin.send_text_message(self.room, "!votar enquete_1 2")
        time.sleep(0.3)

        bot2 = self.start_bot()
        self.assertTrue(wait_until(lambda: bot2.engine.polls["enquete_1"]["total_valid_votes"] == 3))
        self.assertEqual(bot2.engine.polls["enquete_1"]["tally"], {"0": 1, "1": 2})

        # reiniciar de novo nao reconta nada (idempotencia)
        bot2.stop()
        time.sleep(0.3)
        bot3 = self.start_bot()
        time.sleep(1.0)
        self.assertEqual(bot3.engine.polls["enquete_1"]["total_valid_votes"], 3)
        self.assertEqual(len(bot3.engine.polls["enquete_1"]["audit_trail"]), 3)


if __name__ == "__main__":
    unittest.main()

