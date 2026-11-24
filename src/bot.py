"""
Bot Apurador de Votacao Distribuida via Matrix (src/bot.py)

Requisitos do enunciado:
- Votos como EVENTOS PERSONALIZADOS: `br.ufc.dc.sd.poll.vote`
- Estado da enquete em STATE EVENTS: `br.ufc.dc.sd.poll.state` (state_key = poll_id)
- APURACAO por um bot que consome a sala via /sync

Formas de votar (todas viram o mesmo fluxo de apuracao):
1. Evento personalizado `br.ufc.dc.sd.poll.vote` enviado direto por um cliente
   (demo.py, ou Element -> /devtools -> "Send custom event").
2. Comando `!votar <enquete> <n>` no chat: o bot faz a "ponte" e publica um
   `br.ufc.dc.sd.poll.vote` em nome do eleitor (campo `voter`).
3. Clique nos botoes da enquete nativa do Element (eventos `m.poll.response`, MSC3381).

Comandos: !criar_enquete, !criar_enquete_unica, !votar, !parcial, !encerrar,
          !auditar, !enquetes, !ajuda
"""

import html
import shlex
import time
import logging
import threading
from src.matrix_client import MatrixClient
from src.poll_engine import DistributedPollEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("MatrixPollBot")


class MatrixPollBot:
    STATE_EVENT_POLL = MatrixClient.STATE_EVENT_POLL
    CUSTOM_EVENT_VOTE = MatrixClient.CUSTOM_EVENT_VOTE

    def __init__(self, matrix_client: MatrixClient, room_id: str,
                 engine: DistributedPollEngine | None = None, native_polls: bool = True):
        self.client = matrix_client
        self.room_id = room_id
        self.engine = engine or DistributedPollEngine()
        self.native_polls = native_polls
        self.running = False
        self._processed_event_ids: set[str] = set()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------ utilidades
    def reply(self, text: str):
        """Envia texto; o formatted_body em <pre> faz o Element usar fonte monoespacada."""
        self.client.send_text_message(self.room_id, text,
                                      formatted_body=f"<pre><code>{html.escape(text)}</code></pre>")

    def persist_poll_state(self, poll: dict) -> dict:
        """PUT /rooms/{roomId}/state/br.ufc.dc.sd.poll.state/{poll_id}"""
        return self.client.put_state_event(self.room_id, self.STATE_EVENT_POLL, poll["poll_id"], poll)

    def recover_state_from_room(self) -> int:
        """Crash recovery: GET /rooms/{roomId}/state e reconstrucao das enquetes."""
        state_events = self.client.get_room_state(self.room_id)
        count = self.engine.restore_from_state_events(state_events, self.STATE_EVENT_POLL)
        logger.info("Recuperadas %d enquete(s) dos State Events da sala.", count)
        return count

    @staticmethod
    def _parse_args(text: str) -> list[str]:
        normalized = text.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
        try:
            return shlex.split(normalized)
        except ValueError:
            return normalized.split()

    # -------------------------------------------------------- votos (DAG)
    def _extract_vote(self, event: dict) -> dict | None:
        """Converte um evento do DAG em um voto normalizado, ou None se nao for voto."""
        ev_type = event.get("type")
        content = event.get("content", {}) or {}
        sender = event.get("sender", "")

        if ev_type == self.CUSTOM_EVENT_VOTE:
            from_bridge = sender == self.client.user_id and content.get("voter")
            try:
                option_index = int(content.get("option_index", -1))
            except (TypeError, ValueError):
                option_index = -1
            return {
                "poll_id": str(content.get("poll_id", "")).strip(),
                "voter": content["voter"] if from_bridge else sender,
                "option_index": option_index,
                "vote_id": content.get("origin_event_id") if from_bridge and content.get("origin_event_id")
                else event.get("event_id"),
                "via": "!votar (ponte)" if from_bridge else "evento personalizado",
            }

        if ev_type in MatrixClient.POLL_RESPONSE_TYPES:
            start_id = (content.get("m.relates_to") or {}).get("event_id")
            poll = self.engine.find_by_native_event(start_id) if start_id else None
            if poll is None:
                return None
            body = content.get("org.matrix.msc3381.poll.response") or content.get("m.poll.response") or {}
            answers = body.get("answers") or content.get("m.selections") or []
            try:
                option_index = int(answers[0]) - 1
            except (IndexError, ValueError, TypeError):
                return None  # resposta vazia = eleitor desmarcou; ignoramos
            return {"poll_id": poll["poll_id"], "voter": sender, "option_index": option_index,
                    "vote_id": event.get("event_id"), "via": "botao do Element"}
        return None

    def handle_vote_event(self, event: dict, quiet: bool = False) -> str | None:
        vote = self._extract_vote(event)
        if vote is None:
            return None
        res = self.engine.apply_vote_event(
            poll_id=vote["poll_id"], voter=vote["voter"], option_index=vote["option_index"],
            event_id=event.get("event_id", "?"),
            origin_server_ts=event.get("origin_server_ts"), vote_id=vote["vote_id"],
        )
        if res.get("state_changed"):
            self.persist_poll_state(res["poll"])
        if quiet or res.get("duplicate"):
            return None
        if not res["accepted"]:
            return f"[VOTO REJEITADO] {vote['voter']}: {res['reason']}"
        poll = res["poll"]
        if res.get("updated"):
            return (f"[VOTO ALTERADO] {vote['voter']}: '{res['previous_option']}' -> '{res['chosen_option']}' "
                    f"em {vote['poll_id']} (via {vote['via']}) | total {poll['total_valid_votes']} | "
                    f"state v{poll['state_version']}")
        return (f"[VOTO COMPUTADO] {vote['voter']} -> '{res['chosen_option']}' em {vote['poll_id']} "
                f"(via {vote['via']}) | total {poll['total_valid_votes']} | state v{poll['state_version']}")

    def _bridge_vote_command(self, sender: str, parts: list[str], origin_event_id: str | None) -> str | None:
        if len(parts) < 3:
            return "Uso: !votar <id_enquete> <numero_da_opcao>   (ex: !votar enquete_1 2)"
        try:
            opt_num = int(parts[2])
        except ValueError:
            return "O numero da opcao deve ser inteiro (1, 2, 3...)."
        if origin_event_id and self.engine.has_processed(origin_event_id):
            return None  # este !votar ja foi apurado antes (ex.: bot reiniciou)
        # Publica o evento personalizado no DAG; ele volta pelo /sync e e apurado la.
        # txnId derivado da mensagem original: se reenviado, o homeserver deduplica.
        self.client.send_vote_event(self.room_id, poll_id=parts[1], option_index=opt_num - 1,
                                    voter_override=sender, origin_event_id=origin_event_id,
                                    source="chat_command_bridge",
                                    txn_id=f"bridge_{origin_event_id}" if origin_event_id else None)
        return None

    # ---------------------------------------------------------- comandos
    def handle_text_command(self, sender: str, body: str, event_id: str | None = None) -> str | None:
        text = body.strip()
        if not text.startswith("!"):
            return None
        parts = self._parse_args(text)
        if not parts:
            return None
        cmd = parts[0].lower()

        if cmd in ("!criar_enquete", "!criar_enquete_unica", "!nova_enquete"):
            policy = "STRICT_ONCE" if cmd == "!criar_enquete_unica" else "CHANGE_ALLOWED"
            args = parts[1:]
            if "|" in text:
                args = [s.strip().strip('"') for s in text[len(parts[0]):].split("|") if s.strip()]
            if len(args) < 3:
                return ('Uso: !criar_enquete "Pergunta?" "Opcao 1" "Opcao 2" ...\n'
                        "  ou: !criar_enquete Pergunta? | Opcao 1 | Opcao 2\n"
                        "Use !criar_enquete_unica para proibir troca de voto.")
            try:
                poll = self.engine.create_poll(args[0], args[1:], created_by=sender, policy=policy)
            except ValueError as exc:
                return f"Erro: {exc}"
            if self.native_polls:
                try:
                    res = self.client.send_native_poll_start(self.room_id, poll["question"], poll["options"])
                    poll["native_poll_event_id"] = res.get("event_id")
                except Exception as exc:
                    logger.warning("Nao foi possivel criar a enquete nativa: %s", exc)
            state_res = self.persist_poll_state(poll)
            opts = "\n".join(f"  [{i + 1}] {o}" for i, o in enumerate(poll["options"]))
            return (f"=== ENQUETE CRIADA: {poll['poll_id']} ===\n"
                    f"Pergunta : {poll['question']}\n"
                    f"Politica : {poll['policy']} | criada por {sender}\n"
                    f"Estado   : {self.STATE_EVENT_POLL} (state_key={poll['poll_id']}, {state_res.get('event_id')})\n"
                    f"Opcoes:\n{opts}\n"
                    f"Vote com: !votar {poll['poll_id']} <numero>"
                    + ("  (ou clique nos botoes da enquete acima)" if poll.get("native_poll_event_id") else ""))

        if cmd == "!votar":
            return self._bridge_vote_command(sender, parts, event_id)

        if cmd in ("!parcial", "!resultado"):
            poll_id = parts[1] if len(parts) > 1 else self.engine.latest_poll_id()
            if not poll_id:
                return "Nenhuma enquete criada ainda. Use !criar_enquete."
            return self.engine.format_poll_report(poll_id, "APURACAO EM TEMPO REAL")

        if cmd in ("!encerrar", "!fechar"):
            poll_id = parts[1] if len(parts) > 1 else self.engine.latest_poll_id()
            poll = self.engine.polls.get(poll_id or "")
            if poll is None:
                return f"Enquete `{poll_id}` nao encontrada."
            if sender not in (poll["created_by"], self.client.user_id):
                return f"Apenas quem criou a enquete ({poll['created_by']}) pode encerra-la."
            self.engine.close_poll(poll_id, closed_by=sender)
            self.persist_poll_state(poll)
            report = self.engine.format_poll_report(poll_id, "ATA FINAL - ENQUETE ENCERRADA")
            if poll.get("native_poll_event_id"):
                try:
                    self.client.send_native_poll_end(self.room_id, poll["native_poll_event_id"],
                                                     f"Enquete {poll_id} encerrada pelo bot apurador.")
                except Exception as exc:
                    logger.warning("Nao foi possivel encerrar a enquete nativa: %s", exc)
            return report

        if cmd == "!auditar":
            poll_id = parts[1] if len(parts) > 1 else self.engine.latest_poll_id()
            if not poll_id:
                return "Nenhuma enquete disponivel."
            return self.engine.format_audit_report(poll_id)

        if cmd in ("!enquetes", "!listar"):
            self.recover_state_from_room()
            if not self.engine.polls:
                return "Nenhuma enquete nos State Events desta sala."
            lines = [f"=== ENQUETES NOS STATE EVENTS DA SALA ({len(self.engine.polls)}) ==="]
            for pid, p in self.engine.polls.items():
                lines.append(f"- {pid}: \"{p['question']}\" | {p['status']} | {p['total_valid_votes']} voto(s) | "
                             f"{p['policy']} | v{p['state_version']}")
            return "\n".join(lines)

        if cmd in ("!ajuda", "!help"):
            return ("=== BOT DE VOTACAO DISTRIBUIDA (MATRIX) ===\n"
                    '!criar_enquete "Pergunta?" "Op 1" "Op 2"   cria enquete (pode trocar voto)\n'
                    '!criar_enquete_unica "Pergunta?" "A" "B"   voto unico, sem troca\n'
                    "!votar <id> <n>      vota (gera evento br.ufc.dc.sd.poll.vote)\n"
                    "!parcial [id]        placar atual\n"
                    "!encerrar [id]       fecha a enquete (so o criador)\n"
                    "!auditar [id]        trilha de votos processados\n"
                    "!enquetes            lista enquetes salvas nos State Events\n"
                    f"Estado: {self.STATE_EVENT_POLL} | Voto: {self.CUSTOM_EVENT_VOTE}")

        return f"Comando desconhecido: {cmd}. Digite !ajuda."

    # ------------------------------------------------------------ /sync
    def _room_timeline(self, sync_data: dict) -> tuple[list[dict], bool]:
        room = sync_data.get("rooms", {}).get("join", {}).get(self.room_id, {})
        timeline = room.get("timeline", {})
        return timeline.get("events", []), bool(timeline.get("limited"))

    def process_sync_response(self, sync_data: dict, catch_up: bool = False) -> int:
        """
        Processa um lote do /sync em ordem.
        catch_up=True (na inicializacao): apura apenas votos perdidos enquanto o bot estava
        fora do ar, em silencio, sem reexecutar comandos antigos como !criar_enquete.
        """
        if self.room_id in sync_data.get("rooms", {}).get("invite", {}):
            try:
                self.client.join_room(self.room_id)
                logger.info("Convite aceito para %s", self.room_id)
            except Exception as exc:
                logger.warning("Falha ao aceitar convite: %s", exc)

        events, limited = self._room_timeline(sync_data)
        if limited:
            logger.warning("Timeline 'limited': o servidor omitiu eventos antigos deste lote.")

        processed = 0
        for event in events:
            ev_id = event.get("event_id")
            if not ev_id or ev_id in self._processed_event_ids:
                continue
            self._processed_event_ids.add(ev_id)
            ev_type = event.get("type")
            sender = event.get("sender")

            if ev_type == self.CUSTOM_EVENT_VOTE or ev_type in MatrixClient.POLL_RESPONSE_TYPES:
                reply = self.handle_vote_event(event, quiet=catch_up)
                if reply:
                    self.reply(reply)
                processed += 1
                continue

            if ev_type == "m.room.message":
                content = event.get("content", {})
                body = content.get("body", "") if content.get("msgtype") == "m.text" else ""
                # Ignora mensagens geradas pelo proprio bot, MAS permite que o dono da conta
                # envie comandos `!comando` no Element caso esteja usando a propria conta do DC/UFC!
                if not body.strip().startswith("!"):
                    continue
                if catch_up:
                    parts = self._parse_args(body)
                    if parts and parts[0].lower() == "!votar":
                        self._bridge_vote_command(sender, parts, ev_id)
                        processed += 1
                    continue
                reply = self.handle_text_command(sender, body, event_id=ev_id)
                if reply:
                    self.reply(reply)
                processed += 1
        return processed

    def start(self, block: bool = True):
        """
        1. Entra na sala.  2. Recupera o estado (State Events).
        3. Sync inicial: apura votos emitidos enquanto o bot estava fora (catch-up).
        4. Loop de long-polling.
        """
        self.running = True
        try:
            res = self.client.join_room(self.room_id)
            self.room_id = res.get("room_id", self.room_id)
        except Exception as exc:
            logger.warning("join_room: %s", exc)

        try:
            ok, mine, needed = self.client.can_send_state(self.room_id, self.STATE_EVENT_POLL)
            if not ok:
                logger.error("ATENCAO: o bot tem power level %s mas precisa de %s para gravar State Events. "
                             "Promova o bot a Moderador nas configuracoes da sala.", mine, needed)
        except Exception:
            pass  # homeserver local de testes nao implementa power levels

        try:
            self.recover_state_from_room()
        except Exception as exc:
            logger.warning("Falha ao recuperar estado: %s", exc)

        init = self.client.sync(timeout_ms=0, timeline_limit=100)
        recovered = self.process_sync_response(init, catch_up=True)
        if recovered:
            logger.info("Catch-up: %d voto(s) do historico verificados.", recovered)

        if block:
            self._sync_loop()
        else:
            self._thread = threading.Thread(target=self._sync_loop, daemon=True)
            self._thread.start()

    def _sync_loop(self):
        while self.running:
            try:
                data = self.client.sync(timeout_ms=10000)
                if not self.running:
                    break
                self.process_sync_response(data)
            except Exception as exc:
                if not self.running:
                    break
                logger.error("Erro no loop /sync: %s", exc)
                time.sleep(2.0)

    def stop(self):
        self.running = False

