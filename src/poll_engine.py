"""
Motor de Apuracao - Maquina de Estados Deterministica (src/poll_engine.py)

Ideia central (replicacao de maquina de estados): o estado de uma enquete e uma
FUNCAO DETERMINISTICA da sequencia de eventos da sala. Qualquer processo que aplique
os mesmos eventos na mesma ordem chega ao mesmo placar.

Regras implementadas:
1. Um voto valido por eleitor (chave = sender Matrix, ex.: @alice:matrix.org).
2. Politicas de conflito para votos do mesmo eleitor:
   - CHANGE_ALLOWED (Last-Write-Wins): vence o voto com maior (origin_server_ts, event_id).
     O event_id desempata timestamps iguais -> ordem total deterministica.
   - STRICT_ONCE (First-Write-Wins): o primeiro voto e definitivo.
3. Idempotencia: cada voto tem um `vote_id` (event_id, ou o event_id da mensagem original
   quando o voto veio pela ponte `!votar`). Um vote_id ja apurado nunca e contado de novo.
4. Votos que chegam depois da transicao OPEN -> CLOSED sao rejeitados.
5. O estado completo e serializavel em JSON para ser gravado num Matrix State Event e
   restaurado depois de um crash.
"""

import time
from datetime import datetime, timezone

MAX_AUDIT_ENTRIES = 300  # State events tem limite de 64 KiB no Matrix


class DistributedPollEngine:
    def __init__(self, default_policy: str = "CHANGE_ALLOWED"):
        self.default_policy = default_policy
        self.polls: dict[str, dict] = {}
        self._poll_counter = 0

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

    # ----------------------------------------------------------- criacao
    def create_poll(self, question: str, options: list[str], created_by: str,
                    poll_id: str | None = None, policy: str | None = None) -> dict:
        options = [o.strip() for o in options if o.strip()]
        if len(options) < 2:
            raise ValueError("Uma enquete precisa ter pelo menos 2 opcoes.")
        if len(options) > 20:
            raise ValueError("Maximo de 20 opcoes por enquete.")

        if not poll_id:
            self._poll_counter += 1
            poll_id = f"enquete_{self._poll_counter}"
            while poll_id in self.polls:
                self._poll_counter += 1
                poll_id = f"enquete_{self._poll_counter}"

        poll = {
            "poll_id": poll_id,
            "question": question.strip(),
            "options": options,
            "status": "OPEN",
            "policy": policy or self.default_policy,
            "created_by": created_by,
            "created_at": self._now_iso(),
            "closed_at": None,
            "closed_by": None,
            "native_poll_event_id": None,   # id da enquete nativa do Element (se criada)
            "ballots": {},                  # eleitor -> {option_index, vote_id, event_id, ts}
            "tally": {str(i): 0 for i in range(len(options))},
            "total_valid_votes": 0,
            "updated_votes_count": 0,
            "rejected_votes_count": 0,
            "audit_trail": [],
            "processed_vote_ids": [],       # para idempotencia (inclui entradas antigas da auditoria)
            "state_version": 1,
        }
        self.polls[poll_id] = poll
        return poll

    # ---------------------------------------------------------- consultas
    def has_processed(self, vote_id: str) -> bool:
        return any(vote_id in p.get("processed_vote_ids", []) for p in self.polls.values())

    def find_by_native_event(self, start_event_id: str) -> dict | None:
        for p in self.polls.values():
            if p.get("native_poll_event_id") == start_event_id:
                return p
        return None

    def latest_poll_id(self) -> str | None:
        return list(self.polls.keys())[-1] if self.polls else None

    # ------------------------------------------------------------ internos
    @staticmethod
    def _recompute_tally(poll: dict):
        tally = {str(i): 0 for i in range(len(poll["options"]))}
        for ballot in poll["ballots"].values():
            key = str(ballot["option_index"])
            if key in tally:
                tally[key] += 1
        poll["tally"] = tally
        poll["total_valid_votes"] = sum(tally.values())

    @staticmethod
    def _record(poll: dict, vote_id: str, event_id: str, voter: str, option_index: int,
                ts: int, decision: str):
        poll["audit_trail"].append({
            "vote_id": vote_id, "event_id": event_id, "voter": voter,
            "option_index": option_index, "ts": ts, "decision": decision,
        })
        if len(poll["audit_trail"]) > MAX_AUDIT_ENTRIES:
            poll["audit_trail"] = poll["audit_trail"][-MAX_AUDIT_ENTRIES:]
        poll["processed_vote_ids"].append(vote_id)
        if decision.startswith("REJECTED"):
            poll["rejected_votes_count"] += 1
        poll["state_version"] += 1

    # ------------------------------------------------------------ apuracao
    def apply_vote_event(self, poll_id: str, voter: str, option_index: int, event_id: str,
                         origin_server_ts: int | None = None, vote_id: str | None = None) -> dict:
        """
        Aplica um voto vindo do DAG. Retorna:
          accepted (bool), reason (str), state_changed (bool), poll (dict), updated (bool)...
        """
        ts = int(origin_server_ts or time.time() * 1000)
        vote_id = vote_id or event_id

        poll = self.polls.get(poll_id)
        if poll is None:
            return {"accepted": False, "state_changed": False,
                    "reason": f"Enquete `{poll_id}` nao encontrada."}

        # 1. Idempotencia: o mesmo voto (mesmo vote_id) nunca e contado duas vezes
        if vote_id in poll["processed_vote_ids"]:
            return {"accepted": False, "state_changed": False, "duplicate": True,
                    "reason": f"Voto `{vote_id}` ja apurado anteriormente (idempotencia)."}

        def reject(decision: str, reason: str) -> dict:
            self._record(poll, vote_id, event_id, voter, option_index, ts, decision)
            return {"accepted": False, "state_changed": True, "poll": poll, "reason": reason}

        # 2. Enquete encerrada -> voto tardio
        if poll["status"] != "OPEN":
            return reject("REJECTED_POLL_CLOSED",
                          f"A enquete `{poll_id}` ja esta ENCERRADA (voto chegou apos o fechamento).")

        # 3. Opcao invalida
        if not 0 <= option_index < len(poll["options"]):
            return reject("REJECTED_INVALID_OPTION",
                          f"Opcao invalida ({option_index + 1}). Escolha entre 1 e {len(poll['options'])}.")

        existing = poll["ballots"].get(voter)
        new_ballot = {"option_index": option_index, "vote_id": vote_id, "event_id": event_id, "ts": ts}

        # 4. Primeiro voto deste eleitor
        if existing is None:
            poll["ballots"][voter] = new_ballot
            self._recompute_tally(poll)
            self._record(poll, vote_id, event_id, voter, option_index, ts, "ACCEPTED_NEW_VOTE")
            return {"accepted": True, "updated": False, "state_changed": True, "poll": poll,
                    "chosen_option": poll["options"][option_index]}

        # 5. Eleitor ja votou: resolve o conflito conforme a politica
        if poll["policy"] == "STRICT_ONCE":
            prev = poll["options"][existing["option_index"]]
            return reject("REJECTED_DUPLICATE_VOTER_STRICT",
                          f"Voto unico (STRICT_ONCE): {voter} ja votou em '{prev}'.")

        # Last-Write-Wins com ordem total (ts, event_id)
        if (ts, event_id) < (existing["ts"], existing["event_id"]):
            return reject("REJECTED_STALE_TIMESTAMP",
                          "Voto mais antigo que o voto atual do eleitor (chegou fora de ordem).")

        prev_idx = existing["option_index"]
        poll["ballots"][voter] = new_ballot
        poll["updated_votes_count"] += 1
        self._recompute_tally(poll)
        self._record(poll, vote_id, event_id, voter, option_index, ts,
                     f"UPDATED_FROM_{prev_idx + 1}_TO_{option_index + 1}")
        return {"accepted": True, "updated": True, "state_changed": True, "poll": poll,
                "previous_option": poll["options"][prev_idx],
                "chosen_option": poll["options"][option_index]}

    def close_poll(self, poll_id: str, closed_by: str) -> dict:
        poll = self.polls.get(poll_id)
        if poll is None:
            raise KeyError(f"Enquete `{poll_id}` nao encontrada.")
        if poll["status"] != "CLOSED":
            poll["status"] = "CLOSED"
            poll["closed_at"] = self._now_iso()
            poll["closed_by"] = closed_by
            poll["state_version"] += 1
        return poll

    # ------------------------------------------------------ crash recovery
    def restore_from_state_events(self, state_events: list[dict], state_event_type: str) -> int:
        """Reconstroi as enquetes a partir dos State Events `br.ufc.dc.sd.poll.state` da sala."""
        restored = 0
        for ev in state_events:
            content = ev.get("content")
            if ev.get("type") != state_event_type or not isinstance(content, dict):
                continue
            poll_id = ev.get("state_key") or content.get("poll_id")
            if not poll_id or "question" not in content or "options" not in content:
                continue
            content.setdefault("processed_vote_ids",
                               [a.get("vote_id", a.get("event_id")) for a in content.get("audit_trail", [])])
            content.setdefault("native_poll_event_id", None)
            self.polls[poll_id] = content
            restored += 1
            if poll_id.startswith("enquete_"):
                try:
                    self._poll_counter = max(self._poll_counter, int(poll_id.split("_", 1)[1]))
                except ValueError:
                    pass
        return restored

    # ----------------------------------------------------------- relatorios
    def format_poll_report(self, poll_id: str, title_prefix: str = "APURACAO PARCIAL") -> str:
        p = self.polls.get(poll_id)
        if p is None:
            return f"Enquete `{poll_id}` nao encontrada. Use !enquetes para listar."

        total = p["total_valid_votes"]
        status = "ABERTA" if p["status"] == "OPEN" else "ENCERRADA"
        max_votes = max(p["tally"].values(), default=0)
        lines = [
            f"=== {title_prefix}: [{p['poll_id']}] ===",
            f"Pergunta : {p['question']}",
            f"Estado   : {status} | politica {p['policy']} | state v{p['state_version']}",
            "-" * 52,
        ]
        for idx, opt in enumerate(p["options"]):
            count = p["tally"].get(str(idx), 0)
            pct = (count / total * 100.0) if total else 0.0
            filled = int(round(pct / 10.0))
            bar = "#" * filled + "." * (10 - filled)
            lead = "  <- lider" if total and count == max_votes else ""
            lines.append(f"[{idx + 1}] {opt[:18]:<18} {bar} {count:>3} ({pct:5.1f}%){lead}")
        lines.append("-" * 52)
        lines.append(f"Eleitores: {total} | trocas de voto: {p['updated_votes_count']} | "
                     f"rejeitados: {p['rejected_votes_count']}")
        if p["status"] == "OPEN":
            lines.append(f"Votar: !votar {p['poll_id']} <numero>")
        else:
            lines.append(f"Encerrada em {p['closed_at']} por {p['closed_by']}")
        return "\n".join(lines)

    def format_audit_report(self, poll_id: str, limit: int = 25) -> str:
        p = self.polls.get(poll_id)
        if p is None:
            return f"Enquete `{poll_id}` nao encontrada."
        trail = p["audit_trail"][-limit:]
        lines = [
            f"=== AUDITORIA [{p['poll_id']}] ({len(p['audit_trail'])} voto(s) processados) ===",
            f"Estado: br.ufc.dc.sd.poll.state / state_key={p['poll_id']}",
        ]
        if not trail:
            lines.append("(nenhum voto recebido)")
        for i, a in enumerate(trail, start=1):
            lines.append(f"{i:>2}. {a['voter']} -> opcao {a['option_index'] + 1} | {a['decision']} | {a['event_id']}")
        return "\n".join(lines)

