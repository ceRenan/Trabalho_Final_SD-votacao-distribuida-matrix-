"""
Assistente de configuracao para usar o bot em um chat Matrix REAL (Element + matrix.org).

IMPORTANTE: o bot precisa de uma CONTA SEPARADA da sua conta pessoal, porque ele
ignora as mensagens enviadas por ele mesmo (senao entraria em loop).

O assistente:
1. Autentica a conta do bot (senha recomendada: permite renovar o token sozinho).
2. Cria uma sala nova SEM criptografia e convida voce (recomendado: o bot vira admin
   e pode gravar State Events) OU entra numa sala existente e verifica as permissoes.
3. Envia uma mensagem de teste e grava .env e .matrix_session.json.
"""

import getpass
from src.config import save_env, save_session
from src.matrix_client import MatrixClient


def ask(prompt: str, default: str = "") -> str:
    value = input(f"{prompt}{f' [{default}]' if default else ''}: ").strip()
    return value or default


def main():
    print("=" * 74)
    print(" CONFIGURACAO DO BOT DE VOTACAO NO SERVIDOR MATRIX (DC/UFC ou Matrix.org)")
    print("=" * 74)
    print("Servidor da disciplina (DC/UFC): https://matrix.dc.ufc.br")
    print("Como o login do DC/UFC usa SSO (Authentik), use a opcao de Access Token:")
    print("  No Element (https://app.element.io): clique no seu Avatar -> Todas as")
    print("  Configuracoes -> Ajuda e Sobre -> Avancado -> Token de Acesso.\n")

    hs = ask("1. URL do homeserver", "https://matrix.dc.ufc.br")
    if not hs.startswith(("http://", "https://")):
        hs = "https://" + hs
    client = MatrixClient(hs)
    client.on_tokens_changed = save_session

    print("\n2. Autenticacao da conta que rodara o Bot")
    print("   (1) Access Token [Recomendado para matrix.dc.ufc.br com SSO Authentik]")
    print("   (2) Usuario e senha (apenas para servidores sem SSO)")
    mode = ask("   Opcao", "1")
    user, password, token = "", "", ""
    try:
        if mode == "1":
            token = getpass.getpass("   Access token (oculto, ou Enter para colar visivel): ").strip() or input("   Access token: ").strip()
            client.access_token = token
            client.whoami()
            user = client.user_id
        else:
            user = ask("   Usuario (ex: @usuario:matrix.dc.ufc.br)")
            password = getpass.getpass("   Senha (oculta): ").strip()
            client.login(user, password)
            user = client.user_id
        print(f"[+] Autenticado como {user} (device {client.device_id})")
    except Exception as exc:
        print(f"[!] Falha na autenticacao: {exc}")
        return

    print("\n3. Sala")
    print("   (1) o bot cria uma sala nova sem criptografia e convida voce [recomendado]")
    print("   (2) usar uma sala existente (o bot precisa ser Moderador)")
    room_mode = ask("   Opcao", "1")
    try:
        if room_mode == "2":
            room_input = ask("   ID da sala (!xxxx:matrix.org) ou alias (#sala:matrix.org)")
            room_id = client.join_room(room_input).get("room_id", room_input)
            try:
                ok, mine, needed = client.can_send_state(room_id, MatrixClient.STATE_EVENT_POLL)
                if not ok:
                    print(f"[!] O bot tem power level {mine}, mas precisa de {needed} para gravar State Events.")
                    print("    No Element: Configuracoes da sala -> Cargos e permissoes -> promova o bot a Moderador.")
            except Exception:
                pass
            try:
                client.get_state_event(room_id, "m.room.encryption")
                print("[!] ATENCAO: esta sala tem criptografia. O bot NAO consegue ler mensagens criptografadas.")
                print("    Crie uma sala sem criptografia (ou escolha a opcao 1).")
            except Exception:
                pass  # 404 = sala sem criptografia (ok)
        else:
            invites = ask("   Seu(s) usuario(s) para convidar, separados por virgula (ex: @fulano:matrix.org)")
            invite_list = [u.strip() for u in invites.split(",") if u.strip()]
            name = ask("   Nome da sala", "Votacao Distribuida SD")
            room_id = client.create_room(name, invite=invite_list,
                                         topic="Enquetes apuradas por bot - trabalho de Sistemas Distribuidos")
            print(f"[+] Sala criada: {room_id}. Aceite o convite no Element.")
        print(f"[+] Sala: {room_id}")
        client.send_text_message(room_id, "Bot de votacao online. Digite !ajuda para ver os comandos.")
    except Exception as exc:
        print(f"[!] Falha ao preparar a sala: {exc}")
        return

    save_env({
        "MATRIX_HOMESERVER": hs,
        "MATRIX_USER": user,
        "MATRIX_PASSWORD": password,
        "MATRIX_ACCESS_TOKEN": token,
        "MATRIX_ROOM_ID": room_id,
        "DEFAULT_POLL_POLICY": "CHANGE_ALLOWED",
        "NATIVE_POLLS": "true",
    })
    save_session(client)
    print("\n[+] Configuracao salva em .env e .matrix_session.json (ambos ignorados pelo git).")
    print("[*] Agora rode:  python run_bot.py")


if __name__ == "__main__":
    main()

