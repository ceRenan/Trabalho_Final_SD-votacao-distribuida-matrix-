# Votação e Enquete Distribuída via Protocolo Matrix

Trabalho prático da disciplina de **Sistemas Distribuídos**.  
Implementação de um sistema de votação e enquetes utilizando a **Matrix Client-Server API v3**, com votos representados por eventos personalizados (`br.ufc.dc.sd.poll.vote`), estado da enquete armazenado em *State Events* (`br.ufc.dc.sd.poll.state`) e apuração realizada por um bot em Python.

---

## Estrutura do Projeto

- `src/matrix_client.py` — Cliente HTTP para a API Client-Server v3 do Matrix (`/login`, `/refresh`, `/sync`, `/send`, `/state`), com *retry* e controle de idempotência por `txnId`.
- `src/poll_engine.py` — Motor de apuração: regras de negócio da votação, controle de 1 voto por usuário (*Last-Write-Wins* ou *First-Write-Wins*), rejeição de votos pós-encerramento e reconstrução de estado.
- `src/bot.py` — Bot apurador que consome o `/sync` da sala, processa eventos de voto e comandos do chat e atualiza os *State Events*.
- `src/config.py` — Leitura do `.env` e persistência de sessão (`.matrix_session.json`).
- `src/mock_homeserver.py` — Servidor Matrix local em memória (Flask) para execução offline e testes automatizados.
- `setup_real_matrix.py` — Script interativo para configurar a conexão com uma sala real no Element (`matrix.org`).
- `run_bot.py` — Inicia o bot conectado ao servidor configurado no `.env`.
- `demo.py` — Demonstração local que sobe o servidor, o bot e 5 eleitores, executando 7 cenários de teste.
- `tests/test_system.py` — Testes unitários e de integração (`unittest`).

---

## Arquitetura e Funcionamento

```
+-------------------------------------------------------------------------+
|                        HOMESERVER MATRIX (DAG)                          |
|            Sala: !sala:matrix.org ou servidor local (API v3)            |
|  - Eventos de Timeline: br.ufc.dc.sd.poll.vote e m.room.message         |
|  - Eventos de Estado  : br.ufc.dc.sd.poll.state (state_key = poll_id)   |
+-------------------+---------------------------------+-------------------+
                    ^                                 ^
   PUT /state       | GET /v3/sync                    | PUT /send/.../{txnId}
   (Salva placar)   | (Long-Polling + next_batch)     | (Votos e comandos)
                    v                                 v
+-----------------------------------+   +---------------------------------+
|      BOT APURADOR (Python)        |   |       CLIENTES / ELEITORES      |
|  - Cliente HTTP Matrix (API v3)   |   |  - Element (Web / Desktop / App)|
|  - Motor de apuracao (poll_engine)|   |  - Botoes nativos ou comandos   |
|  - Recuperacao via GET /state     |   |    (!criar_enquete, !votar...)  |
+-----------------------------------+   +---------------------------------+
```

### Relação com Conceitos de Sistemas Distribuídos
1. **Eventos Personalizados (`br.ufc.dc.sd.poll.vote`):** cada voto é registrado no histórico (DAG) da sala via `PUT /_matrix/client/v3/rooms/{roomId}/send/br.ufc.dc.sd.poll.vote/{txnId}`. O bot também aceita cliques nos botões de enquete nativa do Element (`org.matrix.msc3381.poll.response`) e o comando `!votar`, convertendo-os para o mesmo fluxo de apuração.
2. **Replicação de Estado (`br.ufc.dc.sd.poll.state`):** o estado de cada enquete (pergunta, opções, status `OPEN`/`CLOSED`, votos por usuário, placar e auditoria) é gravado em um *State Event* indexado por `state_key = poll_id`.
3. **Tolerância a Falhas (*Crash Recovery*):** ao iniciar, o bot consulta `GET /_matrix/client/v3/rooms/{roomId}/state` para restaurar as enquetes já existentes na sala e faz o *catch-up* dos votos enviados enquanto estava offline, sem recontar votos já apurados.
4. **Concorrência por Usuário:** os votos são indexados pelo usuário Matrix (`@usuario:servidor`). No modo padrão (`CHANGE_ALLOWED`), votos repetidos do mesmo usuário atualizam sua escolha anterior (*Last-Write-Wins* ordenado por `(origin_server_ts, event_id)`). No modo `STRICT_ONCE` (`!criar_enquete_unica`), apenas o primeiro voto é aceito.
5. **Idempotência (`txnId`):** falhas de rede durante o envio de um evento são tratadas com retransmissão usando o mesmo `txnId`, evitando duplicação no servidor.
6. **Ordenação e Encerramento:** após o comando `!encerrar`, o *State Event* passa para `CLOSED` e qualquer evento de voto subsequente no fluxo do `/sync` é rejeitado.

---

## Comandos Disponíveis na Sala

| Comando | Descrição |
| :--- | :--- |
| `!criar_enquete "Pergunta?" "Op1" "Op2"` | Cria uma enquete aberta (permite trocar o voto). |
| `!criar_enquete_unica "Pergunta?" "Op1" "Op2"` | Cria uma enquete onde cada usuário só pode votar uma única vez. |
| `!votar <id_enquete> <num_opcao>` | Registra um voto na enquete (emite um evento `br.ufc.dc.sd.poll.vote`). |
| `!parcial [id_enquete]` | Mostra o placar parcial com barras de progresso e porcentagens. |
| `!encerrar [id_enquete]` | Encerra a enquete (apenas quem criou pode encerrar) e exibe o resultado final. |
| `!auditar [id_enquete]` | Lista os eventos de voto recebidos na sala e a decisão da apuração para cada um. |
| `!enquetes` | Lista todas as enquetes salvas nos *State Events* da sala. |
| `!ajuda` | Exibe o resumo dos comandos no chat. |

---

## Como Executar

### 1. Instalar dependências
É necessário Python 3.10 ou superior:

```bash
python -m pip install -r requirements.txt
```

### 2. Executar a demonstração local (sem precisar de conta externa)
O script `demo.py` inicia o servidor Matrix local na porta `8008`, conecta o bot e 5 eleitores simulados e executa 7 cenários (criação de enquete, votação, troca de voto, falha de rede com *retry* idempotente, queda e recuperação do bot, voto após encerramento e auditoria):

```bash
python demo.py
```

Para abrir um terminal interativo ao final da demonstração e testar comandos manualmente:

```bash
python demo.py -i
```

### 3. Executar conectado ao servidor Matrix do DC/UFC (`matrix.dc.ufc.br`) ou `matrix.org`
Como o servidor do Departamento de Computação (`https://matrix.dc.ufc.br`) utiliza login SSO via Authentik (`authentik.dc.ufc.br`), a conexão é feita pelo **Token de Acesso (Access Token)** da sua conta já logada no Element:

1. Acesse [https://app.element.io](https://app.element.io) conectado ao servidor `matrix.dc.ufc.br`.
2. Pegue o seu **Token de Acesso**: clique no seu perfil (canto superior esquerdo) -> **Todas as configurações** -> **Ajuda e sobre** -> role até **Avançado** -> clique em **Token de acesso** e copie o token.
3. No terminal do projeto, execute o assistente:
   ```bash
   python setup_real_matrix.py
   ```
   - Aperte `Enter` para usar o servidor padrão `https://matrix.dc.ufc.br`.
   - Escolha a opção `1` (Access Token) e cole o token copiado.
   - Escolha criar uma sala nova sem criptografia (e convide os colegas do grupo, ex.: `@colega:matrix.dc.ufc.br`) ou informe o ID de uma sala de testes já criada pelo grupo.
4. Inicie o bot:
   ```bash
   python run_bot.py
   ```
5. No Element, abra a sala criada e envie `!ajuda` ou `!criar_enquete "Qual algoritmo usar?" "Raft" "Paxos" "PBFT"`. *(O bot aceita comandos enviados tanto pela própria conta que está rodando o script quanto pelos demais integrantes do grupo na sala).*

### 4. Executar os testes automatizados
```bash
python -m unittest discover -s tests -v
```

