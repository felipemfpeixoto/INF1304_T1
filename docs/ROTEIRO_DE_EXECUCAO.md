# Roteiro de Execução, Testes e Mapa de Requisitos

Este documento serve a três usos: **(1)** subir o sistema do zero, **(2)** demonstrar cada requisito funcionando e **(3)** mostrar, item por item, onde cada requisito é atendido no código.

Referências de código estão no formato `arquivo` › símbolo. Toda evidência citada foi **gerada por script** e fica em `reports/logs/` (`make evidence` a regenera do zero).

---

## Parte 1: Preparação (uma vez)

```bash
docker info >/dev/null && echo "Docker ok"     # Docker Desktop/Engine ativo
make clean                                       # ambiente limpo (apaga volumes do projeto)
make build                                       # constrói as imagens
make up                                          # ~1 min: 3 brokers saudáveis, tópico criado, sensores e consumidores no ar
make test-cluster                                # T0: confirma que está tudo certo (deve terminar em PASS)
```

Layout sugerido para acompanhar (3 terminais lado a lado):

| Terminal | Comando | Para mostrar |
|---|---|---|
| A | `make logs` | consumidores processando, alertas e banners de rebalanceamento |
| B | `while true; do clear; docker compose exec -T kafka-1 kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group smartfactory-processors; sleep 2; done` | quem consome qual partição, offsets e lag |
| C | (comandos do roteiro) | ações e testes |

---

## Parte 2: Roteiro de demonstração (~15 min)

Cada passo diz **o que rodar**, **o que aparece** e **o que observar**.

### Passo 1: Arquitetura no ar (2 min)
- **Rodar:** `make health`
- **Aparece:** 3 brokers `healthy`, 4 sensores, 3 consumidores; quórum KRaft com `CurrentVoters: [1,2,3]`; tópico `dados-sensores` com `PartitionCount: 3`, `ReplicationFactor: 3`, `min.insync.replicas=2`; grupo com 1 partição por consumidor.
- **Observar:** líderes de partição espalhados entre brokers; ISR completo; alertas já acumulando em `alerts.log`.

### Passo 2: Sensores, dados e alertas (2 min)
- **Rodar:** `make logs` (terminal A) e `make alerts`.
- **Aparece:** linhas `[NORMAL]` e `[ALERTA WARNING|CRITICAL]` com sensor, partição, offset e motivos; JSON de alerta com `consumer_id`, `partition`, `offset`, `reasons`, `telemetry`.
- **Observar:** os limites vêm de `config/sensor_thresholds.env` (sem constantes no código).

### Passo 3: Balanceamento entre consumidores (1 min)
- **Rodar:** `make test-balance` (T1).
- **Aparece:** cada consumidor com exatamente 1 partição; sensores por partição (chave `sensor_id`); banners `PARTITIONS ASSIGNED`.
- **Evidência:** `reports/logs/T1_balanceamento.log`.

### Passo 4: Queda de consumidor e rebalanceamento (3 min)
- **Rodar:** `make test-consumer-fail MODE=graceful` e depois `make test-consumer-fail MODE=hard` (T2).
- **Aparece:** o consumidor dono de uma partição **com tráfego** é derrubado; outro assume a partição; offsets continuam avançando; banners `PARTITIONS REVOKED/ASSIGNED` nos sobreviventes; o consumidor volta e o 1:1 é restaurado.
- **Observar:** no modo `graceful` a reatribuição é quase imediata (o consumidor avisa que saiu); no modo `hard` leva cerca de `session.timeout.ms` (10 s), porque o coordenador só percebe pela **ausência de heartbeats**. Os dois tempos medidos ficam nos logs.
- **Evidência:** `reports/logs/T2_falha_consumidor_graceful.log`, `T2_falha_consumidor_hard.log`.

### Passo 5: Queda de broker Kafka (3 min)
- **Rodar:** `make test-broker-fail MODE=hard KIND=controller` (T3).
- **Aparece:** líder do quórum KRaft derrubado com SIGKILL; nova eleição (epoch aumenta); líderes de partição migram; ISR cai para 2 (`min.insync.replicas=2` mantém as escritas); sensores e consumidores seguem; carga contínua com `acks=all` termina com **delta de offsets == mensagens enviadas** (zero perda/duplicação); broker religado volta ao ISR.
- **Evidência:** `reports/logs/T3_falha_broker_hard_controller.log` (mais `T3_falha_broker_graceful_follower.log`).
- **Wireshark (opcional, 2 min):** `make capture-pcap` (T9) e abrir `reports/pcaps/failover_broker.pcap` (ver Parte 5).

### Passo 6: Elasticidade e carga (3 min)
- **Rodar:** `make test-elasticity` (T5) e, se houver tempo, `make test-load` (T4) e `make test-sensors` (T6).
- **Aparece (T5):** o mesmo lote de mensagens é esvaziado em menos tempo com 2 e 3 consumidores; com 4 e 5 **não há ganho** (excedentes ociosos, "hot standby").
- **Aparece (T4):** surto de 100 mil mensagens, lag sobe e é absorvido até zero.
- **Aparece (T6):** vazão cresce ao adicionar sensores (`make scale-sensors N=8`), distribuídos pelas partições.
- **Evidência:** `T5_elasticidade.log`, `T4_carga.log`, `T6_escala_sensores.log`.

### Passo 7: O limite do sistema (1 min, opcional)
- **Rodar:** `make test-two-brokers` (T7).
- **Aparece:** com 2 de 3 brokers fora, escritas `acks=all` são **rejeitadas** (sem quórum/ISR); o sistema recupera sozinho ao religar.
- **Observar:** é uma escolha consciente (consistência acima de disponibilidade); documentado na seção "o que não funcionou" do relatório.

### Passo 8: Suíte completa
- `make test-all` roda tudo e gera `reports/logs/RESULTADO_TESTES.txt` (tabela PASS/FAIL). `make evidence` faz `clean` + `up` + `test-all` do zero (~15-20 min).

---

## Parte 3: Matriz de testes

Todos falham com código de saída ≠ 0 se alguma verificação falhar. "Evidência" é o arquivo em `reports/logs/`.

| ID | Comando | Assunto | Verificações principais (asserções) | Evidência |
|---|---|---|---|---|
| T0 | `make test-cluster` | Cluster | 3 brokers `healthy`; 4 produtores e 3 consumidores; quórum com 3 votantes; P=3, R=3, `min.insync.replicas=2`; ISR completo; **as 3 partições recebem dados** | `T0_cluster_inicial.log` |
| T1 | `make test-balance` | Balanceamento | 3 membros, 1 partição cada, sem ociosos; sensores distribuídos por partição; listener registrou atribuições | `T1_balanceamento.log` |
| T2 | `make test-consumer-fail MODE=graceful\|hard` | Falha de consumidor | partição de tráfego reatribuída; 2 membros restantes; offset avança; lag baixo; banners; volta a 1:1; em `hard`, detecção ≈ `session.timeout.ms` | `T2_falha_consumidor_*.log` |
| T3 | `make test-broker-fail MODE=… KIND=…` | Falha de broker | líderes realocados; ISR=2; novo líder do quórum (epoch↑) quando é o controller; sensores gravando; 0 lotes descartados; consumidores ok; **delta de offsets == enviadas**; ISR restaurado | `T3_falha_broker_*.log`, `T3_perf_*.out` |
| T4 | `make test-load` | Carga | vazão com `acks=all`+R=3; lag absorvido; todas as mensagens consumidas; grupo estável | `T4_carga.log`, `T4_perf_test.out` |
| T5 | `make test-elasticity` | Elasticidade | tempo de esvaziamento por nº de consumidores; ≥1,8× de ganho de 1→3; sem ganho de 3→4/5; ociosos = N−P | `T5_elasticidade.log` |
| T6 | `make test-sensors` | Escala de produtores | vazão cresce com sensores extras; todas as partições recebem; consumidores sem lag | `T6_escala_sensores.log` |
| T7 | `make test-two-brokers` | Limite de tolerância | escrita `acks=all` rejeitada sem quórum; clientes não crasham; recuperação completa | `T7_dois_brokers_fora.log` |
| T8 | `make test-alerts` | Alertas | arquivo compartilhado entre réplicas; não é truncado por restart; continua recebendo | `T8_persistencia_alertas.log` |
| T9 | `make capture-pcap` | Análise de rede (Wireshark) | `.pcap` gerado; broker derrubado encerra conexões (FIN/RST); tráfego continua com sobreviventes | `T9_captura_pcap.log`, `T9_wireshark_resumo.txt`, `reports/pcaps/failover_broker.pcap` |
| T10 | `make unit-test` | Detecção de anomalias | regras NORMAL/WARNING/CRITICAL nos limites exatos | saída do `unittest` |

---

## Parte 4: Mapa de requisitos → implementação → como comprovar

### 4.1 Metas gerais

| Requisito | Onde implementamos | Como satisfazemos | Como ver funcionando |
|---|---|---|---|
| Balanceamento de carga, elasticidade e failover com Kafka em Docker | `docker-compose.yml` (cluster + serviços), `Makefile`, `scripts/` | Cluster Kafka de 3 brokers em containers; consumidores em grupo (balanceamento); escala de réplicas (elasticidade); testes de queda (failover) | Partes 2 e 3 |
| Monitoramento de sensores de uma fábrica inteligente | `producer/sensor_producer.py` › `SensorTelemetryProducer`; serviços `producer-*` no compose | Quatro setores (linha de produção, refrigeração, empacotamento, fundição), cada um com perfil próprio de temperatura, vibração e consumo | `make logs` |
| Sensores enviam temperatura, vibração e consumo continuamente | `sensor_producer.py` › `generate_telemetry_payload`, `run` | Payload JSON `{sensor_id, setor, temperatura, vibracao, consumo_energia_kw, timestamp}` a cada `INTERVALO_ENVIO_SEG` | `make logs` (linhas `[Msg #N] Enviada por ...`) |
| Processamento em tempo real para detectar anomalias | `consumer/data_processor.py` › `SmartFactoryConsumer.evaluate_telemetry`, `run` | Cada mensagem é avaliada contra limites de aviso e críticos | `make logs` (`[ALERTA ...]`); `make unit-test` |
| **Tolerância a falhas** (o sistema não para se uma parte cair) | Replicação R=3 + `min.insync.replicas=2` + quórum KRaft de 3 (`docker-compose.yml` › `x-kafka-env`); reconexão com backoff (`connect()`); `metadata_max_age_ms` curto | Falha de 1 broker ou de 1 consumidor não interrompe o processamento; o limite (2 brokers) é documentado | T2, T3, T7 |
| **Escala com facilidade** conforme os sensores aumentam | Serviço `sensor-extra` (perfil `scale`) no compose; `make scale-sensors`; chave de partição = `sensor_id` | Novos sensores são réplicas da mesma imagem, sem mudança de código; a carga se espalha pelas partições | T6 |
| **Balanceamento de carga** entre processadores | Consumer group único (`KAFKA_GROUP_ID`); 3 partições; `SmartFactoryRebalanceListener` | O Kafka distribui as partições entre os consumidores do grupo | T1, T5 |

### 4.2 Componentes

| Componente | Onde implementamos | Como satisfazemos | Como ver funcionando |
|---|---|---|---|
| **Sensores** em containers que geram dados periódicos (JSON) | Serviços `producer-linha-producao`, `producer-refrigeracao`, `producer-empacotamento`, `producer-fundicao` (+ `sensor-extra`) em `docker-compose.yml`; imagem `producer/Dockerfile` | 4 containers distintos, mesma imagem, perfil do setor por variáveis de ambiente | `docker compose ps`; T0 |
| Cada sensor publica no tópico `dados-sensores` | `sensor_producer.py` › `connect` (`KafkaProducer`), `run` (`producer.send(topic, key=sensor_id, ...)`); `KAFKA_TOPIC` no `.env` | `acks=all`, chave `sensor_id` | T1 (sensores por partição) |
| **Cluster Kafka** com 2 ou mais instâncias em containers diferentes | `docker-compose.yml` › `kafka-1`, `kafka-2`, `kafka-3` (âncoras `x-kafka`, `x-kafka-env`) | 3 brokers em containers distintos, modo KRaft (broker+controller) | T0 |
| Tópico dividido em partições, com replicação | `Makefile` › `create-topic` (usa `TOPIC_PARTITIONS`, `TOPIC_REPLICATION_FACTOR`); brokers com `KAFKA_MIN_INSYNC_REPLICAS` | 3 partições, fator de replicação 3, ISR mínimo 2 | T0 (`describe --topic`) |
| **Consumidores** em Python que consomem `dados-sensores` | `consumer/data_processor.py` (`kafka-python-ng`); serviço `consumer` (3 réplicas) | Consumidores Python em containers | T0, T1 |
| Detecção de valores acima dos limites | `data_processor.py` › `evaluate_telemetry` (limites `WARN_*`/`MAX_*`) | Classifica `NORMAL`/`WARNING`/`CRITICAL` por temperatura, vibração e potência | T10 |
| Mesmo grupo de consumo, compartilhando a carga (um lê a partição A, outro a B) | `KAFKA_GROUP_ID=smartfactory-processors` em `config/sensor_thresholds.env` | Todas as réplicas usam o mesmo `group.id` | T1 |
| **Logger** de alertas para análise posterior | `data_processor.py` › `record_alert`; volume `alerts-data` no compose; `ALERT_LOG_PATH` | Cada anomalia vira uma linha JSON em `/var/log/smartfactory/alerts.log`, compartilhado entre réplicas | T8; `make alerts` |

### 4.3 Objetivos do projeto

| Objetivo | Onde implementamos | Como satisfazemos | Como ver funcionando |
|---|---|---|---|
| **1.** Cluster Kafka com múltiplos brokers e tópicos com partições e replicação | `docker-compose.yml`; `Makefile` › `up`, `create-topic` | `make up` sobe 3 brokers (com healthcheck) e cria o tópico P=3/R=3 | T0 |
| **2.** Sensores como produtores Kafka em containers Docker distintos | 4 serviços `producer-*` (+ `sensor-extra`) | Cada sensor é um container com `SENSOR_ID` e setor próprios | T0, T6 |
| **3.** Múltiplos consumidores com balanceamento automático | Serviço `consumer` (3 réplicas); grupo único; `SmartFactoryRebalanceListener` | O Kafka atribui as partições; o listener registra `REVOKED`/`ASSIGNED` | T1, T5 |
| **4a.** Parar um broker Kafka e o sistema continua | `scripts/simulate_broker_failure.sh` (`graceful`/`hard`, `controller`/`follower`) | Derruba o broker e verifica failover de líderes, quórum, ISR, produtores/consumidores, **zero perda** e recuperação | T3 (e T9); limite em T7 |
| **4b.** Parar um consumidor e outro assume a partição (rebalanço) | `scripts/simulate_consumer_failure.sh` (`graceful`/`hard`) | Derruba o dono de uma partição com tráfego; outro assume; o processamento continua | T2 |
| **5.** Comportamento sob carga e falhas visível por logs | `scripts/test_carga.sh`, `test_elasticidade.sh`, `capture_failover_pcap.sh`; logs em `reports/logs/` | Curva de lag sob carga, tabela de elasticidade, logs de rebalanceamento e captura de pacotes | T4, T5, T9 |

### 4.4 Artefatos do projeto

| Artefato | Onde | Observação |
|---|---|---|
| Documentação de instalação e uso | `README.md`, `reports/relatorio_tecnico.md` §3 | pré-requisitos e `make build && make up` |
| Explicação da arquitetura | `reports/relatorio_tecnico.md` §2; diagrama no `README.md` §1 | |
| Testes de falha e resultados | `reports/relatorio_tecnico.md` §5; scripts T2, T3, T7 | evidências em `reports/logs/T2_*`, `T3_*`, `T7_*`; números vêm dos logs |
| Automação | `Makefile` | `make help` lista todos os alvos |
| Código-fonte dos produtores e consumidores | `producer/sensor_producer.py`, `consumer/data_processor.py` | Python com DocStrings |
| Dependências | `producer/requirements.txt`, `consumer/requirements.txt` | não há `pom.xml`: produtores e consumidores são Python |
| Serviços | `docker-compose.yml` | 3 brokers, 4 sensores (+extras), consumidores; `docker compose config -q` valida |
| Scripts de simulação de falhas | `scripts/simulate_consumer_failure.sh`, `simulate_broker_failure.sh`, `test_dois_brokers_fora.sh` | modos `graceful` e `hard` |
| Logs de rebalanceamento | `reports/logs/T2_*.log`, `T1_balanceamento.log`, `T5_elasticidade.log` | contêm os banners `PARTITIONS REVOKED/ASSIGNED` capturados dos consumidores |
| Elasticidade | `scripts/test_elasticidade.sh`, `test_escala_sensores.sh`; alvos `scale-*` | T5 e T6 |

### 4.5 Convenções de código

| Convenção | Como está atendida | Como conferir |
|---|---|---|
| Código documentado com DocStrings | Módulos, classes e métodos de `producer/sensor_producer.py` e `consumer/data_processor.py` têm DocString (Args/Returns/Raises); os scripts têm cabeçalho explicativo | `grep -c '"""' producer/sensor_producer.py consumer/data_processor.py` |
| Sem constantes fixas no código | `config/sensor_thresholds.env` (carregado via `env_file` no compose) + `environment:` por serviço; Makefile e scripts leem o mesmo arquivo. Externalizados: limites, tópico/partições/replicação, timeouts de sessão/heartbeat/metadados, retries, backoff, custo de processamento, perfil dos setores | `grep -nE "os.getenv" producer/*.py consumer/*.py`; `README.md` §5 |
| Evidências reprodutíveis | Todos os logs são gerados por script; nada é escrito à mão | cabeçalho de cada log (data, commit, argumentos) |

---

## Parte 5: Wireshark

`make capture-pcap` (T9) executa um container `nicolaka/netshoot` que compartilha a rede do produtor `smartfactory-producer-1` e grava `reports/pcaps/failover_broker.pcap` enquanto o líder do quórum sofre SIGKILL.

Como abrir e o que mostrar:
1. Abrir `reports/pcaps/failover_broker.pcap` no Wireshark.
2. Filtro `kafka`: o Wireshark decodifica o protocolo (Metadata, Produce, Fetch...).
3. Filtro `tcp.flags.fin==1 || tcp.flags.reset==1`: encerramento das conexões com o broker derrubado (o IP do broker derrubado está em `reports/logs/T9_wireshark_resumo.txt`).
4. Filtro `tcp.flags.syn==1 && tcp.flags.ack==0`: novas conexões do cliente.
5. Clique direito num pacote de dados › *Follow › TCP Stream* para ver as mensagens.

Observações: o nome do broker some do DNS do Docker enquanto o container está parado; por isso aparecem avisos `DNS lookup failed` nos clientes Python. Isso é esperado.
