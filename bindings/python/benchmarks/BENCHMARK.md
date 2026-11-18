# Benchmark Comparativo: `amqp-rs` vs `amqp-client-python` (0.2.2 / 0.2.3)

Ambiente de Testes:
- **CPU / SO**: Linux x86_64
- **Python**: 3.14.3 (free-threaded / nogil)
- **Broker**: RabbitMQ 3.x (Docker Alpine)
- **Versões**:
  - `amqp-rs`: 0.2.0 (bindings Rust / PyO3)
  - `amqp-client-python`: 0.2.2 (Yanked) / 0.2.3 (Corrigida) (Pure Python / Pika)
  - `uvloop`: 0.22.1

---

## 1. Publicação e Inscrição (Publish / Subscribe)

### A. Sequencial (uvloop) — 300.000 mensagens

Cada mensagem é publicada e aguardada individualmente em loop (`await eventbus.publish(...)`).

| Implementação | Total de Mensagens | Tempo Total | Mensagens / Segundo |
| :--- | :--- | :--- | :--- |
| **Pure Python (`amqp-client-python 0.2.2 / 0.2.3`)** | 300.000 | 164.68 s | **1.821,66 msg/s** |
| **Rust (`amqp-rs 0.2.0`)** | 300.000 | 137.88 s | **2.175,79 msg/s** (~1.2x) |

---

### B. Concorrente (uvloop + `asyncio.gather`) — 300.000 mensagens

Publicações disparadas concorrentemente em batch através de `asyncio.gather(*[publish(...)])`.

| Implementação | Mensagens Testadas | Tempo | Mensagens / Segundo |
| :--- | :--- | :--- | :--- |
| **Pure Python (`amqp-client-python 0.2.2` - Yanked)** | 10.000 | 5.70 s | **1.755,20 msg/s** |
| **Pure Python (`amqp-client-python 0.2.2` - Yanked)** | 30.000 | 37.19 s | **806,80 msg/s** |
| **Pure Python (`amqp-client-python 0.2.2` - Yanked)** | 300.000 | *Degradação O(N²)\** | *Gargalo de confirmação* |
| **Pure Python (`amqp-client-python 0.2.3` - Corrigida)** | 10.000 | 5.49 s | **1.821,50 msg/s** |
| **Pure Python (`amqp-client-python 0.2.3` - Corrigida)** | 30.000 | 18.90 s | **1.587,30 msg/s** |
| **Pure Python (`amqp-client-python 0.2.3` - Corrigida)** | 300.000 | ~180.20 s | **~1.664,80 msg/s** (Linear) |
| **Rust (`amqp-rs 0.2.0` com TLS)** | 300.000 | 17.50 s | **17.139,98 msg/s** |

> **\* Nota Técnica sobre a versão 0.2.2 (Yanked) vs 0.2.3:**
> Na versão 0.2.2, o tratamento de confirmação em lote (`on_delivery_confirmation` em `async_channel.py`) varria todo o dicionário `_deliveries` a cada frame de confirmação (`[tag for tag in list(self._deliveries.keys()) if tag <= delivery_tag]`) sem remover as tags confirmadas de imediato, introduzindo complexidade $O(N^2)$ e reprocessamento contínuo das mesmas tags.
>
> Na **versão 0.2.3**, a confirmação em lote foi refatorada para remoção atômica em $O(1)$ amortizado (`while self._deliveries ... pop(first_tag)` aproveitando a ordem monotônica de entrega do AMQP) com cancelamento imediato de timers de timeout. Isso restaurou a escala linear estrita (~1.600 - 1.820 msg/s) sem travamentos.
>
> Ainda assim, o **`amqp-rs`** atinge **17.139,98 msg/s** sustentados (mais de 10x mais rápido que a implementação pure Python), completando as 300k mensagens concorrentes com confirmações e mTLS em apenas **17.5 segundos**.

---

### C. Paralelo Multi-Threaded (Rust + uvloop) — 300.000 mensagens

Distribuição da carga entre múltiplos núcleos de CPU utilizando `asyncio.to_thread` com instâncias isoladas de `uvloop` compartilhando o barramento nativo do Rust com TLS.

| Implementação | Total de Mensagens | Tempo Total | Mensagens / Segundo |
| :--- | :--- | :--- | :--- |
| **Pure Python (`amqp-client-python 0.2.2`)** | 300.000 | *Não suportado* | *Pika não é thread-safe* |
| **Rust (`amqp-rs 0.2.0` com TLS)** | 300.000 | 27.95 s | **10.733,84 msg/s** |

---

## 2. Remote Procedure Call (RPC)

### Sequencial (asyncio) — 300.000 chamadas RPC

Requisições RPC sequenciais 1-a-1 com resposta e confirmação.

| Implementação | Total de Chamadas | Tempo Total | Chamadas / Segundo |
| :--- | :--- | :--- | :--- |
| **Pure Python (`amqp-client-python 0.2.2`)** | 300.000 | 323.43 s | **927,55 req/s** |
| **Rust (`amqp-rs 0.2.0`)** | 300.000 | 526.26 s | **570,06 req/s** |

> **Nota sobre RPC Sequencial 1-a-1:**
> No cenário sequencial estrito onde apenas 1 mensagem viaja por vez, a pure Python se beneficia da ausência de alternância de contexto FFI por frame. No entanto, em cenários concorrentes de RPC (`uv_rpc_rs_concurrent`), o `amqp-rs` ultrapassa **23.000 req/s**.
