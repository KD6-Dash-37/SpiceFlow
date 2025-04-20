# 🔮 SpiceFlow

**SpiceFlow** is a real-time, multi-venue market data system built in Rust. It orchestrates WebSocket connections to crypto exchanges like **Deribit** and **Binance**, managing subscriptions to market data feeds such as order books.

The system is powered by a modular, actor-based architecture — separating concerns like:
- Reference data fetching
- WebSocket orchestration
- Order book management
- Broadcasting of processed data

It’s designed for high performance, scalability, and observability, with an emphasis on reliability and support for rapid multi-exchange expansion.


## 🚀 Features

- 📡 WebSocket streaming from multiple exchanges
- ⚙️ Modular actor-based orchestration system
- 🧮 Real-time order book construction
- 📤 ZeroMQ broadcasting layer
- 🧪 Offline fixtures for dev/testing
- 🔍 Tracing instrumentation


## 🧪 Development Features

The server provides feature flags to support offline development, faster iteration, and isolated actor testing. These can be used individually or combined to streamline your dev workflow.

---

### 🔧 `dev-fixtures`

Enable this feature to **bypass live HTTP APIs** and load instrument metadata from local JSON files. This is useful when you're:

- Developing offline.
- Avoiding rate limits or unstable external dependencies.
- Working with fixed datasets for reproducible testing.

#### ✅ Behavior

- Replaces live HTTP calls (e.g., to Binance) with fixture files.
- Fixtures are loaded from:

```
/fixtures/
```

#### 🚀 Enable it

```bash
cargo run -p server --features dev-fixtures
```

---

### 🧪 `dev-ws-only`

Enable this feature to **run only the WebSocket actor** without spinning up Routers, OrderBooks, or Broadcast layers. Ideal for **early-stage exchange integration** or **isolated debugging**.

#### ✅ Behavior

- Creates a **dummy RouterActor** with no logic.
- Runs a stripped-down [`WorkflowKind::WebSocketOnly`] task chain.
- Initializes and monitors only the WebSocket actor.
- Marks the workflow complete once the first heartbeat is received.

#### 🚀 Enable it

```bash
cargo run -p server --features dev-ws-only
```

Or combine with fixtures:

```bash
cargo run -p server --features "dev-fixtures dev-ws-only"
```

#### 🧰 Use Cases

- Rapid development of WebSocket actor behavior and message handling.
- Debugging connection, parsing, or routing logic independently.
- Integration testing WebSocket streams without full orchestration.

#### ❌ What it skips

- No actual RouterActor, OrderBookActor, or BroadcastActor are created.
- No expectation of subscribe/unsubscribe confirmations.

---
