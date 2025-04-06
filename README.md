# 🔮 SpiceFlow

**SpiceFlow** is a real-time, multi-venue market data system built in Rust. It orchestrates WebSocket connections to crypto exchanges like **Deribit** and **Binance**, managing subscriptions to market data feeds such as order books.

The system is powered by a modular, actor-based architecture — separating concerns like:
- Reference data fetching
- WebSocket orchestration
- Order book management
- Broadcasting of processed data

It’s designed for high performance, scalability, and observability, with an emphasis on reliability and support for rapid multi-exchange expansion.

---

## 🚀 Features

- 📡 WebSocket streaming from multiple exchanges
- ⚙️ Modular actor-based orchestration system
- 🧮 Real-time order book construction
- 📤 ZeroMQ broadcasting layer
- 🧪 Offline fixtures for dev/testing
- 🔍 Tracing instrumentation

---


### 🧪 Dev Fixtures

To support offline development and faster iteration, the server supports loading instrument data from local JSON fixtures instead of live HTTP APIs.

#### Usage

Enable the feature flag when running or testing:

```bash
cargo run -p server --features dev-fixtures
```

This swaps out real HTTP calls (e.g., to Binance) for static fixture files located in:

```
/fixtures/
```

#### Notes

- Dev-only logic lives in `server/src/devtools`, gated with `#[cfg(feature = "dev-fixtures")]`.
- Fixture files are not committed to version control.
- To fall back to production behavior, simply omit the flag:

```bash
cargo run -p server
```
