# SpiceFlow


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
