# mqtt-lib Integration Reference

Purpose: how MQDB configures MQTT-over-WebSocket in the `mqtt5` broker library, for browser clients.

## Status: implemented

WebSocket support is available in the `mqtt5` broker via `WebSocketConfig`.

---

## Confirmed API

```rust
use mqtt5::broker::config::WebSocketConfig;
use mqtt5::broker::{BrokerConfig, MqttBroker};

let config = BrokerConfig::new()
    .with_bind_address(([0, 0, 0, 0], 1883))
    .with_websocket(
        WebSocketConfig::new()
            .with_bind_address(([0, 0, 0, 0], 8080))
            .with_path("/mqtt")
    );

let mut broker = MqttBroker::with_config(config).await?;
broker.run().await?;
```

**WebSocketConfig builder methods:**

| Method | Description |
|--------|-------------|
| `with_bind_address(addr)` | Address to listen on |
| `with_path(path)` | WebSocket endpoint path (default: `/mqtt`) |
| `with_tls(use_tls: bool)` | Enable TLS (WSS) |

The `mqtt` subprotocol and binary frame handling are managed internally by the codec.

Cluster mode uses this API. Agent mode (`mqdb agent start --ws-bind`) does not: MQDB binds the WebSocket port itself
(`crates/mqdb-agent/src/agent/websocket.rs`) so it can answer a plain HTTP `GET /health` with `200` before any
upgrade. Every other request is handed to `mqtt5::broker::websocket_server::accept_websocket_connection` with the
default `/mqtt` path and `mqtt` subprotocol, and the upgraded stream runs in an `mqtt5` `ClientHandler` built from the
broker's router, auth provider, storage, stats and resource monitor.

- The port accepts connections only once the agent is ready (broker, internal handler and event publisher), so
  `/health` never reports `200` for a node that cannot serve requests yet.
- `/health` is answered before the broker's connection limits are checked; those limits apply to WebSocket upgrades.
- Reading the request and completing the handshake must finish within 10 seconds, and connections still in that
  phase are closed when the agent shuts down. Agent shutdown waits for every WebSocket connection to close.

---

## Testing

```bash
# Test with websocat
websocat ws://127.0.0.1:8080/mqtt

# Agent mode health check on the same port
curl -i http://127.0.0.1:8080/health

# Test with browser console
const ws = new WebSocket('ws://127.0.0.1:8080/mqtt', 'mqtt');
ws.binaryType = 'arraybuffer';
ws.onopen = () => console.log('Connected');
ws.onmessage = (e) => console.log('Received:', e.data);
```

Browser clients use the `mqtt5-wasm` crate; the server uses `mqtt5`, through `WebSocketConfig` in cluster mode and through `accept_websocket_connection` plus `ClientHandler` in agent mode.
