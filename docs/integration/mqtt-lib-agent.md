# mqtt-lib Integration Reference

Purpose: how MQDB configures MQTT-over-WebSocket in the `mqtt5` broker library, for browser clients.

## Status: implemented

WebSocket support is available in the `mqtt5` broker via `WebSocketConfig`.

---

## Confirmed API

```rust
use mqtt5::{BrokerConfig, WebSocketConfig};

let config = BrokerConfig::new()
    .with_bind_address("0.0.0.0:1883".parse().unwrap())  // TCP
    .with_websocket(
        WebSocketConfig::new()
            .with_bind_address("0.0.0.0:8080".parse().unwrap())
            .with_path("/mqtt")
    );

let broker = Broker::new(config);
broker.run().await?;
```

**WebSocketConfig builder methods:**

| Method | Description |
|--------|-------------|
| `with_bind_address(addr)` | Address to listen on |
| `with_path(path)` | WebSocket endpoint path (default: `/mqtt`) |
| `with_tls(use_tls: bool)` | Enable TLS (WSS) |

The `mqtt` subprotocol and binary frame handling are managed internally by the codec.

---

## Testing

```bash
# Test with websocat
websocat ws://127.0.0.1:8080/mqtt

# Test with browser console
const ws = new WebSocket('ws://127.0.0.1:8080/mqtt', 'mqtt');
ws.binaryType = 'arraybuffer';
ws.onopen = () => console.log('Connected');
ws.onmessage = (e) => console.log('Received:', e.data);
```

Browser clients use the `mqtt5-wasm` crate; the server uses `mqtt5` with `WebSocketConfig`.
