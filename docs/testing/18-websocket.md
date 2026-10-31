# WebSocket Listener

[Back to index](README.md)

Manual tests for agent mode's WebSocket listener (`--ws-bind`). Assumes a built CLI per 01-setup, `curl`, the `mqttv5` CLI, and a password file `passwd` with a user `e2e` / `e2epw` (`mqdb passwd e2e -b e2epw -f passwd`).

## Overview

`--ws-bind` serves two things on one port:

| Request | Result |
|---------|--------|
| WebSocket upgrade on `/mqtt` (subprotocol `mqtt`) | MQTT 5 session, same auth and ACL as the TCP listener |
| `GET /health` (query string allowed, with or without upgrade headers) | `200` with `{"status":"ok"}` once the agent is ready |
| Upgrade on any other path | `404` from the WebSocket handshake |
| Plain HTTP request to any other path | connection closed without a response |

The port is bound at startup, but connections are only served once the agent is ready: broker, internal request handler (including its response publisher) and event publisher. Before that, TCP connections complete into the kernel backlog and wait, so a TCP-level load balancer check passes while an HTTP `GET /health` gets no answer until the agent is ready. MQTT over WebSocket depends on that readiness; if the agent never becomes ready, the listener closes the port and only MQTT over TCP works. `/health` reports that startup readiness. It does not track the internal tasks afterwards, so a handler that stopped later would still report `200`.

Limits:
- Reading the request and completing the handshake must finish within 10 seconds.
- At most 128 connections can be between accept and the broker's CONNACK. An enhanced-auth (SCRAM, JWT) exchange before the CONNACK keeps the slot. A connection that upgrades and never completes CONNECT keeps its slot until the broker's 10-second connect timeout closes it. Further connections wait in the kernel backlog until a slot frees.
- `/health` probes use the same slots. While all 128 are held, `/health` waits like any new WebSocket client, so a load balancer sees the same unavailability a client would.
- `/health` is answered before the broker's connection limits are checked. The limits apply to WebSocket upgrades.
- On agent shutdown, connections that have not received their CONNACK are closed at once, connected MQTT 5 sessions get the broker's shutdown DISCONNECT (MQTT 3.1.1 sessions are closed without one), and any connection still open after 5 seconds is closed.

Cluster mode uses mqtt5's built-in WebSocket listener and has no `/health` route.

---

## 53. MQTT over WebSocket

```bash
mqdb agent start --db /tmp/mqdb-ws --bind 127.0.0.1:1883 --ws-bind 127.0.0.1:8080 --passwd passwd

mqttv5 sub --url ws://127.0.0.1:8080/mqtt -t ws/check -n 1 -u e2e -P e2epw --non-interactive &
sleep 1
mqttv5 pub --url ws://127.0.0.1:8080/mqtt -t ws/check -m over-ws -u e2e -P e2epw --non-interactive
wait
mqttv5 pub --url ws://127.0.0.1:8080/mqtt -t ws/check -m x -u e2e -P wrong --non-interactive
```

**Expected:** the subscriber prints `over-ws` and exits. The last publish fails with `Connection refused: BadUsernameOrPassword`.

## 54. Health Check on the WebSocket Port

Continuing from 53:

```bash
curl -i http://127.0.0.1:8080/health
curl -i 'http://127.0.0.1:8080/health?probe=lb'
curl -i http://127.0.0.1:8080/mqtt
curl -i http://127.0.0.1:8080/other -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
    -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ=='
```

**Expected:**
- The first two return `HTTP/1.1 200 OK` with body `{"status":"ok"}`.
- `GET /mqtt` without an upgrade returns nothing: `curl: (52) Empty reply from server`.
- The upgrade on `/other` returns `HTTP/1.1 404 Not Found`.

## 55. An Idle Connection Does Not Block Other Clients

Continuing from 53. Hold one TCP connection that never sends a request, then connect a WebSocket client and probe `/health` while it is open:

```bash
python3 -c "import socket, time; s = socket.create_connection(('127.0.0.1', 8080)); time.sleep(12)" &
sleep 1
time mqttv5 pub --url ws://127.0.0.1:8080/mqtt -t ws/check -m behind-idle -u e2e -P e2epw --non-interactive
curl -s -m 3 http://127.0.0.1:8080/health
wait
```

**Expected:** the publish succeeds in well under a second, and `/health` returns `{"status":"ok"}` while the idle connection is still open.

## 56. WebSocket Port Already in Use

```bash
python3 -c "import socket, time; s = socket.socket(); s.bind(('127.0.0.1', 8081)); s.listen(); time.sleep(10)" &
sleep 1
mqdb agent start --db /tmp/mqdb-ws-taken --bind 127.0.0.1:1884 --ws-bind 127.0.0.1:8081 --passwd passwd
wait
```

**Expected:** the agent exits at startup with an error naming the listener and address, e.g. `WebSocket listener failed to bind 127.0.0.1:8081: Address already in use (os error 48)` (the OS error text varies by platform).
