# Sparkplug Aware

[Back to index](README.md)

Manual tests for agent mode's Sparkplug Aware MQTT server (`--sparkplug-aware`). Assumes a built CLI per 01-setup, `mosquitto_pub` and `mosquitto_sub`, and a password file `passwd` with a user `e2e` / `e2epw` (`mqdb passwd e2e -b e2epw -f passwd`).

## Overview

With `--sparkplug-aware`, the agent stores the latest NBIRTH and DBIRTH from each Sparkplug edge node and device. It republishes them retained, at QoS 1, on:

| Birth | Certificate topic |
|-------|-------------------|
| `spBv1.0/<group>/NBIRTH/<edge>` | `$sparkplug/certificates/spBv1.0/<group>/NBIRTH/<edge>` |
| `spBv1.0/<group>/DBIRTH/<edge>/<device>` | `$sparkplug/certificates/spBv1.0/<group>/DBIRTH/<edge>/<device>` |

The broker writes certificates itself; while the feature is on, no client (admins included) may publish to `$sparkplug/...`. Agent mode only (cluster mode: #172).

A new NBIRTH clears that edge's stored DBIRTH certificates, so only devices in the latest birth sequence keep one. Each clear is the standard MQTT retained delete: an empty retained message, which live subscribers to `$sparkplug/certificates/#` also receive. A zero-length message on a DBIRTH certificate topic means "this device's certificate was removed", not a birth with no metrics.

Limitations:
- A DBIRTH from an edge's previous connection that is handled after its new NBIRTH (a reconnect that takes over a live session) is stored and survives. Telling the two apart would need the `bdSeq` metric from the protobuf payload.
- Certificates are ordinary retained messages and stay stored when the agent restarts without `--sparkplug-aware`. Clients can then overwrite them like any topic. Clear one with an empty retained publish: `mosquitto_pub -V mqttv5 -u e2e -P e2epw -r -n -t '$sparkplug/certificates/spBv1.0/<group>/NBIRTH/<edge>'`.

Use MQTT 5 clients (`-V mqttv5`). MQTT 3.1.1 subscribers get MQTT 5 properties in retained payloads until LabOverWire/mqtt-lib#213 is fixed.

---

## 44. Certificates for Births

```bash
mqdb agent start --db /tmp/mqdb-sparkplug --bind 127.0.0.1:1883 --passwd passwd --sparkplug-aware

mosquitto_pub -V mqttv5 -u e2e -P e2epw -t 'spBv1.0/plant/NBIRTH/edge-1' -m 'nbirth-bdseq-0'
mosquitto_pub -V mqttv5 -u e2e -P e2epw -t 'spBv1.0/plant/DBIRTH/edge-1/press-4' -m 'dbirth-press-4'
mosquitto_pub -V mqttv5 -u e2e -P e2epw -t 'spBv1.0/plant/NDATA/edge-1' -m 'ndata'

mosquitto_sub -V mqttv5 -u e2e -P e2epw -t '$sparkplug/certificates/#' -F '%t retain=%r %p' -W 3
```

**Expected:** two retained certificates and nothing for NDATA:
```
$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-1 retain=1 nbirth-bdseq-0
$sparkplug/certificates/spBv1.0/plant/DBIRTH/edge-1/press-4 retain=1 dbirth-press-4
```

## 45. Latest Birth Wins, Removed Devices Are Cleared

Continuing from 44, re-birth the edge without `press-4`, with a new device `press-7`:

```bash
mosquitto_pub -V mqttv5 -u e2e -P e2epw -t 'spBv1.0/plant/NBIRTH/edge-1' -m 'nbirth-bdseq-1'
mosquitto_pub -V mqttv5 -u e2e -P e2epw -t 'spBv1.0/plant/DBIRTH/edge-1/press-7' -m 'dbirth-press-7'

mosquitto_sub -V mqttv5 -u e2e -P e2epw -t '$sparkplug/certificates/#' -F '%t retain=%r %p' -W 3
```

**Expected:** the new NBIRTH and only `press-7`; `press-4` is gone:
```
$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-1 retain=1 nbirth-bdseq-1
$sparkplug/certificates/spBv1.0/plant/DBIRTH/edge-1/press-7 retain=1 dbirth-press-7
```

## 46. Certificates Cannot Be Forged

```bash
mosquitto_pub -V mqttv5 -u e2e -P e2epw -q 1 -r -t '$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-1' -m 'forged'
```

**Expected:** `Publish 1 failed: Not authorized.` Re-running the subscriber from 45 still shows `nbirth-bdseq-1`.

## 47. Restrictive ACL

Restart the agent with an ACL that grants nobody publish rights on `$sparkplug/#`:

```bash
cat > acl <<'EOF'
user * topic $DB/# permission readwrite
user e2e topic spBv1.0/# permission readwrite
user e2e topic $sparkplug/# permission read
EOF
mqdb agent start --db /tmp/mqdb-sparkplug-acl --bind 127.0.0.1:1883 --passwd passwd --acl acl --sparkplug-aware
```

Repeat 44.

**Expected:** the same two certificates. The broker stores them directly, so the ACL does not apply to it.

## 48. Feature Off

Restart the agent without `--sparkplug-aware` and repeat 44.

**Expected:** no certificates. Publishing to `$sparkplug/...` is allowed, as for any other topic.
