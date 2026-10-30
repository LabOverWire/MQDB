# Sharing

[Back to index](README.md)

Manual tests for sharing ownership-enabled records (`share`, `unshare`, `shares`, `shared`) in agent and cluster mode. Design: [diagram-sharing.md](../design/diagram-sharing.md). Uses password auth, so a grantee is a username.

Setup (agent):
```bash
mqdb passwd alice -b alice -f passwd
mqdb passwd bob -b bob -f passwd
mqdb passwd carol -b carol -f passwd
mqdb passwd admin -b admin -f passwd

mqdb agent start --db /tmp/mqdb-sharing --bind 127.0.0.1:1883 \
    --passwd passwd --admin-users admin --ownership docs=userId

mqdb constraint add docs --fk ref:docs:id:set_null --user admin --pass admin
```

The FK on `ref` is a self-reference, which is what a cascade follows. Every command below is followed by `--user <name> --pass <name>` for the user named in the comment. Denials return `{"code":403,...}` and exit non-zero; the message is `forbidden` in agent mode and `permission denied` in cluster mode.

---

## 49. View and Edit Grants

```bash
mqdb create docs -d '{"id":"d1","userId":"alice","title":"D1"}'   # alice
mqdb read docs d1                                                  # bob: 403
mqdb share docs d1 bob --permission view                           # alice
mqdb read docs d1                                                  # bob: ok
mqdb update docs d1 -d '{"title":"X"}'                             # bob: 403
mqdb share docs d1 bob --permission edit                           # alice
mqdb update docs d1 -d '{"title":"EditedByBob"}'                   # bob: ok
mqdb delete docs d1                                                # bob: 403, delete is owner-only
mqdb share docs d1 carol                                           # bob: 403, only the owner or an admin shares
```

**Expected:** each share returns `{"status":"shared","grantee":"bob","permission":...,"resources_shared":1}`. Sharing again at `view` demotes an `edit` grant.

## 50. Listing Grants

```bash
mqdb shares docs d1     # alice: one grant, grantee "bob", permission "edit"
mqdb shares docs d1     # bob: 403, owner/admin only
mqdb shared docs        # bob: the d1 record itself (title "EditedByBob")
mqdb list docs          # bob: [] (list stays owner-filtered; shared records come from `shared`)
```

## 51. Cascade, Unshare, Delete

```bash
mqdb create docs -d '{"id":"d2","userId":"alice","title":"D2"}'               # alice
mqdb create docs -d '{"id":"d3","userId":"alice","title":"D3","ref":"d2"}'    # alice
mqdb share docs d3 bob                     # alice: resources_shared 2 (d3 and d2)
mqdb read docs d2                          # bob: ok
mqdb unshare docs d3 bob                   # alice: {"status":"unshared"}
mqdb read docs d2                          # bob: 403
mqdb share docs d3 carol --no-cascade      # alice: resources_shared 1

mqdb unshare docs d1 bob                   # alice
mqdb read docs d1                          # bob: 403
mqdb share docs d1 bob                     # alice
mqdb delete docs d1                        # alice
mqdb create docs -d '{"id":"d1","userId":"alice","title":"D1 again"}'         # alice
mqdb shares docs d1                        # alice: [] (delete cleared the grants)
mqdb read docs d1                          # bob: 403
```

## 52. Cluster

Start a 3-node cluster as in [10-cluster.md](10-cluster.md), adding `--passwd passwd --admin-users admin --ownership docs=userId` to every `mqdb cluster start`, and add the FK constraint with `--broker 127.0.0.1:1883`. Then run sections 49-51 with a different `--broker` per step (for example, create and share on 1883, read and update as bob on 1884 and 1885), so the requests are forwarded to the record's primary.

**Expected:** the same results as in agent mode, except for these differences:
- `mqdb read docs d2` after `unshare docs d3 bob` still succeeds, because unshare never cascades in a cluster ([#247](https://github.com/LabOverWire/MQDB/issues/247)).
- `share docs d3 bob` returns `resources_shared: 2` only when d2's partition has the same primary as d3. Otherwise it returns 1 and bob gets 403 on d2. A cascade skips a reference held by another primary and does not follow references beyond it ([#122](https://github.com/LabOverWire/MQDB/issues/122)).
- A raw MQTT share without a `cascade` field does not cascade ([#248](https://github.com/LabOverWire/MQDB/issues/248)); the CLI always sends the field:
  ```bash
  mosquitto_rr -h 127.0.0.1 -p 1883 -u alice -P alice -t '$DB/docs/d3/share' -e resp/share \
      -m '{"grantee":"carol","permission":"view"}' -W 5
  ```

## Verification Checklist

- [ ] View grant allows read, denies update; edit grant allows update; delete stays owner-only
- [ ] Only the owner or an admin can share or list `shares`; `shared` returns the granted records
- [ ] Cascade grants the referenced record; `--no-cascade` grants only the named record
- [ ] Unshare revokes access (agent: across the cascade too)
- [ ] Deleting a record clears its grants
- [ ] Cluster: the same results across nodes, with the differences listed in section 52
