# Sharing

[Back to index](README.md)

Manual tests for sharing ownership-enabled records (`share`, `unshare`, `shares`, `shared`) in agent and cluster mode. Design: [diagram-sharing.md](../design/diagram-sharing.md). Uses password auth, so a grantee is a username.

Setup (agent):
```bash
mqdb passwd alice -b alice -f passwd
mqdb passwd bob -b bob -f passwd
mqdb passwd carol -b carol -f passwd
mqdb passwd dave -b dave -f passwd
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

**Expected:** both `share` calls print the following (the second with `"permission": "edit"`):
```json
{
  "data": {
    "grantee": "bob",
    "permission": "view",
    "resources_shared": 1,
    "status": "shared"
  },
  "status": "ok"
}
```
Sharing again at `view` demotes an `edit` grant.

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
mqdb unshare docs d3 bob                   # alice: see below
mqdb read docs d2                          # bob: 403
mqdb share docs d3 carol --no-cascade      # alice: resources_shared 1

mqdb unshare docs d1 bob                   # alice
mqdb read docs d1                          # bob: 403
mqdb share docs d1 bob                     # alice
mqdb delete docs d1                        # alice
mqdb create docs -d '{"id":"d1","userId":"alice","title":"D1 again"}'         # alice
mqdb shares docs d1                        # alice: [] (the direct delete cleared the grants; an FK cascade delete does not, #253)
mqdb read docs d1                          # bob: 403
```

`unshare` prints:
```json
{
  "data": {
    "grantee": "bob",
    "status": "unshared"
  },
  "status": "ok"
}
```

## 52. Cluster

Start a 3-node cluster with the same password file, which must contain `admin` (`dev start-cluster` adds `--admin-users admin`). Then add the FK constraint:
```bash
mqdb dev start-cluster --nodes 3 --clean --passwd passwd --ownership docs=userId --license <key>
mqdb constraint add docs --fk ref:docs:id:set_null --broker 127.0.0.1:1883 --user admin --pass admin
```

Run sections 49 and 50 with a different `--broker` per step, so requests are forwarded to the record's primary. For example, create and share on 1883, and read and update as bob on 1884 and 1885. **Expected:** the same results as in agent mode.

A cluster cascade grants only referenced records held by the shared record's primary ([#122](https://github.com/LabOverWire/MQDB/issues/122)), so the result depends on where the records live. Set the pair's ids in shell variables, create the pair, and share it:
```bash
D2=d2 D3=d3
mqdb create docs -d "{\"id\":\"$D2\",\"userId\":\"alice\",\"title\":\"D2\"}" --broker 127.0.0.1:1883              # alice
mqdb create docs -d "{\"id\":\"$D3\",\"userId\":\"alice\",\"title\":\"D3\",\"ref\":\"$D2\"}" --broker 127.0.0.1:1883  # alice
mqdb share docs "$D3" bob --broker 127.0.0.1:1884                                                        # alice
```
- `resources_shared: 2` means `$D2` has the same primary as `$D3`. Continue with this pair.
- `resources_shared: 1` means `$D2` is held by another primary and was not granted, so `mqdb read docs "$D2"` as bob returns 403 (#122). Set new ids (`D2=d2b D3=d3b`, ...) and repeat the three commands until a share reports 2. Every command below uses the variables, so it runs against the pair that reported 2.

With that pair:
```bash
mqdb unshare docs "$D3" bob --broker 127.0.0.1:1885   # alice
mqdb read docs "$D3" --broker 127.0.0.1:1884          # bob: 403
mqdb read docs "$D2" --broker 127.0.0.1:1884          # bob: ok, unshare does not cascade in a cluster (#247)
```

On the same pair, a raw MQTT share without a `cascade` field does not cascade in a cluster ([#248](https://github.com/LabOverWire/MQDB/issues/248)). The CLI share that follows grants `$D2`, which shows the pair is co-located:
```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u alice -P alice -t "\$DB/docs/$D3/share" -e resp/share \
    -m '{"grantee":"dave","permission":"view"}' -W 5
mqdb read docs "$D2" --broker 127.0.0.1:1884          # dave: 403
mqdb share docs "$D3" dave --broker 127.0.0.1:1883    # alice: resources_shared 2 (the CLI sends cascade: true)
mqdb read docs "$D2" --broker 127.0.0.1:1884          # dave: ok
```
**Expected:** the raw share prints `{"data":{"grantee":"dave","permission":"view","resources_shared":1,"status":"shared"},"status":"ok"}`. In agent mode the same raw share reports `"resources_shared":2`.

## Verification Checklist

- [ ] View grant allows read, denies update; edit grant allows update; delete stays owner-only
- [ ] Only the owner or an admin can share or list `shares`; `shared` returns the granted records
- [ ] Cascade grants the referenced record; `--no-cascade` grants only the named record
- [ ] Unshare revokes access (agent: across the cascade too)
- [ ] A direct delete clears the record's grants
- [ ] Cluster: the same results across nodes, with the differences shown in section 52 (the full list of cluster gaps is in README.md under "Sharing Operations")
