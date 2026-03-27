# Quick Test, Complete Verification & Additions Checklists

[Back to index](README.md)

## Quick Test Checklist

Run through these commands to validate core functionality.

> **Tip:** For quick development testing with the `dev-insecure` feature build, you can use
> `mqdb agent start --db /tmp/quicktest --anonymous` and omit `--user`/`--pass` from all commands.

```bash
# 0. Setup credentials
mqdb passwd admin -b admin -f /tmp/quicktest-passwd

# 1. Start agent
mqdb agent start --db /tmp/quicktest --passwd /tmp/quicktest-passwd --admin-users admin

# 2. CRUD operations (in a second terminal)
mqdb create users --data '{"name": "Test User", "email": "test@example.com"}' --user admin --pass admin
# Note the returned ID (e.g., 1)
mqdb read users <id> --user admin --pass admin
mqdb update users <id> --data '{"age": 25}' --user admin --pass admin
mqdb list users --user admin --pass admin
mqdb delete users <id> --user admin --pass admin

# 3. Filtering and sorting
mqdb create items --data '{"name": "A", "value": 10}' --user admin --pass admin
mqdb create items --data '{"name": "B", "value": 20}' --user admin --pass admin
mqdb create items --data '{"name": "C", "value": 5}' --user admin --pass admin
mqdb list items --filter 'value>5' --sort value:desc --user admin --pass admin

# 4. Schema validation
echo '{"name": {"type": "string", "required": true}}' > /tmp/schema.json
mqdb schema set validated --file /tmp/schema.json --user admin --pass admin
mqdb create validated --data '{}' --user admin --pass admin  # Should fail

# 5. Constraints
mqdb constraint add items --unique name --user admin --pass admin
mqdb create items --data '{"name": "A", "value": 100}' --user admin --pass admin  # Should fail (duplicate name)

# 6. Subscriptions (in separate terminal)
# Terminal 3: mqdb watch items --user admin --pass admin
# Terminal 2: mqdb create items --data '{"name": "D", "value": 15}' --user admin --pass admin

# 7. Consumer groups
mqdb consumer-group list --user admin --pass admin

# 8. Backup
mqdb backup create --user admin --pass admin
mqdb backup list --user admin --pass admin

# 9. Cleanup
mqdb dev kill --agent
rm -rf /tmp/quicktest /tmp/quicktest-passwd
```

---

## Complete Verification Checklist

Run through this checklist to verify MQDB works completely:

### Agent Mode
- [ ] Agent start/stop (with `--passwd`)
- [ ] Agent status returns health JSON
- [ ] CRUD operations (create, read, update, delete, list)
- [ ] Filtering and sorting
- [ ] Output formats (`--format json`, `--format table`, `--format csv`)
- [ ] Schema validation
- [ ] Index management (`index add`)
- [ ] Constraints (unique, not-null, foreign key)
- [ ] Unique constraint enforced on updates (conflict returns 409)
- [ ] Unique constraint allows non-unique field updates
- [ ] Unique constraint old value released on update
- [ ] Cascade delete removes children
- [ ] Set-null nullifies FK field (children remain, version bumped)
- [ ] Cascade delete emits Delete ChangeEvents for children
- [ ] Set-null emits Update ChangeEvents for modified children
- [ ] Watch/Subscribe
- [ ] Subscribe with consumer group (load-balanced mode)
- [ ] Consumer groups (list, show)
- [ ] Backup/Restore

### Agent Mode — Index Range Queries
- [ ] Range filters (>, >=, <, <=) on indexed field return correct results
- [ ] Combined range (>= AND <=) on indexed field returns correct range
- [ ] Range on indexed + equality on non-indexed filters correctly
- [ ] Boundary precision (inclusive vs exclusive) correct

### Cluster Mode
- [ ] Single-node cluster start
- [ ] Multi-node cluster formation
- [ ] Raft leader election
- [ ] Partition assignment (256 partitions distributed)
- [ ] Cross-node data routing
- [ ] Cluster status command
- [ ] Cluster CRUD (create, read, update, delete, list)
- [ ] Cluster list with filters
- [ ] UPDATE merges fields (not replace)
- [ ] Schema set broadcasts to all nodes
- [ ] Schema get works on all nodes

### Cluster Resilience
- [ ] Data persistence on restart
- [ ] Replication verification
- [ ] Primary failure with replica takeover
- [ ] Node rejoin and catch-up

### MQTT Protocol (Cluster)
- [ ] QoS 0 cross-node delivery
- [ ] QoS 1 cross-node delivery
- [ ] QoS 2 cross-node delivery (exactly-once)
- [ ] Retained messages across nodes
- [ ] Retained message survives node failure
- [ ] Single-level wildcard (+) cross-node
- [ ] Multi-level wildcard (#) cross-node
- [ ] LWT cross-node delivery
- [ ] Session persistence across failover
- [ ] No message duplication under load

### Cluster Mode — Index Range Queries
- [ ] Range query returns same results from all nodes
- [ ] Combined range correct from non-owner node
- [ ] Range + non-indexed filter works across nodes
- [ ] Updated records reflected in range queries
- [ ] Deleted records excluded from range queries

### Constraints (Cluster)
- [ ] Unique constraint enforced across nodes (create)
- [ ] Unique constraint enforced across nodes (update conflict returns 409)
- [ ] Unique constraint old value released on update (value recycling)
- [ ] Foreign key validated across nodes
- [ ] Cascade delete removes children across nodes
- [ ] Set-null nullifies FK field across nodes
- [ ] Cascade delete emits ChangeEvents for deleted children
- [ ] Set-null emits Update ChangeEvents for modified children

### Ownership Enforcement
- [ ] List filters by authenticated sender
- [ ] Non-owner update returns 403 Forbidden
- [ ] Non-owner delete returns 403 Forbidden
- [ ] Owner update succeeds
- [ ] Owner delete succeeds
- [ ] Internal (no sender) bypasses ownership
- [ ] Ownership works across cluster nodes

### License Enforcement
- [ ] `mqdb license verify` accepts valid token
- [ ] `mqdb license verify` rejects expired token
- [ ] `mqdb license verify` rejects tampered token
- [ ] Missing `iat` rejected
- [ ] Future `iat` rejected
- [ ] Duration >5 years rejected
- [ ] Wrong issuer rejected
- [ ] Pro tier with cluster feature rejected
- [ ] Agent with vault requires license
- [ ] Cluster mode requires enterprise license
- [ ] Invalid license falls back to free tier (agent)
- [ ] Runtime expiry check shuts down agent after license expires
- [ ] Runtime expiry check shuts down cluster node after license expires

### Monitoring
- [ ] Health endpoint returns correct status
- [ ] Health updates on node failure

### Performance
- [ ] `mqdb bench pubsub` runs successfully
- [ ] `mqdb bench db` runs successfully
- [ ] `mqdb dev bench pubsub` (auto-start) runs successfully
- [ ] `mqdb dev bench db` (auto-start) runs successfully
- [ ] `mqdb dev profile pubsub` produces profiling output
- [ ] `mqdb dev baseline save/list/compare` works correctly

---

## Complete Verification Checklist Additions

### Query Limits (Section 32)
- [ ] 17 filters returns 400 error
- [ ] 5 sort fields returns 400 error
- [ ] 16 filters succeeds (boundary)
- [ ] 4 sort fields succeeds (boundary)

### Sort Edge Cases (Section 33)
- [ ] Records missing sort field sort to front (ascending)
- [ ] Multi-field sort applies correct priority

### Filter Edge Cases (Section 34)
- [ ] Filter on nonexistent field without schema excludes records
- [ ] Filter on nonexistent field with schema returns 400
- [ ] Sort on nonexistent field with schema returns 400
- [ ] Projection on nonexistent field with schema returns 400
- [ ] Contradictory filters return empty set
- [ ] Multiple filters on same field narrows results

### MQTT-Only Features (Section 35)
- [ ] In operator via MQTT payload
- [ ] In operator with empty array returns empty set
- [ ] Includes/relationships via MQTT payload

### Index Interactions (Section 36, Agent Only)
- [ ] Index on existing data backfills correctly
- [ ] Index add is idempotent
- [ ] First Eq filter uses index, rest post-filter

### Cluster Scatter-Gather (Section 37)
- [ ] Sort consistency across all nodes
- [ ] Filter+sort consistent across all nodes
- [ ] No duplicate records in results
- [ ] Pagination works in cluster mode

### License Verification (Section 39-42)
- [ ] `mqdb license verify` shows valid license details
- [ ] Expired license rejected
- [ ] Tampered token rejected
- [ ] Missing `iat` rejected
- [ ] Future `iat` rejected
- [ ] Duration >5 years rejected
- [ ] Wrong/missing issuer rejected
- [ ] `nbf` in future rejected
- [ ] Pro + cluster feature rejected
- [ ] Agent + vault + valid license starts
- [ ] Agent + vault without license fails
- [ ] Cluster + enterprise license starts
- [ ] Cluster without license fails
- [ ] Runtime expiry shuts down agent
- [ ] Runtime expiry shuts down cluster node

### Agent vs Cluster Format (Section 38)
- [ ] Create response format identical
- [ ] List response format identical
- [ ] Delete response format identical
- [ ] ID format identical (partition-prefixed hex in both modes)
- [ ] Validation error messages identical
- [ ] Pagination works in both modes
