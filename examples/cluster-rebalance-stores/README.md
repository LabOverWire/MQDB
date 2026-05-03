# cluster-rebalance-stores E2E

Diagnostic end-to-end test that exercises the partition-snapshot path added
in `crates/mqdb-cluster/src/cluster/store_manager/partition_io.rs` for the
five DB stores beyond `db_data`.

## What it does

1. Starts a 3-node cluster (QUIC transport, mTLS).
2. Registers schemas, a unique constraint, and a foreign-key constraint.
3. Populates posts + a child comment.
4. Joins a 4th node, which forces partitions to rebalance onto it.
5. After rebalance, queries each store via node 4 and asserts behavior.

The 4th-node join is what triggers replica promotion via partition snapshots.
Whatever node 4 ends up serving has to come from the snapshot stream.

## Running

```bash
export MQDB_LICENSE_FILE=/path/to/enterprise.license
./run.sh
```

The script exits 0 when all hard assertions pass, non-zero otherwise.
Build the release binary first (`cargo build --release --bin mqdb`) — the
script invokes `target/release/mqdb` directly.

## What is hard-asserted vs observed

**Hard assertions** (drive the exit code):

- `DbDataStore`: pre-rebalance posts are readable via node 4 — proves the
  partition snapshot is delivering `db_data`. This is regression coverage
  for PR #50.
- Cluster setup, partition rebalance to node 4, schema/constraint creation
  via node 1.

**Observations** (printed but do not affect exit code):

- `SchemaStore`, `ConstraintStore`: schemas and constraints are replicated
  through `PartitionId::ZERO` today (see `node_controller/db_ops.rs` and
  `store_manager/constraint_ops.rs`). The current snapshot fix adds them
  to the export wire stream, but whether a given schema/constraint arrives
  on node 4 depends on whether the partition matching `schema_partition()`
  of the entity actually rebalanced to node 4. This is a cluster design
  issue (schemas should arguably be cluster-wide broadcast state, not
  partition-scoped) that is broader than the snapshot fix.
- `UniqueStore`: duplicates are mostly rejected via node 4, but transient
  acceptances can occur immediately after promotion while 2-phase forwards
  settle. The script retries each duplicate before counting it as accepted.

**Stores not exercised here** (covered by unit roundtrip tests instead):

- `IndexStore`: `mqdb index add` returns "index management is only
  supported in agent mode" today, so there's no public CLI path to drive
  index entries through the cluster. Unit roundtrip:
  `crates/mqdb-cluster/src/cluster/db/index_store.rs::tests::export_import_roundtrip_preserves_partition`.
- `FkValidationStore`: entries are transient (only present while a 2-phase
  FK forward is in flight), making them impractical to capture in a
  snapshot taken at an arbitrary moment. Unit roundtrip:
  `crates/mqdb-cluster/src/cluster/db/fk_store.rs::tests::export_import_roundtrip_preserves_partition`.

## Logs

To save logs from a run for inspection, set `SAVE_LOGS=1`:

```bash
SAVE_LOGS=1 ./run.sh
ls /tmp/cluster-rebalance-stores-logs/
```

Per-node logs (`node1.log`, `node2.log`, `node3.log`, `node4.log`) are
written to `$TEST_DIR` during the run and copied out on exit when
`SAVE_LOGS` is set.
