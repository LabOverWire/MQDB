# 7. Discussion

## 7.1 Applicability

MQDB's unified architecture is designed for a specific class of workloads, and we state its boundaries explicitly.

**Where the architecture fits.** The system is most appropriate for IoT data management where devices already communicate via MQTT, edge and fog computing deployments where operational simplicity is a primary concern (one binary replaces three or more systems), and applications that require both real-time publish/subscribe messaging and persistent CRUD operations on the same data — for example, a sensor network where devices publish readings that must be both stored for later query and forwarded to real-time dashboards.

**Where the architecture does not fit.** MQDB is not a replacement for analytical databases, relational systems requiring multi-table joins, applications depending on SQL expressiveness (aggregation, grouping, subqueries, window functions), or workloads requiring strong linearizability guarantees across the entire dataset. An IoT deployment that needs complex time-series analytics should use Apache IoTDB or TimescaleDB; one that needs relational queries should use PostgreSQL. MQDB addresses the operational data management layer — the system that sits between devices and the analytics tier — not the analytics tier itself.

## 7.2 Consistency Trade-offs

MQDB's consistency model is a deliberate design choice, not an oversight, and we characterize it precisely.

**What is guaranteed.** Within a single partition, writes are applied in the order assigned by the primary (per-partition causal ordering). A client reading from the primary after a successful write will observe the write. Change events (CDC) are delivered in sequence order within each entity.

**What is not guaranteed.** Cross-partition ordering is not guaranteed — two writes to different partitions may be observed in different orders by different clients. Cluster-wide linearizability is not provided. If the primary fails after applying a write locally but before the replica receives it, the write is lost. In cluster mode, cross-partition side effects — unique constraint commits, secondary index updates — are fire-and-forget: if the message is lost, the data write has already succeeded, and the constraint or index state is temporarily inconsistent until TTL-based cleanup (30 seconds for unique constraints). In agent mode, this gap does not exist because the storage engine commits data, indexes, and constraints in a single atomic batch. These properties place MQDB in the same consistency class as Redis replication and MongoDB with write concern `w:1`.

**The durability gap.** The current implementation returns success to the client after the primary applies a write locally, without waiting for replica acknowledgment. The infrastructure for stronger durability guarantees exists: a `QuorumTracker` component can await replica confirmation before returning success. This mechanism is currently used only for session creation (where losing a session causes client disconnection). Activating it for data writes would provide at-least-replica-acknowledged durability at the cost of write latency — a classic availability-consistency trade-off.

**Comparison to consensus-per-write systems.** Systems like CockroachDB [cockroachdb] and Spanner provide linearizability by running consensus (Raft or Paxos) on every write. MQDB runs Raft only for control plane decisions (partition assignment, cluster membership). This is the same architectural choice made by Redis Cluster and MongoDB's default write concern: prioritize throughput and latency over strong consistency. For MQDB's target workloads — IoT sensor data, device state management, edge data collection — this trade-off is appropriate. Sensor readings are temporally ordered by their timestamp, not by their commit order, and brief windows of stale reads are acceptable.

## 7.3 The Convergence Thesis

The systems surveyed in Section 2 reveal a convergence pattern: messaging systems are growing database capabilities, and databases are growing messaging interfaces. MQDB contributes to this thesis by demonstrating the mapping in the opposite direction from most prior work.

Kafka began as a distributed log and grew an internal metadata database (KRaft). EMQX began as an MQTT broker and added persistent, replicated storage (Durable Storage). Both approached storage from the messaging side — adding persistence to a system that already handled message routing.

Harper approached from the database side — exposing an existing database through MQTT topic semantics. MQDB shares this direction but takes it further: the topic hierarchy is not merely an interface to the database but the foundation of the entire system — partition routing, replication addressing, constraint enforcement, and change notification are all expressed in topic semantics.

The contribution of this paper is not that unification is possible (Harper and others have demonstrated this pragmatically) but that the mapping is formalizable, that it is sufficient for a specific and practically useful class of database operations, and that the resulting architecture achieves competitive performance while eliminating the two-system problem described in Section 1. The architectural-workload experiments in §6.4 — change-feed delivery, unique-constraint contention, cascade delete — are the quantitative backing for this thesis: each exercises a primitive that the MQTT-as-database mapping supplies natively and that the separated baselines must simulate through bridge code with weaker atomicity guarantees.

## 7.4 Limitations and Future Work

**QoS-to-durability mapping.** The non-mapping described in Section 3.3 is a concrete opportunity. MQTT QoS levels could map to database durability semantics: QoS 0 for primary-only acknowledgment (current behavior), QoS 1 for single-replica confirmation (using the existing QuorumTracker), and QoS 2 for full-quorum durability. This would extend the topic-to-database mapping to include durability as a protocol-level parameter, without introducing any new protocol mechanisms.

**Multi-record transactions.** MQTT has no BEGIN/COMMIT semantics. Each publish is an independent operation. In agent mode, individual operations are fully atomic — a single fjall `WriteBatch` commits record bytes, every touched secondary-index entry, every constraint-reservation entry, and the change-event outbox row together, and the broker publishes the Request/Response reply only after that commit returns (§6.1). In cluster mode, individual operations are atomic within their partition, but cross-partition side effects (constraint commits, index updates) are eventually consistent. Implementing multi-record atomicity across partitions would require either a transaction coordinator layered on top of MQTT topics (e.g., `$DB/_tx/begin`, `$DB/_tx/{txid}/commit`) or a saga pattern using compensating operations. Both approaches would add complexity that may conflict with the system's design goal of simplicity.

**Cross-entity operations.** The topic-to-database mapping addresses one entity per operation. Joins, cross-entity aggregations, and materialized views are not supported. Reactive aggregates — where a subscription to one entity triggers computation across related entities — are architecturally possible (MQTT subscriptions could drive materialized view maintenance) but are not implemented.

**Vault in cluster mode.** Extending field-level encryption to distributed deployments requires distributing per-user encryption keys across nodes while maintaining the zero-knowledge property. This is a key management problem that existing systems (e.g., HashiCorp Vault, AWS KMS) solve through dedicated infrastructure. Integrating such a protocol into MQDB's cluster mode is future work.

**Subscription snapshots.** Current CDC subscriptions deliver future events only. A useful extension would be initial state delivery — when a client subscribes to `$DB/users/events/#`, it could optionally receive the current state of all records before receiving subsequent change events. This is analogous to Kafka's "replay from beginning" capability and would require a snapshot mechanism coordinated with the subscription start point.

**Correlation data in cluster mode.** The MQTT 5.0 request-response pattern works correctly in agent mode but has a known issue in cluster mode where correlation data is not preserved through the internal forwarding path. This is an implementation bug, not an architectural limitation — the wire protocol supports correlation data, but the final publish step drops it. Fixing this is straightforward engineering work.

**Cluster scalability evaluation.** The evaluation in Section 6 covers single-node agent mode. Measuring cluster scalability — throughput scaling across nodes, partition routing overhead, replication cost, and mesh topology impact — requires a multi-machine deployment with isolated network hardware. This evaluation is planned for a follow-up study using the cluster architecture described in Section 4.2.
