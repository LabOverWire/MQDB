# Literature Diary

Tracking all sources, search strategies, and findings for the VLDB paper.

---

## Session 1 — 2026-04-11: Initial landscape survey

### Search strategy
Broad multi-angle search: MQTT + database convergence, MQTT broker persistence, IoT database architecture, pub/sub as database primitive.

### Findings by tier

#### Tier 1: Direct competitors (must cite and differentiate)

1. **HarperDB (now Harper)**
   - Closest existing system to MQDB's thesis
   - Tables map to MQTT topic namespaces; retain flag = record upsert
   - Uses LMDB, eventual consistency (no Raft), QoS 0/1 only, no single-level wildcards
   - No academic paper — product docs and blog posts only
   - **Differentiation**: Harper validates the intuition but lacks formal analysis, makes weaker consistency guarantees, no consensus protocol

2. **EMQX Durable Storage (DS) — EMQX 6.0**
   - Persistent message storage via RocksDB + Raft replication
   - Storage model: Topic-Timestamp-Value triple; streams abstraction for wildcard handling
   - **Differentiation**: Adds persistence *to* a broker (database subordinate to messaging). No CRUD, no transactional semantics, no SQL

3. **FairCom MQ**
   - C++ MQTT broker with built-in JSON/SQL database, supports MQTT 3.1/3.1.1/5
   - Store-and-forward persistence for reliability
   - **Differentiation**: Unification is operational (reliability), not architectural (topic hierarchy doesn't define data model)

4. **Machbase Neo**
   - Time-series DB in C with MQTT ingestion (e.g., `db/append/TAG`)
   - Eliminates separate broker + ingestion app
   - **Differentiation**: MQTT is a convenience layer, not the foundational abstraction

#### Tier 2: Academic papers on MQTT broker architecture

5. **Longo & Redondi — "Design and Implementation of an Advanced MQTT Broker for Distributed Pub/Sub Scenarios"**
   - Computer Networks (Elsevier), 2023
   - Distributed MQTT brokers with per-topic routing, in-band signaling
   - Relevant for topic-based routing at broker level; no database semantics
   - NOTE: Previously attributed to "Amico et al." — corrected after citation retrieval

6. **TBMQ — "A Distributed Architecture for MQTT Messaging: The Case of TBMQ"**
   - Journal of Big Data (Springer), 2025
   - Kafka for routing, dedicated per-client Kafka topics
   - Architecture: broker + Kafka + Redis + PostgreSQL — the "two-system problem" exemplified

7. **TBMQ P2P paper**
   - Preprints.org, 2025
   - Redis Cluster for session persistence, Kafka for routing
   - 1M msg/s linear scalability — shows multi-system overhead

#### Tier 3: IoT databases with MQTT integration

8. **Apache IoTDB**
   - VLDB 2020, SIGMOD 2023
   - Native time-series DBMS, TsFile format, tree-structured device namespace
   - Built-in MQTT service where topic = timeseries, configurable PayloadFormatter
   - 10M values/sec ingestion
   - **Differentiation**: MQTT bolted on as ingestion protocol; internal model is time-series tree, not topic hierarchy
   - **Most important academic comparison** — sets the bar for evaluation rigor

9. **"Lightweight Data Storage and Caching Solution for MQTT Broker on Edge"**
   - Conference paper (ResearchGate), 2024
   - SQLite + Redis within Mosquitto on Siemens Industrial Edge
   - Typical duct-tape engineering approach

#### Tier 4: Foundational citations

10. **Raft** — Ongaro & Ousterhout, USENIX ATC 2014
11. **Kafka** — Kreps et al., NetDB 2011; KRaft (KIP-500/KIP-595) for messaging→database convergence
12. **Pub/sub survey** — Eugster et al., ACM Computing Surveys, 2003
13. **CockroachDB** — Taft et al., SIGMOD 2020 — template for distributed DB systems paper
14. **YCSB** — Cooper et al., SoCC 2010 — benchmark methodology

#### Tier 5: Newly discovered (Session 2)

15. **Vargas, Bacon, Moody — "Integrating Databases with Publish/Subscribe"**
   - DEBS '05, IEEE, pp. 392–397
   - Extended version: BNCOD, Springer LNCS, 2008
   - Only paper found that addresses pub/sub + database integration directly
   - Relevant as prior work, though from a different era (pre-MQTT 5.0)

### Identified gap
No academic paper formally argues that MQTT's topic hierarchy and subscription semantics are sufficient to implement a distributed database. All existing systems either add persistence to a broker, bolt MQTT onto a database, or unify pragmatically without formal treatment. **Search of VLDB/SIGMOD/SOSP/OSDI 2020–2026 proceedings confirms: zero papers with "MQTT" in the title.**

---

## Session 2 — 2026-04-11: Full citation retrieval

### Search strategy
Systematic DOI and metadata retrieval for all papers identified in Session 1, plus targeted searches for gaps: Harper academic publications, "pub/sub as database" in DB literature, MQTT in top-tier DB conferences.

### Full citations

#### Standards

**[mqtt5]** Andrew Banks, Ed Briggs, Ken Borgendale, Rahul Gupta (eds.). "MQTT Version 5.0." OASIS Standard, 7 March 2019. https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html. ISO equivalent: ISO/IEC 20922:2016 (covers v3.1.1).

#### Academic papers with DOIs

**[iotdb-vldb20]** Chen Wang, Xiangdong Huang, Jialin Qiao, Tian Jiang, Lei Rui, Jinrui Zhang, Rong Kang, Julian Feinauer, Kevin A. McGrail, Peng Wang, Diaohan Luo, Jun Yuan, Jianmin Wang, Jiaguang Sun. "Apache IoTDB: Time-Series Database for Internet of Things." Proc. VLDB Endowment (PVLDB), Vol. 13, No. 12, pp. 2901–2904, 2020. DOI: `10.14778/3415478.3415504`

**[iotdb-sigmod23]** Chen Wang, Jialin Qiao, Xiangdong Huang, Shaoxu Song, Haonan Hou, Tian Jiang, Lei Rui, Jianmin Wang, Jiaguang Sun. "Apache IoTDB: A Time Series Database for IoT Applications." Proc. ACM Manag. Data (PACMMOD), Vol. 1, No. 2, Article 195, 27 pages, 2023. DOI: `10.1145/3589775`

**[raft]** Diego Ongaro, John Ousterhout. "In Search of an Understandable Consensus Algorithm." 2014 USENIX Annual Technical Conference (USENIX ATC '14), Philadelphia, PA, pp. 305–319, 2014. ISBN: 978-1-931971-10-2. Best Paper Award.

**[cockroachdb]** Rebecca Taft, Irfan Sharif, Andrei Matei, Nathan VanBenschoten, Jordan Lewis, Tobias Grieger, Kai Niemi, Andy Woods, Anne Birzin, Raphael Poss, Paul Bardea, Amruta Ranade, Ben Darnell, Bram Gruneir, Justin Jaffray, Lucy Zhang, Peter Mattis. "CockroachDB: The Resilient Geo-Distributed SQL Database." Proc. ACM SIGMOD, Portland, OR, pp. 1493–1509, 2020. DOI: `10.1145/3318464.3386134`

**[kafka]** Jay Kreps, Neha Narkhede, Jun Rao. "Kafka: A Distributed Messaging System for Log Processing." Proc. NetDB Workshop, Athens, Greece, 2011. No DOI (workshop paper).

**[longo23]** Edoardo Longo, Alessandro E. C. Redondi. "Design and Implementation of an Advanced MQTT Broker for Distributed Pub/Sub Scenarios." Computer Networks, Vol. 224, Article 109601, 2023. Elsevier. DOI: `10.1016/j.comnet.2023.109601`

**[tbmq]** Andrii I. Shvaika, Dmytro I. Shvaika, Dmytro I. Landiak, Volodymyr O. Artemchuk. "A Distributed Architecture for MQTT Messaging: The Case of TBMQ." Journal of Big Data, Vol. 12, Article 224, 2025. Springer Nature. DOI: `10.1186/s40537-025-01271-x`

**[ycsb]** Brian F. Cooper, Adam Silberstein, Erwin Tam, Raghu Ramakrishnan, Russell Sears. "Benchmarking Cloud Serving Systems with YCSB." Proc. 1st ACM Symp. Cloud Computing (SoCC '10), Indianapolis, IN, pp. 143–154, 2010. DOI: `10.1145/1807128.1807152`

**[pubsub-survey]** Patrick Th. Eugster, Pascal A. Felber, Rachid Guerraoui, Anne-Marie Kermarrec. "The Many Faces of Publish/Subscribe." ACM Computing Surveys, Vol. 35, No. 2, pp. 114–131, 2003. DOI: `10.1145/857076.857078`

**[vargas05]** Luis Vargas, Jean Bacon, Ken Moody. "Integrating Databases with Publish/Subscribe." Proc. Intl. Workshop on Distributed Event-Based Systems (DEBS '05), IEEE, pp. 392–397, 2005. Extended: BNCOD, Springer LNCS, 2008. DOI: `10.1007/978-3-540-70504-8_11`

#### Technical references (no DOI)

**[kraft]** Colin McCabe. "KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum." Apache Kafka Improvement Proposal, 2019. https://cwiki.apache.org/confluence/display/KAFKA/KIP-500

**[kraft-raft]** Colin McCabe, Jason Gustafson. "KIP-595: A Raft Protocol for the Metadata Quorum." Apache Kafka Improvement Proposal, 2019. https://cwiki.apache.org/confluence/display/KAFKA/KIP-595

**[emqx-ds]** "Design for Durable Storage." EMQX Enterprise Documentation. https://docs.emqx.com/en/emqx/latest/design/durable-storage.html

**[emqx-eip]** "RocksDB Message Persistence." EMQX Improvement Proposal 0023. https://github.com/emqx/eip/blob/main/active/0023-rocksdb-message-persistence.md

**[harper]** Harper (formerly HarperDB). Product documentation. https://docs.harperdb.io/. Blog: "Unpacking the Hype: MQTT Databases and Their Role in IoT." https://www.harper.fast/post/unpacking-the-hype-mqtt-databases-and-their-role-in-iot

### Search results: gaps confirmed

1. **Harper academic papers**: NONE found. Product docs and blog posts only. Must cite as industry reference.
2. **MQTT in VLDB/SIGMOD/SOSP/OSDI 2020–2026**: ZERO papers with "MQTT" in the title. IoTDB papers are the closest.
3. **"Pub/sub as database" in DB literature**: Only Vargas et al. (2005) found. No recent work addresses topic-space-as-data-model convergence.
4. **Kafka Streams / ksqlDB formalization**: No formal academic papers found. ksqlDB is documented only in Confluent product docs.

### TODO remaining
- [ ] Find TPC-IoT benchmark specification reference
- [ ] Search for any edge computing + MQTT + database integration surveys (2023–2026)
- [ ] Verify Vargas et al. extended version DOI against actual content
