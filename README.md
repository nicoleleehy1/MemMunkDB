# MemMunk

LevelDB-style LSM-Tree key-value store built from scratch in Java — memtable, WAL, SSTable flushing, size-tiered compaction, and a live REST API deployed on Railway.

🔗 **[memmunk-production.up.railway.app](https://memmunk-production.up.railway.app)**

---

## What it is

MemMunk is a storage engine modeled after LevelDB and the architecture underlying DynamoDB and Cassandra. Every layer is implemented from scratch with no storage library dependencies.

**Write path:** `put/delete` → WAL → MemTable → (threshold) → SSTable flush

**Read path:** MemTable → SSTables (newest-first)

**Compaction:** size-tiered merge, triggered manually or by background thread

---

## Architecture

| File | Role |
|------|------|
| `store/LSMStore.java` | Public API — put, get, delete, flush, compact |
| `memtable/MemTable.java` | In-memory TreeMap; tombstones for deletes |
| `wal/WriteAheadLog.java` | Append-only WAL, replayed on startup after a crash |
| `sstable/SSTable.java` | Immutable on-disk file; Bloom filter + sparse index + binary records |
| `sstable/SSTableIndex.java` | Sparse index: every 16th key → byte offset (binary search on lookup) |
| `sstable/SSTableEntry.java` | Record type used during compaction |
| `compaction/Compactor.java` | Size-tiered compaction: 4 tiers, merges when ≥ 4 files accumulate per tier |
| `server/RestServer.java` | HTTP server exposing 7 REST endpoints, no external dependencies |

---

## REST API

| Endpoint | Behavior |
|----------|----------|
| `GET /get?key=k` | Returns `{"key":"k","value":"v"}` or 404 |
| `POST /put` | Body: `{"key":"…","value":"…"}` → `{"ok":true}` |
| `DELETE /delete?key=k` | Writes tombstone → `{"ok":true}` |
| `GET /scan?prefix=p` | Returns all live keys matching prefix as JSON array |
| `POST /flush` | Flushes memtable to SSTable |
| `POST /compact` | Runs size-tiered compaction |
| `GET /debug` | Full JSON snapshot of memtable, SSTables, and compaction tiers |

---

## Running locally

**Prerequisites:** Java 17+, Maven 3.6+

```bash
git clone https://github.com/your-username/memmunk
cd memmunk/lsm-store
mvn package -DskipTests
java -jar target/lsm-store-1.0-SNAPSHOT.jar
# → http://localhost:8080
```

**Run tests:**
```bash
mvn test
# 20/20 passing
```

---

## Stack

- Java 17 — core engine and HTTP server (`com.sun.net.httpserver`)
- Maven + maven-shade-plugin — fat JAR build
- Guava — Bloom filter
- Snappy — SSTable block compression
- JUnit 5 — 20 tests covering all read/write/compaction paths
- Railway — deployment

---

## Why this matters

LSM-Trees are the storage engine behind LevelDB, RocksDB, Cassandra, DynamoDB, and InfluxDB. The core insight is that sequential disk writes are orders of magnitude faster than random ones — so instead of updating data in place, you append to a log and periodically merge sorted files. MemMunk implements all the pieces that make this work: crash recovery via WAL, fast reads via Bloom filters and sparse indexes, and space reclamation via compaction.
