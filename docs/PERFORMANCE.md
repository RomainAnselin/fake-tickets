# Ingestion Rate & Throughput Estimates

> Derived from `test/log_excerpt.txt` (sync baseline), `test/output_sstables.txt` (record size),
> and `src/faketickets.py` (async parameters).

---

## Methodology & Assumptions

### Synchronous baseline — `log_excerpt.txt`

| Metric | Value |
|---|---|
| Log window observed | 20:47:08.910 → 20:50:14.551 (185.6 s) |
| Records in window | 180,000 (18 × 10k checkpoints) |
| Average inter-checkpoint interval | ~10.3 s per 10,000 records |
| Derived rate | **969.6 rec/s** |

### Record size — `output_sstables.txt` + schema

| Metric | Value |
|---|---|
| SSTables bb-3 to bb-6 `Data.db` | ~13 MB each, flushed every ~6 min |
| Records per 6-min window @ sync rate | ~349,200 |
| Compressed on-disk bytes/record | ~37 B (LZ4, ~3× ratio) |
| **Uncompressed logical bytes/record** | **~110 B** (id 4B + ticket 4B + time 8B + ownedby ~8B + notes ~62B + overhead ~24B) |

### Async parameters — `src/faketickets.py` `insert_paced_async()`

| Parameter | Value |
|---|---|
| `concurrency` (default) | 50 in-flight futures |
| `target_ops` (default) | 200 ops/s |
| `chunk_size` = concurrency × 2 | 100 records |
| `sleep_delay` = chunk_size / target_ops | 0.5 s per chunk → **paced ceiling = 200 rec/s** |
| Unpaced ceiling (`target_ops = 0`) | ~5,000 rec/s conservative (single DSE node, `concurrency=50`) |

---

## Records & Data Ingested per Hour

| Scenario | Rate (rec/s) | Records / hr | Data / hr (uncompressed) |
|---|---:|---:|---:|
| **Sync (current)** | 969.6 | ~3,490,704 (~3.5M) | ~366 MB |
| **Paced async** (`target_ops=200`) | 200 | ~720,000 (0.72M) | ~75 MB |
| **Unpaced async** (`target_ops=0`) | ~5,000 | ~18,000,000 (18M) | ~1.84 GB |

On-disk figures are roughly **3× lower** than the uncompressed column above due to LZ4 compression
(e.g. ~122 MB/hr on disk for the sync scenario).

---

## Key Finding: Paced Async at Default Settings is Slower Than Sync

The default `target_ops=200` pacing gate issues a `time.sleep(0.5)` after every 100-record chunk,
hard-capping throughput at **200 rec/s** — well below what the synchronous loop naturally achieves
at ~970 rec/s. The async machinery is entirely idle during the sleep.

| Comparison | Multiplier | Interpretation |
|---|---:|---|
| Paced async vs Sync | 0.21× | Paced async produces ~1/5 of sync throughput — a regression |
| Sync vs Paced async | 4.8× | Sync is nearly 5× faster than default paced async |
| Unpaced async vs Sync | 5.2× | Unpaced async gives ~5× improvement over sync baseline |
| Unpaced async vs Paced async | 25× | Removing the sleep multiplies throughput 25× |

---

## Recommended `target_ops` Values

| Goal | `target_ops` | Expected rec/s | Records / hr |
|---|---|---:|---:|
| Match sync baseline | `1000` | ~1,000 | ~3.6M |
| 2× improvement over sync | `2000` | ~2,000 | ~7.2M |
| Maximum throughput | `0` (disabled) | 5,000 – 15,000+ | 18M – 54M+ |

With pacing fully disabled, `execute_concurrent_with_args(concurrency=50)` against a local DSE node
with SSD storage can realistically reach **5,000 – 15,000+ rec/s**, translating to
**1.8 – 6+ GB/hr** of logical data ingested.
