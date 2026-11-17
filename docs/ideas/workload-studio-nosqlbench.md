# Idea: a separate "workload studio" built on NoSQLBench

> **Status:** idea, recorded 2026-11-17. Nothing built. This is meant to be a
> **separate project**, not part of fake-tickets.

## 1. Where the idea comes from

1. **Custom workloads without code.** In fake-tickets, adding a workload means
   writing Python ([04](../04-ADDING-A-WORKLOAD.md)). The idea was a "custom CQL"
   workload in the UI: paste a `CREATE TABLE` and an `INSERT` with placeholders
   (`{uuid}`, `{int:1..100}`, `{text:50}`), choose a rate, and run it.
2. **Owner's decision (2026-11-17):** don't build that inside fake-tickets. Make
   it a separate project that **uses [NoSQLBench](https://docs.nosqlbench.io/)
   (nb5)** as the engine, with a UI on top. The goal is to **reproduce existing
   workloads** from a cluster's schema and table statistics, using nb5's
   **cqlgen**.

## 2. Why NoSQLBench

NoSQLBench already has the hard parts:
- a CQL driver (`cqld4`);
- data generation recipes ("bindings");
- workload YAML files.

**cqlgen** turns a real schema into a workload. That is exactly what support
reproductions need, and it's something fake-tickets can't do.

What the new project would add is everything around the engine: a UI,
connection storage, runs that can be started and stopped, and a Kubernetes
deployment. fake-tickets already solves those parts.

## 3. cqlgen in brief

Source: <https://docs.nosqlbench.io/reference/apps/cqlgen/>, read 2026-11-17.
The page calls cqlgen new; its development is "just starting".

**Command:**
```
nb5 cqlgen myschema.cql myworkload.yaml myhistograms
```
The three arguments are, in order:
1. the CQL schema file;
2. the workload YAML to write (an existing file is never overwritten);
3. optionally, a stats file from `nodetool tablestats >tablestats`.

**What it generates by default:** for each table, a main phase with reads,
writes, updates and a 10-row partition scan, in a 1:1:1:1 ratio. Single-key
tables get no scan.

**What tablestats changes:** with a tablestats file, the reads, writes and
estimated partition counts weight the workload across tables and data sizes,
and key values are kept within the estimated partition count.

**Configuration:** two files, copied out of nb5 to edit:
- `nb5 --copy cqlgen/cqlgen.conf`: the processing steps ("transformers"):
  - obfuscate keyspace, table and column names;
  - choose the keyspaces to include;
  - exclude tables by traffic;
  - set replication;
  - set timeouts;
  - decide how phases are built.
- `nb5 --copy cqlgen/cqlgen-bindings.yaml`: default data generators per type.
  Add one there if you get `UnresolvedBindingsException`.

**Caveats the page states:**
- tablestats covers a single node, and only since its last restart. Adjust
  `partition_multiplier` to compensate.
- The default multiplier of 10 assumes even data distribution, replication
  factor 3 and 30 nodes.
- UDTs become blobs.
- Replication defaults to SimpleStrategy, which is only suitable for local tests.
- Table properties other than `durable_writes` are dropped, including
  compaction settings. A reproduction must set compaction itself.
- Transformers that lack input data pass through silently.

**Not covered on that page:** the structure of the generated YAML, and the nb5
command that runs it.

## 4. What the UI could do

1. **Import:** upload a schema (`DESCRIBE` output) and, optionally, tablestats
   from one or more nodes.
2. **Generate:** run cqlgen with a config built from the form: keyspaces to
   include, obfuscation on or off, replication, partition multiplier.
3. **Review:** show the generated workload per table (ratios, bindings, key
   cardinality). Allow edits, and a raw YAML view for experts.
4. **Table settings:** add compaction, gc_grace and TTL options, which cqlgen
   drops (see §3). fake-tickets' "Table & compaction" box covers this.
5. **Run:** choose a connection and a rate, then start, watch and stop the run.
6. **Library:** save generated workloads so they can be rerun and compared.

## 5. What can be reused from fake-tickets

| fake-tickets piece | Reuse |
|---|---|
| Encrypted connection store (scrypt + AES-GCM, Astra SCB upload) | As is |
| Login (none / local / LDAP), TLS from smallstep with hot reload | As is |
| Run manager: process per run, persisted state, interrupted on restart | Adapt: the process becomes `nb5` |
| Compaction monitor (system views, compaction history) | As is; it only needs a CQL session |
| Rate control and "How much" UI, run page, Kubernetes manifests | Adapt |

## 6. Open questions (to check before starting)

- **Rate control:** can nb5 change its rate while running, as the fake-tickets
  slider does, or must a run restart?
- **Metrics:** how does nb5 expose live metrics, and in what format, for the
  run page?
- **Image:** nb5 is a Java application. What is its size and memory use in a
  UBI 9 image on a small homelab?
- **Licence:** check NoSQLBench's licence, and how to pin and upgrade its
  version.
- **Maturity:** how complete is cqlgen today ("development is just starting")?
  Which schemas break it (UDTs, collections, counters)?
- **Privacy:** customer schemas are sensitive. Should obfuscation be on by
  default, and the original schema not be stored after generation?
- **fake-tickets:** does it stay as is, for hand-written workloads (doc 04),
  with this project only for "reproduce a real workload"?
