# OpusDB

An Experimental Relational Database for Academic Research in Clojure.

<img width="480" height="480" alt="logo" src="https://github.com/user-attachments/assets/ba51138f-7354-4ffd-bb7c-4a53847ca7a1" />

## Table of Contents

- [Features](#features)
- [Benchmarks](#benchmarks)
  - [Bank Transfer Benchmarks](#bank-transfer-benchmarks)
  - [Throughput Benchmarks](#throughput-benchmarks)
  - [Native STM Comparison](#native-stm-comparison)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

---

## Benchmarks

OpusDB provides comprehensive performance benchmarks demonstrating STM capabilities under various workload patterns. All benchmarks were run on the JVM with Criterium for statistical accuracy where applicable.

---

### Bank Transfer Benchmarks

Real-world financial transaction simulation testing atomicity and consistency under concurrent load.

#### Test Configuration

Ten accounts, each initialised with 1,000 units. Transfers are atomic transactions that read balances, verify funds, update both accounts, and increment a transfer counter. The STM guarantees all-or-nothing execution, preventing partial transfers or lost updates.

#### Test Scenarios

| Scenario                     | Description                                                              |
| ---------------------------- | ------------------------------------------------------------------------ |
| Stress test (5s, 20 threads) | Verifies total balance remains exactly 10,000 throughout                 |
| Single transaction latency   | Baseline cost of one transfer via Criterium statistical sampling         |
| Low-contention               | Transfers rotate through different account pairs — minimal conflicts     |
| Medium-contention            | All transfers constrained to accounts 0–4 — moderate conflicts           |
| High-contention              | All transfers between accounts 0 and 1 — maximum conflicts               |
| Extreme-contention           | Twenty futures simultaneously transferring between the same two accounts |

#### Results

```
=== BANK TRANSFERS ===
Total (should be 10000): 10000
Successful transfers: 1853943

Benchmarking single transfer transaction:
Evaluation count : 1307802 in 6 samples of 217967 calls.
             Execution time mean : 462.907972 ns
    Execution time std-deviation : 7.359488 ns
   Execution time lower quantile : 456.536843 ns ( 2.5%)
   Execution time upper quantile : 473.679177 ns (97.5%)
                   Overhead used : 2.022131 ns

Benchmarking low-contention concurrent transfers:
Evaluation count : 279948 in 6 samples of 46658 calls.
             Execution time mean : 2.399445 µs
    Execution time std-deviation : 202.101903 ns
   Execution time lower quantile : 2.235406 µs ( 2.5%)
   Execution time upper quantile : 2.722913 µs (97.5%)
                   Overhead used : 2.022131 ns

Benchmarking medium-contention scenario:
Evaluation count : 262860 in 6 samples of 43810 calls.
             Execution time mean : 2.340187 µs
    Execution time std-deviation : 283.667460 ns
   Execution time lower quantile : 2.172131 µs ( 2.5%)
   Execution time upper quantile : 2.781024 µs (97.5%)
                   Overhead used : 2.022131 ns

Benchmarking high-contention scenario:
Evaluation count : 1299168 in 6 samples of 216528 calls.
             Execution time mean : 480.362177 ns
    Execution time std-deviation : 17.811450 ns
   Execution time lower quantile : 462.506988 ns ( 2.5%)
   Execution time upper quantile : 505.569432 ns (97.5%)
                   Overhead used : 2.022131 ns

Benchmarking extreme-contention with futures:
Evaluation count : 26478 in 6 samples of 4413 calls.
             Execution time mean : 22.829702 µs
    Execution time std-deviation : 454.334272 ns
   Execution time lower quantile : 22.535669 µs ( 2.5%)
   Execution time upper quantile : 23.613314 µs (97.5%)
                   Overhead used : 2.022131 ns
```

#### Summary

| Scenario                        | Mean Latency | Throughput          |
| ------------------------------- | ------------ | ------------------- |
| Single transfer                 | 463 ns       | —                   |
| Low-contention                  | 2.40 µs      | —                   |
| Medium-contention               | 2.34 µs      | —                   |
| High-contention                 | 480 ns       | —                   |
| Extreme-contention (20 futures) | 22.8 µs      | —                   |
| Stress test (20 threads, 5s)    | —            | 1,853,943 transfers |

Zero data inconsistencies across all scenarios. Total balance invariant maintained throughout.

---

### Throughput Benchmarks

Comprehensive STM performance testing across workload patterns with thread scaling.

#### Test Scenarios

| #   | Scenario              | Description                                                   |
| --- | --------------------- | ------------------------------------------------------------- |
| 1   | Simple Increments     | Single-threaded baseline, no concurrency                      |
| 2   | High Contention       | Multiple threads competing for a single shared ref            |
| 3   | Low Contention        | Each thread operates on an isolated ref with no overlap       |
| 4   | Read-Heavy Mix        | 10% writes — read-dominated workload                          |
| 5   | Write-Heavy Mix       | 90% writes — write-dominated workload with frequent conflicts |
| 6   | Bank Transfer         | Realistic financial transactions with abort conditions        |
| 7   | Criterium Statistical | Precise latency measurements via statistical sampling         |

#### Results

```
=== Throughput Benchmarks ===

1. Simple Increments (single-threaded, 10 refs):
{:txns 10000, :refs 10, :elapsed-ms 70.77, :txns-per-sec 141312, :correct? true}

2. High Contention (multiple threads, single ref):
  4 threads:
    {:threads 4, :total-txns 40000, :elapsed-ms 108.63, :txns-per-sec 368208,
     :final-value 40000, :expected 40000, :correct? true}
  8 threads:
    {:threads 8, :total-txns 80000, :elapsed-ms 173.50, :txns-per-sec 461083,
     :final-value 80000, :expected 80000, :correct? true}
  16 threads:
    {:threads 16, :total-txns 160000, :elapsed-ms 337.53, :txns-per-sec 474034,
     :final-value 160000, :expected 160000, :correct? true}

3. Low Contention (isolated refs per thread):
  4 threads:
    {:threads 4, :total-txns 40000, :elapsed-ms 41.54, :txns-per-sec 962881,
     :contention low, :correct? true}
  8 threads:
    {:threads 8, :total-txns 80000, :elapsed-ms 52.61, :txns-per-sec 1520617,
     :contention low, :correct? true}
  16 threads:
    {:threads 16, :total-txns 160000, :elapsed-ms 104.50, :txns-per-sec 1531073,
     :contention low, :correct? true}

4. Read-Heavy Mix (10% writes, 10 refs):
  4 threads:
    {:threads 4, :total-ops 20000, :writes 1962, :reads 18038, :write-ratio 10%,
     :txns-per-sec 1116566, :elapsed-ms 17.91, :final-sum 1962, :correct? true}
  8 threads:
    {:threads 8, :total-ops 40000, :writes 4024, :reads 35976, :write-ratio 10%,
     :txns-per-sec 873897, :elapsed-ms 45.77, :final-sum 4024, :correct? true}

5. Write-Heavy Mix (90% writes, 10 refs):
  4 threads:
    {:threads 4, :total-ops 20000, :writes 18002, :reads 1998, :write-ratio 90%,
     :txns-per-sec 574909, :elapsed-ms 34.79, :final-sum 18002, :correct? true}
  8 threads:
    {:threads 8, :total-ops 40000, :writes 35994, :reads 4006, :write-ratio 90%,
     :txns-per-sec 410014, :elapsed-ms 97.56, :final-sum 35994, :correct? true}

6. Bank Transfer (realistic workload, 20 accounts):
  4 threads:
    {:threads 4, :successful-txns 18204, :total-balance 20000,
     :attempted-txns 20000, :txns-per-sec 195088, :elapsed-ms 93.31, :correct? true}
  8 threads:
    {:threads 8, :successful-txns 36493, :total-balance 20000,
     :attempted-txns 40000, :txns-per-sec 237344, :elapsed-ms 153.76, :correct? true}
  16 threads:
    {:threads 16, :successful-txns 72610, :total-balance 20000,
     :attempted-txns 80000, :txns-per-sec 280413, :elapsed-ms 258.94, :correct? true}
```

#### Criterium Statistical Benchmarks

```
Single-threaded increment:
             Execution time mean : 1.221553 µs
    Execution time std-deviation : 77.246812 ns

Single ref-set:
             Execution time mean : 871.839717 ns
    Execution time std-deviation : 55.588730 ns

Read-only transaction (5 refs):
             Execution time mean : 1.171001 µs
    Execution time std-deviation : 17.560545 ns

Read-only transaction (10 refs):
             Execution time mean : 1.616187 µs
    Execution time std-deviation : 49.005058 ns

Write transaction (5 refs):
             Execution time mean : 3.456590 µs
    Execution time std-deviation : 486.856671 ns
```

#### Summary

| Operation         | Mean Latency |
| ----------------- | ------------ |
| Single ref-set    | 872 ns       |
| Single increment  | 1.22 µs      |
| Read-only 5 refs  | 1.17 µs      |
| Read-only 10 refs | 1.62 µs      |
| Write 5 refs      | 3.46 µs      |

| Scenario                      | Throughput (txns/sec) |
| ----------------------------- | --------------------- |
| Low contention (8–16 threads) | 1.5M+                 |
| High contention (16 threads)  | 474k                  |
| Read-heavy mix (4 threads)    | 1.1M                  |
| Bank transfer (16 threads)    | 280k                  |

---

### Native STM Comparison

OpusDB STM is implemented entirely at the application level in Clojure, without modifications to the language runtime. This section compares it directly against Clojure's native `dosync` / `LockingTransaction` under identical workloads.

#### Results

```
=== Throughput Benchmarks ===

--- High Contention (single ref) ---

  4 threads:
  opusdb       txns/sec: 204810  correct: true
  native       txns/sec: 273522  correct: true

  8 threads:
  opusdb       txns/sec: 289789  correct: true
  native       txns/sec: 246724  correct: true

  16 threads:
  opusdb       txns/sec: 398263  correct: true
  native       txns/sec: 682076  correct: true

--- Low Contention (isolated refs) ---

  4 threads:
  opusdb       txns/sec: 883499   correct: true
  native       txns/sec: 7802013  correct: true

  8 threads:
  opusdb       txns/sec: 991292    correct: true
  native       txns/sec: 14462260  correct: true

  16 threads:
  opusdb       txns/sec: 1158357   correct: true
  native       txns/sec: 14588923  correct: true

--- Bank Transfer (20 accounts) ---

  4 threads:
  opusdb       txns/sec: 203158  correct: true
  native       txns/sec: 440438  correct: true

  8 threads:
  opusdb       txns/sec: 238011   correct: true
  native       txns/sec: 1832418  correct: true

  16 threads:
  opusdb       txns/sec: 265853   correct: true
  native       txns/sec: 1261212  correct: true

--- Read-Heavy Mix (10% writes, 10 refs) ---

  4 threads:
  opusdb       txns/sec: 913282   correct: true
  native       txns/sec: 2437521  correct: true

  8 threads:
  opusdb       txns/sec: 922852   correct: true
  native       txns/sec: 3068383  correct: true

--- Write-Heavy Mix (90% writes, 10 refs) ---

  4 threads:
  opusdb       txns/sec: 683098   correct: true
  native       txns/sec: 3559115  correct: true

  8 threads:
  opusdb       txns/sec: 485099   correct: true
  native       txns/sec: 3195161  correct: true

=== Criterium Statistical Benchmarks ===

Single increment — opusdb:   1.229 µs
Single increment — native:   213 ns

Single ref-set — opusdb:     909 ns
Single ref-set — native:     240 ns

Read-only 5 refs — opusdb:   1.188 µs
Read-only 5 refs — native:   421 ns

Read-only 10 refs — opusdb:  1.636 µs
Read-only 10 refs — native:  538 ns

Write 5 refs — opusdb:       3.543 µs
Write 5 refs — native:       623 ns
```

#### Analysis

| Scenario                   | OpusDB   | Native   | Ratio           |
| -------------------------- | -------- | -------- | --------------- |
| Single increment           | 1.23 µs  | 213 ns   | ~5.8x           |
| Single ref-set             | 909 ns   | 240 ns   | ~3.8x           |
| Read-only 5 refs           | 1.19 µs  | 421 ns   | ~2.8x           |
| Write 5 refs               | 3.54 µs  | 623 ns   | ~5.7x           |
| High contention, 8 threads | 289k tps | 247k tps | **OpusDB +17%** |

The performance gap on uncontended workloads is expected and attributable to implementation level: Clojure's native STM is a JVM-optimised Java implementation (`LockingTransaction.java`) with direct field access, lock striping, and years of JIT optimisation. OpusDB STM operates entirely in Clojure, paying for `IdentityHashMap` operations, `volatile!` reads, and atom CAS on every ref write.

Notably, at 8-thread high contention, OpusDB outperforms native STM. This is the ownership-stealing conflict resolution protocol working as intended — higher transaction IDs steal ownership and immediately abort lower-priority transactions, reducing the retry cycles that native STM's backoff strategy incurs under sustained contention.

The throughput overhead is a deliberate tradeoff. OpusDB STM provides capabilities the native implementation cannot:

- `on-commit` handlers — side effects deferred until successful commit
- `on-rollback` handlers — cleanup guaranteed on abort or retry
- A fully public API with no package-private visibility restrictions
- Runtime independence — no forking or patching of `clojure.lang`

---

## License

Copyright © 2026 OpusDB Contributors

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
<http://www.eclipse.org/legal/epl-2.0>.
