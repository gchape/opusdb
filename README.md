# OpusDB

An Experimental Relational Database for Academic Research in Clojure.

<img width="480" height="480" alt="opusdb_logo_v4" src="https://github.com/user-attachments/assets/13578c35-8b05-4b83-8903-e9f78c7cbd1c" />

## Table of Contents

- [Features](#features)
- [Benchmarks](#benchmarks)
  - [Bank Transfer Benchmarks](#bank-transfer-benchmarks)
  - [Throughput Benchmarks](#throughput-benchmarks)
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
Successful transfers: 1989346

Benchmarking single transfer transaction:
Evaluation count : 278160 in 6 samples of 46360 calls.
             Execution time mean : 2.226707 µs
    Execution time std-deviation : 58.027795 ns
   Execution time lower quantile : 2.158768 µs ( 2.5%)
   Execution time upper quantile : 2.309807 µs (97.5%)
                   Overhead used : 2.037598 ns

Found 1 outliers in 6 samples (16.6667 %)
        low-severe       1 (16.6667 %)
 Variance from outliers : 13.8889 % Variance is moderately inflated by outliers

Benchmarking low-contention concurrent transfers:
(Rotating through different account pairs)
Evaluation count : 274746 in 6 samples of 45791 calls.
             Execution time mean : 2.281300 µs
    Execution time std-deviation : 67.210583 ns
   Execution time lower quantile : 2.227851 µs ( 2.5%)
   Execution time upper quantile : 2.384188 µs (97.5%)
                   Overhead used : 2.037598 ns

Benchmarking medium-contention scenario:
(All transfers touching accounts 0-4)
Evaluation count : 265344 in 6 samples of 44224 calls.
             Execution time mean : 2.315170 µs
    Execution time std-deviation : 77.470009 ns
   Execution time lower quantile : 2.236750 µs ( 2.5%)
   Execution time upper quantile : 2.428696 µs (97.5%)
                   Overhead used : 2.037598 ns

Benchmarking high-contention scenario:
(All transfers between accounts 0 and 1)
Evaluation count : 271194 in 6 samples of 45199 calls.
             Execution time mean : 2.309667 µs
    Execution time std-deviation : 78.939990 ns
   Execution time lower quantile : 2.243977 µs ( 2.5%)
   Execution time upper quantile : 2.430316 µs (97.5%)
                   Overhead used : 2.037598 ns

Benchmarking extreme-contention with futures:
(20 threads all transferring from/to same 2 accounts)
Evaluation count : 402 in 6 samples of 67 calls.
             Execution time mean : 1.717133 ms
    Execution time std-deviation : 97.714707 µs
   Execution time lower quantile : 1.609434 ms ( 2.5%)
   Execution time upper quantile : 1.826020 ms (97.5%)
                   Overhead used : 2.037598 ns
```

#### Summary

| Scenario                        | Mean Latency | Throughput          |
| ------------------------------- | ------------ | ------------------- |
| Single transfer                 | 2.23 µs      | —                   |
| Low-contention                  | 2.28 µs      | —                   |
| Medium-contention               | 2.32 µs      | —                   |
| High-contention                 | 2.31 µs      | —                   |
| Extreme-contention (20 futures) | 1.72 ms      | —                   |
| Stress test (20 threads, 5s)    | —            | 1,989,346 transfers |

Zero data inconsistencies across all scenarios. Total balance invariant maintained throughout.

---

### Throughput Benchmarks

Comprehensive STM performance testing across workload patterns with thread scaling.

#### Test Scenarios

| #   | Scenario              | Description                                                   |
| --- | --------------------- | ------------------------------------------------------------- |
| 1   | High Contention       | Multiple threads competing for a single shared ref            |
| 2   | Low Contention        | Each thread operates on an isolated ref with no overlap       |
| 3   | Bank Transfer         | Realistic financial transactions with abort conditions        |
| 4   | Read-Heavy Mix        | 10% writes — read-dominated workload                          |
| 5   | Write-Heavy Mix       | 90% writes — write-dominated workload with frequent conflicts |
| 6   | Criterium Statistical | Precise latency measurements via statistical sampling         |

#### Results

```
=== Throughput Benchmarks ===

--- High Contention (single ref) ---

  4 threads:
  opusdb  txns/sec: 339950  correct: true

  8 threads:
  opusdb  txns/sec: 357034  correct: true

  16 threads:
  opusdb  txns/sec: 432081  correct: true

--- Low Contention (isolated refs) ---

  4 threads:
  opusdb  txns/sec: 833943  correct: true

  8 threads:
  opusdb  txns/sec: 767328  correct: true

  16 threads:
  opusdb  txns/sec: 1158596  correct: true

--- Bank Transfer (20 accounts) ---

  4 threads:
  opusdb  txns/sec: 296916  correct: true

  8 threads:
  opusdb  txns/sec: 116288  correct: true

  16 threads:
  opusdb  txns/sec: 101728  correct: true

--- Read-Heavy Mix (10% writes, 10 refs) ---

  4 threads:
  opusdb  txns/sec: 1060610  correct: true

  8 threads:
  opusdb  txns/sec: 1456467  correct: true

--- Write-Heavy Mix (90% writes, 10 refs) ---

  4 threads:
  opusdb  txns/sec: 420145  correct: true

  8 threads:
  opusdb  txns/sec: 271186  correct: true
```

#### Criterium Statistical Benchmarks

```
Single increment — opusdb:
Evaluation count : 522468 in 6 samples of 87078 calls.
             Execution time mean : 1.195893 µs
    Execution time std-deviation : 38.364063 ns
   Execution time lower quantile : 1.143009 µs ( 2.5%)
   Execution time upper quantile : 1.238103 µs (97.5%)
                   Overhead used : 2.037598 ns

Single ref-set — opusdb:
Evaluation count : 649356 in 6 samples of 108226 calls.
             Execution time mean : 948.088967 ns
    Execution time std-deviation : 32.831345 ns
   Execution time lower quantile : 916.947074 ns ( 2.5%)
   Execution time upper quantile : 999.723314 ns (97.5%)
                   Overhead used : 2.037598 ns

Found 1 outliers in 6 samples (16.6667 %)
        low-severe       1 (16.6667 %)
 Variance from outliers : 13.8889 % Variance is moderately inflated by outliers

Read-only 5 refs — opusdb:
Evaluation count : 618246 in 6 samples of 103041 calls.
             Execution time mean : 977.382602 ns
    Execution time std-deviation : 29.236433 ns
   Execution time lower quantile : 948.148038 ns ( 2.5%)
   Execution time upper quantile : 1.008833 µs (97.5%)
                   Overhead used : 2.037598 ns

Read-only 10 refs — opusdb:
Evaluation count : 410340 in 6 samples of 68390 calls.
             Execution time mean : 1.416513 µs
    Execution time std-deviation : 47.489930 ns
   Execution time lower quantile : 1.380883 µs ( 2.5%)
   Execution time upper quantile : 1.492322 µs (97.5%)
                   Overhead used : 2.037598 ns

Found 1 outliers in 6 samples (16.6667 %)
        low-severe       1 (16.6667 %)
 Variance from outliers : 13.8889 % Variance is moderately inflated by outliers

Write 5 refs — opusdb:
Evaluation count : 185784 in 6 samples of 30964 calls.
             Execution time mean : 3.338396 µs
    Execution time std-deviation : 40.997854 ns
   Execution time lower quantile : 3.292780 µs ( 2.5%)
   Execution time upper quantile : 3.390519 µs (97.5%)
                   Overhead used : 2.037598 ns
```

#### Summary

| Operation         | Mean Latency |
| ----------------- | ------------ |
| Single ref-set    | 948 ns       |
| Single increment  | 1.20 µs      |
| Read-only 5 refs  | 977 ns       |
| Read-only 10 refs | 1.42 µs      |
| Write 5 refs      | 3.34 µs      |

| Scenario                      | Throughput (txns/sec) |
| ----------------------------- | --------------------- |
| Low contention (16 threads)   | 1.16M                 |
| High contention (16 threads)  | 432k                  |
| Read-heavy mix (8 threads)    | 1.46M                 |
| Bank transfer (4 threads)     | 297k                  |

---

## License

Copyright © 2026 OpusDB Contributors

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
<http://www.eclipse.org/legal/epl-2.0>.
