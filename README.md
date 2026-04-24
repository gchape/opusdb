# OpusDB

An Experimental Relational Database for Academic Research in Clojure.

<img width="480" height="480" alt="logo" src="https://github.com/user-attachments/assets/ba51138f-7354-4ffd-bb7c-4a53847ca7a1" />

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

Ten accounts, each initialised with 1,000 units. Transfers are atomic transactions that read balances, verify funds, update both accounts, and increment a transfer counter via `on-commit` hook. The STM guarantees all-or-nothing execution, preventing partial transfers or lost updates.

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
Total (should be 10000 ): 10000
Successful transfers: 542858

Benchmarking single transfer transaction:
Evaluation count : 304242 in 6 samples of 50707 calls.
             Execution time mean : 2.029188 µs
    Execution time std-deviation : 146.741959 ns
   Execution time lower quantile : 1.887760 µs ( 2.5%)
   Execution time upper quantile : 2.199594 µs (97.5%)
                   Overhead used : 2.032557 ns

Benchmarking low-contention concurrent transfers:
(Rotating through different account pairs)
Evaluation count : 295398 in 6 samples of 49233 calls.
             Execution time mean : 2.099264 µs
    Execution time std-deviation : 60.774061 ns
   Execution time lower quantile : 2.038572 µs ( 2.5%)
   Execution time upper quantile : 2.192463 µs (97.5%)
                   Overhead used : 2.032557 ns

Found 1 outliers in 6 samples (16.6667 %)
        low-severe       1 (16.6667 %)
 Variance from outliers : 13.8889 % Variance is moderately inflated by outliers

Benchmarking medium-contention scenario:
(All transfers touching accounts 0-4)
Evaluation count : 304680 in 6 samples of 50780 calls.
             Execution time mean : 2.057422 µs
    Execution time std-deviation : 107.990063 ns
   Execution time lower quantile : 1.961023 µs ( 2.5%)
   Execution time upper quantile : 2.224575 µs (97.5%)
                   Overhead used : 2.032557 ns

Benchmarking high-contention scenario:
(All transfers between accounts 0 and 1)
Evaluation count : 299142 in 6 samples of 49857 calls.
             Execution time mean : 2.136859 µs
    Execution time std-deviation : 127.840159 ns
   Execution time lower quantile : 2.019817 µs ( 2.5%)
   Execution time upper quantile : 2.303959 µs (97.5%)
                   Overhead used : 2.032557 ns

Benchmarking extreme-contention with futures:
(20 threads all transferring from/to same 2 accounts)
Evaluation count : 990 in 6 samples of 165 calls.
             Execution time mean : 813.164454 µs
    Execution time std-deviation : 105.108940 µs
   Execution time lower quantile : 691.146261 µs ( 2.5%)
   Execution time upper quantile : 974.551748 µs (97.5%)
                   Overhead used : 2.032557 ns

Found 1 outliers in 6 samples (16.6667 %)
        low-severe       1 (16.6667 %)
 Variance from outliers : 31.6015 % Variance is moderately inflated by outliers
```

#### Summary

| Scenario                        | Mean Latency | Throughput        |
| ------------------------------- | ------------ | ----------------- |
| Single transfer                 | 2.03 µs      | —                 |
| Low-contention                  | 2.10 µs      | —                 |
| Medium-contention               | 2.06 µs      | —                 |
| High-contention                 | 2.14 µs      | —                 |
| Extreme-contention (20 futures) | 813 µs       | —                 |
| Stress test (20 threads, 5s)    | —            | 542,858 transfers |

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
  opusdb  txns/sec: 321715  correct: true

  8 threads:
  opusdb  txns/sec: 379340  correct: true

  16 threads:
  opusdb  txns/sec: 407955  correct: true

--- Low Contention (isolated refs) ---

  4 threads:
  opusdb  txns/sec: 1228010  correct: true

  8 threads:
  opusdb  txns/sec: 1253084  correct: true

  16 threads:
  opusdb  txns/sec: 1163300  correct: true

--- Bank Transfer (20 accounts) ---

  4 threads:
  opusdb  txns/sec: 375293  correct: true

  8 threads:
  opusdb  txns/sec: 148432  correct: true

  16 threads:
  opusdb  txns/sec: 137998  correct: true

--- Read-Heavy Mix (10% writes, 10 refs) ---

  4 threads:
  opusdb  txns/sec: 902827  correct: true

  8 threads:
  opusdb  txns/sec: 2096313  correct: true

  16 threads:
  opusdb  txns/sec: 1558007  correct: true

--- Write-Heavy Mix (90% writes, 10 refs) ---

  4 threads:
  opusdb  txns/sec: 436538  correct: true

  8 threads:
  opusdb  txns/sec: 266945  correct: true

  16 threads:
  opusdb  txns/sec: 113074  correct: true
```

#### Criterium Statistical Benchmarks

```
Single increment — opusdb:
Evaluation count : 461892 in 6 samples of 76982 calls.
             Execution time mean : 1.282516 µs
    Execution time std-deviation : 31.923214 ns
   Execution time lower quantile : 1.228732 µs ( 2.5%)
   Execution time upper quantile : 1.314263 µs (97.5%)
                   Overhead used : 2.032557 ns

Single ref-set — opusdb:
Evaluation count : 585174 in 6 samples of 97529 calls.
             Execution time mean : 1.026886 µs
    Execution time std-deviation : 37.358346 ns
   Execution time lower quantile : 984.148428 ns ( 2.5%)
   Execution time upper quantile : 1.071615 µs (97.5%)
                   Overhead used : 2.032557 ns

Read-only 5 refs — opusdb:
Evaluation count : 591780 in 6 samples of 98630 calls.
             Execution time mean : 1.109876 µs
    Execution time std-deviation : 51.566390 ns
   Execution time lower quantile : 1.055797 µs ( 2.5%)
   Execution time upper quantile : 1.157703 µs (97.5%)
                   Overhead used : 2.032557 ns

Read-only 10 refs — opusdb:
Evaluation count : 406830 in 6 samples of 67805 calls.
             Execution time mean : 1.557434 µs
    Execution time std-deviation : 30.515230 ns
   Execution time lower quantile : 1.529976 µs ( 2.5%)
   Execution time upper quantile : 1.604886 µs (97.5%)
                   Overhead used : 2.032557 ns

Write 5 refs — opusdb:
Evaluation count : 175032 in 6 samples of 29172 calls.
             Execution time mean : 3.728842 µs
    Execution time std-deviation : 106.124248 ns
   Execution time lower quantile : 3.610080 µs ( 2.5%)
   Execution time upper quantile : 3.838749 µs (97.5%)
                   Overhead used : 2.032557 ns
```

#### Summary

| Operation         | Mean Latency |
| ----------------- | ------------ |
| Single ref-set    | 1.03 µs      |
| Single increment  | 1.28 µs      |
| Read-only 5 refs  | 1.11 µs      |
| Read-only 10 refs | 1.56 µs      |
| Write 5 refs      | 3.73 µs      |

| Scenario                      | Throughput (txns/sec) |
| ----------------------------- | --------------------- |
| Low contention (4–16 threads) | 1.1M–1.2M             |
| High contention (16 threads)  | 408k                  |
| Read-heavy mix (8 threads)    | 2.1M                  |
| Bank transfer (4 threads)     | 375k                  |

## License

Copyright © 2026 OpusDB Contributors

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
<http://www.eclipse.org/legal/epl-2.0>.
