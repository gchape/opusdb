## Bank Transfer Benchmark

The bank consists of 10 accounts, each initialized with 1000 units. Transfers are implemented as atomic transactions that:
- Read source and destination balances
- Verify sufficient funds
- Update both accounts and increment a transfer counter
- Abort if funds are insufficient

The STM guarantees that either all operations complete or none do, preventing partial transfers or lost updates.

### Test Scenarios

**Throughput measurement (5 second stress test)**

Twenty threads execute transfers concurrently for 5 seconds. Each thread randomly selects two different accounts and transfers a random amount between 1-50 units. After completion, the benchmark verifies that the total across all accounts remains exactly 10,000 units—any deviation indicates a violation of transactional consistency. The number of successful transfers indicates overall system throughput.

**Single transaction latency**

Measures the baseline cost of a single transfer operation (account 0 to account 1, transferring 10 units) using Criterium's statistical sampling. This establishes the fundamental transaction overhead without any concurrency.

**Moderate concurrency (10 parallel transfers)**

Ten futures execute simultaneous transfers in a ring pattern: thread i transfers from account i to account (i+1) mod 10. Each transfers 5 units. This scenario tests whether the STM can parallelize non-conflicting transactions effectively, since each account is only accessed by at most two threads.

**Maximum contention (20 threads, 2 accounts)**

Twenty threads simultaneously attempt to transfer 1 unit from account 0 to account 1. This creates a pathological case where all transactions conflict on the same two refs. Performance here depends entirely on the conflict resolution mechanism—specifically how efficiently the retry logic and backoff strategy handle serialized access to heavily contested resources.

### Results

; Evaluating file: bank_tran.clj
; 
; === BANK TRANSFERS ===
; Total (should be 10000 ): 10000
; Successful transfers: 331432
; 
; Benchmarking single transfer transaction:
; Evaluation count : 154200 in 6 samples of 25700 calls.
;              Execution time mean : 4.224818 µs
;     Execution time std-deviation : 284.678826 ns
;    Execution time lower quantile : 3.966891 µs ( 2.5%)
;    Execution time upper quantile : 4.542472 µs (97.5%)
;                    Overhead used : 2.432259 ns
; 
; Benchmarking concurrent transfers:
; (10 concurrent transfer operations)
; Evaluation count : 180 in 6 samples of 30 calls.
;              Execution time mean : 3.659315 ms
;     Execution time std-deviation : 212.438316 µs
;    Execution time lower quantile : 3.468588 ms ( 2.5%)
;    Execution time upper quantile : 3.992595 ms (97.5%)
;                    Overhead used : 2.432259 ns
; 
; Found 1 outliers in 6 samples (16.6667 %)
; 	low-severe	 1 (16.6667 %)
;  Variance from outliers : 14.5203 % Variance is moderately inflated by outliers
; 
; Benchmarking high-contention scenario:
; (20 threads all transferring from/to same 2 accounts)
; Evaluation count : 9246 in 6 samples of 1541 calls.
;              Execution time mean : 78.742568 µs
;     Execution time std-deviation : 4.561345 µs
;    Execution time lower quantile : 73.106659 µs ( 2.5%)
;    Execution time upper quantile : 84.362971 µs (97.5%)
;                    Overhead used : 2.432259 ns

- Single STM transactions complete in ~4 µs.
- The system sustains over 330k successful transfers in 5 seconds under randomized concurrent load.
- Performance degrades predictably under worst-case contention, with no pathological retry behavior observed.

## Santa Claus Problem Benchmark

The system models:

* **9 reindeer**, all of which must return before a toy delivery
* **10 elves**, requesting help in groups of **3**
* **Reindeer have priority** over elves

All coordination is implemented using atomic STM transactions without explicit locking.

### Test Scenario

**10-second concurrent simulation**

* 9 reindeer threads repeatedly return from vacation
* 10 elf threads repeatedly request help
* Reindeer sleep for 100–300 ms between returns
* Elves sleep for 50–150 ms between requests

Santa’s actions emerge from transactional rules:

* A delivery occurs only when all reindeer are present
* Elf meetings occur only when at least three elves are waiting
* Elf meetings are blocked while a reindeer delivery is pending

### Correctness Guarantees

* Deliveries occur only when all 9 reindeer are present
* Elves are helped strictly in groups of three
* Reindeer deliveries are never interrupted by elf meetings
* No deadlock or livelock
* Both elves and reindeer make progress (no starvation)

### Results

```
Toy deliveries:         41
Elf meetings:           334
Reindeer still waiting: 2
Elves still waiting:    1
```

Remaining waiters are expected due to abrupt simulation termination.

### Concurrent Transaction Benchmark

Measures the cost of executing all Santa-related transactions concurrently.

* 19 concurrent transactions (9 reindeer + 10 elves)
* Shared STM state with read/write contention
* Measured using Criterium

```
Execution time mean : 3.599055 ms
Execution time std-deviation : 221.537723 µs
```

This indicates bounded retries and predictable performance under contention.