(ns opusdb.atomic.stm-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [opusdb.atomic.stm :as stm]))

(deftest test-ref-creation-and-deref
  (testing "Creating and dereferencing refs"
    (let [r (stm/ref 42)]
      (is (= 42 (stm/deref r))))))

(deftest test-ref-set-in-transaction
  (testing "Setting ref value in transaction"
    (let [r (stm/ref 10)]
      (stm/dosync
       (stm/ref-set r 20))
      (is (= 20 (stm/deref r))))))

(deftest test-ref-set-outside-transaction-throws
  (testing "ref-set outside transaction throws exception"
    (let [r (stm/ref 10)]
      (is (thrown? IllegalStateException
                   (stm/ref-set r 20))))))

(deftest test-alter-in-transaction
  (testing "Altering ref value in transaction"
    (let [r (stm/ref 5)]
      (stm/dosync
       (stm/alter r inc))
      (is (= 6 (stm/deref r))))))

(deftest test-alter-with-args
  (testing "Altering ref with function and arguments"
    (let [r (stm/ref 10)]
      (stm/dosync
       (stm/alter r + 5 3))
      (is (= 18 (stm/deref r))))))

(deftest test-multiple-refs-in-transaction
  (testing "Multiple refs in single transaction"
    (let [r1 (stm/ref 10)
          r2 (stm/ref 20)]
      (stm/dosync
       (stm/ref-set r1 15)
       (stm/ref-set r2 25))
      (is (= 15 (stm/deref r1)))
      (is (= 25 (stm/deref r2))))))

(deftest test-transaction-atomicity
  (testing "Transaction is atomic - all or nothing"
    (let [r1 (stm/ref 10)
          r2 (stm/ref 20)]
      (try
        (stm/dosync
         (stm/ref-set r1 100)
         (throw (Exception. "Intentional failure"))
         (stm/ref-set r2 200))
        (catch Exception _))
      ;; Both refs should retain original values
      (is (= 10 (stm/deref r1)))
      (is (= 20 (stm/deref r2))))))

(deftest test-read-consistency-in-transaction
  (testing "Reads within transaction see consistent snapshot"
    (let [r1 (stm/ref 10)
          r2 (stm/ref 20)
          results (atom [])]
      (stm/dosync
       (swap! results conj (stm/deref r1))
       (stm/ref-set r1 100)
       (swap! results conj (stm/deref r1))  ; Should see the write
       (swap! results conj (stm/deref r2)))
      (is (= [10 100 20] @results)))))

(deftest test-nested-dosync
  (testing "Nested transactions flatten to outer transaction"
    (let [r (stm/ref 10)]
      (stm/dosync
       (stm/dosync
        (stm/ref-set r 20))
       (is (= 20 (stm/deref r))))  ; Inner change visible in outer
      (is (= 20 (stm/deref r))))))

(deftest test-concurrent-increments
  (testing "Concurrent increments maintain consistency"
    (let [r (stm/ref 0)
          n-threads 10
          n-increments 100
          threads (doall
                   (for [_ (range n-threads)]
                     (Thread.
                      #(dotimes [_ n-increments]
                         (stm/dosync
                          (stm/alter r inc))))))]
      (doseq [t threads] (.start t))
      (doseq [t threads] (.join t))
      (is (= (* n-threads n-increments) (stm/deref r))))))

(deftest test-concurrent-transfer
  (testing "Concurrent transfers between accounts"
    (let [account1 (stm/ref 1000)
          account2 (stm/ref 1000)
          n-threads 10
          n-transfers 50
          transfer-amount 10

          transfer (fn [from to amount]
                     (stm/dosync
                      (stm/alter from - amount)
                      (stm/alter to + amount)))

          threads (doall
                   (for [i (range n-threads)]
                     (Thread.
                      #(dotimes [_ n-transfers]
                         (if (even? i)
                           (transfer account1 account2 transfer-amount)
                           (transfer account2 account1 transfer-amount))))))]

      (doseq [t threads] (.start t))
      (doseq [t threads] (.join t))

      ;; Total should be conserved
      (is (= 2000 (+ (stm/deref account1) (stm/deref account2)))))))

(deftest test-read-write-conflict
  (testing "Read-write conflicts trigger retry"
    (let [r (stm/ref 0)
          barrier (promise)
          results (atom [])

          writer (Thread.
                  #(do
                     @barrier  ; Wait for reader to start
                     (Thread/sleep 50)
                     (stm/dosync
                      (stm/ref-set r 100))))

          reader (Thread.
                  #(stm/dosync
                    (let [v1 (stm/deref r)]
                      (swap! results conj [:read-1 v1])
                      (deliver barrier true)
                      (Thread/sleep 100)  ; Let writer commit
                      (let [v2 (stm/deref r)]
                        (swap! results conj [:read-2 v2])
                        (stm/ref-set r (+ v2 10))))))]

      (.start writer)
      (.start reader)
      (.join writer)
      (.join reader)

      ;; Reader should retry and see consistent view
      (is (= 110 (stm/deref r))))))

(deftest test-consistent-snapshot
  (testing "Transaction sees consistent point-in-time snapshot"
    (let [r1 (stm/ref 10)
          r2 (stm/ref 20)
          observed (atom [])]

      ;; Modify r1 outside transaction
      (stm/dosync (stm/ref-set r1 15))

      ;; Transaction should see consistent snapshot
      (stm/dosync
       (swap! observed conj [(stm/deref r1) (stm/deref r2)])
       (Thread/sleep 50)
       (swap! observed conj [(stm/deref r1) (stm/deref r2)]))

      ;; Both reads should see same values
      (is (= [15 20] (first @observed)))
      (is (= [15 20] (second @observed))))))

(deftest test-empty-transaction
  (testing "Empty transaction completes successfully"
    (is (nil? (stm/dosync)))))

(deftest test-transaction-return-value
  (testing "Transaction returns last expression value"
    (let [r (stm/ref 10)]
      (is (= 42 (stm/dosync
                 (stm/ref-set r 20)
                 42))))))

(deftest test-multiple-writes-same-ref
  (testing "Multiple writes to same ref in transaction"
    (let [r (stm/ref 0)]
      (stm/dosync
       (stm/ref-set r 10)
       (stm/ref-set r 20)
       (stm/ref-set r 30))
      (is (= 30 (stm/deref r))))))

(deftest test-read-own-write
  (testing "Transaction can read its own writes"
    (let [r (stm/ref 10)]
      (stm/dosync
       (stm/ref-set r 20)
       (is (= 20 (stm/deref r)))
       (stm/alter r inc)
       (is (= 21 (stm/deref r)))))))

(deftest test-concurrent-readers
  (testing "Multiple concurrent readers don't conflict"
    (let [r (stm/ref 42)
          n-threads 20
          results (atom [])
          threads (doall
                   (for [_ (range n-threads)]
                     (Thread.
                      #(stm/dosync
                        (swap! results conj (stm/deref r))))))]

      (doseq [t threads] (.start t))
      (doseq [t threads] (.join t))

      ;; All should read the same value
      (is (= n-threads (count @results)))
      (is (every? #(= 42 %) @results)))))

(deftest test-high-contention-stress
  (testing "High contention on single ref"
    (let [r (stm/ref 0)
          n-threads 20
          n-ops 100
          threads (doall
                   (for [_ (range n-threads)]
                     (Thread.
                      #(dotimes [_ n-ops]
                         (stm/dosync
                          (stm/alter r inc))))))]

      (doseq [t threads] (.start t))
      (doseq [t threads] (.join t))

      (is (= (* n-threads n-ops) (stm/deref r))))))

(deftest test-many-refs-stress
  (testing "Transaction with many refs"
    (let [n-refs 50
          refs (vec (repeatedly n-refs #(stm/ref 0)))]

      (stm/dosync
       (doseq [r refs]
         (stm/alter r inc)))

      (is (every? #(= 1 (stm/deref %)) refs)))))

(deftest test-long-running-transaction
  (testing "Long-running transaction eventually succeeds"
    (let [r (stm/ref 0)
          result (atom nil)

          interferor (Thread.
                      #(dotimes [_ 10]
                         (Thread/sleep 10)
                         (stm/dosync
                          (stm/alter r inc))))

          long-tx (Thread.
                   #(reset! result
                            (stm/dosync
                             (let [start (stm/deref r)]
                               (Thread/sleep 150)
                               (stm/ref-set r (+ start 1000))
                               (stm/deref r)))))]

      (.start interferor)
      (.start long-tx)
      (.join interferor)
      (.join long-tx)

      ;; Long transaction should eventually succeed
      (is (>= @result 1000)))))

(deftest test-deref-outside-transaction
  (testing "Deref outside transaction reads latest committed value"
    (let [r (stm/ref 10)]
      (stm/dosync (stm/ref-set r 20))
      (is (= 20 (stm/deref r)))
      (stm/dosync (stm/ref-set r 30))
      (is (= 30 (stm/deref r))))))

(deftest test-on-commit-basic
  (testing "on-commit callback executes after successful transaction"
    (let [r (stm/ref 0)
          executed (atom false)]
      (stm/dosync
       (stm/ref-set r 42)
       (stm/on-commit #(reset! executed true)))

      (is (= 42 (stm/deref r)))
      (is @executed "on-commit callback should have executed"))))

(deftest test-on-rollback-basic
  (testing "on-rollback callback executes when transaction fails"
    (let [r (stm/ref 0)
          rollback-executed (atom false)]
      (try
        (stm/dosync
         (stm/ref-set r 42)
         (stm/on-rollback #(reset! rollback-executed true))
         (throw (Exception. "Intentional failure")))
        (catch Exception _))

      (is (= 0 (stm/deref r)) "ref should retain original value")
      (is @rollback-executed "on-rollback callback should have executed"))))

(deftest test-multiple-commit-callbacks
  (testing "Multiple on-commit callbacks all execute"
    (let [r (stm/ref 0)
          results (atom [])]
      (stm/dosync
       (stm/ref-set r 100)
       (stm/on-commit #(swap! results conj :first))
       (stm/on-commit #(swap! results conj :second))
       (stm/on-commit #(swap! results conj :third)))

      (is (= 100 (stm/deref r)))
      (is (= 3 (count @results)))
      (is (= #{:first :second :third} (set @results))))))

(deftest test-multiple-rollback-callbacks
  (testing "Multiple on-rollback callbacks all execute"
    (let [r (stm/ref 0)
          results (atom [])]
      (try
        (stm/dosync
         (stm/ref-set r 100)
         (stm/on-rollback #(swap! results conj :first))
         (stm/on-rollback #(swap! results conj :second))
         (stm/on-rollback #(swap! results conj :third))
         (throw (Exception. "Force rollback")))
        (catch Exception _))

      (is (= 0 (stm/deref r)))
      (is (= 3 (count @results)))
      (is (= #{:first :second :third} (set @results))))))

(deftest test-commit-callbacks-not-executed-on-rollback
  (testing "on-commit callbacks don't execute when transaction rolls back"
    (let [r (stm/ref 0)
          commit-executed (atom false)
          rollback-executed (atom false)]
      (try
        (stm/dosync
         (stm/ref-set r 42)
         (stm/on-commit #(reset! commit-executed true))
         (stm/on-rollback #(reset! rollback-executed true))
         (throw (Exception. "Force rollback")))
        (catch Exception _))

      (is (= 0 (stm/deref r)))
      (is (not @commit-executed) "on-commit should not execute on rollback")
      (is @rollback-executed "on-rollback should execute"))))

(deftest test-rollback-callbacks-not-executed-on-commit
  (testing "on-rollback callbacks don't execute when transaction commits"
    (let [r (stm/ref 0)
          commit-executed (atom false)
          rollback-executed (atom false)]
      (stm/dosync
       (stm/ref-set r 42)
       (stm/on-commit #(reset! commit-executed true))
       (stm/on-rollback #(reset! rollback-executed true)))

      (is (= 42 (stm/deref r)))
      (is @commit-executed "on-commit should execute on success")
      (is (not @rollback-executed) "on-rollback should not execute on success"))))

(deftest test-commit-callback-sees-committed-values
  (testing "on-commit callback can read committed ref values"
    (let [r1 (stm/ref 10)
          r2 (stm/ref 20)
          observed (atom nil)]
      (stm/dosync
       (stm/ref-set r1 100)
       (stm/ref-set r2 200)
       (stm/on-commit #(reset! observed [(stm/deref r1) (stm/deref r2)])))

      (is (= [100 200] @observed)))))

(deftest test-rollback-callback-sees-original-values
  (testing "on-rollback callback sees original values after rollback"
    (let [r1 (stm/ref 10)
          r2 (stm/ref 20)
          observed (atom nil)]
      (try
        (stm/dosync
         (stm/ref-set r1 100)
         (stm/ref-set r2 200)
         (stm/on-rollback #(reset! observed [(stm/deref r1) (stm/deref r2)]))
         (throw (Exception. "Force rollback")))
        (catch Exception _))

      (is (= [10 20] @observed)))))

(deftest test-commit-callbacks-with-retry
  (testing "on-commit callbacks execute only on final successful commit"
    (let [r (stm/ref 0)
          commit-count (atom 0)
          barrier (promise)]

      ;; Start a conflicting transaction
      (let [conflicting (future
                          @barrier
                          (Thread/sleep 50)
                          (stm/dosync (stm/ref-set r 999)))]

        ;; This transaction will retry
        (stm/dosync
         (let [v (stm/deref r)]
           (deliver barrier true)
           (Thread/sleep 100)  ; Let conflicting tx commit
           (stm/ref-set r (inc v))
           (stm/on-commit #(swap! commit-count inc))))

        @conflicting)

      ;; Callback should execute exactly once, on final commit
      (is (= 1 @commit-count)))))

(deftest test-rollback-callbacks-execute-on-every-retry
  (testing "on-rollback callbacks execute on each retry attempt"
    (let [r (stm/ref 0)
          rollback-count (atom 0)
          barrier (promise)]

      ;; Start a conflicting transaction
      (let [conflicting (future
                          @barrier
                          (Thread/sleep 50)
                          (stm/dosync (stm/ref-set r 999)))]

        ;; This transaction will retry
        (stm/dosync
         (let [v (stm/deref r)]
           (deliver barrier true)
           (Thread/sleep 100)  ; Let conflicting tx commit
           (stm/ref-set r (inc v))
           (stm/on-rollback #(swap! rollback-count inc))))

        @conflicting)

      ;; Rollback should have been called at least once for retry
      (is (>= @rollback-count 1)))))

(deftest test-on-commit-outside-transaction-throws
  (testing "on-commit outside transaction throws exception"
    (is (thrown? IllegalStateException
                 (stm/on-commit #(println "This should fail"))))))

(deftest test-on-rollback-outside-transaction-throws
  (testing "on-rollback outside transaction throws exception"
    (is (thrown? IllegalStateException
                 (stm/on-rollback #(println "This should fail"))))))

(deftest test-commit-callback-with-side-effects
  (testing "on-commit callbacks can perform side effects"
    (let [r (stm/ref 0)
          log (atom [])]
      (stm/dosync
       (stm/ref-set r 42)
       (stm/on-commit #(swap! log conj :logged))
       (stm/on-commit #(swap! log conj :notified)))

      (is (= 42 (stm/deref r)))
      (is (= 2 (count @log)))
      (is (contains? (set @log) :logged))
      (is (contains? (set @log) :notified)))))

(deftest test-rollback-callback-cleanup
  (testing "on-rollback callbacks can perform cleanup"
    (let [r (stm/ref 0)
          resource (atom :acquired)
          cleaned-up (atom false)]
      (try
        (stm/dosync
         (stm/ref-set r 42)
         (stm/on-rollback #(do
                             (reset! resource :released)
                             (reset! cleaned-up true)))
         (throw (Exception. "Simulate failure")))
        (catch Exception _))

      (is (= 0 (stm/deref r)))
      (is (= :released @resource))
      (is @cleaned-up))))

(deftest test-nested-transaction-callbacks
  (testing "Callbacks in nested transactions are handled correctly"
    (let [r (stm/ref 0)
          outer-commit (atom false)
          inner-commit (atom false)]
      (stm/dosync
       (stm/on-commit #(reset! outer-commit true))
       (stm/dosync
        (stm/ref-set r 42)
        (stm/on-commit #(reset! inner-commit true))))

      (is (= 42 (stm/deref r)))
      ;; Both callbacks should execute since nested dosync flattens
      (is @outer-commit)
      (is @inner-commit))))

(deftest test-callback-exceptions-dont-prevent-commit
  (testing "Exceptions in commit callbacks don't prevent transaction commit"
    (let [r (stm/ref 0)
          first-executed (atom false)
          third-executed (atom false)]
      (try
        (stm/dosync
         (stm/ref-set r 42)
         (stm/on-commit #(reset! first-executed true))
         (stm/on-commit #(throw (Exception. "Callback error")))
         (stm/on-commit #(reset! third-executed true)))
        (catch Exception _))

      ;; Transaction should still commit
      (is (= 42 (stm/deref r)))
      ;; First callback should have executed
      (is @first-executed))))

(deftest test-concurrent-transactions-with-callbacks
  (testing "Callbacks work correctly with concurrent transactions"
    (let [r (stm/ref 0)
          commit-log (atom [])
          n-threads 10
          threads (doall
                   (for [i (range n-threads)]
                     (Thread.
                      #(stm/dosync
                        (stm/alter r inc)
                        (stm/on-commit
                         (fn [] (swap! commit-log conj i)))))))]

      (doseq [t threads] (.start t))
      (doseq [t threads] (.join t))

      (is (= n-threads (stm/deref r)))
      (is (= n-threads (count @commit-log)))
      (is (= (set (range n-threads)) (set @commit-log))))))