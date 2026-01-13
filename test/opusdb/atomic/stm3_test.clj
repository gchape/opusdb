(ns opusdb.atomic.stm3-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [opusdb.atomic.stm3 :as stm]))

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