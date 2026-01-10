(ns opusdb.opustm.event-mgr-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [opusdb.opustm.event-mgr :as em]))

(use-fixtures :each
  (fn [f]
    (em/clear-events!)
    (f)
    (em/clear-events!)))

(deftest test-transaction-requirement
  (testing "operations require dosync transaction"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"must be called inside dosync"
                          (em/on :test/event identity)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"must be called inside dosync"
                          (em/once :test/event identity)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"must be called inside dosync"
                          (em/dismiss :test/event identity)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"must be called inside dosync"
                          (em/emit :test/event)))))

(deftest test-on-registration
  (testing "registers persistent event handler"
    (let [calls (atom [])]
      (dosync
       (em/on :test/event #(swap! calls conj %)))
      (dosync
       (em/emit :test/event :data1)
       (em/emit :test/event :data2))
      (is (= [:data1 :data2] @calls)))))

(deftest test-on-with-args
  (testing "registers handler with additional arguments"
    (let [calls (atom [])]
      (dosync
       (em/on :test/event #(swap! calls conj [%1 %2 %3]) :arg1 :arg2))
      (dosync
       (em/emit :test/event :context))
      (is (= [[:context :arg1 :arg2]] @calls)))))

(deftest test-once-registration
  (testing "registers handler that fires only once"
    (let [calls (atom [])]
      (dosync
       (em/once :test/event #(swap! calls conj %)))
      (dosync
       (em/emit :test/event :data1)
       (em/emit :test/event :data2))
      (is (= [:data1] @calls)))))

(deftest test-once-with-args
  (testing "registers once handler with additional arguments"
    (let [calls (atom [])]
      (dosync
       (em/once :test/event #(swap! calls conj [%1 %2]) :extra-arg))
      (dosync
       (em/emit :test/event :context)
       (em/emit :test/event :context2))
      (is (= [[:context :extra-arg]] @calls)))))

(deftest test-dismiss-by-function
  (testing "removes all handlers matching function"
    (let [calls (atom [])
          handler #(swap! calls conj %)]
      (dosync
       (em/on :test/event handler :arg1)
       (em/on :test/event handler :arg2)
       (em/dismiss :test/event handler))
      (dosync
       (em/emit :test/event :data))
      (is (= [] @calls)))))

(deftest test-emit-without-context
  (testing "emits event without context"
    (let [calls (atom [])]
      (dosync
       (em/on :test/event #(swap! calls conj [:no-context %]) :arg1))
      (dosync
       (em/emit :test/event))
      (is (= [[:no-context :arg1]] @calls)))))

(deftest test-emit-with-context
  (testing "emits event with context passed to handlers"
    (let [calls (atom [])]
      (dosync
       (em/on :test/event #(swap! calls conj [%1 %2]) :arg1))
      (dosync
       (em/emit :test/event {:user-id 123}))
      (is (= [[{:user-id 123} :arg1]] @calls)))))

(deftest test-multiple-handlers
  (testing "invokes all registered handlers"
    (let [calls1 (atom [])
          calls2 (atom [])]
      (dosync
       (em/on :test/event #(swap! calls1 conj %))
       (em/on :test/event #(swap! calls2 conj %)))
      (dosync
       (em/emit :test/event :data))
      (is (= [:data] @calls1))
      (is (= [:data] @calls2)))))

(deftest test-mixed-persistent-and-once
  (testing "handles mix of persistent and once handlers"
    (let [persistent-calls (atom [])
          once-calls (atom [])]
      (dosync
       (em/on :test/event #(swap! persistent-calls conj %))
       (em/once :test/event #(swap! once-calls conj %)))
      (dosync
       (em/emit :test/event :data1)
       (em/emit :test/event :data2))
      (is (= [:data1 :data2] @persistent-calls))
      (is (= [:data1] @once-calls)))))

(deftest test-clear-events
  (testing "clears all registered handlers"
    (let [calls (atom [])]
      (dosync
       (em/on :test/event1 #(swap! calls conj :e1))
       (em/on :test/event2 #(swap! calls conj :e2)))
      (em/clear-events!)
      (dosync
       (em/emit :test/event1)
       (em/emit :test/event2))
      (is (= [] @calls)))))

(deftest test-handler-error-handling
  (testing "continues processing handlers when one throws exception"
    (let [calls (atom [])]
      (dosync
       (em/on :test/event (fn [_] (throw (Exception. "Handler error"))))
       (em/on :test/event #(swap! calls conj %)))
      (dosync
       (em/emit :test/event :data))
      (is (= [:data] @calls)))))

(deftest test-multiple-event-keys
  (testing "handlers are isolated by event key"
    (let [calls1 (atom [])
          calls2 (atom [])]
      (dosync
       (em/on :test/event1 #(swap! calls1 conj %))
       (em/on :test/event2 #(swap! calls2 conj %)))
      (dosync
       (em/emit :test/event1 :data1)
       (em/emit :test/event2 :data2))
      (is (= [:data1] @calls1))
      (is (= [:data2] @calls2)))))

(deftest test-no-handlers
  (testing "emitting event with no handlers doesn't error"
    (is (nil? (dosync (em/emit :nonexistent/event))))
    (is (nil? (dosync (em/emit :nonexistent/event :context))))))

(deftest test-event-context-binding
  (testing "event context is properly bound during emit"
    (let [captured-context (atom nil)]
      (dosync
       (em/on :test/event (fn [ctx] (reset! captured-context ctx))))
      (dosync
       (em/emit :test/event {:test-data 123}))
      (is (= {:test-data 123} @captured-context)))))

(deftest test-return-values
  (testing "public functions return nil"
    (dosync
     (is (nil? (em/on :test/event identity)))
     (is (nil? (em/once :test/event identity)))
     (is (nil? (em/dismiss :test/event identity)))
     (is (nil? (em/emit :test/event))))))