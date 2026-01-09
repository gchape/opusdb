(ns opusdb.cache.splay-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [opusdb.cache.splay :as cache]))

(deftest test-make-cache
  (testing "Creating cache with default LRU type"
    (let [c (cache/make-cache 3 nil)]
      (is (= 3 (:size c)))
      (is (= "LRU" (:type c)))
      (is (nil? @(:tree c)))))

  (testing "Creating cache with explicit MRU type"
    (let [c (cache/make-cache 5 nil "MRU")]
      (is (= 5 (:size c)))
      (is (= "MRU" (:type c))))))

(deftest test-put-and-get
  (testing "Put and get single item"
    (let [c (cache/make-cache 3 nil)]
      (cache/put! c 1 "one")
      (is (= "one" (cache/get c 1)))))

  (testing "Get non-existent key returns nil"
    (let [c (cache/make-cache 3 nil)]
      (cache/put! c 1 "one")
      (is (nil? (cache/get c 99)))))

  (testing "Overwriting existing key updates value"
    (let [c (cache/make-cache 3 nil)]
      (cache/put! c 1 "one")
      (cache/put! c 1 "ONE")
      (is (= "ONE" (cache/get c 1))))))

(deftest test-lru-eviction
  (testing "LRU evicts least recently used item"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          c (cache/make-cache 3 eviction-fn "LRU")]
      (cache/put! c 1 "one")
      (cache/put! c 2 "two")
      (cache/put! c 3 "three")
      (cache/put! c 4 "four")

      (is (= 1 (count @evicted)))
      (is (nil? (cache/get c 1)))
      (is (= "four" (cache/get c 4)))))

  (testing "LRU multiple evictions"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          c (cache/make-cache 2 eviction-fn "LRU")]
      (cache/put! c 1 "one")
      (cache/put! c 2 "two")
      (cache/put! c 3 "three")
      (cache/put! c 4 "four")

      (is (= 2 (count @evicted))))))

(deftest test-mru-eviction
  (testing "MRU evicts most recently used item"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          c (cache/make-cache 3 eviction-fn "MRU")]
      (cache/put! c 1 "one")
      (cache/put! c 2 "two")
      (cache/put! c 3 "three")
      (cache/put! c 4 "four")

      (is (= 1 (count @evicted)))
      (is (= "four" (cache/get c 4)))))

  (testing "MRU with access pattern"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          c (cache/make-cache 3 eviction-fn "MRU")]
      (cache/put! c 1 "one")
      (cache/put! c 2 "two")
      (cache/put! c 3 "three")

      ;; Access key 2 → becomes root
      (cache/get c 2)

      ;; Insert key 4 → evict MRU (key 2)
      (cache/put! c 4 "four")

      (is (= 1 (count @evicted)))
      (is (nil? (cache/get c 2)))
      (is (= "four" (cache/get c 4))))))

(deftest test-eviction-callback
  (testing "Eviction callback is called with correct args"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj {:key k :value v}))
          c (cache/make-cache 2 eviction-fn)]
      (cache/put! c 1 "one")
      (cache/put! c 2 "two")
      (cache/put! c 3 "three")

      (is (= [{:key 1 :value "one"}] @evicted))))

  (testing "No callback when eviction-fn is nil"
    (let [c (cache/make-cache 2 nil)]
      (cache/put! c 1 "one")
      (cache/put! c 2 "two")
      (cache/put! c 3 "three")
      (is (= "three" (cache/get c 3))))))

(deftest test-edge-cases
  (testing "Cache of size 1"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          c (cache/make-cache 1 eviction-fn)]
      (cache/put! c 1 "one")
      (is (= "one" (cache/get c 1)))
      (cache/put! c 2 "two")
      (is (= [[1 "one"]] @evicted))
      (is (nil? (cache/get c 1)))
      (is (= "two" (cache/get c 2)))))

  (testing "Empty cache get returns nil"
    (let [c (cache/make-cache 3 nil)]
      (is (nil? (cache/get c 1)))))

  (testing "Put with nil value"
    (let [c (cache/make-cache 3 nil)]
      (cache/put! c 1 nil)
      (is (nil? (cache/get c 1)))))

  (testing "Large keys and values"
    (let [c (cache/make-cache 3 nil)]
      (cache/put! c 999999 "large-key-value")
      (is (= "large-key-value" (cache/get c 999999))))))

(deftest test-multiple-caches
  (testing "Multiple caches are independent"
    (let [c1 (cache/make-cache 2 nil)
          c2 (cache/make-cache 3 nil)]
      (cache/put! c1 1 "cache1-one")
      (cache/put! c2 1 "cache2-one")

      (is (= "cache1-one" (cache/get c1 1)))
      (is (= "cache2-one" (cache/get c2 1)))

      (cache/put! c1 2 "cache1-two")
      (cache/put! c2 2 "cache2-two")

      (is (= "cache1-two" (cache/get c1 2)))
      (is (= "cache2-two" (cache/get c2 2))))))

(deftest test-splay-on-access
  (testing "Put operation splays inserted key to root"
    (let [c (cache/make-cache 5 nil)]
      (cache/put! c 1 "one")
      (cache/put! c 2 "two")
      (cache/put! c 3 "three")
      (is (= 3 (:key @(:tree c)))))))
