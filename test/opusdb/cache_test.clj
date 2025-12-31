(ns opusdb.cache-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [opusdb.cache :as cache]))

;; Reset cache state between tests
(defn reset-cache-state [f]
  (reset! cache/cache-map {})
  (reset! cache/cache-opts-map {})
  (reset! cache/id 0)
  (f))

(use-fixtures :once reset-cache-state)

;; ---------- Basic API tests ----------
(deftest test-make-cache
  (testing "Creating cache with default LRU type"
    (let [initial-id @cache/id
          cid (cache/make-cache 3 nil)]
      (is (= (inc initial-id) cid))
      (is (nil? (@cache/cache-map cid)))
      (is (= {:size 3 :eviction-fn nil :type "LRU"}
             (@cache/cache-opts-map cid)))))

  (testing "Creating cache with explicit MRU type"
    (let [initial-id @cache/id
          cid (cache/make-cache 5 nil "MRU")]
      (is (= (inc initial-id) cid))
      (is (= "MRU" (:type (@cache/cache-opts-map cid))))))

  (testing "Multiple caches have unique IDs"
    (let [initial-id @cache/id
          cid1 (cache/make-cache 3 nil)
          cid2 (cache/make-cache 5 nil)
          cid3 (cache/make-cache 2 nil)]
      (is (= (inc initial-id) cid1))
      (is (= (+ initial-id 2) cid2))
      (is (= (+ initial-id 3) cid3))
      (is (not= cid1 cid2))
      (is (not= cid2 cid3))
      (is (not= cid1 cid3)))))

(deftest test-put-and-get
  (testing "Put and get single item"
    (let [cid (cache/make-cache 3 nil)]
      (cache/put cid 1 "one")
      (is (= "one" (cache/get cid 1)))))

  (testing "Get non-existent key returns nil"
    (let [cid (cache/make-cache 3 nil)]
      (cache/put cid 1 "one")
      (is (nil? (cache/get cid 99)))))

  (testing "Overwriting existing key updates value"
    (let [cid (cache/make-cache 3 nil)]
      (cache/put cid 1 "one")
      (cache/put cid 1 "ONE")
      (is (= "ONE" (cache/get cid 1))))))

;; ---------- LRU eviction tests ----------
(deftest test-lru-eviction
  (testing "LRU evicts least recently used item"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          cid (cache/make-cache 3 eviction-fn "LRU")]
      (cache/put cid 1 "one")
      (cache/put cid 2 "two")
      (cache/put cid 3 "three")
      (cache/put cid 4 "four")

      (is (= 1 (count @evicted)))
      (is (nil? (cache/get cid 1)))
      (is (= "four" (cache/get cid 4)))))

  (testing "LRU multiple evictions"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          cid (cache/make-cache 2 eviction-fn "LRU")]
      (cache/put cid 1 "one")
      (cache/put cid 2 "two")
      (cache/put cid 3 "three")
      (cache/put cid 4 "four")

      (is (= 2 (count @evicted))))))

;; ---------- MRU eviction tests ----------
(deftest test-mru-eviction
  (testing "MRU evicts most recently used item (root)"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          cid (cache/make-cache 3 eviction-fn "MRU")]
      (cache/put cid 1 "one")
      (cache/put cid 2 "two")
      (cache/put cid 3 "three")
      (cache/put cid 4 "four")

      (is (= 1 (count @evicted)))
      (is (= "four" (cache/get cid 4)))))

  (testing "MRU with access pattern"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          cid (cache/make-cache 3 eviction-fn "MRU")]
      (cache/put cid 1 "one")
      (cache/put cid 2 "two")
      (cache/put cid 3 "three")

      ;; Access key 2, making it the root
      (cache/get cid 2)

      ;; Add key 4, should evict key 2 (most recently accessed)
      (cache/put cid 4 "four")

      (is (= 1 (count @evicted)))
      (is (nil? (cache/get cid 2)))
      (is (= "four" (cache/get cid 4))))))

;; ---------- change-type tests ----------
(deftest test-change-type
  (testing "Change cache type from LRU to MRU"
    (let [cid (cache/make-cache 3 nil "LRU")]
      (is (= "LRU" (:type (@cache/cache-opts-map cid))))
      (cache/change-type cid "MRU")
      (is (= "MRU" (:type (@cache/cache-opts-map cid))))))

  (testing "Change cache type from MRU to LRU"
    (let [cid (cache/make-cache 3 nil "MRU")]
      (is (= "MRU" (:type (@cache/cache-opts-map cid))))
      (cache/change-type cid "LRU")
      (is (= "LRU" (:type (@cache/cache-opts-map cid)))))))

;; ---------- Eviction callback tests ----------
(deftest test-eviction-callback
  (testing "Eviction callback is called with correct args"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj {:key k :value v}))
          cid (cache/make-cache 2 eviction-fn)]
      (cache/put cid 1 "one")
      (cache/put cid 2 "two")
      (cache/put cid 3 "three")

      (is (= [{:key 1 :value "one"}] @evicted))))

  (testing "No callback when eviction-fn is nil"
    (let [cid (cache/make-cache 2 nil)]
      (cache/put cid 1 "one")
      (cache/put cid 2 "two")
      (cache/put cid 3 "three")
      ;; Should not throw
      (is (= "three" (cache/get cid 3))))))

;; ---------- Edge cases ----------
(deftest test-edge-cases
  (testing "Cache of size 1"
    (let [evicted (atom [])
          eviction-fn (fn [k v] (swap! evicted conj [k v]))
          cid (cache/make-cache 1 eviction-fn)]
      (cache/put cid 1 "one")
      (is (= "one" (cache/get cid 1)))
      (cache/put cid 2 "two")
      (is (= [[1 "one"]] @evicted))
      (is (nil? (cache/get cid 1)))
      (is (= "two" (cache/get cid 2)))))

  (testing "Empty cache get returns nil"
    (let [cid (cache/make-cache 3 nil)]
      (is (nil? (cache/get cid 1)))))

  (testing "Put with nil value"
    (let [cid (cache/make-cache 3 nil)]
      (cache/put cid 1 nil)
      (is (nil? (cache/get cid 1)))))

  (testing "Large keys and values"
    (let [cid (cache/make-cache 3 nil)]
      (cache/put cid 999999 "large-key-value")
      (is (= "large-key-value" (cache/get cid 999999))))))

;; ---------- Multiple cache instances ----------
(deftest test-multiple-caches
  (testing "Multiple caches are independent"
    (let [cid1 (cache/make-cache 2 nil)
          cid2 (cache/make-cache 3 nil)]
      (cache/put cid1 1 "cache1-one")
      (cache/put cid2 1 "cache2-one")

      (is (= "cache1-one" (cache/get cid1 1)))
      (is (= "cache2-one" (cache/get cid2 1)))

      (cache/put cid1 2 "cache1-two")
      (cache/put cid2 2 "cache2-two")

      (is (= "cache1-two" (cache/get cid1 2)))
      (is (= "cache2-two" (cache/get cid2 2))))))

;; ---------- Splay tree behavior ----------
(deftest test-splay-on-access
  (testing "Put operation splays inserted key to root"
    (let [cid (cache/make-cache 5 nil)]
      (cache/put cid 1 "one")
      (cache/put cid 2 "two")
      (cache/put cid 3 "three")

      (let [tree (@cache/cache-map cid)]
        (is (= 3 (:key tree)))))))
