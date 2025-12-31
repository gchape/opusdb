(ns opusdb.cache
  (:refer-clojure :exclude [get type remove]))

(def id (atom 0))
(def cache-map (atom {}))
(def cache-opts-map (atom {}))

(defn- make-node [k v]
  {:key k :value v :left nil :right nil})

(defn- rotate-right [tree]
  (let [l (:left tree)]
    (if l
      (assoc l :right (assoc tree :left (:right l)))
      tree)))

(defn- rotate-left [tree]
  (let [r (:right tree)]
    (if r
      (assoc r :left (assoc tree :right (:left r)))
      tree)))

(defn- is-leaf? [tree]
  (every? nil? [(:left tree) (:right tree)]))

(defn- leftmost-key [tree]
  (if (:left tree)
    (recur (:left tree))
    (:key tree)))

(defn- leftmost-node [tree]
  (if (:left tree)
    (recur (:left tree))
    tree))

(defn- size [tree]
  (if (nil? tree)
    0
    (+ 1
       (size (:left tree))
       (size (:right tree)))))

(defn- splay [tree k]
  (cond
    (nil? tree) nil
    (= k (:key tree)) tree
    (neg? (compare k (:key tree)))
    (let [l (:left tree)]
      (cond
        (nil? l) tree
        (neg? (compare k (:key l)))
        (rotate-right
         (rotate-right
          (assoc tree :left (splay (:left l) k))))
        (pos? (compare k (:key l)))
        (rotate-right
         (rotate-left
          (assoc tree :left (splay (:right l) k))))
        :else
        (rotate-right tree)))
    :else
    (let [r (:right tree)]
      (cond
        (nil? r) tree
        (pos? (compare k (:key r)))
        (rotate-left
         (rotate-left
          (assoc tree :right (splay (:right r) k))))
        (neg? (compare k (:key r)))
        (rotate-left
         (rotate-right
          (assoc tree :right (splay (:left r) k))))
        :else
        (rotate-left tree)))))

(defn get [cid k]
  (let [tree (splay (@cache-map cid) k)]
    (when tree
      (swap! cache-map assoc cid tree)
      (letfn [(get_ [tree k]
                (cond
                  (nil? tree) nil
                  (neg? (compare k (:key tree))) (get_ (:left tree) k)
                  (pos? (compare k (:key tree))) (get_ (:right tree) k)
                  :else (:value tree)))]
        (get_ tree k)))))

(defn- remove [tree type]
  (condp = type
    "LRU"
    (letfn [(remove-leftmost [tree]
              (cond
                (nil? tree) nil
                (nil? (:left tree)) (:right tree)
                :else (assoc tree :left (remove-leftmost (:left tree)))))]
      (remove-leftmost tree))
    "MRU"
    (cond
      (nil? tree) nil
      (is-leaf? tree) nil
      (nil? (:left tree)) (:right tree)
      (nil? (:right tree)) (:left tree)
      :else
      (let [left (:left tree)
            right (:right tree)
            new-right (splay right (leftmost-key right))]
        (assoc new-right :left left)))))

(defn- put- [tree k v]
  (cond
    (nil? tree)
    (make-node k v)
    (neg? (compare k (:key tree)))
    (assoc tree :left (put- (:left tree) k v))
    (pos? (compare k (:key tree)))
    (assoc tree :right (put- (:right tree) k v))
    :else
    (assoc tree :value v)))

(defn put [cid k v]
  (let [tree (@cache-map cid)
        {:keys [size type eviction-fn]} (@cache-opts-map cid)]
    (if (>= (opusdb.cache/size tree) size)
      (let [evicted-node (condp = type
                           "LRU" (leftmost-node tree)
                           "MRU" tree)
            new-tree (remove tree type)]
        (when eviction-fn
          (eviction-fn (:key evicted-node) (:value evicted-node)))
        (swap! cache-map assoc cid (splay (put- new-tree k v) k)))
      (swap! cache-map assoc cid (splay (put- tree k v) k)))))

(defn change-type
  [cid new-type]
  (swap! cache-opts-map assoc-in [cid :type] new-type))

(defn make-cache
  ([size eviction-fn]
   (make-cache size eviction-fn "LRU"))
  ([size eviction-fn type]
   (let [cid (swap! id inc)]
     (swap! cache-map assoc cid nil)
     (swap! cache-opts-map assoc cid {:size size
                                      :eviction-fn eviction-fn
                                      :type type})
     cid)))
