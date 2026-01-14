(ns opusdb.cache.splay
  (:refer-clojure :exclude [get type remove vals]))

(defrecord Cache [tree size type eviction-fn count])

(defn- rotate-right [{left :left :as node}]
  (if left (assoc left :right (assoc node :left (:right left))) node))

(defn- rotate-left [{right :right :as node}]
  (if right (assoc right :left (assoc node :right (:left right))) node))

(defn- leaf? [{:keys [left right]}]
  (and (nil? left) (nil? right)))

(defn- leftmost-key [{left :left key :key}]
  (if left (recur left) key))

(defn- leftmost-node [{left :left :as node}]
  (if left (recur left) node))

(defn- splay [node key]
  (when node
    (let [cmp (compare key (:key node))]
      (cond
        (zero? cmp) node
        (neg? cmp)
        (if-let [left (:left node)]
          (let [cmp-left (compare key (:key left))]
            (cond
              (neg? cmp-left) (-> node (assoc :left (splay (:left left) key)) rotate-right rotate-right)
              (pos? cmp-left) (-> node (assoc :left (splay (:right left) key)) rotate-left rotate-right)
              :else (rotate-right node)))
          node)
        :else
        (if-let [right (:right node)]
          (let [cmp-right (compare key (:key right))]
            (cond
              (pos? cmp-right) (-> node (assoc :right (splay (:right right) key)) rotate-left rotate-left)
              (neg? cmp-right) (-> node (assoc :right (splay (:left right) key)) rotate-right rotate-left)
              :else (rotate-left node)))
          node)))))

(defn- remove [node cache-type]
  (case cache-type
    "LRU" (if-let [left (:left node)]
            (assoc node :left (remove left cache-type))
            (:right node))
    "MRU" (cond
            (nil? node) nil
            (leaf? node) nil
            (nil? (:left node)) (:right node)
            (nil? (:right node)) (:left node)
            :else (assoc (splay (:right node) (leftmost-key (:right node))) :left (:left node)))))

(defn- insert [node key val]
  (if node
    (let [cmp (compare key (:key node))]
      (cond
        (neg? cmp) (assoc node :left (insert (:left node) key val))
        (pos? cmp) (assoc node :right (insert (:right node) key val))
        :else (assoc node :value val)))
    {:key key :value val}))

(defn- contains-key? [node key]
  (when node
    (let [cmp (compare key (:key node))]
      (cond
        (zero? cmp) true
        (neg? cmp) (recur (:left node) key)
        :else (recur (:right node) key)))))

(defn put! [{:keys [tree size type eviction-fn count]} k v]
  (let [root @tree
        existing? (contains-key? root k)
        full? (and (>= @count size) (not existing?))]
    (when full?
      (let [victim (if (= type "LRU") (leftmost-node root) root)]
        (when eviction-fn (eviction-fn (:key victim) (:value victim)))))
    (when-not existing? (swap! count inc))
    (reset! tree (splay (insert (if full? (remove root type) root) k v) k))))

(defn get [{:keys [tree]} k]
  (when-let [root (splay @tree k)]
    (reset! tree root)
    (loop [node root]
      (when node
        (let [cmp (compare k (:key node))]
          (cond
            (zero? cmp) (:value node)
            (neg? cmp) (recur (:left node))
            :else (recur (:right node))))))))

(defn vals [{:keys [tree]}]
  (letfn [(walk [node]
            (when node (concat (walk (:left node)) [(:value node)] (walk (:right node)))))]
    (vec (walk @tree))))

(defn make-cache
  ([size eviction-fn] (make-cache size eviction-fn "LRU"))
  ([size eviction-fn cache-type] (->Cache (atom nil) size cache-type eviction-fn (atom 0))))