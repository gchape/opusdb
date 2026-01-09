(ns opusdb.cache.splay
  (:refer-clojure :exclude [get type remove vals]))

(defrecord Cache [tree size type eviction-fn count])

(defn- make-node [k v]
  {:key k :value v})

(defn- rotate-right [{l :left :as t}]
  (if l
    (assoc l :right (assoc t :left (:right l)))
    t))

(defn- rotate-left [{r :right :as t}]
  (if r
    (assoc r :left (assoc t :right (:left r)))
    t))

(defn- is-leaf? [{:keys [left right]}]
  (and (nil? left) (nil? right)))

(defn- leftmost-key [{l :left k :key}]
  (if l (recur l) k))

(defn- leftmost-node [{l :left :as t}]
  (if l (recur l) t))

(defn- splay [t k]
  (cond
    (nil? t) nil
    (zero? (compare k (:key t))) t
    (neg? (compare k (:key t)))
    (let [l (:left t)]
      (cond
        (nil? l) t
        (neg? (compare k (:key l)))
        (-> t
            (assoc :left (splay (:left l) k))
            rotate-right
            rotate-right)
        (pos? (compare k (:key l)))
        (-> t
            (assoc :left (splay (:right l) k))
            rotate-left
            rotate-right)
        :else (rotate-right t)))
    :else
    (let [r (:right t)]
      (cond
        (nil? r) t
        (pos? (compare k (:key r)))
        (-> t
            (assoc :right (splay (:right r) k))
            rotate-left
            rotate-left)
        (neg? (compare k (:key r)))
        (-> t
            (assoc :right (splay (:left r) k))
            rotate-right
            rotate-left)
        :else (rotate-left t)))))

(defn- remove [t cache-type]
  (case cache-type
    "LRU"
    (if-let [l (:left t)]
      (assoc t :left (remove l cache-type))
      (:right t))
    "MRU"
    (cond
      (nil? t) nil
      (is-leaf? t) nil
      (nil? (:left t)) (:right t)
      (nil? (:right t)) (:left t)
      :else (assoc (splay (:right t) (leftmost-key (:right t)))
                   :left (:left t)))))

(defn- put [t k v]
  (cond
    (nil? t) (make-node k v)
    (neg? (compare k (:key t))) (assoc t :left (put (:left t) k v))
    (pos? (compare k (:key t))) (assoc t :right (put (:right t) k v))
    :else (assoc t :value v)))

(defn- key-exists? [t k]
  (loop [node t]
    (cond
      (nil? node) false
      (neg? (compare k (:key node))) (recur (:left node))
      (pos? (compare k (:key node))) (recur (:right node))
      :else true)))

(defn put! [{:keys [tree size type eviction-fn count]} k v]
  (let [t @tree
        current-count @count
        existing? (key-exists? t k)]
    (if (and (>= current-count size) (not existing?))
      ;; Cache is full and this is a new key - need to evict
      (let [victim (if (= type "LRU") (leftmost-node t) t)
            t' (remove t type)]
        (when eviction-fn
          (eviction-fn (:key victim) (:value victim)))
        (reset! tree (splay (put t' k v) k)))
      ;; Cache has space or we're updating existing key
      (do
        (when-not existing?
          (swap! count inc))
        (reset! tree (splay (put t k v) k))))))

(defn get [{:keys [tree]} k]
  (when-let [t (splay @tree k)]
    (reset! tree t)
    (loop [node t]
      (cond
        (nil? node) nil
        (neg? (compare k (:key node))) (recur (:left node))
        (pos? (compare k (:key node))) (recur (:right node))
        :else (:value node)))))

(defn vals [{:keys [tree]}]
  (letfn [(walk [t]
            (when t
              (concat
               (walk (:left t))
               [(:value t)]
               (walk (:right t)))))]
    (vec (walk @tree))))

(defn make-cache
  ([size eviction-fn]
   (make-cache size eviction-fn "LRU"))
  ([size eviction-fn cache-type]
   (->Cache (atom nil) size cache-type eviction-fn (atom 0))))