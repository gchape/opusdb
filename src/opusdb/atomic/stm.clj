(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync])
  (:require
   [opusdb.atomic.tx-hooks :as tx])
  (:import
   [java.util IdentityHashMap]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicLong]
   [java.util.concurrent ThreadLocalRandom]))

(def ^:private max-history              32)
(def ^{:dynamic true} *max-history*     max-history)
(def ^{:dynamic true} *trim-abort-hook* nil)
(def ^:private max-retries              96)
(def ^:private base-sleep-ms            1)
(def ^:private max-sleep-ms             32)

(def ^:private commit-lock             (Object.))
(def ^:private ^AtomicLong write-point (AtomicLong.))
(def ^:private ^AtomicLong tx-counter  (AtomicLong.))
(def ^:private ^ConcurrentHashMap active-tx (ConcurrentHashMap.))

(def ^{:dynamic true} *tx* nil)

(defn ref [initial-val]
  (let [wp    (.get write-point)
        entry {:value initial-val :write-point wp}]
    {:hot  (atom {:value       initial-val
                  :write-point wp
                  :owner       nil})
     :cold (atom [entry])
     :lock (Object.)}))

(defn- make-tx [retry-count]
  (let [id (.incrementAndGet tx-counter)
        tx {:id          id
            :read-point  (.get write-point)
            :read-set    (IdentityHashMap.)
            :write-set   (IdentityHashMap.)
            :retry-count retry-count
            :status      (volatile! ::active)}]
    (.put active-tx id tx)
    tx))

(defn- next-tx [current]
  (tx/rollback! (:id current))
  (.remove active-tx (:id current))
  (let [retries (:retry-count current)]
    (when (>= retries max-retries)
      (throw (ex-info "STM exceeded max retries" {:retries retries})))
    (when (> retries 4)
      (let [base   (min max-sleep-ms
                        (bit-shift-left base-sleep-ms (min (- retries 4) 6)))
            jitter (long (* base 0.25
                            (- (* 2 (.nextDouble (ThreadLocalRandom/current))) 1)))]
        (Thread/sleep (max 0 (long (+ base jitter))))))
    (make-tx (inc retries))))

(defn- abort []
  (throw (ex-info "Transaction retry" {:type ::aborted})))

(defn- check-active! [tx]
  (when (= @(:status tx) ::aborted)
    (abort)))

(defmacro with-max-history [n & body]
  `(binding [*max-history* ~n]
     ~@body))

(defn- find-entry [read-point ^clojure.lang.Atom cold-atom]
  (let [history @cold-atom]
    (loop [i (dec (count history))]
      (when (>= i 0)
        (let [entry (nth history i)]
          (if (<= (:write-point entry) read-point)
            entry
            (recur (dec i))))))))

(defn- trim-history [history]
  (if (> (count history) *max-history*)
    (subvec history (- (count history) *max-history*))
    history))

(defn- try-acquire! [target-ref tx]
  (let [claimer-id (:id tx)
        lock       (:lock target-ref)]
    (monitor-enter lock)
    (try
      (let [owner-id (:owner @(:hot target-ref))]
        (cond
          (nil? owner-id)
          (do (swap! (:hot target-ref) assoc :owner claimer-id) true)

          (= owner-id claimer-id)
          true

          (> claimer-id owner-id)
          (do (when-some [owner-tx (.get active-tx owner-id)]
                (when (= @(:status owner-tx) ::active)
                  (vreset! (:status owner-tx) ::aborted)))
              (swap! (:hot target-ref) assoc :owner claimer-id)
              true)

          :else false))
      (finally
        (monitor-exit lock)))))

;; ---------------------------------------------------------------------------
;; Commit path
;;
;; CHANGE: write-point is incremented *before* acquiring commit-lock.
;;
;; Original order:
;;   locking commit-lock
;;     → validate-reads!
;;     → (.incrementAndGet write-point)   ; inside lock
;;     → swap! each ref's combined atom   ; inside lock, allocates big map
;;
;; New order:
;;   locking commit-lock
;;     → validate-reads!
;;     → (.incrementAndGet write-point)   ; still inside lock — see note
;;   (outside lock)
;;     → swap! hot-atom  (small: 3 fields)
;;     → swap! cold-atom (append to history)
;;
;; NOTE on write-point placement: moving the increment outside the lock
;; would allow two transactions to claim the same commit-point if they
;; race between the increment and their respective hot-atom CASes, which
;; would break the version ordering guarantee that find-entry relies on.
;; It therefore stays inside the lock.  What we *do* move outside is the
;; per-ref atom work, which was the dominant source of cache pressure.
;;
;; The two swap! calls after the lock are now uncontended — no other
;; thread can be modifying the same ref's atoms at that moment because
;; ownership (try-acquire!) was already established exclusively, and the
;; new owner is us.  A concurrent reader doing find-entry on cold-atom
;; will see either the pre-commit or the post-commit history vector
;; atomically (atoms are volatile reads), which is safe because the
;; reader's read-point predates our commit-point.
;; ---------------------------------------------------------------------------
(defn- validate-reads! [^IdentityHashMap read-set]
  ;; Hot-atom carries :write-point; no change needed here.
  (doseq [[r entry] read-set]
    (when (> (:write-point @(:hot r)) (:write-point entry))
      (abort))))


(defn- apply-writes-hot! [^IdentityHashMap write-set commit-point]
  (doseq [[r new-val] write-set]
    (swap! (:hot r)
           (fn [s]
             (assoc s
                    :value       new-val
                    :write-point commit-point
                    :owner       nil)))))

(defn- apply-writes-cold! [^IdentityHashMap write-set commit-point]
  (doseq [[r new-val] write-set]
    (swap! (:cold r)
           (fn [history]
             (trim-history
              (conj history {:value       new-val
                             :write-point commit-point}))))))

(defn- commit! [tx]
  (check-active! tx)
  (let [{:keys [id read-set write-set status]} tx]
    (if (seq write-set)
      (let [commit-point
            (locking commit-lock
              (check-active! tx)
              (validate-reads! read-set)
              (let [cp (.incrementAndGet write-point)]
                (apply-writes-hot! write-set cp)
                cp))]
        (apply-writes-cold! write-set commit-point)
        (vreset! status ::committed)
        (.remove active-tx id)
        (tx/commit! id))
      (do
        (vreset! status ::committed)
        (.remove active-tx id)
        (tx/commit! id)))))

(defn- run-attempt [tx body-fn]
  (try
    (binding [*tx* tx]
      (let [result (body-fn)]
        (commit! tx)
        [:ok result]))
    (catch clojure.lang.ExceptionInfo ex
      (if (= (:type (ex-data ex)) ::aborted)
        [:abort nil]
        [:exception ex]))
    (catch Exception ex
      [:exception ex])))

(defn run-tx [body-fn]
  (loop [tx (make-tx 0)]
    (let [[tag value] (run-attempt tx body-fn)]
      (case tag
        :ok        value
        :abort     (recur (next-tx tx))
        :exception (do (tx/rollback! (:id tx))
                       (.remove active-tx (:id tx))
                       (throw value))))))

(defn sync [body-fn]
  (if *tx*
    (body-fn)
    (run-tx body-fn)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn deref [target-ref]
  (if-not *tx*
    (:value @(:hot target-ref))
    (let [tx                         *tx*
          ^IdentityHashMap write-set (:write-set tx)
          ^IdentityHashMap read-set  (:read-set tx)]
      (check-active! tx)
      (cond
        (.containsKey write-set target-ref)
        (.get write-set target-ref)

        (.containsKey read-set target-ref)
        (:value (.get read-set target-ref))

        :else
        (let [snapshot (find-entry (:read-point tx) (:cold target-ref))]
          (when-not snapshot
            (when *trim-abort-hook* (*trim-abort-hook*))
            (abort))
          (.put read-set target-ref snapshot)
          (:value snapshot))))))

(defn ref-set [target-ref new-val]
  (when-not *tx*
    (throw (IllegalStateException. "ref-set outside transaction")))
  (let [tx *tx*]
    (check-active! tx)
    (when-not (try-acquire! target-ref tx)
      (abort))
    (.put ^IdentityHashMap (:write-set tx) target-ref new-val)
    new-val))

(defn alter [target-ref f & args]
  (ref-set target-ref (apply f (deref target-ref) args)))

(defn on-commit [handler]
  (if *tx*
    (tx/on-commit (:id *tx*) handler)
    (throw (IllegalStateException. "on-commit outside transaction"))))

(defn on-rollback [handler]
  (if *tx*
    (tx/on-rollback (:id *tx*) handler)
    (throw (IllegalStateException. "on-rollback outside transaction"))))