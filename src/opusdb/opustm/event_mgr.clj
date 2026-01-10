(ns opusdb.opustm.event-mgr
  (:refer-clojure :exclude [clear]))

(def ^:dynamic *event-context* nil)

(defonce ^:private event-registry (atom {}))

(defrecord EventHandler [event-fn event-args once?])

(defn- ensure-transaction! []
  (when-not (clojure.lang.LockingTransaction/isRunning)
    (throw (ex-info "Operation must be called inside dosync transaction"
                    {:operation ::transaction-required}))))

(defn- invoke-handler [{:keys [event-fn event-args]} context]
  (try
    (if (some? context)
      (apply event-fn context event-args)
      (apply event-fn event-args))
    (catch Exception e
      (println "Error executing event handler:" (.getMessage e))
      nil)))

(defn clear-events!
  "Clear all registered event handlers."
  []
  (reset! event-registry {}))

(defn on
  "Register a persistent handler for an event."
  [event-key f & args]
  (ensure-transaction!)
  (swap! event-registry update event-key
         (fn [handlers]
           (conj (or handlers #{}) (->EventHandler f args false))))
  nil)

(defn once
  "Register a handler that is automatically removed after first emit."
  [event-key f & args]
  (ensure-transaction!)
  (swap! event-registry update event-key
         (fn [handlers]
           (conj (or handlers #{}) (->EventHandler f args true))))
  nil)

(defn dismiss
  "Remove registered handler(s) for an event."
  [event-key f]
  (ensure-transaction!)
  (swap! event-registry update event-key
         (fn [handlers]
           (set (remove #(= (:event-fn %) f) handlers))))
  nil)

(defn emit
  "Emit an event, invoking all registered handlers."
  ([event-key]
   (ensure-transaction!)
   (let [handlers (get @event-registry event-key)
         once-handlers (filter :once? handlers)]
     (doseq [handler handlers]
       (invoke-handler handler *event-context*))
     (when (seq once-handlers)
       (swap! event-registry update event-key
              #(apply disj (set %) once-handlers))))
   nil)
  ([event-key context]
   (binding [*event-context* context]
     (emit event-key))))