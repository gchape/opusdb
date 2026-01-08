(ns opusdb.tx-mgr
  (:refer-clojure :exclude [read])
  (:require
   [opusdb.buffer :as buff]
   [opusdb.buffer-mgr :as bm]
   [opusdb.log-mgr :as lm]
   [opusdb.page :as p])
  (:import
   [java.nio ByteBuffer]))

(defrecord Row   [block-id offset size data])

(defrecord TxMgr [buffer-mgr log-mgr tx-counter row-store])

(defn- row-id [block-id offset]
  [(:file-name block-id) (:index block-id) offset])

(defn- get-row
  [tx-mgr block-id offset row-size]
  (let [rid (row-id block-id offset)]
    (or (get @(:row-store tx-mgr) rid)
        (dosync
         (if-let [existing (get @(:row-store tx-mgr) rid)]
           existing
           (let [buffer (bm/pin-buffer (:buffer-mgr tx-mgr) block-id)
                 ^ByteBuffer page   (:page buffer)
                 data   (byte-array row-size)]
             (.position page ^int offset)
             (.get page data)
             (bm/unpin-buffer (:buffer-mgr tx-mgr) buffer)
             (let [row-ref (ref (->Row block-id offset row-size data))]
               (alter (:row-store tx-mgr) assoc rid row-ref)
               row-ref)))))))

(defn field-size [type & [max-len]]
  (case type
    :int    4
    :long   8
    :float  4
    :double 8
    :bool   1
    :string (+ 4 max-len)))

(def ^:private readers
  {:int    (fn [^ByteBuffer bb off] (.getInt bb off))
   :long   (fn [^ByteBuffer bb off] (.getLong bb off))
   :float  (fn [^ByteBuffer bb off] (.getFloat bb off))
   :double (fn [^ByteBuffer bb off] (.getDouble bb off))
   :bool   (fn [^ByteBuffer bb off] (not= 0 (.get bb ^int off)))
   :string (fn [^ByteBuffer bb off] (p/get-string bb off))})

(def ^:private writers
  {:int    (fn [^ByteBuffer bb off v] (.putInt bb ^int off v))
   :long   (fn [^ByteBuffer bb off v] (.putLong bb ^int off v))
   :float  (fn [^ByteBuffer bb off v] (.putFloat bb ^int off v))
   :double (fn [^ByteBuffer bb off v] (.putDouble bb ^int off v))
   :bool   (fn [^ByteBuffer bb off v] (.put bb ^int off (byte (if v 1 0))))
   :string (fn [^ByteBuffer bb off v] (p/put-string bb ^int off v))})

(defn read
  [tx-mgr block-id row-off field-off row-size type]
  (let [^Row row @(get-row tx-mgr block-id row-off row-size)
        bb       (ByteBuffer/wrap (:data row))
        f        (get readers type)]
    (when-not f
      (throw (ex-info "Unknown field type" {:type type})))
    (f bb field-off)))

(defn write
  [tx-mgr block-id row-off field-off row-size type value]
  (let [row-ref (get-row tx-mgr block-id row-off row-size)
        ^Row row @row-ref
        bb       (ByteBuffer/wrap (:data row))
        f        (get writers type)]
    (when-not f
      (throw (ex-info "Unknown field type" {:type type})))
    (f bb field-off value)))

(defn make-schema [fields]
  (let [layout (reduce
                (fn [{:keys [offset layout]} [name type & opts]]
                  (let [size (field-size type (first opts))]
                    {:offset (+ offset size)
                     :layout (conj layout
                                   {:name name
                                    :type type
                                    :offset offset
                                    :size size
                                    :opts opts})}))
                {:offset 0 :layout []}
                fields)]
    {:fields    (:layout layout)
     :row-size  (:offset layout)
     :field-map (into {} (map (fn [f] [(:name f) f]) (:layout layout)))}))

(defn read-field [tx-mgr schema block-id row-off field-name]
  (let [{:keys [type offset]} (get-in schema [:field-map field-name])]
    (read tx-mgr block-id row-off offset (:row-size schema) type)))

(defn write-field [tx-mgr schema block-id row-off field-name value]
  (let [{:keys [type offset]} (get-in schema [:field-map field-name])]
    (write tx-mgr block-id row-off offset (:row-size schema) type value)))

(defn read-row [tx-mgr schema block-id row-off]
  (reduce
   (fn [m {:keys [name]}]
     (assoc m name (read-field tx-mgr schema block-id row-off name)))
   {}
   (:fields schema)))

(defn write-row [tx-mgr schema block-id row-off field-values]
  (doseq [[field-name value] field-values]
    (write-field tx-mgr schema block-id row-off field-name value)))

(defn- log-update [log-mgr tx-id block-id offset data]
  (lm/append log-mgr
             (.getBytes
              (pr-str {:tx-id tx-id
                       :block-id block-id
                       :offset offset
                       :data (vec data)}))))

(defn- log-commit [log-mgr tx-id]
  (lm/append log-mgr
             (.getBytes (pr-str {:tx-id tx-id :commit true}))))

(def ^:dynamic *write-tracker* nil)

(defn finalize-writes [tx-mgr]
  (doseq [[_ row-ref] @(:row-store tx-mgr)]
    (let [{:keys [block-id offset data]} @row-ref]
      (when *write-tracker*
        (swap! *write-tracker*
               conj {:block-id block-id
                     :offset offset
                     :data data})))))

(defn commit-tx [tx-mgr tx-id writes]
  (doseq [{:keys [block-id offset data]} writes]
    (log-update (:log-mgr tx-mgr) tx-id block-id offset data))
  (log-commit (:log-mgr tx-mgr) tx-id)
  (lm/flush (:log-mgr tx-mgr))
  (doseq [{:keys [block-id offset data]} writes]
    (let [buffer (bm/pin-buffer (:buffer-mgr tx-mgr) block-id)
          ^ByteBuffer page   (:page buffer)]
      (.position page ^int offset)
      (.put page ^bytes data)
      (buff/smear buffer tx-id -1)
      (bm/unpin-buffer (:buffer-mgr tx-mgr) buffer)))
  (bm/flush (:buffer-mgr tx-mgr) tx-id))

(defmacro with-tx [tx-mgr & body]
  `(let [tx-id# (dosync (alter (:tx-counter ~tx-mgr) inc))
         writes# (atom [])]
     (binding [*write-tracker* writes#]
       (dosync ~@body)
       (finalize-writes ~tx-mgr))
     (commit-tx ~tx-mgr tx-id# @writes#)))

(defn recover [tx-mgr]
  (let [committed (atom #{})
        updates   (atom [])]
    (doseq [^bytes rec (seq (:log-mgr tx-mgr))]
      (let [entry (clojure.core/read-string (String. rec))]
        (if (:commit entry)
          (swap! committed conj (:tx-id entry))
          (swap! updates conj entry))))
    (doseq [{:keys [tx-id block-id offset data]} @updates]
      (when (contains? @committed tx-id)
        (let [buffer (bm/pin-buffer (:buffer-mgr tx-mgr) block-id)
              ^ByteBuffer page   (:page buffer)]
          (.position page ^int offset)
          (.put page ^bytes (byte-array data))
          (buff/smear buffer tx-id -1)
          (bm/unpin-buffer (:buffer-mgr tx-mgr) buffer))))
    {:recovered (count @committed)}))

(defn make-tx-mgr [buffer-mgr log-mgr]
  (let [tx-mgr (->TxMgr buffer-mgr log-mgr (ref 0) (ref {}))]
    (recover tx-mgr)
    tx-mgr))
