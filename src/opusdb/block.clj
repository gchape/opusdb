(ns opusdb.block)

(definterface IBlock
  (^String fileName [])
  (^int blockNum []))

(defrecord Block [^String file, ^int block]
  IBlock
  (fileName [this]
    "Returns the file name associated with the block."
    (:file this))
  (blockNum [this]
    "Returns the block number of this block."
    (:block this))
  Object
  (toString [this]
    (str "[file " (:file this) ", " "block " (:block this) "]")))
