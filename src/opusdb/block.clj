(ns opusdb.block)

(definterface IBlock
  (^String fileName [])
  (^int blockNum []))

(defrecord Block [^String file, ^int block]
  IBlock
  (fileName [_]
    file)
  (blockNum [_]
    block)
  Object
  (toString [_]
    (str "[file " file ", " "block " block "]")))

(defn make-block
  "Creates a new Block with the specified file name and block number.
  
  Arguments:
    file  - The name of the file containing this block
    block - The block number within the file
  
  Returns:
    A Block record representing the file location"
  [^String file ^long block]
  (Block. file block))
