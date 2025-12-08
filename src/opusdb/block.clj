(ns opusdb.block)

(defn make-block
  "Creates a new Block with the specified file name and block number.
  
  Arguments:
    file-name  - The name of the file containing this block
    block-id - The block number within the file
  
  Returns:
    A Block record representing the file location"
  [^String file-name ^long block-id]
  {:file-name file-name :block-id block-id})
