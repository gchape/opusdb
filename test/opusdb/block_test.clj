(ns opusdb.block-test
  (:require [clojure.test :as test]
            [opusdb.block :refer [->Block]]))

(test/deftest block-tests
  (test/is
   (= (->Block "A.txt" 0)
      (->Block "A.txt" 0))
   "Two identical Blocks with the same file name and block number should be equal")

  (test/is
   (not= (->Block "A.txt" 0)
         (->Block "B.txt" 0))
   "Two Blocks with different file names should not be equal")

  (test/is
   (= (.fileName (->Block "A.txt" 0)) "A.txt")
   "The file name of the Block should be 'A.txt'")

  (test/is
   (= (.blockNum (->Block "A.txt" 0)) 0)
   "The block number of the Block should be 0"))
