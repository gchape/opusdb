(ns opusdb.stm)

(defmulti on :type)

(defmethod on ::success
  [])

(defmethod on ::failure
  [])