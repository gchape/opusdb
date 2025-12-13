(ns opusdb.unchecked.math)

(def *int
  (fn* [x y & more]
       (reduce unchecked-multiply-int 1 (into [x y] more))))

(def +int
  (fn* [x y & more]
       (reduce unchecked-add-int 0 (into [x y] more))))

(def -int
  (fn* [x y]
       (unchecked-subtract-int x y)))
