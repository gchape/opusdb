(ns opusdb.benchmark.game-of-life
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm4 :as stm4]))

(defn make-grid
  "Create a size x size grid of STM refs (0 = dead, 1 = alive)."
  [size]
  (vec (for [_ (range size)]
         (vec (for [_ (range size)]
                (stm4/ref (rand-int 2)))))))

(defn neighbors
  "Return coordinates of neighbors for cell (i,j) in a grid of given size."
  [i j size]
  (for [di [-1 0 1] dj [-1 0 1]
        :when (not (and (= di 0) (= dj 0)))
        :let [ni (+ i di) nj (+ j dj)]
        :when (and (>= ni 0) (< ni size) (>= nj 0) (< nj size))]
    [ni nj]))

(defn live-neighbors
  "Count live neighbors for a cell inside a transaction."
  [grid i j]
  (let [size (count grid)]
    (reduce + (for [[ni nj] (neighbors i j size)]
                (stm4/deref ((grid ni) nj))))))

(defn next-state
  "Compute next state of a cell based on current value and live neighbors."
  [current live]
  (cond
    (= current 1) (if (or (= live 2) (= live 3)) 1 0)
    (= current 0) (if (= live 3) 1 0)))

(defn step
  "Perform one Game of Life step concurrently on a grid using STM."
  [grid num-threads]
  (let [size (count grid)
        cells (vec (for [i (range size) j (range size)] [i j]))
        chunk-size (int (Math/ceil (/ (count cells) num-threads)))
        chunks (partition-all chunk-size cells)]
    (doseq [chunk chunks]
      (let [thread (Thread.
                    (fn []
                      (doseq [[i j] chunk]
                        (stm4/dosync
                         (let [current (stm4/deref ((grid i) j))
                               live    (live-neighbors grid i j)]
                           (stm4/ref-set ((grid i) j) (next-state current live)))))))]
        (.start thread)
        (.join thread)))))

(defn run
  "Run Game of Life for num-steps on a size x size grid with num-threads."
  [size num-steps num-threads]
  (let [grid (make-grid size)]
    (dotimes [_ num-steps]
      (step grid num-threads))
    (mapv (fn [row] (mapv stm4/deref row)) grid)))

(crit/quick-bench (run 100 25 10))