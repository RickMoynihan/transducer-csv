(ns transduce-csv.parallel
  (:require [clojure.java.io :as io]
            [clojure.core.protocols :as pr]
            [clojure.core.reducers :as red]
            [clojure.core.async :as async])
  (:import [java.util.concurrent TimeUnit ThreadPoolExecutor ArrayBlockingQueue]))

(set! *warn-on-reflection* true)

(comment
  ;; TODO
  (defn exec-task-or-are-we-just-adding-to-the-queue [pool char-array]

    (loop [line (.readLine (io/reader char-array))]
      (let [ret (f val line)]
        (if (reduced? ret)
          @ret
          (do
            (.read r ch-array 0 (* 64 1024))
            (recur ch-array ret))))
      )))

(deftype ParallelCSV [ioable]
  clojure.core.protocols/CollReduce
  (coll-reduce [this f val]
    (let [ch-array (char-array (* 64 1024))]
      (with-open [r ^java.io.BufferedReader (io/reader (.ioable this))]
        (loop [line (.read r ch-array 0 (* 64 1024))
               val val]
          (if line
            (let [ret (f val line)]
              (if (reduced? ret)
                @ret
                (let [line (.read r ch-array 0 (* 64 1024))]
                  (recur line ret))))
            val)))))

  red/CollFold
  (coll-fold [this n combinef reducef]
    (let [processors (.. (Runtime/getRuntime) (availableProcessors))
          q (ArrayBlockingQueue. (* processors 2))
          pool (ThreadPoolExecutor. processors processors 0 TimeUnit/SECONDS q)]

      (future
        ;; read
        (with-open [r ^java.io.BufferedReader (io/reader (.ioable this))]
          (loop [val val]
            (let [ch-array (char-array n)
                  line (.read r ch-array 0 n)]
              #_(if (not= -1 line)
                (exec-task-or-are-we-just-adding-to-the-queue reducef)
                (let [ret (reducef val line)]
                  (if (reduced? ret)
                    @ret
                    (recur ret)))
                val))))
        #_(.put )
        ,,,)

      )))
