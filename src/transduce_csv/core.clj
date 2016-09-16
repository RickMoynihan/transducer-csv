(ns transduce-csv.core
  (:require [clojure.java.io :as io]
            [clojure.core.protocols :as pr]
            [clojure.core.reducers :as red]
            [clojure.core.async :as async])
  (:import [java.util.concurrent TimeUnit ThreadPoolExecutor ArrayBlockingQueue]))

(set! *warn-on-reflection* true)

(deftype CSV [ioable]
  clojure.core.protocols/CollReduce
  (coll-reduce [this f val]
    (with-open [r ^java.io.BufferedReader (io/reader (.ioable this))]
      (loop [line (.readLine r)
             val val]
        (if line
          (let [ret (f val line)]
            (if (reduced? ret)
              @ret
              (recur (.readLine r) ret)))
          val)))))

(comment

  (time (loop [buf ^java.io.BufferedReader (io/reader "/Users/rick/Documents/Swirrl/Data/1gb.csv")]
          (when (.readLine buf)
            (recur buf))))

  (let [size (* 2 8192) charray (byte-array size)]
    (time (loop [buf (io/input-stream "/Users/rick/Documents/Swirrl/Data/1gb.csv")]
            (when (not= -1 (.read buf charray 0 size))
              (recur buf))))) ;; => 355 msecs

  (let [foo (map str)
        bar (into {} [1 2 3 4])])

  (require '[clojure.data.csv])


  (map #{} [1 2 3 4])

  (time (count (clojure.data.csv/read-csv (io/reader "/Users/rick/Documents/Swirrl/Data/1gb.csv"))

               (->> (clojure.data.csv/read-csv (io/reader "/Users/rick/Documents/Swirrl/Data/1gb.csv"))
                    (take 100000)
                    (map identity)
                   )

               )) ;; 50740 msecs

  (time (transduce (comp (map (constantly 1)))
                   +
                   (->CSV "/Users/rick/Documents/Swirrl/Data/1gb.csv"))) ;; 2314 msecs

  (time (transduce (comp (mapcat #(clojure.string/split % #",")) (map (constantly 1)))
                   +
                   (->CSV "/Users/rick/Documents/Swirrl/Data/1gb.csv")))


  (time (sequence (comp (take 10) (map str))
                  (->CSV "/Users/rick/Documents/Swirrl/Data/1gb.csv")))


  (import [org.apache.commons.csv CSVParser CSVFormat CSVRecord]
          [java.nio.charset Charset])

  (let [p (CSVParser/parse (io/file "/Users/rick/Documents/Swirrl/Data/1gb.csv") (Charset/forName "US-ASCII") )]
    p)

  (time (let [c (.parse (CSVFormat/RFC4180) (io/reader "/Users/rick/Documents/Swirrl/Data/1gb.csv"))
              it (.iterator c)]

          (loop [i 0]
            (if (.hasNext it)
              (do (.next it) (recur (inc i)))
              i)
            )))

  )
