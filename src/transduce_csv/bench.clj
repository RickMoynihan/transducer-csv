(ns transduce-csv.bench
  (:require [clojure.java.io :as io]
            [clojure.core.protocols :as pr]
            [transduce-csv.core :as trcsv]
            [clojure.core.reducers :as red]
            [clojure.data.csv :as c.d.csv]
            [clojure.core.async :as async])
  (:import [java.util.concurrent TimeUnit ThreadPoolExecutor ArrayBlockingQueue]
           [com.univocity.parsers.csv CsvParser  CsvParserSettings]))

(set! *warn-on-reflection* true)


(defn readline-recur []
  (time (loop [buf ^java.io.BufferedReader (io/reader "/Users/rick/Documents/Swirrl/Data/1gb.csv")]
          (when (.readLine buf)
            (recur buf)))))

(defn buffered-input-stream-recur []
  (let [size (* 2 8192) charray (byte-array size)]
    (time (loop [buf (io/input-stream "/Users/rick/Documents/Swirrl/Data/1gb.csv")]
          (when (not= -1 (.read buf charray 0 size))
            (recur buf))))))

(defn clj-data-csv []
  (time (-> "/Users/rick/Documents/Swirrl/Data/1gb.csv"
       io/reader
       clojure.data.csv/read-csv
       count)))

(defn transduce-lines []
  (time (transduce (comp (map (constantly 1)))
                   +
                   (trcsv/->CSV "/Users/rick/Documents/Swirrl/Data/1gb.csv"))))

(defn crude-transduce-csv []
  (time (transduce (comp (mapcat (fn [^String s] (.split s ",")))
                         (map (constantly 1)))
                   +
                   (trcsv/->CSV "/Users/rick/Documents/Swirrl/Data/1gb.csv")))) ;; 13244 msecs

(defn univocity-parser []
  (time (let [parser (CsvParser. (CsvParserSettings.))]
          (.beginParsing parser (io/reader "/Users/rick/Documents/Swirrl/Data/1gb.csv"))

          (loop [row (.parseNext parser)
                 i 0]
            #_(when (= i 2) (println (seq row)))
            (when row
              (recur (.parseNext parser) (inc i)))))))

(defn univocity-parser []
  (time (let [parser (CsvParser. (CsvParserSettings.))]
          (.beginParsing parser (io/reader "/Users/rick/Documents/Swirrl/Data/1gb.csv"))

          (loop [row (.parseNext parser)
                 i 0]
            (when row
              (recur (.parseNext parser) (inc i))))))) ;; 4739 msecs

(defn todo []
  (time (sequence (comp (take 10) (map str))
                  (trcsv/->CSV "/Users/rick/Documents/Swirrl/Data/1gb.csv"))))

(comment

  ;; todo


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
            ))))
