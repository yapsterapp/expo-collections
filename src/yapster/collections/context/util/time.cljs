(ns yapster.collections.context.util.time
  (:require
   ["date-fns" :refer [parseJSON add]]))

(defn now
  []
  (js/Date.))

(defn ->inst
  "convert an ISO8601 formatted timestamp to a js Date"
  [val]

  (if (inst? val)
    val
    (some-> val (parseJSON))))

(defn add-t
  "add a duration to t
  (add <t> :days 2)"
  [t & {:as duration}]
  (some-> t
          (->inst)
          (add (clj->js duration))))
