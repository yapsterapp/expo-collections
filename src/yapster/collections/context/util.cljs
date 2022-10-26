(ns yapster.collections.context.util
  (:require
   [clojure.string :as string]
   ))

(def unacceptable-name-chars-regex
  #"[^0-9a-zA-Z_]")

(defn sanitise-name
  [name]
  (string/replace name unacceptable-name-chars-regex "_"))
