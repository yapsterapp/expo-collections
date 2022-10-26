(ns yapster.collections.react-context
  (:require
   ["react" :as react :refer [createContext]]
   [oops.core :refer [oget]]))

(defonce storage-context-db-name (createContext nil))

(def storage-context-db-name-provider (oget storage-context-db-name "Provider"))
