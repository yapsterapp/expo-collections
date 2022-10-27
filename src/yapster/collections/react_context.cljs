(ns yapster.collections.react-context
  (:require
   ["react" :as react :refer [createContext]]
   [malli.experimental :as mx]
   [oops.core :refer [oget]]
   [reagent.core :as r]
   [yapster.collections.schema :as coll.schema]))

(defonce CollectionsContext (createContext nil))
(def CollectionsContextProvider (oget CollectionsContext "Provider"))
(def CollectionsContextConsumer (oget CollectionsContext "Consumer"))

(mx/defn collections-context-provider
  "hiccup CollectionsContextProvider element which permits
   cljs objects as values.
   (i.e. no automatic clj->js on the props)"
  [value :- coll.schema/CollectionsReactContext
   child :- :any]

  (r/create-element
   CollectionsContextProvider
   #js {:value value}
   (r/as-element child)))
