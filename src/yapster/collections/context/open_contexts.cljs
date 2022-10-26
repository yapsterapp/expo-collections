(ns yapster.collections.context.open-contexts
  "caching of expensive storage context resources"
  (:require
   [yapster.collections.context.util :as util]))

;; map of {<db-name> <context>}
(defonce open-storage-contexts (atom {}))

;; map of {<db-name> {<coll-name> <coll-metadata>}}
(defonce open-collections (atom {}))

(defn set-storage-context
  [db-name ctx]
  (let [db-name (util/sanitise-name db-name)]

    (swap!
     open-storage-contexts

     (fn [scs]
       (let [prev (get scs db-name)]

         (if (not (identical? ctx prev))
           (do
             ;; discard any cached open-collections when
             ;; the context changes
             (swap!
              open-collections
              dissoc
              db-name)

             (if (some? ctx)
               (assoc scs db-name ctx)
               (dissoc scs db-name)))

           scs))))))

(defn get-storage-context
  [db-name]
  (let [db-name (util/sanitise-name db-name)]
    (get @open-storage-contexts db-name)))

(defn set-open-collection
  [db-name coll-name coll-metadata]
  (let [db-name (util/sanitise-name db-name)
        coll-name (util/sanitise-name coll-name)]
    (swap!
     open-collections

     assoc-in
     [db-name coll-name]
     coll-metadata)))

(defn get-open-collection
  [db-name coll-name]
  (let [db-name (util/sanitise-name db-name)
        coll-name (util/sanitise-name coll-name)]
    (get-in @open-collections [db-name coll-name])))
