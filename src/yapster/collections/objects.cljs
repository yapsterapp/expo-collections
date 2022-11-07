(ns yapster.collections.objects
  "cross-platform support for collection objects"
  (:require
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget]]
   [promesa.core :as p]
   [yapster.collections.util.time :as util.t]
   [yapster.collections.keys :as keys]
   [yapster.collections.context :as ctx]
   [yapster.collections.context.transactions :as tx]
   [yapster.collections.context.multimethods :as ctx.mm]))

(defn extract-data
  [obj]
  (oget obj "?data"))

(defn extract-objects-metadata
  "extract bulk metadata from a list of collection objects - in particular
   the earliest :updated_at, which can be used for freshness
   checking"
  [coll-objs]
  (let [;; collection updated-at is the earliest updated-at
        ;; of any of the members
        coll-updated-at (->> coll-objs
                             (map #(keys/extract-key [:updated_at] %))
                             (map first)
                             (sort)
                             first)]
    {:yapster.collections/objects coll-objs
     :yapster.collections.objects/updated-at
     (util.t/->inst coll-updated-at)}))

(defn extract-objects-data
  [{coll-objs :yapster.collections/objects
    :as objs-with-metadata}]
  (log/info ::extract-objects-data {})
  (assoc
   objs-with-metadata
   :yapster.collections/objects
   (->> coll-objs
        (map extract-data)
        (clj->js))))

(defn get-collection-objects
  "get collection objects with a single query

   - returns: Promise<{:yapster.collections/objects [<obj>*]
                       :yapster.collections.objects/updated-at <timestamp>}>
   - ctx - the storage context
   - coll-name - name of the collection
   - ids - vector of individual object composite key value vectors"
  [ctx coll-name ids]
  (p/let [coll (ctx/open-collection ctx coll-name)
          get-cb (ctx.mm/-get-collection-objects-cb ctx coll ids)
          tx-cb (tx/fmap-tx-callback
                 get-cb
                 (comp
                  extract-objects-data
                  extract-objects-metadata))]
    (tx/readonly-transaction ctx tx-cb)))

(defn get-collection-object
  "get a single collection object

   - returns Promise<collection-object|error>"
  [ctx coll-name id]
  (get-collection-objects ctx coll-name [id]))

(defn store-collection-objects
  "store a list of collection objects with a single query

   does *not* store any index pages"
  [ctx coll-name objs]
  (p/let [coll (ctx/open-collection ctx coll-name)
          tx-cb (ctx.mm/-store-collection-objects-cb ctx coll objs)]
    (tx/readwrite-transaction ctx tx-cb)))

(defn store-collection-object
  "store a single collection objet"
  [ctx coll-name obj]
  (store-collection-objects ctx coll-name [obj]))
