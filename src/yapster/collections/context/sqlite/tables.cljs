(ns yapster.collections.context.sqlite.tables
  (:require
   [clojure.string :as string]
   [lambdaisland.glogi :as log]
   [promesa.core :as p]
   [yapster.collections.metadata.key-component :as-alias coll.md.kc]
   [yapster.collections.metadata.key-component.sort-order :as-alias coll.md.kc.so]
   [yapster.collections.keys :as coll.keys]
   [yapster.collections.context.util :as util]
   [yapster.collections.context.multimethods :as context.mm]
   [yapster.collections.context.sqlite.sql :as sql]
   [yapster.collections.context.sqlite.transaction-statements :as tx.stmts]))

(defn collection-objects-table-name
  "name of a collection objects table"
  [coll-name]
  (str "collection_objects__"
       (-> coll-name name util/sanitise-name)))

(defn collection-index-key-component-name
  [[kex-kw {sort-order ::coll.md.kc/sort-order} :as _kcspec]]
  (let [ns (some-> kex-kw namespace (string/split "."))
        n (name kex-kw)
        sort-dir (if (= ::coll.md.kc.so/desc sort-order)
                   "DESC"
                   "ASC")
        ncs (into (or ns []) [n sort-dir])]
    (util/sanitise-name
     (string/join "_" ncs))))

(defn collection-index-table-name
  "name of a collection index table, derived from
   the index key-spec"
  [coll-name key-spec]
  (let [key-spec (coll.keys/normalize-keyspec key-spec)
        k-descr (->> key-spec
                     (map collection-index-key-component-name)
                     (string/join "__"))]
    (str "collection_index__"
         (-> coll-name name util/sanitise-name)
         "__"
         k-descr)))

(defn drop-collection-objects-table-query
  [{coll-name :yapster.collections.metadata/name
    :as _coll-metadata}]
  (let [table-name (collection-objects-table-name coll-name)]

    (str "DROP TABLE IF EXISTS " table-name)))

(defn collection-objects-table-fields
  [{coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll-metadata}]
  (let [pk-keyspec (coll.keys/get-keyspec coll-metadata coll-pk-alias)
        pk-fields (sql/pk-fields pk-keyspec)]

    (into
     pk-fields
     [:data
      :created_at
      :updated_at
      :deleted_at])))

(defn collection-objects-table-cols-list
  [coll-metadata]
  (let [table-fields (collection-objects-table-fields coll-metadata)]

    (->> table-fields
         (map name)
         (string/join ","))))

(defn create-collection-objects-table-queries
  "return a query to create a table for collection objects...
   the primary key components will be mapped to columns
   id_0..id_N and declared as a PRIMARY KEY"
  [{coll-name :yapster.collections.metadata/name
    coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll-metadata}]
  (let [pk-keyspec (coll.keys/get-keyspec coll-metadata coll-pk-alias)
        pk-cols-dir-list (sql/pk-cols-dir-list pk-keyspec)
        table-name (collection-objects-table-name coll-name)
        table-cols (collection-objects-table-cols-list
                      coll-metadata)]

    [(drop-collection-objects-table-query coll-metadata)

     (str
      "CREATE TABLE "
      table-name
      " ("
      table-cols
      ", PRIMARY KEY ("
      pk-cols-dir-list
      "))")]))

(defn drop-collection-index-table-query
  [{coll-name :yapster.collections.metadata/name
    :as coll-metadata}
   key-alias]
  (let [key-keyspec (coll.keys/get-keyspec coll-metadata key-alias)
        table-name (collection-index-table-name coll-name key-keyspec)]

    (str "DROP TABLE IF EXISTS " table-name)))

(defn create-collection-index-table-queries
  [{coll-name :yapster.collections.metadata/name
    coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll-metadata}
   key-alias]
  (let [pk-keyspec (coll.keys/get-keyspec coll-metadata coll-pk-alias)
        pk-cols-list (sql/pk-cols-list pk-keyspec)
        pk-cols-dir-list (sql/pk-cols-dir-list pk-keyspec)

        key-keyspec (coll.keys/get-keyspec coll-metadata key-alias)
        key-cols-list (sql/idx-key-cols-list key-keyspec)
        key-cols-dir-list (sql/idx-key-cols-dir-list key-keyspec)

        table-name (collection-index-table-name coll-name key-keyspec)]

    [(drop-collection-index-table-query coll-metadata key-alias)

     (str
      "CREATE TABLE "
      table-name
      " ("
      pk-cols-list
      ","
      key-cols-list ", "
      "prev_id, "
      "next_id, "
      "created_at, "
      "updated_at "
      ", PRIMARY KEY ("
      pk-cols-dir-list
      "))")

     (str
      "CREATE INDEX "
      table-name "__key_index "
      "ON " table-name
      "("
      key-cols-dir-list
      ")")]))

(defn create-collection-queries
  "return all queries to create tables relating to a collection

   there will be queries to drop any existing tables before
   creating new ones, so cleanup failures should not cause errors"
  [{coll-index-keys :yapster.collections.metadata/index-keys
    :as coll-metadata}]
  (let [obj-t-queries (create-collection-objects-table-queries coll-metadata)
        index-t-queries (for [k-alias coll-index-keys]
                          (create-collection-index-table-queries
                           coll-metadata
                           k-alias))]
    (into
     obj-t-queries
     (apply concat index-t-queries))))

(defn drop-collection-queries
  "return queries to drop all tables relating to a collection"
  [{coll-index-keys :yapster.collections.metadata/index-keys
    :as coll-metadata}]
  (let [obj-t-query (drop-collection-objects-table-query coll-metadata)
        index-t-queries (for [k-alias coll-index-keys]
                          (drop-collection-index-table-query
                           coll-metadata
                           k-alias))]
    (into
     [obj-t-query]
     index-t-queries)))

(defn create-collection
  [ctx
   coll-metadata]
  (p/let [queries (create-collection-queries coll-metadata)
          _qrs (tx.stmts/readwrite-statements
               ctx
               queries)]
    (log/info
     ::create-collection
     {:queries queries})

    true))

(defn drop-collection
  [ctx
   coll-metadata]
  (p/let [queries (drop-collection-queries coll-metadata)
          _qrs (tx.stmts/readwrite-statements
               ctx
               queries)]

    (log/info
     ::drop-collection
     {:queries queries})

    true))

(defmethod context.mm/-create-collection :yapster.collections.context/sqlite
  [ctx metadata]
  (create-collection ctx metadata))

(defmethod context.mm/-drop-collection :yapster.collections.context/sqlite
  [ctx metadata]
  (drop-collection ctx metadata))
