(ns yapster.collections.context.sqlite.collection-metadata
  (:require
   [clojure.edn :as edn]
   [promesa.core :as p]
   [malli.experimental :as mx]
   [yapster.collections.schema :as coll.schema]
   [yapster.collections.context.multimethods :as coll.mm]
   [yapster.collections.context.sqlite.transaction-statements :as tx.stmts]))

(def collections-table-name "collections")

(def maybe-create-collections-table-query
  (str "CREATE TABLE IF NOT EXISTS " collections-table-name
       " (name TEXT NOT NULL PRIMARY KEY,"
       "  metadata TEXT NOT NULL)"))

(defn maybe-create-collections-table
  [ctx]
  (tx.stmts/readwrite-statements
   ctx
   [maybe-create-collections-table-query]))

(def select-collection-metadata-query
  (str
   "SELECT * FROM " collections-table-name " WHERE name=?"))

(defn select-collection-metadata-tx
  [ctx collection-name]
  (tx.stmts/readonly-statements
   ctx
   [[select-collection-metadata-query [collection-name]]]))

(defn select-collection-metadata
  [ctx collection-name]
  (p/let [rs (select-collection-metadata-tx ctx collection-name)

          ;; _ (log/info ::select-collection-metadata {:rs rs})
          {metadata-str :metadata
           :as r} (-> rs first (js->clj :keywordize-keys true) first)]

    (when (some? r)
      (edn/read-string metadata-str))))

(def delete-collection-metadata-query
  (str"DELETE FROM " collections-table-name " where name=?"))

(defn delete-collection-metadata
  [ctx collection-name]
  (tx.stmts/readwrite-statements
   ctx
   [[delete-collection-metadata-query [collection-name]]]))

(def insert-collection-metadata-query
  (str "INSERT OR REPLACE INTO "
       collections-table-name
       " (name,metadata) VALUES (?,?)"))

(mx/defn insert-collection-metadata
  [ctx :- coll.schema/CollectionStorageContext
   collection-name :- :string
   metadata :- coll.schema/StoredCollectionMetadata]
  (tx.stmts/readwrite-statements
   ctx
   [[insert-collection-metadata-query [collection-name
                                       (prn-str metadata)]]]))

(defmethod coll.mm/-load-collection-metadata :yapster.collections.context/sqlite
  [ctx coll-name]
  (select-collection-metadata ctx coll-name))

(defmethod coll.mm/-store-collection-metadata :yapster.collections.context/sqlite
  [ctx
   {coll-name :yapster.collections.metadata/name
    :as metadata}]
  (if (some? metadata)
    (insert-collection-metadata ctx coll-name metadata)
    (delete-collection-metadata ctx coll-name)))
