(ns yapster.collections.context.sqlite.objects
  (:require
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget oset!]]
   [yapster.collections.keys :as coll.keys]
   [yapster.collections.context.transactions :as tx]
   [yapster.collections.context.multimethods :as ctx.mm]
   [yapster.collections.context.sqlite.sql :as sql]
   [yapster.collections.context.sqlite.tables :as sqlite.tables]
   [yapster.collections.context.sqlite.transaction-statements :as tx.stmts]))

(defn collection-object-primary-key-conditions
  "objects are identified by a primary key-value... which
   is a composite key with fields named id_<n>"
  [pk-spec ids]

  (let [klen (count pk-spec)
        n-ids (count ids)

        pk-cols-list (sql/pk-cols-list pk-spec)

        pk-val-placeholders (sql/VALUES-placeholders-list klen n-ids)]

    (str
     "(" pk-cols-list ") "
     "IN (" pk-val-placeholders ")")))

(defn get-collection-objects-query
  "SELECT *
   FROM <table>
   WHERE
     (<id-col>+) IN ((...)*)"
  [{coll-name :yapster.collections.metadata/name
    coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}
   ids]
  (let [coll-pk-spec (coll.keys/get-keyspec coll coll-pk-alias)

        object-table-name (sqlite.tables/collection-objects-table-name
                           coll-name)
        pk-key-conditions (collection-object-primary-key-conditions
                           coll-pk-spec
                           ids)]

    [(str
      "SELECT * from "
      object-table-name
      " WHERE "
      pk-key-conditions)

     (-> ids
         (flatten)
         (sql/compatible-query-values))]))

(defn rehydrate-data
  "the .-data field of each object is expected to contain
   a JSON encoded object - rehydrate it"
  [obj]
  ;; (log/debug ::rehydrate-data {:obj obj})
  (let [data-str (oget obj "data")
        data (some-> data-str js/JSON.parse)]
    (if (some? data)
      (do (oset! obj "!data" data)
          obj)
      obj)))

(defn get-collection-objects-cb
  [_ctx coll ids]
  (let [q (get-collection-objects-query coll ids)
        q-cb (tx.stmts/tx-statement-cb q)]
    (tx/fmap-tx-callback
     q-cb
     #(map rehydrate-data %))))

(defmethod ctx.mm/-get-collection-objects-cb :yapster.collections.context/sqlite
  [ctx coll ids]
  (get-collection-objects-cb ctx coll ids))

(defn collection-object-insert-value-list
  "single object value list for a parameterised INSERT ...
   JSON encode the object onto the end of the primary key
   value: [<id_N>+ <JSON-data>]"
  [{coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}
   obj]
  ;; (log/debug ::collection-object-insert-value-list {:coll coll :obj obj})

  (let [coll-pk-spec (coll.keys/get-keyspec coll coll-pk-alias)
        pk-val (coll.keys/extract-key coll-pk-spec obj)]
    (conj
     pk-val
     (js/JSON.stringify obj))))

(defn store-collection-objects-query
  "INSERT INTO <table>
   (<id-col>+,data,created_at,updated_at)
   VALUES (...),(...),...
   ON CONFLICT (<id-col>+)
   DO UPDATE SET
     data=excluded.data,
     updated_at=excluded.updated_at"
  [{coll-name :yapster.collections.metadata/name
    coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}
   objs]
  (let [coll-pk-spec (coll.keys/get-keyspec coll coll-pk-alias)

        table-name (sqlite.tables/collection-objects-table-name
                    coll-name)

        pk-cols-list (sql/pk-cols-list coll-pk-spec)

        values-placeholders (sql/VALUES-templated-placeholders-list
                             (into
                              (vec (repeat (inc (count coll-pk-spec)) :?))
                              ["datetime()" "datetime()"])
                             (count objs))

        values (map (partial collection-object-insert-value-list coll) objs)]

    (log/debug ::store-collection-objects-query
               {:coll coll
                ;; :objs objs
                :pk-cols-list pk-cols-list
                :values-placeholders values-placeholders})


    [(str
      "INSERT INTO " table-name " "
      "(" pk-cols-list ",data,created_at,updated_at) "
      "VALUES "
      values-placeholders " "
      "ON CONFLICT (" pk-cols-list ") "
      "DO UPDATE SET "
      " data=excluded.data, "
      " updated_at=excluded.updated_at")

     (-> values
         (flatten)
         (sql/compatible-query-values))]))

(defn store-collection-objects-cb
  [_ctx coll objs]
  (let [q (store-collection-objects-query coll objs)]
    (tx.stmts/tx-statement-cb q)))

(defmethod ctx.mm/-store-collection-objects-cb :yapster.collections.context/sqlite
  [ctx coll objs]
  (store-collection-objects-cb ctx coll objs))
