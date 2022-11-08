(ns yapster.collections.context.sqlite.indexes
  (:require
   [clojure.string :as string]
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget]]
   [yapster.collections.metadata.key-component.sort-order :as-alias coll.md.kc.so]
   [yapster.collections.keys :as coll.keys]
   [yapster.collections.context.transactions :as tx]
   [yapster.collections.context.multimethods :as ctx.mm]
   [yapster.collections.context.sqlite.sql :as sql]
   [yapster.collections.context.sqlite.transaction-statements :as tx.stmts]
   [yapster.collections.context.sqlite.tables :as sqlite.tables]
   [yapster.collections.context.sqlite.objects :as sqlite.objects]))

(defn index-page-key-conditions
  "items are identified by their primary key-value... for the
   pagination we use the index-keys, and browse the index by
   the last component of the index-key

   e.g. if we have a collection of conversation_messages, then we might
   have a primary key or [org-id conversation-id id] and
   and index-key of [org-id conversation-id message-timestamp]"
  ([key-spec op key-value]
   (index-page-key-conditions nil key-spec op key-value))
  ([table-prefix key-spec op key-value]

   (let [key-value (vec (coll.keys/normalize-key-value key-value))
         full-key-len (count key-spec)
         value-len (count key-value)
         fixed-cols (subvec key-value 0 (dec full-key-len))

         end-key? (< value-len full-key-len)

         key-col-prefix (if (some? table-prefix)
                          (str table-prefix ".k_")
                          "k_")

         fixed-col-conds (->> fixed-cols
                              (coll.keys/indexed-key-fields
                               key-col-prefix)
                              (map name)
                              (map #(str "(" % "=?)"))
                              (into []))

         page-col-cond (when-not end-key?
                         (str "(" key-col-prefix (dec full-key-len) op "?)"))

         all-conds (if (some? page-col-cond)
                     (conj fixed-col-conds page-col-cond)
                     fixed-col-conds)]

     (string/join
      " AND "
      all-conds))))

(defn check-key-value
  "key-values can either be the full length of the key,
   or one component short to query from the start of the index"
  [key-spec
   {after :after
    before :before
    start :start
    :as opts}]
  (let [after? (some? after)
        before? (some? before)
        start? (some? start)

        constraint-count (->> [after? before? start?]
                              (filter identity)
                              (count))

        full-value-required? (or after? before?)
        end-value-required? start?

        key-value (or start after before)

        full-key-length (count key-spec)
        value-length (count key-value)

        full-value? (= full-key-length value-length)
        end-value? (= (dec full-key-length) value-length)]

    (when (not= constraint-count 1)
      (throw (ex-info "exactly one of :before :after :start can be set"
                      {:key-spec key-spec
                       :key-value key-value
                       :opts opts})))

    (when (and full-value-required?
               (not full-value?))
      (throw (ex-info "full key-value required for :after and :before"
                      {:key-spec key-spec
                       :key-value key-value
                       :opts opts})))

    (when (and end-value-required?
               (not end-value?))
      (throw (ex-info "key-value must be missing final component"
                      {:key-spec key-spec
                       :key-value key-value
                       :opts opts})))

    true))

(defn get-index-objects-page-query
  "get a page of collection objects referenced from an index-page

   the result rows will have all the fields of objects (*but* the
   updated_at field will be the earlier of the object/updated_at and
   index/updated_at field), along with the key fields from indexes
   and the prev_id and next_id from indexes"
  [{coll-name :yapster.collections.metadata/name
    coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}
   key-alias
   {start :start
    after :after
    before :before
    limit :limit
    :or {limit 20}
    :as opts}]

  (log/debug ::get-index-objects-page-query
             {:coll-name coll-name
              :key-alias key-alias
              :opts opts})

  (let [key-spec (coll.keys/get-keyspec coll key-alias)
        _ (check-key-value key-spec opts)

        coll-pk-spec (coll.keys/get-keyspec coll coll-pk-alias)

        index-table-name (sqlite.tables/collection-index-table-name
                          coll-name
                          key-spec)

        objects-table-name (sqlite.tables/collection-objects-table-name
                            coll-name)
        objects-table-fields (sqlite.tables/collection-objects-table-fields
                              coll)
        ;; remove the updated_at field, so we can calculate it from the
        ;; earliest of the object and the index-record
        objects-table-fields (remove #(= :updated_at %) objects-table-fields)
        objects-table-cols-list (->> (for [f objects-table-fields]
                                      (str "objs." (name f) " as " (name f)))
                                     (string/join ", "))

        pk-fields (sql/pk-fields coll-pk-spec)

        idx-key-fields (sql/idx-key-fields key-spec)
        order-key-field (last idx-key-fields)
        order-key-component-spec (last key-spec)
        order-dir-key-col (sql/key-col-dir
                            order-key-field
                            order-key-component-spec)

        idx-table-cols-list (->> (for [f (into idx-key-fields [:prev_id :next_id])]
                                   (str "idx." (name f) " as " (name f)))
                                 (string/join ", "))

        sort-order (coll.keys/key-component-sort-order
                    order-key-component-spec)
        after? (some? after)

        key-op (condp = [after? sort-order]
                 [true ::coll.md.kc.so/asc] ">="
                 [true ::coll.md.kc.so/desc] "<="
                 [false ::coll.md.kc.so/asc] "<="
                 [false ::coll.md.kc.so/desc] ">=")

        key-value (or start after before)

        key-conditions (index-page-key-conditions
                        "idx"
                        key-spec
                        key-op
                        key-value)]
    [(str
      "SELECT "
      objects-table-cols-list ", "
      "min(datetime(objs.updated_at),datetime(idx.updated_at)) as updated_at, "
      idx-table-cols-list " "
      "FROM "
      index-table-name " idx "
      "INNER JOIN "
      objects-table-name " objs "
      "ON "
      (->>
       (for [pkf pk-fields]
         (let [pkf (name pkf)]
           (str "objs." pkf "=idx." pkf)))
       (string/join " AND "))
      " WHERE "
      key-conditions
      " ORDER BY " (name order-dir-key-col) " "
      " LIMIT ?" )
     (-> []
         (into key-value)
         (conj limit)
         (sql/compatible-query-values))]))

(defn get-index-objects-page-cb
  [_ctx coll key-alias opts]
  (let [q (get-index-objects-page-query coll key-alias opts)
        q-cb (tx.stmts/tx-statement-cb q)]
    (tx/fmap-tx-callback
     q-cb
     #(map sqlite.objects/rehydrate-data %))))

(defmethod ctx.mm/-get-index-objects-page-cb :yapster.collections.context/sqlite
  [ctx coll key-alias opts]
  (get-index-objects-page-cb ctx coll key-alias opts))

(defn matches-prev-next-id?
  [prev-id? next-id?]
  (fn [r]
    (= [prev-id? next-id?]
       [(some? (oget r "?prev_id"))
        (some? (oget r "?next_id"))])))

(defn store-index-records-queries
  "INSERT INTO <table>
   (<id-col>+,<key-col>+,prev_id,next_id,created_at,updated_at)
   VALUES
   (?,?,...),(?,?,...),...


   NB: multiple queries are issued, depending on the presence of
     :prev_id and :next_id fields on records. missing fields will
     *not* be updated, so will retain their previous value (so that
     records at the beginning or end of a page will retain their
     linkage outside the page)"
  [{coll-name :yapster.collections.metadata/name
    coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}
   key-alias
   index-records]

  (let [coll-pk-spec (coll.keys/get-keyspec coll coll-pk-alias)
        key-spec (coll.keys/get-keyspec coll key-alias)

        table-name (sqlite.tables/collection-index-table-name
                    coll-name
                    key-spec)

        ;; vec of pk fields in index record
        pk-fields (sql/pk-fields coll-pk-spec)
        pk-cols-list (sql/pk-cols-list coll-pk-spec)

        ;; vec of index fields in index record
        idx-key-fields (sql/idx-key-fields key-spec)
        idx-key-cols-list (sql/idx-key-cols-list key-spec)

        ;; general idea is we do 4 update queries, one for each of the
        ;; different combinations of prev_id/next_id field presences

        ;; values for records with neither prev_id nor next_id fields
        no-prev-next-id-values (->> index-records
                                    (filter (matches-prev-next-id? false false ))
                                    (map
                                     (fn [r]
                                       (into
                                        []
                                        (concat
                                         (coll.keys/extract-key pk-fields r)
                                         (coll.keys/extract-key idx-key-fields r))))))

        ;; values for records with both a prev_id and next_id fields
        prev-next-id-values (->> index-records
                                 (filter (matches-prev-next-id? true true))
                                 (map
                                  (fn [r]
                                    (into
                                     []
                                     (concat
                                      (coll.keys/extract-key pk-fields r)
                                      (coll.keys/extract-key idx-key-fields r)
                                      [(oget r "prev_id")
                                       (oget r "next_id")])))))

        ;; values for records with only a next_id field
        next-id-values (->> index-records
                            (filter (matches-prev-next-id? false true))
                            (map
                             (fn [r]
                               (into
                                []
                                (concat
                                 (coll.keys/extract-key pk-fields r)
                                 (coll.keys/extract-key idx-key-fields r)
                                 [(oget r "next_id")])))))

        ;; values for records with only a prev_id field
        prev-id-values (->> index-records
                            (filter (matches-prev-next-id? true false))
                            (map
                             (fn [r]
                               (into
                                []
                                (concat
                                 (coll.keys/extract-key pk-fields r)
                                 (coll.keys/extract-key idx-key-fields r)
                                 [(oget r "prev_id")])))))]

    ;; (log/debug ::store-index-records-query {:index-records index-records
    ;;                                        :values values})

    (filterv
     some?

     [;; query updating neither :prev_id nor :next_id pointers
      (when (not-empty no-prev-next-id-values)
        [(str
          "INSERT INTO " table-name
          "("
          pk-cols-list ","
          idx-key-cols-list ","
          "created_at,updated_at"
          ")"
          " VALUES "
          (sql/VALUES-templated-placeholders-list
           (into (vec (repeat (+ (count coll-pk-spec)
                                 (count key-spec)) :?))
                 ["datetime()" "datetime()"])
           (count index-records)) " "
          "ON CONFLICT (" pk-cols-list ") "
          "DO UPDATE SET "
          (->> (for [idxk idx-key-fields]
                 (let [idxk (name idxk)]
                   (str idxk "=excluded." idxk)))
               (string/join ","))
          ",updated_at=excluded.updated_at")

         (-> no-prev-next-id-values
             (flatten)
             (sql/compatible-query-values))])

      ;; query updating both :prev_id and :next_id pointers
      (when (not-empty prev-next-id-values)
        [(str
          "INSERT INTO " table-name
          "("
          pk-cols-list ","
          idx-key-cols-list ","
          "prev_id,next_id,"
          "created_at,updated_at"
          ")"
          " VALUES "
          (sql/VALUES-templated-placeholders-list
           (into (vec (repeat (+ (count coll-pk-spec)
                                 (count key-spec)
                                 2) :?))
                 ["datetime()" "datetime()"])
           (count index-records)) " "
          "ON CONFLICT (" pk-cols-list ") "
          "DO UPDATE SET "
          (->> (for [idxk idx-key-fields]
                 (let [idxk (name idxk)]
                   (str idxk "=excluded." idxk)))
               (string/join ","))
          ",prev_id=excluded.prev_id,"
          "next_id=excluded.next_id,"
          "updated_at=excluded.updated_at")

         (-> prev-next-id-values
             (flatten)
             (sql/compatible-query-values))])

      ;; query updating records with only :prev_id pointers
      (when (not-empty prev-id-values)
        [(str
          "INSERT INTO " table-name
          "("
          pk-cols-list ","
          idx-key-cols-list ","
          "prev_id,"
          "created_at,updated_at"
          ")"
          " VALUES "
          (sql/VALUES-templated-placeholders-list
           (into (vec (repeat (+ (count coll-pk-spec)
                                 (count key-spec)
                                 1) :?))
                 ["datetime()" "datetime()"])
           (count index-records)) " "
          "ON CONFLICT (" pk-cols-list ") "
          "DO UPDATE SET "
          (->> (for [idxk idx-key-fields]
                 (let [idxk (name idxk)]
                   (str idxk "=excluded." idxk)))
               (string/join ","))
          ",prev_id=excluded.prev_id,"
          "updated_at=excluded.updated_at")

         (-> prev-id-values
             (flatten)
             (sql/compatible-query-values))])


      ;; query updating records with only :next_id pointers
      (when (not-empty next-id-values)
        [(str
          "INSERT INTO " table-name
          "("
          pk-cols-list ","
          idx-key-cols-list ","
          "next_id,"
          "created_at,updated_at"
          ")"
          " VALUES "
          (sql/VALUES-templated-placeholders-list
           (into (vec (repeat (+ (count coll-pk-spec)
                                 (count key-spec)
                                 1) :?))
                 ["datetime()" "datetime()"])
           (count index-records)) " "
          "ON CONFLICT (" pk-cols-list ") "
          "DO UPDATE SET "
          (->> (for [idxk idx-key-fields]
                 (let [idxk (name idxk)]
                   (str idxk "=excluded." idxk)))
               (string/join ","))
          ",next_id=excluded.next_id,"
          "updated_at=excluded.updated_at")

         (-> next-id-values
             (flatten)
             (sql/compatible-query-values))])])))

(defn store-index-records-cb
  [_ctx
   coll
   key-alias
   index-records]
  (if (not-empty index-records)
    (let [qs (store-index-records-queries coll key-alias index-records)]
      (tx.stmts/tx-statements-cb qs))
    tx/noop-tx-callback))

(defmethod ctx.mm/-store-index-records-cb :yapster.collections.context/sqlite
  [ctx coll key-alias index-records]
  (store-index-records-cb ctx coll key-alias index-records))

(defn delete-index-records-query
  "DELETE FROM <table>
   WHERE
     (<id-col>+)
   IN
     ((?,?,...),(?,?,...),...)"
  [{coll-name :yapster.collections.metadata/name
    coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}
   key-alias
   index-records]
  (let [coll-pk-spec (coll.keys/get-keyspec coll coll-pk-alias)
        key-spec (coll.keys/get-keyspec coll key-alias)

        table-name (sqlite.tables/collection-index-table-name
                    coll-name
                    key-spec)

        pk-cols-list (sql/pk-cols-list coll-pk-spec)
        values (->> index-records
                    (map
                     (fn [r]
                       (into
                        []
                        (coll.keys/extract-key coll-pk-spec r)))))]
    [(str "DELETE FROM " table-name " "
          "WHERE (" pk-cols-list ") "
          "IN ("
          (sql/VALUES-placeholders-list
           (count coll-pk-spec)
           (count index-records))
          ")" )

     (-> values
         (flatten)
         (sql/compatible-query-values))]))

(defn delete-index-records-cb
  [_ctx
   coll
   key-alias
   index-records]
  (if (not-empty index-records)
    (let [q (delete-index-records-query coll key-alias index-records)]
      (tx.stmts/tx-statement-cb q))
    tx/noop-tx-callback))

(defmethod ctx.mm/-delete-index-records-cb  :yapster.collections.context/sqlite
  [ctx coll key-alias index-records]
  (store-index-records-cb ctx coll key-alias index-records))
