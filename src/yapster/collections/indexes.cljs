(ns yapster.collections.indexes
  "cross-platform support for pages of index records"
  (:require
   [lambdaisland.glogi :as log]
   [promesa.core :as p]
   [yapster.collections.context.util.time :as util.t]
   [yapster.collections.metadata.key-component.sort-order :as-alias coll.md.kc.so]
   [yapster.collections.keys :as keys]
   [yapster.collections.objects :as objects]
   [yapster.collections.context :as ctx]
   [yapster.collections.context.transactions :as tx]
   [yapster.collections.context.multimethods :as ctx.mm]))

(defn extract-index-records-metadata
  [index-records]
  (let [;; updated-at is the earliest updated-at
        ;; of any of the members
        coll-updated-at (->> index-records
                             (map #(keys/extract-key [:updated_at] %))
                             (map first)
                             (sort)
                             first)]
    {:yapster.collections/index-records index-records
     :yapster.collections.indexes/updated-at
     (util.t/->inst coll-updated-at)}))

;; at this level transactions can be used, but no transactions are visible
;; in the API
;;
(defn get-index-page
  "get a page of collection objects referenced from an index

   returns a promise of the objects and the earliest :updated_at timestamp
   of any of the index-records

   Promise<{:yapster.collections/index-records [<idx-record>*]
            :yapster.collections.index-records/updated-at <timestamp>}>"
  [ctx
   coll-name
   key-alias
   {_after :after _before :before _limit :limit :as opts}]
  (p/let [coll (ctx/open-collection ctx coll-name)
          get-cb (ctx.mm/-get-index-page-cb ctx coll key-alias opts)
          tx-cb (tx/fmap-tx-callback
                 get-cb
                 extract-index-records-metadata)]
    (tx/readonly-transaction ctx tx-cb)))

(defn extract-index-objects-metadata
  "extract update-at metadata from a collection of index-objects"
  [index-objects]
  ;; (log/info ::extract-index-objects-metadata {})
  (let [;; updated-at is the earliest updated-at
        ;; of any of the members
        coll-updated-at (->> index-objects
                             (map #(keys/extract-key [:updated_at] %))
                             (map first)
                             (sort)
                             first)]
    {:yapster.collections/index-objects index-objects
     :yapster.collections.index-objects/updated-at
     (util.t/->inst coll-updated-at)}))

(defn extract-index-objects-data
  "given a collection of index-objects annotated with updated-at
   metadata, extract the content objects"
  [{idx-objs :yapster.collections/index-objects
    :as objs-with-metadata}]
  ;; (log/info ::extract-index-objects-data {})
  (assoc
   objs-with-metadata
   :yapster.collections/index-objects
   (->> idx-objs
        (map objects/extract-data)
        (clj->js))))

(defn get-index-objects-page
  "get a page of collection objects referenced from an index

   returns a promise of the objects and the earliest :updated_at timestamp
   of any of the objects or index records

   Promise<{:yapster.collections/index-objects [<obj>*]
            :yapster.collections.index-objects/updated-at <timestamp>}>"
  [ctx
   coll-name
   key-alias
   {_after :after _before :before _limit :limit :as opts}]
  (p/let [coll (ctx/open-collection ctx coll-name)
          get-cb (ctx.mm/-get-index-objects-page-cb ctx coll key-alias opts)
          tx-cb (tx/fmap-tx-callback
                 get-cb
                 (comp
                  extract-index-objects-data
                  extract-index-objects-metadata))]
    (tx/readonly-transaction ctx tx-cb)))

(defn index-record-key-extractor
  "makes a key-extractor fn to extract the index-key
   from an index-record - it will have the same
   number of components as the index key-spec on the
   collection, like ['k_0', 'k_1', ...]"
  [keyspec]
  (let [idx-kspec (keys/indexed-key-fields :k_ keyspec)]
    (fn [index-record]
      ;; (prn :index-record-key-extractor keyspec idx-kspec index-record)

      (keys/extract-key idx-kspec index-record))))

(defn make-index-record
  "return a javascript object of an index-record
   of obj

   - coll : the collection
   - keyspec : keyspec of the index
   - obj : the object to exract the index-record from"
  [{coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}
   keyspec
   obj]
  (let [coll-pk-spec (keys/get-keyspec coll coll-pk-alias)
        pk-val (keys/extract-key coll-pk-spec obj)

        idx-k-val (keys/extract-key keyspec obj)]

    (clj->js
     (merge
      (keys/indexed-key-fields-map :id_ pk-val)
      (keys/indexed-key-fields-map :k_ idx-k-val)))))

(defn collection-object->index-record
  [coll
   key-alias
   obj]
  (let [keyspec (keys/get-keyspec coll key-alias)]
    (make-index-record coll keyspec obj)))

(defn index-changes
  "compute the insertions and deletions to a local-store
   index-page, given an update with refreshed records
   for the same query constraint

   - old-index-records - existing index records
   - new-object-records - refreshed records for the same query constraint
       (after||before + limit) from the API

   - returns
     {::index-insertions [<index-record>*]
      ::index-deletions  [<index-record>*]}"
  [coll
   keyspec
   old-index-records
   new-object-records]

  ;; TODO
  ;; need to account for the start-of queries... the process will be ...
  ;; only calculate changes within the overlap ...
  ;; - all new-object-records are always inserted
  ;; - old-index-records outside the overlap are never deleted
  ;; - from within the overlap, old-index records missing from
  ;;   the new-object-records will be deleted

  (let [;; cmp will sort the after/before record first
        keyspec (keys/normalize-keyspec keyspec)
        sort-order (keys/key-component-sort-order
                    (last keyspec))

        ;; _ (prn "sort-order" sort-order)

        cmp (if (= ::coll.md.kc.so/asc sort-order)
              compare
              (comp - compare))

        ;; list of key-objects sorted by last key component
        sort-k-objs (->> new-object-records
                    (map (fn [obj] {::key (last (keys/extract-key keyspec obj))
                                   ::obj obj}))
                    (sort-by ::key cmp))

        ;; _ (prn "sort-k-objs" sort-k-objs)

        ;; list of key-index-records sorted by last key component
        idx-kex (index-record-key-extractor keyspec)
        sort-k-idxs (->> old-index-records
                    (map (fn [idxr] {::key (last (idx-kex idxr))
                                    ::idx idxr}))
                    (sort-by ::key cmp))

        ;; _ (prn "sort-k-kdxs" sort-k-idxs)


        merge-obj-idxs (keys/merge-key-sorted-lists
                        ::key
                        cmp
                        sort-k-objs
                        sort-k-idxs)

        ;; _ (prn "merge-obj-idxs" merge-obj-idxs)

        ;; we only consider overlaps of the old-index-records and
        ;; new-object records for change processing.
        ;; we want everything between the first and last ::obj record
        overlap (->> merge-obj-idxs
                     (drop-while #(nil? (::obj %)))
                     (reverse)
                     (drop-while #(nil? (::obj %)))
                     (reverse))

        ;; _ (prn "overlap" overlap)

        ;; insert a record for each new-object-records
        insertions (->> merge-obj-idxs
                        (filter (fn [{obj ::obj}] (some? obj)))
                        (map
                         (fn [{obj ::obj}]
                               (make-index-record coll keyspec obj))))

        ;; _ (prn "insertions" insertions)

        ;; only delete missing records from overlap
        deletions (->> overlap
                       (filter (fn [{obj ::obj}] (nil? obj)))
                       (map (fn [{idx ::idx}] idx)))

        ;; _ (prn "deletions" deletions)
        ]



    {::index-insertions insertions
     ::index-deletions deletions}))

(defn update-index-page
  "given
   - old-index-records - an index page retrieved from local storage, and
   - new-object-records - a list of records retrieved from the API
   update the index to reflect the API

   - insert/update all locally stored objects from the new-object-records
   - insert index-records for any new-object-records which don't exist in
     the old-index-records
   - delete index-records for any old-index-records which are between the
     key extents of the new-object-records but do
     not exist in the new-object-records"
  [ctx
   coll-name
   key-alias
   {_after :after _before :before _limit :limit :as _opts}
   old-index-records
   new-object-records]
  (p/let [coll (ctx/open-collection ctx coll-name)
          key-spec (keys/get-keyspec coll key-alias)

          {idx-insertions ::index-insertions
           idx-deletions ::index-deletions
           :as _idx-chgs} (index-changes
                           coll
                           key-spec
                           old-index-records
                           new-object-records)

          _ (log/info ::update-index-page-changes _idx-chgs )

          store-objects-cb (ctx.mm/-store-collection-objects-cb
                            ctx coll new-object-records)
          insert-cb (ctx.mm/-store-index-records-cb
                     ctx coll key-alias idx-insertions)
          delete-cb (ctx.mm/-delete-index-records-cb
                     ctx coll key-alias idx-deletions)

          tx-cb (tx/conj-tx-callbacks
                 [store-objects-cb
                  insert-cb
                  delete-cb])]

    (tx/readwrite-transaction ctx tx-cb)))

(defn update-object-and-indexes
  "insert or update a collection object and all its indexes"
  [ctx
   coll-name
   obj]

  (log/info ::update-object-and-indexes
            {;; :ctx ctx
             :coll-name coll-name
             :obj obj})

  (p/let [{index-key-aliases :yapster.collections.metadata/index-keys
           :as coll} (ctx/open-collection ctx coll-name)

          store-objects-cb (ctx.mm/-store-collection-objects-cb
                            ctx coll #js [obj])

          store-index-records-cbs
          (for [ka index-key-aliases]
            (let [index-record (collection-object->index-record
                                coll
                                ka
                                obj)]

              ;; (log/info ::update-objects-and-indexes-index-key
              ;;           {:key-alias ka
              ;;            :index-record index-record})
              ;;
              (ctx.mm/-store-index-records-cb
               ctx
               coll
               ka
               [index-record])))

          tx-cb (tx/conj-tx-callbacks
                 (into [store-objects-cb]
                       store-index-records-cbs))]

    (tx/readwrite-transaction ctx tx-cb)))
