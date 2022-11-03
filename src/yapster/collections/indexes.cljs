(ns yapster.collections.indexes
  "cross-platform support for pages of index records"
  (:require
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget oset!]]
   [promesa.core :as p]
   [yapster.collections.metadata.key-component.sort-order :as-alias coll.md.kc.so]
   [yapster.collections.keys :as keys]
   [yapster.collections.context :as ctx]
   [yapster.collections.context.transactions :as tx]
   [yapster.collections.context.multimethods :as ctx.mm]))

;; at this level transactions can be used, but no transactions are visible
;; in the API
;;

(defn get-index-objects-page
  "get a page of collection objects in their storage format"
  [ctx
   coll-name
   key-alias
   {_after :after _before :before _limit :limit :as opts}]
  (p/let [coll (ctx/open-collection ctx coll-name)
          get-cb (ctx.mm/-get-index-objects-page-cb ctx coll key-alias opts)]

    (tx/readonly-transaction ctx get-cb)))

(defn page->annotated-page
  "given:

  - page: a sequence of <record>s

  return a sequence of <annotated-record>s like:

  {::record <record>
   ::prev-record <prev-record>?
   ::next-record <next-record>?}"
  [page]

  (->> [page
        (concat [nil] page)
        (concat (drop 1 page) [nil])]
       (apply
        map
        (fn [record prev-record next-record]
          {::record record
           ::prev-record prev-record
           ::next-record next-record}))))

(defn annotate-page
  "given:

   - annotated-page: a sequence of <annotated-record>s like:
       {::record <record>
        ::prev-record <prev-record>?
        ::next-record <next-record>?
        ...}

  - k: a keyword
  - f: a fn

   return a further annotated sequence of:

   {k (f <annotated-record>)
    ::record <record>
    ::prev-record <prev-record>?
    ::next-record <next-record>?}"

  [k f annotated-page]
  (for [ann-r annotated-page]
    (assoc ann-r k (f ann-r))))

(defn make-extract-id-JSON-fn
  [{coll-pk-alias :yapster.collections.metadata/primary-key
    :as coll}]

  (let [coll-pk-spec (keys/get-keyspec coll coll-pk-alias)
        pk-fields (keys/indexed-key-fields :id_ coll-pk-spec)]
    (fn [r]
      (some-> (keys/extract-key pk-fields r)
              (clj->js)
              (js/JSON.stringify)))))

(defn make-annotate-continuous?
  "make a fn which, when given an annotated index-object-record,
   returns
   - true if all the prev_id and next_id pointers are consistent
   - false if there are any prev_id and next_id inconsistencies"
  [coll]
  (let [extract-id (make-extract-id-JSON-fn coll)]

    (fn annotate-continuous?
      [{record ::record
        prev-record ::prev-record
        next-record ::next-record}]

      ;; (log/info ::annotate-continuous?
      ;;           {::record record
      ;;            ::prev-record prev-record
      ;;            ::next-record next-record})

      (let [record-id (extract-id record)
            record-prev-id (oget record "?prev_id")
            record-next-id (oget record "?next_id")

            prev-record-id (some-> prev-record extract-id)
            prev-record-next-id (some-> prev-record (oget "?next_id"))

            next-record-id (some-> next-record extract-id)
            next-record-prev-id (some-> next-record (oget "?prev_id"))]

        ;; check all forward and backward links
        (and
         (or (nil? prev-record-id)
             (= record-id prev-record-next-id))

         (or (nil? prev-record-id)
             (= record-prev-id prev-record-id))

         (or (nil? next-record-id)
             (= record-next-id next-record-id))

         (or (nil? next-record-id)
             (= record-id next-record-prev-id)))))))

(defn add-prev-next-id-fields
  "add :prev_id and :next_id fields to index-records,
   with values being the JSON encoded id of the previous/next record"
  [coll
   index-records]

  (let [extract-id-JSON (make-extract-id-JSON-fn coll)

        ;; [record prev-record next-record]
        record-prev-next (->> [index-records
                               (concat [nil] index-records)
                               (concat (drop 1 index-records) [nil])]
                              (apply map vector))]

    (->> record-prev-next
         (map
          (fn [[r p n]]
            [r
             (extract-id-JSON p)
             (extract-id-JSON n)]))
         (map
          (fn [[r p n]]
            (cond-> r
              (some? p) (oset! "!prev_id" p)
              (some? n) (oset! "!next_id" n)))))))

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

  ;; we only calculate deletions within the overlap ...
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

        ;; list of new-object-records sorted by last key component
        sort-k-objs (->> new-object-records
                    (map (fn [obj] {::key (last (keys/extract-key keyspec obj))
                                   ::obj obj}))
                    (sort-by ::key cmp))

        first-obj-key (-> sort-k-objs first ::key)
        last-obj-key (-> sort-k-objs last ::key)

        ;; _ (prn "sort-k-objs" sort-k-objs)

        ;; list of old-index-records sorted by last key component
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

        ;; insert an index-record for each new-object-records. :prev_id
        ;; and :next_id values are added to each index record, with
        ;; the first record getting no :prev_id and the last record
        ;; getting no :next_id
        insertions (->> merge-obj-idxs
                        (filter (fn [{obj ::obj}] (some? obj)))
                        (map
                         (fn [{obj ::obj}]
                               (make-index-record coll keyspec obj)))
                        (add-prev-next-id-fields coll))

        ;; _ (prn "insertions" insertions)

        ;; we only consider overlaps of the old-index-records and
        ;; new-object records for change processing.
        ;; we want everything with a key in the range of the
        ;; keys of the first and last ::obj record
        overlap (->> merge-obj-idxs
                     (drop-while #(< (cmp (::key %) first-obj-key) 0))
                     (reverse)
                     (drop-while #(> (cmp (::key %) last-obj-key) 0))
                     (reverse))

        ;; _ (prn "overlap" overlap)

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

          _ (log/debug ::update-index-page-changes _idx-chgs )

          store-objects-cb (when (not-empty new-object-records)
                             (ctx.mm/-store-collection-objects-cb
                              ctx coll new-object-records))
          insert-cb (when (not-empty idx-insertions)
                      (ctx.mm/-store-index-records-cb
                       ctx coll key-alias idx-insertions))
          delete-cb (when (not-empty idx-deletions)
                      (ctx.mm/-delete-index-records-cb
                       ctx coll key-alias idx-deletions))

          callbacks (filter
                     some?
                     [store-objects-cb
                      insert-cb
                      delete-cb])

          tx-cb (when (not-empty callbacks)
                  (tx/conj-tx-callbacks callbacks))]

    (if (some? tx-cb)
      (tx/readwrite-transaction ctx tx-cb)
      true)))

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
