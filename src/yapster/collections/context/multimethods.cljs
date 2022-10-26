(ns yapster.collections.context.multimethods)

;; StorageContext related methods

(defmulti -open-storage-context
  "open a storage context, to be used with the other collections multimethods

   returns Promise<StorageContext>,
     or nil or Promise<nil> if the impl is not available"
  (fn [impl _db-name]
    impl))

(defmulti -delete-storage-context
  "permanently delete a storage context

   returns Promise<true|error>"
  (fn [impl _db-name]
    impl))

(defn context-impl-dispatch-value
  [{impl :yapster.collections.context/impl
       :as _ctx}]
  impl)

(defmulti -check-storage-context
  "test a storage context object is operational and
   return
     - ctx if it is operational
     - nil if it is not"
  (fn [ctx]
    (context-impl-dispatch-value ctx)))

;; CollectionMetadata related methods

(defmulti -load-collection-metadata
  "load collection metadata from a db

   returns nil or the collection metadata"
  (fn [ctx _coll-name]
    (context-impl-dispatch-value ctx)))

(defmulti -store-collection-metadata
  "store collection metadata in a db

   if metadata is nil then it will be removed from the db"
  (fn [ctx _metadata]
    (context-impl-dispatch-value ctx)))

(defmulti -create-collection
  "create storage resources for a collection"
  (fn [ctx _metadata]
    (context-impl-dispatch-value ctx)))

(defmulti -drop-collection
  "drop storage resources for a collection"
  (fn [ctx _metadata]
    (context-impl-dispatch-value ctx)))

;; transaction related methods

(defmulti -readonly-transaction
  "perform a read-only transaction object for the StorageContext,
   calling the transaction callback as:

   (tx-cb tx resolve reject opts)

   returns Promiset<result|error>"
  (fn [ctx _tx-cb]
    (context-impl-dispatch-value ctx)))

(defmulti -readwrite-transaction
  "perform a read-write transaction object for the StorageContext,
   calling the transaction callback as:

   (tx-cb tx resolve reject opts)

   returns Promiset<result|error>"
  (fn [ctx _tx-cb]
    (context-impl-dispatch-value ctx)))

;; collection store related methods
;; these produce callbacks which can be run in a
;; transaction. we have an internal callback based
;; interface because (unlike IndexedDB) WebSQL has a
;; continuation-based API, which can be wrapped
;; in a promise API but can't be passed transaction
;; callbacks implemented with promises

(defmulti -get-collection-objects-cb
  "get objects fron a collection"
  (fn [ctx _coll _ids]
    (context-impl-dispatch-value ctx)))

(defmulti -store-collection-objects-cb
  "store objects in a collection"
  (fn [ctx _coll _objs]
    (context-impl-dispatch-value ctx)))

(defmulti -get-index-page-cb
  "get a page of index-records for a collection"
  (fn [ctx
      _coll
      _key-alias
      {_after :after _before :before _limit :limit :as _opts}]
    (context-impl-dispatch-value ctx)))

(defmulti -get-index-objects-page-cb
  "get collection objects referenced from an index-page"
  (fn [ctx
      _coll
      _key-alias
      {_after :after _before :before _limit :limit :as _opts}]
    (context-impl-dispatch-value ctx)))

(defmulti -store-index-records-cb
  "store multiple index records"
  (fn [ctx
      _coll
      _key-alias
      _index-records]
    (context-impl-dispatch-value ctx)))

(defmulti -delete-index-records-cb
  "delete multiple index records"
  (fn [ctx
      _coll
      _key-alias
      _index-records]
    (context-impl-dispatch-value ctx)))
