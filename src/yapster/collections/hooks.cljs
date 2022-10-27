(ns yapster.collections.hooks
  (:require
   ["react" :as react :refer [useContext]]
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget oset! ocall]]
   [promesa.core :as p]
   [yapster.collections :as-alias coll]
   [yapster.collections.keys :as coll.keys]
   [yapster.collections.metadata :as coll.md]
   [yapster.collections.indexes :as coll.idxs]
   [yapster.collections.context :as coll.ctx]
   [yapster.collections.react-context :as coll.react-ctx]
   [yapster.collections.hooks.react-query :as rq]
   [yapster.collections.hooks.index-storage-context :as hooks.isc]))

;; some notes on the approach:
;;
;; - serve only from local storage (no live merge)
;; - refresh regularly from local-storage
;;   - uses query invalidation
;; - refresh local-storage from the network when:
;;   - at the end of an index
;;   - the index hasn't been refreshed recently
;;   - the index is disjoint (need to add next-id to the index)
;;     - disjoints happen when records move in the index, e.g. to the top of the list
;;     - still return results around a disjoint, but refresh from the API

(defn use-collection-index-object
  "hook to use a collection-object, with option to prepopulate

   - obj-or-obj-id : either the pre-fetched (js) object value, or
                     the object-id"
  ([coll-name
    obj-or-obj-id]


   (let [{db-name ::coll/db-name
          :as _rc} (useContext coll.react-ctx/CollectionsContext)

         {coll-pk-alias ::coll.md/primary-key
          :as coll} (coll.md/strict-get-collection-metadata coll-name)

         obj? (object? obj-or-obj-id)

         obj-id (if obj?
                  (coll.keys/extract-key coll coll-pk-alias obj-or-obj-id)
                  obj-or-obj-id)

         query-key (rq/object-query-key coll-name obj-id)]

     (log/trace ::use-collection-index-object
                {:coll-name coll-name
                 :obj-id obj-id
                 :query-key query-key
                 :obj? obj?})

     (rq/use-query
      query-key

      (fn [& _args]
        (hooks.isc/load-collection-object-query
         db-name
         coll-name
         obj-id))

      (merge
        {}
        (when obj?
          {:initialData obj-or-obj-id}))))))

(defn invalidate-collection-index-object
  "invalidate a collection object, causing it to be refetched
   from local store"
  [{_db-name ::coll/db-name
    query-client ::coll/query-client
    :as _react-ctx}
   coll-name
   obj-or-obj-id]
  (let [{coll-pk-alias ::coll.md/primary-key
         :as coll} (coll.md/strict-get-collection-metadata coll-name)

        obj? (object? obj-or-obj-id)

        obj-id (if obj?
                 (coll.keys/extract-key coll coll-pk-alias obj-or-obj-id)
                 obj-or-obj-id)

        query-key (rq/object-query-key coll-name obj-id)]

    (rq/invalidate-queries
     query-client
     query-key)))

(defn use-invalidate-collection-index-object-fn
  "a hook returning a fn to invalidate a collection index object

   returns: a partial of invalidate-collection-index-object with
            query-client and db-name filled in"
  []
  (let [react-ctx (useContext coll.react-ctx/CollectionsContext)]
    (partial
     invalidate-collection-index-object
     react-ctx)))

(defn use-collection-index
  "a hook for infinite-queries on a
   locally persisted collection, based on
   react-query useInfiniteQuery & yapster.collections

   coll-name : the name of the collection
   key-alias : the key in the collection to browse records by
   key-data : full key value, or last-component-missing key-value.
              if the last component is missing, browse from the
             'start' of the index
   query-opts : {:limit <limit>}
              - limit: page size"

  ([coll-name key-alias key-data]
   (use-collection-index coll-name key-alias key-data {}))
  ([coll-name
    key-alias
    key-data
    {limit :limit :as query-opts}]
   (let [coll (coll.md/strict-get-collection-metadata coll-name)

         react-ctx (useContext coll.react-ctx/CollectionsContext)

         qkey (rq/index-query-key coll key-alias key-data)

         _ (log/info ::use-collection-index
                     {:react-context react-ctx

                      :coll-name coll-name
                      :key-alias key-alias
                      :qkey qkey
                      :key-data key-data
                      :limit limit})

         infq (rq/use-infinite-query
               qkey

               (partial
                hooks.isc/load-collection-page-query
                react-ctx
                coll
                key-alias
                key-data
                query-opts)

               {:getNextPageParam
                (fn [last-page pages]
                  (if (not-empty last-page)
                    #js ["next" last-page pages]
                    js/undefined))

                :getPreviousPageParam
                (fn [first-page pages]
                  (if (not-empty first-page)
                    #js ["previous" first-page pages]
                    js/undefined))})

         ;; the hook fn is called repeatedly - data is up to date
         ;; and can be closed-over in the fetch-start-page / fetch-end-page fns
         data-pages (oget infq "?data.pages")

         fetch-start-page
         (fn [opts]
           ;; "start" is like "previous", but with a forced API fetch
           (let [page-param #js ["start"
                                 (->> data-pages
                                      (filter not-empty)
                                      (first))
                                 data-pages]]
             (log/info
              ::fetch-start-page
              {:data-pages-counts (mapv count data-pages)})
             (ocall
              infq
              "fetchPreviousPage"
              (oset! (or opts #js {}) "!pageParam" page-param))))

         fetch-end-page
         (fn [opts]
           ;; "end" is like "next", but with a forced API fetch
           (let [page-param #js ["end"
                                 (->> data-pages
                                      (filter not-empty)
                                      (last))
                                 data-pages]]
             (log/info
              ::fetch-end-page
              {:data-pages-counts (mapv count data-pages)})
             (ocall
              infq
              "fetchNextPage"
              (oset! (or opts #js {}) "!pageParam" page-param))))]

     (-> infq
         (oset! "!fetchStartPage" fetch-start-page)
         (oset! "!fetchEndPage" fetch-end-page)))))

(defn invalidate-collection-index
  "invalidate a collection index query, causing it to be refetched
   from local store (and maybe from the API)

   requires a query-client to be provided - it may be convenient
   to use the use-invalidate-collection-index-fn hook which returns
   a partial of this fn with the query-client filled in"

  [{db-name ::coll/db-name
    query-client ::coll/query-client
    :as _react-ctx}
   coll-name
   key-alias
   key-data]
  (p/let [ctx (coll.ctx/open-storage-context db-name)
          coll (coll.ctx/open-collection ctx coll-name)

          query-key (rq/index-query-key coll key-alias key-data)]

    (rq/invalidate-queries
     query-client
     query-key)))

(defn use-invalidate-collection-index-fn
  "a hook returning a fn to invalidate a collection index...
   returns a partial of the invalidate-collection-index fn with
   query-client and db-name filled in"
  []
  (let [react-ctx (useContext coll.react-ctx/CollectionsContext)]
    (partial invalidate-collection-index
             react-ctx)))

(defn refetch-collection-index-start
  "force a network fetch from the very start of the collection index, update
   the local-store, and invalidate active queries

   we often know when we want to force a network access at the start of
   a collection - e.g. the user pulls the start of a list down in the UI - and
   this function provides an explicit way of forcing such an access

   best invoked via the use-refetch-collection-index-start-fn hook, which
   will return a partial fn with the query-client, auth-info and db-name
   values filled in"

  ([react-ctx
    coll-name
    key-alias
    key-data]
   (refetch-collection-index-start
    react-ctx
    coll-name
    key-alias
    key-data
    {}))

  ([{db-name ::coll/db-name
     :as react-ctx}
    coll-name
    key-alias
    key-data
    query-opts]

   (log/info ::refetch-collection-index-start
             {:react-ctx react-ctx
              :coll-name coll-name
              :key-alias key-alias
              :key-data key-data
              :query-opts query-opts})

   (p/let [ctx (coll.ctx/open-storage-context db-name)
           coll (coll.ctx/open-collection ctx coll-name)

           query-key (rq/index-query-key coll key-alias key-data)]

     (log/info ::refetch-collection-index-start-impl
               {:ctx ctx
                :coll coll
                :query-key query-key})

     (hooks.isc/load-refetch-start-collection-page
      react-ctx
      query-key
      ctx
      coll
      key-alias
      key-data
      query-opts))))

(defn use-refetch-collection-index-start-fn
  "a hook returning a function to force network refetching
   of the start of a collection"
  []
  (let [react-ctx (useContext coll.react-ctx/CollectionsContext)]
    (partial refetch-collection-index-start
             react-ctx)))

(defn update-object-and-indexes
  [{db-name ::coll/db-name
    :as react-ctx}
   coll-name
   obj]
  (p/let [ctx (coll.ctx/open-storage-context db-name)
          {index-key-aliases :yapster.collections.metadata/index-keys
           :as coll} (coll.ctx/open-collection ctx coll-name)

          _ (coll.idxs/update-object-and-indexes ctx coll-name obj)]

    (invalidate-collection-index-object
     react-ctx
     coll-name
     obj)

    (doseq [ka index-key-aliases]
      (let [key-data (coll.keys/extract-key coll ka obj)]
        (invalidate-collection-index
         react-ctx
         coll-name
         ka
         key-data)))

    true))

(defn use-update-object-and-indexes-fn
  []
  (let [react-ctx (useContext coll.react-ctx/CollectionsContext)]
    (partial update-object-and-indexes
             react-ctx)))
