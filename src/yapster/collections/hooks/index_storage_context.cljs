(ns yapster.collections.hooks.index-storage-context
  (:require
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget]]
   [promesa.core :as p]
   [yapster.collections :as-alias coll]
   [yapster.collections.keys :as keys]
   [yapster.collections.context :as coll.ctx]
   [yapster.collections.context.util.time :as util.t]
   [yapster.collections.metadata :as coll.md]
   [yapster.collections.objects :as coll.objs]
   [yapster.collections.indexes :as coll.idxs]
   [yapster.collections.hooks.react-query :as rq]
   [yapster.collections.hooks.index-api :as hooks.index-api]))

(defn refetch-page*
  "fetch a page of records from the API and integrate the results
   into the local store"
  [{query-client ::coll/query-client
    :as _react-ctx}
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   query-opts
   index-objects
   fetch-page-fn]

  (p/let [;; convert the objects to index records
          index-records (map
                         (partial
                          coll.idxs/collection-object->index-record
                          coll
                          key-alias)
                         index-objects)

          ;; fetch from the API
          r (fetch-page-fn)

          ;; update local store
          _ (coll.idxs/update-index-page
             ctx
             coll-name
             key-alias
             query-opts
             index-records
             r)]

    ;; cause react-query to refetch all the query data
    (rq/invalidate-queries
     query-client
     query-key)

    true))

(defn load-maybe-refetch-collection-page*
  "load a page from the local store, refresh from the API if
   necessary"
  [react-ctx
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   {force-refetch? ::force-refetch?
    limit :limit
    after-key :after
    before-key :before
    start-key :start
    stale-threshold-s :stale-threshold-s
    thrash-threshold-s :thrash-threshold-s
    :as query-opts
    :or {limit 10
         stale-threshold-s (* 12 3600)
         thrash-threshold-s 600}}
   fetch-page-fn]

  (p/let [start? (some? start-key)

          ;; query an extra record for the overlap
          ;; if it's not a start page
          query-opts (assoc query-opts
                            :limit
                            (if start? limit (inc limit)))

          {idx-objs :yapster.collections/index-objects
           idx-objs-updated-at :yapster.collections.index-objects/updated-at
           :as _idx-objs-page} (coll.idxs/get-index-objects-page
                                ctx
                                coll-name
                                key-alias
                                query-opts)

          ;; remove any results which overlap with the previous page
          remove-key (or after-key before-key)
          keyspec (keys/get-keyspec coll key-alias)
          idx-objs (remove
                    (fn [obj]
                      (=
                       (keys/extract-key keyspec obj)
                       remove-key))
                    idx-objs)

          ;; _ (log/info ::load-collection-page-idx-objects {})

          now (util.t/now)

          ;; page is stale if any of the objects are older than 4 hours
          stale-threshold-t (util.t/add-t now :seconds (- stale-threshold-s))
          stale? (some-> idx-objs-updated-at
                         (< stale-threshold-t))

          ;; prevent refetch loops, where a short final
          ;; page causes repeated API requests
          refetch-threshold-t (util.t/add-t now :seconds (- thrash-threshold-s))
          refetch-safe? (or (nil? idx-objs-updated-at)
                            (< idx-objs-updated-at refetch-threshold-t))

          short? (< (count idx-objs) limit)

          ;; refresh if forced,
          ;; or the page is stale,
          ;; or the page is undersized
          refetch? (or
                    force-refetch?
                    (and refetch-safe?
                         (or
                          stale?
                          short?)))]

    (log/trace ::load-maybe-refetch-collection-page*
               {:coll-name coll-name
                :key-alias key-alias
                :query-opts query-opts
                :count-idx-objs (count idx-objs)
                :now now
                :idx-objs-updated-at idx-objs-updated-at
                :refetch-threshold-t refetch-threshold-t
                :force-refetch? force-refetch?
                :stale? stale?
                :short? short?
                :refetch-safe? refetch-safe?
                :refetch? refetch?})

    (when refetch?
      (refetch-page*
       react-ctx
       query-key
       ctx
       coll
       key-alias
       query-opts
       idx-objs
       fetch-page-fn)

      ;; promesa waits on each step in the *body* of a let too...
      ;; but we don't want that here, so 'lose' the refetch-page
      ;; result promise
      true)

    (when (not-empty idx-objs)
      (clj->js
       idx-objs))))

(defn load-start-collection-page
  [react-ctx
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   {_limit :limit
    :as query-opts}]
  (let [keyspec (keys/get-keyspec coll key-alias)
        query-opts (if (keys/end-key? keyspec key-data)
                     (assoc query-opts
                            :start key-data)
                     (assoc query-opts
                            :after key-data))

        fetch-page-fn (fn []
                        (hooks.index-api/observe-fetch-start-collection-page
                         react-ctx
                         coll
                         key-alias
                         key-data
                         query-opts))]

    (log/debug ::load-start-collection-page
               {:coll-name coll-name
                :key-alias key-alias
                :key-data key-data
                :query-opts query-opts})

    (load-maybe-refetch-collection-page*
     react-ctx
     query-key
     ctx
     coll
     key-alias
     query-opts
     fetch-page-fn)))

(defn load-refetch-start-collection-page
  "like load-start-collection-page, but forces a refetch of
   the first page"
  [react-ctx
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   {limit :limit
    :or {limit 10}
    :as query-opts}]
  (let [keyspec (keys/get-keyspec coll key-alias)
        query-opts (assoc query-opts
                          :limit limit
                          ::force-refetch? true)

        query-opts (if (keys/end-key? keyspec key-data)
                     (assoc query-opts
                            :start key-data)
                     (assoc query-opts
                            :after key-data))

        fetch-page-fn (fn []
                        (hooks.index-api/observe-fetch-start-collection-page
                         react-ctx
                         coll
                         key-alias
                         key-data
                         query-opts))]

    (log/debug ::load-refetch-start-collection-page
               {:coll-name coll-name
                :key-alias key-alias
                :key-data key-data
                :query-opts query-opts})

    (load-maybe-refetch-collection-page*
     react-ctx
     query-key
     ctx
     coll
     key-alias
     query-opts
     fetch-page-fn)))

(defn load-next-collection-page
  [react-ctx
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   {_limit :limit
    :as query-opts}
   last-record]

  ;; (log/info ::fetch-next-collection-page
  ;;           {:coll-name coll-name
  ;;            :key-alias key-alias
  ;;            :last-record last-record
  ;;            :obj? (object? last-record)})

  (p/let [keyspec (keys/get-keyspec coll key-alias)
          key-value (keys/extract-key keyspec last-record)
          query-opts (assoc
                      query-opts
                      :after key-value)

          fetch-page-fn (fn []
                          (hooks.index-api/observe-fetch-next-collection-page
                           react-ctx
                           coll
                           key-alias
                           key-data
                           query-opts
                           last-record))

          _ (log/debug ::load-next-collection-page
                       {:query-key query-key
                        :coll-name coll-name
                        :key-alias key-alias
                        :key-data key-data
                        :query-opts query-opts})]

    (load-maybe-refetch-collection-page*
     react-ctx
     query-key
     ctx
     coll
     key-alias
     query-opts
     fetch-page-fn)))

(defn load-previous-collection-page
  [react-ctx
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   {_limit :limit
    :as query-opts}
   first-record]
  (let [keyspec (keys/get-keyspec coll key-alias)
        key-value (keys/extract-key keyspec first-record)
        query-opts (assoc
                    query-opts
                    :before key-value)

        fetch-page-fn (fn []
                        (hooks.index-api/observe-fetch-previous-collection-page
                         react-ctx
                         coll
                         key-alias
                         key-data
                         query-opts
                         first-record))]

    (log/debug ::load-previous-collection-page
               {:query-key query-key
                :coll-name coll-name
                :key-alias key-alias
                :key-data key-data
                :query-opts query-opts})

    (load-maybe-refetch-collection-page*
     react-ctx
     query-key
     ctx
     coll
     key-alias
     query-opts
     fetch-page-fn)))

(defn load-collection-page-query
  "a react-query query-function to fetch a page of objects from a
   collection's local storage

   returns: Promise<[object*]>"
  [{db-name ::coll/db-name
    query-client ::coll/query-client
    :as react-ctx}
   {coll-name ::coll.md/name
    :as _coll}
   key-alias
   key-data
   {limit :limit
    :as query-opts}

   qfn-ctx]

  (p/let [ctx (coll.ctx/open-storage-context db-name)
          coll (coll.ctx/open-collection ctx coll-name)

          ;; default the limit to 10
          query-opts (merge
                      {:limit 10}
                      query-opts)

          query-key (oget qfn-ctx "queryKey")
          [page-dir
           current-page
           current-pages
           :as _page-param] (oget qfn-ctx "?pageParam")

          log-info {:coll-name coll-name
                    :key-alias key-alias
                    :key-data key-data
                    :limit limit
                    :query-key query-key
                    :page-dir page-dir
                    :current-page-count (count current-page)
                    :current-pages-count (count current-pages)}]

    ;; (log/debug ::load-collection-page log-info)

    ;; (log/info ::load-collection-page-qfn-ctx
    ;;           {:qfn-ctx-keys
    ;;            (keys
    ;;             (js->cljkw qfn-ctx))})

    (p/let [page (condp = page-dir
                   ;; just like previous, but force an API fetch
                   "start" (load-previous-collection-page
                            react-ctx
                            query-key
                            ctx
                            coll
                            key-alias
                            key-data
                            (assoc query-opts ::force-refetch? true)
                            (first current-page))

                   "previous" (load-previous-collection-page
                               react-ctx
                               query-key
                               ctx
                               coll
                               key-alias
                               key-data
                               query-opts
                               (first current-page))

                   "next" (load-next-collection-page
                           react-ctx
                           query-key
                           ctx
                           coll
                           key-alias
                           key-data
                           query-opts
                           (last current-page))

                   ;; just like next, but force an API fetch
                   "end" (load-next-collection-page
                          react-ctx
                          query-key
                          ctx
                          coll
                          key-alias
                          key-data
                          (assoc query-opts ::force-refetch? true)
                          (last current-page))

                   (load-start-collection-page
                    react-ctx
                    query-key
                    ctx
                    coll
                    key-alias
                    key-data
                    query-opts))]

      ;; update the query-cache for each object on the page
      (doseq [obj page]
        (rq/write-collection-object-cache query-client coll obj))

      (log/info ::load-collection-page-query-results
                (assoc
                 log-info
                 :retrieved-page-count (count page)))

      page)))


(defn load-collection-object-query
  [{db-name ::coll/db-name
    :as _react-ctx}
   coll-name
   obj-id]

  (log/debug ::load-collection-object-query
             {:coll-name coll-name
              :obj-or-obj-id obj-id})

  (p/let [ctx (coll.ctx/open-storage-context db-name)]

    (coll.objs/get-collection-object
     ctx
     coll-name
     obj-id)))
