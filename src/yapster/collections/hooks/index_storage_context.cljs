(ns yapster.collections.hooks.index-storage-context
  (:require
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget]]
   [promesa.core :as p]
   [yapster.collections :as-alias coll]
   [yapster.collections.keys :as keys]
   [yapster.collections.context :as coll.ctx]
   [yapster.collections.util.time :as util.t]
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
    _limit :limit
    after-key :after
    before-key :before
    start-key :start
    stale-threshold-s :stale-threshold-s
    :as query-opts
    :or {stale-threshold-s (* 24 3600)}}
   fetch-page-fn]

  (p/let [now (util.t/now)

          ;; record is stale if it was last updated before
          ;; stale-threshold-s seconds ago
          stale-threshold-t (util.t/add-t now :seconds (- stale-threshold-s))

          annotate-stale? (fn [{r ::coll.idxs/record}]
                            (let [upd-t (util.t/->inst
                                         (oget r "updated_at"))]
                              (log/trace ::stale?
                                        {:upd-t upd-t
                                         :stale-threshold-t stale-threshold-t})
                              (< upd-t stale-threshold-t)))

          r-idx-objs (coll.idxs/get-index-objects-page
                      ctx
                      coll-name
                      key-alias
                      query-opts)

          ;; remove any results which overlap with the previous page
          remove-key (or after-key before-key)
          keyspec (keys/get-keyspec coll key-alias)

          ;; these are the results for the UI
          idx-objs (->> r-idx-objs
                        (map coll.objs/extract-data)
                        (remove
                         #(= remove-key (keys/extract-key keyspec %))))

          ;; here we annotate index-object records with attributes useful
          ;; for determining what (if anything) we need to refetch
          ann-idx-objs (->> r-idx-objs
                            (coll.idxs/page->annotated-page)
                            (coll.idxs/annotate-page
                             ::continuous?
                             (coll.idxs/make-annotate-continuous? coll))
                            (coll.idxs/annotate-page
                             ::stale?
                             annotate-stale?))

          ;; _ (log/info ::load-collection-page-idx-objects {})

          ;; is an annotated index record stale or discontinuous?
          requires-refetch? (fn [{stale? ::stale?
                                 continuous? ::continuous?
                                 record ::coll.idxs/record
                                 :as _ann-idx-obj}]

                              (cond

                                (not continuous?)
                                [::discontinuous record nil]

                                stale?
                                [::stale record nil]))

          [refetch-reason
           refetch-from-index-record
           refetch-from-key] (cond

                               ;; if forced, duplicate the original fetch
                               (and force-refetch? (some? start-key))
                               [::forced nil nil]

                               (and force-refetch? (some? before-key))
                               [::forced (first r-idx-objs) before-key]

                               (and force-refetch? (some? after-key))
                               [::forced (last r-idx-objs) after-key]

                               ;; refetch from the first stale or
                               ;; discontinuous record, if any
                               :else
                               (some requires-refetch?
                                     (if (some? before-key)
                                       (reverse ann-idx-objs)
                                       ann-idx-objs)))

          ;; the annotated records are index-objects, not raw client objects
          refetch-from-record (when (some? refetch-from-index-record)
                                (coll.objs/extract-data refetch-from-index-record))

          ;; use a record to get the key, if we have one... fall back
          ;; to an after/before key provided to use-collection-index
          refetch-from-key (if (some? refetch-from-record)
                             (keys/extract-key keyspec refetch-from-record)
                             refetch-from-key)

          ;; we don't refetch when short (i.e. fewer records than requested)...
          ;; that leads to thrashing the API when the remote
          ;; collection is really short or empty...
          ;; instead we have a useEffect hook called from
          ;; the use-collection-index hook which forces an
          ;; initial API refresh, and the fetchStartPage
          ;; and fetchEndPage additions to the use-infinite-query
          ;; which also force API refreshes
          ;;
          refetch? (some? refetch-reason)]

    ;; this is probably the core collections logging ... you can see
    ;; a local page was loaded, and why refetch decisions were made
    (log/info ::load-maybe-refetch-collection-page*
              {:coll-name coll-name
               :key-alias key-alias
               :query-opts query-opts
               :retrieved-count (count r-idx-objs)
               :filtered-count (count idx-objs)
               :now now
               :stale-threshold-t stale-threshold-t
               :force-refetch? force-refetch?
               :refetch? refetch?
               :refetch-reason refetch-reason
               ;; :refetch-from-record refetch-from-record
               :refetch-from-key refetch-from-key
               ;; happens with a forced refetch, after or before
               ;; and no local data - i.e. a deep-link
               :refetch-from-record-nil? (nil? refetch-from-record)})

    (when refetch?
      (cond

        (some? refetch-from-key)
        (refetch-page*
         react-ctx
         query-key
         ctx
         coll
         key-alias
         query-opts
         idx-objs
         (partial fetch-page-fn
                  refetch-from-key
                  refetch-from-record))

        ;; only refetch from the start if forced...
        (and force-refetch?
             (some? start-key))
        (refetch-page*
         react-ctx
         query-key
         ctx
         coll
         key-alias
         query-opts
         idx-objs
         ;; start refetch fn takes no extra params
         fetch-page-fn))

      ;; promesa waits on each step in the *body* of a let too...
      ;; but we don't want that here, so 'lose' the refetch-page
      ;; result promise
      true)

    ;; better to return an empty array than a nil because
    ;; goog.array/concat seems to treat nils as [nil], which leads
    ;; to undesired outcomes when pages are concatenated
    (clj->js
     idx-objs)))

(defn load-start-collection-page
  [react-ctx
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   start-key-data
   {rq-limit :limit
    :or {rq-limit 20}
    :as query-opts}]
  (let [query-opts (assoc query-opts
                          :limit rq-limit
                          :start start-key-data)

        fetch-page-fn (fn [key-data last-record]
                        (if (some? key-data)

                          ;; if we get called with key-data then it's because
                          ;; we're on a start page, with an API that returns
                          ;; fewer records than the page, and there are
                          ;; discontinuities or stale records meaning we need
                          ;; to refresh from part-way down the page - it's
                          ;; always a next-page query
                          (hooks.index-api/observe-fetch-next-collection-page
                           react-ctx
                           coll
                           key-alias
                           key-data
                           last-record
                           query-opts)

                          (hooks.index-api/observe-fetch-start-collection-page
                           react-ctx
                           coll
                           key-alias
                           start-key-data
                           query-opts)))]

    (log/debug ::load-start-collection-page
               {:coll-name coll-name
                :key-alias key-alias
                :start-key-data start-key-data
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
   coll
   key-alias
   start-key-data
   query-opts]

  (load-start-collection-page
   react-ctx
   query-key
   ctx
   coll
   key-alias
   start-key-data
   (assoc query-opts ::force-refetch? true)))

(defn load-next-collection-page
  [react-ctx
   query-key
   ctx
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   _last-record
   {rq-limit :limit
    :or {rq-limit 20}
    :as query-opts}
   ]

  ;; (log/info ::fetch-next-collection-page
  ;;           {:coll-name coll-name
  ;;            :key-alias key-alias
  ;;            :last-record last-record
  ;;            :obj? (object? last-record)})

  (p/let [query-opts (assoc
                      query-opts
                      ;; inc limit to account for overlap record
                      :limit (inc rq-limit)
                      :after key-data)

          fetch-page-fn (fn [key-data last-record]
                          (hooks.index-api/observe-fetch-next-collection-page
                           react-ctx
                           coll
                           key-alias
                           key-data
                           last-record
                           query-opts))

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
   _first-record
   {rq-limit :limit
    :or {rq-limit 20}
    :as query-opts}]
  (let [query-opts (assoc
                    query-opts
                    ;; inc limit to account for overlap record
                    :limit (inc rq-limit)
                    :before key-data)

        fetch-page-fn (fn [key-data first-record]
                        (hooks.index-api/observe-fetch-previous-collection-page
                         react-ctx
                         coll
                         key-alias
                         key-data
                         first-record
                         query-opts))]

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
  "a react-query use-infinite query-function to fetch a page of objects
   from a collection's local storage

   returns: Promise<[object*]>"
  [{db-name ::coll/db-name
    query-client ::coll/query-client
    :as react-ctx}
   {coll-name ::coll.md/name
    :as _coll}
   key-alias
   key-data
   query-opts

   qfn-ctx]

  (p/let [ctx (coll.ctx/open-storage-context db-name)
          coll (coll.ctx/open-collection ctx coll-name)

          ;; default the limit to 20
          query-opts (merge
                      {:limit 20}
                      query-opts)

          query-key (oget qfn-ctx "queryKey")
          [page-dir
           current-page
           current-pages
           :as _page-param] (oget qfn-ctx "?pageParam")

          log-info {:coll-name coll-name
                    :key-alias key-alias
                    :key-data key-data
                    :query-opts query-opts
                    :query-key query-key
                    :page-dir page-dir
                    :current-page-count (count current-page)
                    :current-pages-count (count current-pages)}

          keyspec (keys/get-keyspec coll key-alias)
          extract-key-value #(keys/extract-key keyspec %)]

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
                            (-> current-page first extract-key-value)
                            (first current-page)
                            (assoc query-opts ::force-refetch? true))

                   "previous" (load-previous-collection-page
                               react-ctx
                               query-key
                               ctx
                               coll
                               key-alias
                               (-> current-page first extract-key-value)
                               (first current-page)
                               query-opts)

                   "next" (load-next-collection-page
                           react-ctx
                           query-key
                           ctx
                           coll
                           key-alias
                           (-> current-page last extract-key-value)
                           (last current-page)
                           query-opts)

                   ;; just like next, but force an API fetch
                   "end" (load-next-collection-page
                          react-ctx
                          query-key
                          ctx
                          coll
                          key-alias
                          (-> current-page last extract-key-value)
                          (last current-page)
                          (assoc query-opts ::force-refetch? true))

                   (if (keys/end-key? keyspec key-data)

                     (load-start-collection-page
                      react-ctx
                      query-key
                      ctx
                      coll
                      key-alias
                      key-data
                      query-opts)

                     ;; TODO this one is interesting - a deep link must
                     ;; currently specify the index-key in key-data, but that's
                     ;; probably not great, because the index-key may not
                     ;; uniquely identify a record (both recent-conversations
                     ;; and conversations have this issue)
                     ;;
                     ;; so for deep links we really need a seed key with
                     ;; a primary-key, which can be used to fetch a single
                     ;; record and then start to fill out the collection either
                     ;; side of the seed
                     (load-next-collection-page
                      react-ctx
                      query-key
                      ctx
                      coll
                      key-alias
                      key-data
                      query-opts
                      nil)))]

      ;; update the query-cache for each object on the page
      (doseq [obj page]
        (rq/write-collection-object-cache query-client coll obj))

      (log/debug ::load-collection-page-query-results
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
