(ns yapster.collections.hooks.react-query
  "react-query wrapper functions, all
   in one place"
  (:require
   ["@tanstack/react-query" :as rq]
   [lambdaisland.glogi :as log]
   [promesa.core :as p]
   [oops.core :refer [oget ocall]]
   [yapster.collections.metadata :as coll.md]
   [yapster.collections.keys :as coll.keys]))

(defn use-query-client
  []
  (rq/useQueryClient))

(defn index-query-key
  "a key for a react-query useInfiniteQuery on a collection"
  [{coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data]

  ;; (log/info ::index-query-key
  ;;           {:coll coll
  ;;            :key-alias key-alias
  ;;            :key-data key-data})

  (let [keyspec (coll.keys/get-keyspec coll key-alias)
        key-comp-cnt (count keyspec)

        ;; remove any final key component
        key-data (-> key-data
                     vec
                     (subvec 0 (dec key-comp-cnt)))]

    (->> [coll-name
          ::index
          key-alias
          key-data]
         (flatten)
         (map str)
         (clj->js))))

(defn object-query-key
  "a key for a react-query useQuery for a collection object"
  [coll-name id]

   (->> [coll-name
         ::object
         id]
        (flatten)
        (map str)
        (clj->js)))

(defn write-collection-object-cache
  "write an object to a react-query collection-object cache"
  ([query-client
    {pk-alias ::coll.md/primary-key
     :as coll}
    obj]

   (let [pkspec (coll.keys/get-keyspec coll pk-alias)
         obj-id (coll.keys/extract-key pkspec obj)]
     (write-collection-object-cache query-client coll obj-id obj)))

  ([query-client
    {coll-name ::coll.md/name
     :as _coll}
    obj-id
    obj-val]

   ;; (log/info ::write-collection-object-cache {:obj-id obj-id
   ;;                                            :obj-val obj-val})

   (let [obj-key (object-query-key coll-name obj-id)]
     (ocall query-client "setQueryData" obj-key obj-val))))

(defn invalidate-queries
  [query-client
   query-key]
  (log/debug ::invalidate-queries query-key)
  (ocall
   query-client
   "invalidateQueries"
   (clj->js query-key)))

(defn observe-uncached-query
  "observe an uncached react-query, avoiding hooks ...

   we do this to get react-query benefits on top
   of a normal promise
    - network status sensitivity
    - retry

   we also do this so that we can
   trigger a query-invlidation from
   inside a query function, and have it
   run after the query function (on-success)
   - because there is borkage if query-invalidation
   is run inside a query function

   returns Promise<rq-result>
     - calls any on-success fn with the query data
     - calls any on-error fn with the error"
  ([query-client query-key query-fn]
   (observe-uncached-query query-client query-key query-fn {}))
  ([query-client
    query-key
    query-fn
    {on-success :on-success
     on-error :on-error}]

   (let [r-p (p/deferred)

         obs (new rq/QueryObserver
                  query-client
                  (clj->js {:queryKey query-key
                            :queryFn query-fn

                            ;; do not cache results - always
                            ;; wait for the query fn
                            :cacheTime 0}))

         unsubscribe-a (atom nil)]

     (reset!
      unsubscribe-a

      (ocall obs
             "subscribe"

             (fn [rq-status]

               (let [is-loading? (oget rq-status "isLoading")
                     is-success? (oget rq-status "isSuccess")
                     is-error? (oget rq-status "isError")]

                 ;; (log/trace ::observe-query-subscription
                 ;;            {:is-loading? is-loading?
                 ;;             :is-success? is-success?
                 ;;             :is-error? is-error?})

                 (cond
                   is-success?
                   (let [data (oget rq-status "data")]
                     (p/resolve!
                      r-p
                      data)
                     (when (some? @unsubscribe-a)
                       (@unsubscribe-a))
                     (when (some? on-success)
                       (on-success data)))

                   is-error?
                   (let [error (oget rq-status "error")]
                     (p/reject!
                      r-p
                      error)
                     (when (some? @unsubscribe-a)
                       (@unsubscribe-a))
                     (when (some? on-error)
                       (on-error error)))

                   is-loading?
                   true)))))
     r-p)))

(defn use-query
  [query-key
   query-fn
   opts]
  (rq/useQuery
   (clj->js query-key)
   query-fn
   (clj->js opts)))

(defn use-infinite-query
  [query-key
   query-fn
   opts]
  (rq/useInfiniteQuery
   (clj->js query-key)
   query-fn
   (clj->js opts)))
