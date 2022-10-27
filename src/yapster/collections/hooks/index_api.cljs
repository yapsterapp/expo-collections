(ns yapster.collections.hooks.index-api
  (:require
   [lambdaisland.glogi :as log]
   [yapster.collections :as-alias coll]
   [yapster.collections.metadata :as-alias coll.md]
   [yapster.collections.keys :as coll.keys]
   [yapster.collections.util.cljs :refer [js->cljkw]]
   [yapster.collections.hooks.fetch.multimethods :as fetch.mm]
   [yapster.collections.hooks.fetch.template-get-json]))

(defn key-data-map
  [key-data]
  (coll.keys/indexed-key-fields-map :k_ key-data))

(defn observe-fetch-start-collection-page
  "observe a react-query for an API request for a
   start page of collection objects

   returns: Promise<[collection-object*]>"
  ([react-ctx
    coll
    key-alias
    key-data
    query-opts]
   (observe-fetch-start-collection-page
    react-ctx
    coll
    key-alias
    key-data
    query-opts
    {}))

  ([{api-data ::coll/api-data
     :as react-ctx}
    {coll-name ::coll.md/name
     :as coll}
    key-alias
    key-data
    query-opts
    {_on-success :on-success
     _on-error :on-error
     :as observer-opts}]

   (let [kdm (-> key-data (js->cljkw) key-data-map)

         request-data (merge
                       api-data
                       {:key-data kdm
                        :query-opts query-opts})]

     (log/debug
      ::observe-fetch-start-collection-page
      {:coll-name coll-name
       :key-alias key-alias
       :key-data key-data
       :query-opts query-opts})

     (fetch.mm/observe-fetch-page
      react-ctx
      coll
      key-alias
      request-data
      ::start-page
      observer-opts))))

(defn observe-fetch-next-collection-page
  "observe a react-query for an API request for a
   next page of collection objects

   returns: Promise<[collection-object*]>"
  ([react-ctx
    coll
    key-alias
    key-data
    query-opts
    last-record]
   (observe-fetch-next-collection-page
    react-ctx
    coll
    key-alias
    key-data
    query-opts
    last-record
    {}))

  ([{api-data ::coll/api-data
     :as react-ctx}
    {coll-name ::coll.md/name
     :as coll}
    key-alias
    key-data
    query-opts
    last-record
    {_on-success :on-success
     _on-error :on-error
     :as observer-opts}]

   (let [kdm (-> key-data (js->cljkw) key-data-map)
         last-record (js->cljkw last-record)

         request-data (merge
                       api-data
                       {:key-data kdm
                        :query-opts query-opts
                        :last-record last-record})]

     (log/debug
      ::observe-fetch-next-collection-page
      {:coll-name coll-name
       :key-alias key-alias
       :key-data key-data
       :query-opts query-opts})

     (fetch.mm/observe-fetch-page
      react-ctx
      coll
      key-alias
      request-data
      ::next-page
      observer-opts))))

(defn observe-fetch-previous-collection-page
  "observe a react-query for an API request for a
   previous page of collection objects

   returns: Promise<[collection-object*]>"
  ([react-ctx
    coll
    key-alias
    key-data
    query-opts
    first-record]
   (observe-fetch-previous-collection-page
    react-ctx
    coll
    key-alias
    key-data
    query-opts
    first-record
    {}))

  ([{api-data ::coll/api-data
     :as react-ctx}
    {coll-name ::coll.md/name
     :as coll}
    key-alias
    key-data
    query-opts
    first-record
    {_on-success :on-success
     _on-error :on-error
     :as observer-opts}]

   (let [kdm (-> key-data (js->cljkw) key-data-map)
         first-record (js->cljkw first-record)

         request-data (merge
                       api-data
                       {:key-data kdm
                        :query-opts query-opts
                        :first-record first-record})]

     (log/debug
      ::observe-fetch-previous-collection-page
      {:coll-name coll-name
       :key-alias key-alias
       :key-data key-data
       :query-opts query-opts})

     (fetch.mm/observe-fetch-page
      react-ctx
      coll
      key-alias
      request-data
      ::previous-page
      observer-opts))))
