(ns yapster.collections.hooks.index-api
  (:require
   [lambdaisland.glogi :as log]
   [yapster.collections :as-alias coll]
   [yapster.collections.metadata :as-alias coll.md]
   [yapster.collections.keys :as coll.keys]
   [yapster.collections.util.cljs :refer [js->cljkw]]
   [yapster.collections.hooks.fetch.multimethods :as fetch.mm]
   [yapster.collections.hooks.fetch.template-get-json]))

(defn observe-fetch-start-collection-page
  "observe a react-query for an API request for a
   start page of collection objects

   returns: Promise<[collection-object*]>"

  [{api-data ::coll/api-data
    :as react-ctx}
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   query-opts]

  (let [keyspec (coll.keys/get-keyspec coll key-alias)
        kdm (-> key-data (js->cljkw) (coll.keys/key-value-map keyspec))

        request-data (merge
                      api-data
                      {:key-data kdm
                       :query-opts query-opts})]

    (log/info
     ::observe-fetch-start-collection-page
     {:coll-name coll-name
      :key-alias key-alias
      :key-data kdm
      :query-opts query-opts})

    (fetch.mm/observe-fetch-page
     react-ctx
     coll
     key-alias
     request-data
     ::start-page
     {})))

(defn observe-fetch-next-collection-page
  "observe a react-query for an API request for a
   next page of collection objects

   returns: Promise<[collection-object*]>"
  [{api-data ::coll/api-data
    :as react-ctx}
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   last-record
   query-opts]

  (let [keyspec (coll.keys/get-keyspec coll key-alias)
        kdm (-> key-data (js->cljkw) (coll.keys/key-value-map keyspec))

        request-data (merge
                      api-data
                      {:key-data kdm
                       :query-opts query-opts
                       :last-record (js->cljkw last-record)})]

    (log/info
     ::observe-fetch-next-collection-page
     {:coll-name coll-name
      :key-alias key-alias
      :key-data kdm
      :query-opts query-opts})

    (fetch.mm/observe-fetch-page
     react-ctx
     coll
     key-alias
     request-data
     ::next-page
     {})))

(defn observe-fetch-previous-collection-page
  "observe a react-query for an API request for a
   previous page of collection objects

   returns: Promise<[collection-object*]>"

  [{api-data ::coll/api-data
    :as react-ctx}
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   key-data
   first-record
   query-opts]

  (let [keyspec (coll.keys/get-keyspec coll key-alias)
        kdm (-> key-data (js->cljkw) (coll.keys/key-value-map keyspec))

        request-data (merge
                      api-data
                      {:key-data kdm
                       :query-opts query-opts
                       :first-record (js->cljkw first-record)})]

    (log/info
     ::observe-fetch-previous-collection-page
     {:coll-name coll-name
      :key-alias key-alias
      :key-data kdm
      :query-opts query-opts})

    (fetch.mm/observe-fetch-page
     react-ctx
     coll
     key-alias
     request-data
     ::previous-page
     {})))
