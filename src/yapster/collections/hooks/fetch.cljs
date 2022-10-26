(ns yapster.collections.hooks.fetch
  (:require
   [cljstache.core :as mustache]
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget oget+]]
   [promesa.core :as p]
   [yapster.collections.keys :as coll.keys]
   [yapster.collections.hooks.react-query :as rq]
   [yapster.collections.util.cljs :refer [js->cljkw]]
   [yapster.collections.metadata :as-alias coll.md]
   [yapster.collections.metadata.api :as-alias coll.md.api]))

(defn fetch
  "execute a fetch API request, and transform failures
   into throws"
  [url
   opts]
  (p/let [r (js/fetch url (clj->js opts))
          r-status (oget r "?ok")]

    (if r-status

      r

      (throw
       (ex-info "HTTP error"
                {:url url
                 :opts opts
                 :response r})))))

(defn fetch-page*
  "js/fetch a collection page from the yapster API"
  [auth-info
   {coll-name ::coll.md/name
    :as coll}
   key-alias
   url-path]

  (p/let [{api-key :apiKey
           origin :origin
           :as _auth-info} (js->cljkw auth-info)

          {method ::coll.md.api/method
           response-path ::coll.md.api/response-value-path} (get-in
                                                             coll
                                                             [::coll.md/index-apis
                                                              key-alias])

          fetch-opts {:method (or method "GET")
                       :headers {:Content-Type "application/json"
                                 :Authorization (str "Token " api-key)}}

          fetch-url (str origin url-path)

          log-info {:coll-name coll-name
                    :key-alias key-alias
                    :fetch-url fetch-url
                    :fetch-opts fetch-opts}

          _ (log/debug ::fetch-page* log-info)

          response (fetch fetch-url fetch-opts)

          body-json (.json response)

          page (if (some? response-path)
                 (oget+ body-json response-path)
                 body-json)]

    (log/debug ::fetch-page*-response
               (assoc
                log-info
                :page-count (count page)))

    page))

(defn key-data-map
  [key-data]
  (coll.keys/indexed-key-fields-map :k_ key-data))

(defn observe-fetch-start-collection-page
  "observe a react-query for an API request for a
   start page of collection objects

   returns: Promise<[collection-object*]>"
  ([query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts]
   (observe-fetch-start-collection-page
    query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    {}))

  ([query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    {_on-success :on-success
     _on-error :on-error
     :as observer-opts}]

   (let [{origin :origin
          :as auth-info} (js->cljkw auth-info)

         path-template (get-in coll [::coll.md/index-apis
                                     key-alias
                                     ::coll.md.api/path-template])

         kdm (key-data-map key-data)

         url-path (mustache/render
                   path-template
                   {:auth-info auth-info
                    :key-data kdm
                    :query-opts query-opts})]

     (log/debug ::observe-fetch-start-collection-page
                {:auth-info auth-info
                 :key-data kdm
                 :key-data-map kdm
                 :query-opts query-opts
                 :path-template path-template
                 :url-path url-path})

     (rq/observe-query
      query-client
      [origin url-path]
      (fn [& _args]
        (fetch-page* auth-info coll key-alias url-path))
      observer-opts)

     (fetch-page* auth-info coll key-alias url-path))))

(defn observe-fetch-next-collection-page
  "observe a react-query for an API request for a
   next page of collection objects

   returns: Promise<[collection-object*]>"
  ([query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    last-record]
   (observe-fetch-next-collection-page
    query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    last-record
    {}))

  ([query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    last-record
    {_on-success :on-success
     _on-error :on-error
     :as observer-opts}]
   (let [{origin :origin
          :as auth-info} (js->cljkw auth-info)
         key-data (js->cljkw key-data)
         last-record (js->cljkw last-record)

         path-template (get-in coll [::coll.md/index-apis
                                     key-alias
                                     ::coll.md.api/next-page-path-template])

         kdm (key-data-map key-data)

         url-path (mustache/render
                   path-template
                   {:auth-info auth-info
                    :key-data kdm
                    :query-opts query-opts
                    :last-record last-record})]

     (log/debug ::observe-fetch-next-collection-page
                {:auth-info auth-info
                 :key-data key-data
                 :key-data-map kdm
                 :query-opts query-opts
                 :last-record last-record
                 :path-template path-template
                 :url-path url-path})

     (rq/observe-query
      query-client
      [origin url-path]
      (fn [& _args]
        (fetch-page* auth-info coll key-alias url-path))
      observer-opts))))

(defn observe-fetch-previous-collection-page
  "observe a react-query for an API request for a
   previous page of collection objects

   returns: Promise<[collection-object*]>"
  ([query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    first-record]
   (observe-fetch-previous-collection-page
    query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    first-record
    {}))

  ([query-client
    auth-info
    coll
    key-alias
    key-data
    query-opts
    first-record
    {_on-success :on-success
     _on-error :on-error
     :as observer-opts}]
   (let [{origin :origin
          :as auth-info} (js->cljkw auth-info)
         key-data (js->cljkw key-data)
         first-record (js->cljkw first-record)

         path-template (get-in coll [::coll.md/index-apis
                                     key-alias
                                     ::coll.md.api/previous-page-path-template])
         kdm (key-data-map key-data)

         url-path (mustache/render
                   path-template
                   {:auth-info auth-info
                    :key-data kdm
                    :query-opts query-opts
                    :first-record first-record})]

     (log/debug ::observe-fetch-previous-collection-page
                {:auth-info auth-info
                 :key-data key-data
                 :key-data-map kdm
                 :query-opts query-opts
                 :last-record first-record
                 :path-template path-template
                 :url-path url-path})

     (rq/observe-query
      query-client
      [origin url-path]
      (fn [& _args]
        (fetch-page* auth-info coll key-alias url-path))
      observer-opts))))
