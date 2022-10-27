(ns yapster.collections.hooks.fetch.template-get-json
  (:require
   [cljstache.core :as mustache]
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget+]]
   [promesa.core :as p]
   [yapster.collections :as-alias coll]
   [yapster.collections.hooks.fetch :as-alias hooks.fetch]
   [yapster.collections.hooks.index-api :as-alias hooks.index-api]
   [yapster.collections.hooks.react-query :as rq]
   [yapster.collections.metadata :as-alias coll.md]
   [yapster.collections.metadata.api :as-alias coll.md.api]
   [yapster.collections.hooks.fetch.multimethods :as fetch.mm]
   [yapster.collections.hooks.fetch.error-fetch :as error-fetch]))

(defn GET->JSON-page
  [react-ctx
   fetch-url
   fetch-opts
   response-path]
  (p/let [response (error-fetch/error-fetch react-ctx fetch-url fetch-opts)

          body-json (.json response)

          page (if (some? response-path)
                 (oget+ body-json response-path)
                 body-json)]
    ;; (log/trace
    ;;  ::GET->JSON-page
    ;;  {:fetch-url fetch-url
    ;;   :fetch-opts fetch-opts
    ;;   :page page})

    page))

(defn observe-fetch-page-template-GET->JSON
  "use fetch to perform a GET of a url expanded
   from a mustache template in def-collection metadata"
  [{query-client ::coll/query-client
    api-origin ::coll/api-origin
    api-headers ::coll/api-headers
    :as react-ctx}
   coll
   key-alias
   request-data
   page-type
   {_on-success :on-success
    _on-error :on-error
    :as observer-opts}]
  (let [{_handler ::coll.md.api/handler
         start-page-path-template ::coll.md.api/start-page-path-template
         next-page-path-template ::coll.md.api/next-page-path-template
         previous-page-path-template ::coll.md.api/previous-page-path-template
         response-path ::coll.md.api/response-value-path
         :as _api-config} (get-in coll [::coll.md/index-apis
                                        key-alias])]

    (p/catch
        (p/let [path-template (case page-type

                                ::hooks.index-api/start-page
                                start-page-path-template

                                ::hooks.index-api/next-page
                                next-page-path-template

                                ::hooks.index-api/previous-page
                                previous-page-path-template)

                url-path (mustache/render
                          path-template
                          request-data)

                fetch-opts {:method "GET"
                            :headers (merge
                                      api-headers
                                      {:Accept "application/json"})}

                fetch-url (str api-origin url-path)

                _ (log/debug
                   ::observe-fetch-page
                   {;; don't log very noisy stuff here
                    ;; :react-ctx react-ctx
                    ;; :api-config api-config
                    ;; :request-data request-data
                    :page-type page-type
                    :fetch-url fetch-url
                    :fetch-opts fetch-opts})

                r (rq/observe-uncached-query
                   query-client
                   ["GET->JSON-page" fetch-url]
                   (fn [& _args]
                     (GET->JSON-page react-ctx
                                     fetch-url
                                     fetch-opts
                                     response-path))
                   observer-opts)]

          (log/debug
           ::observe-fetch-page-result
           {:page-type page-type
            :fetch-url fetch-url
            :fetch-opts fetch-opts
            :count (count r)
            ;; :react-ctx react-ctx
            ;; :api-config api-config
            })

          r)

        (fn [e]
          (log/error
           ::observe-fetch-page-error
           {:react-ctx react-ctx
            :page-type page-type
            :request-data request-data
            :error e})

          (throw e)))))

(defmethod fetch.mm/observe-fetch-page ::hooks.fetch/template-GET->JSON
  [react-ctx
   coll
   key-alias
   request-data
   page-type
   observer-opts]

  (observe-fetch-page-template-GET->JSON
   react-ctx
   coll
   key-alias
   request-data
   page-type
   observer-opts))
