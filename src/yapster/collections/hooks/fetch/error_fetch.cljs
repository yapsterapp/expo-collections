(ns yapster.collections.hooks.fetch.error-fetch
  (:require
   [oops.core :refer [oget]]
   [promesa.core :as p]
   [yapster.collections :as-alias coll]))

(defn error-fetch
  "execute a fetch API request, and transform failures
   into throws"
  [{_query-client ::coll/query-client
    _api-origin ::coll/api-origin
    {_auth-info :auth-info
     :as _api-data} ::coll/api-data
    :as _react-ctx}
   url
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
