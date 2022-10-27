(ns yapster.collections.hooks.fetch.multimethods
  (:require
   [yapster.collections.hooks.fetch :as-alias hooks.fetch]
   [yapster.collections.metadata :as-alias coll.md]
   [yapster.collections.metadata.api :as-alias coll.md.api]))

(defmulti observe-fetch-page
  "use a request handler to make an API request for a page of data
   with react-query

   returns: Promise<value>"
  (fn [_react-ctx
      coll
      key-alias
      _request-data
      _page-type
      _observer-opts]
    (let [{handler ::coll.md.api/handler
           :as _api-config} (get-in coll [::coll.md/index-apis
                                          key-alias])]
      (or
       handler
       ::hooks.fetch/template-GET->JSON)))

  :default ::hooks.fetch/template-GET->JSON)
