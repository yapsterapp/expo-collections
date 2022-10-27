(ns yapster.collections
  (:require
   [yapster.collections.schema]
   [yapster.collections.metadata :as metadata]
   [yapster.collections.keys]
   [yapster.collections.context]
   [yapster.collections.context.sqlite]
   [yapster.collections.context.indexeddb]
   [yapster.collections.objects]
   [yapster.collections.indexes]
   [yapster.collections.hooks :as hooks]

   [yapster.collections.keys-test]
   [yapster.collections.indexes-test]))

(def def-collection
  "define a collection"
  metadata/def-collection)



(def use-collection-index
  "a hook to return a react-query infinite query on a colletion"
  hooks/use-collection-index)

(def use-invalidate-collection-index-fn
  "a hook returning a fn to invalidate a collection"
  hooks/use-invalidate-collection-index-fn)

(def use-refetch-collection-index-start-fn
  "a hook returning a fn to force an API fetch of the
   start of a collection (and invalidate, causing a UI refresh)"
  hooks/use-refetch-collection-index-start-fn)



(def use-collection-index-object
  "a hook returning a react-query on an object from a collection index"
  hooks/use-collection-index-object)

(def use-invalidate-collection-index-object-fn
  "a hook returning a fn to invalidate a collection index object"
  hooks/use-invalidate-collection-index-object-fn)



(def use-update-object-and-indexes-fn
  "a hook returning a fn to update an object and its indexes"
  hooks/use-update-object-and-indexes-fn)
