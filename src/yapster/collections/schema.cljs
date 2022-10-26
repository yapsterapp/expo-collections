(ns yapster.collections.schema
  (:require
   [malli.core :as m]
   [malli.util :as mu]))

(def collections-metadata-version
  "increment this version to cause all storage resources to be
   dropped and recreated - such as may be required when the
   table format is changed (but the collection value-schemas
   are unchanged)"
  0)

(def CollectionItemKeyComponentOpts
  [:map
   [:yapster.collections.metadata.key-component/sort-order
    {:optional true}
    [:or
     [:= :yapster.collections.metadata.key-component.sort-order/asc]
     [:= :yapster.collections.metadata.key-component.sort-order/desc]]]])

(def CollectionItemKeyComponentSpec
  "a key component is defined by a keyword and, optionally,
   a map of options. the keyword defines how to extract a
   value from a record, with the namespace defining a path
   through nested maps and the name a field"
  [:or
   :keyword
   [:tuple :keyword]
   [:tuple :keyword CollectionItemKeyComponentOpts]])

(def CollectionItemKeySpec
  "defines a multi-component key on a collection. each component
   consists of a keyword, defining how the component value can
   be extracted from an object, and, optionally, some options
   defining such things as the sort-order for that component in the key"
   ;; the key is a composite of attributes. namespaced keywords can be
   ;; used to safely descend a hierarchy
  [:vector CollectionItemKeyComponentSpec])

(def stored-collection-metadata-keys
  "these keys of the CollectionMetadata will be persisted to a
   collection storage context. any change in the values of these
   keys for a collection will result in the collection storage
   for that collection being dropped and re-populated from the
   network"
  #{:yapster.collections.metadata/version
    :yapster.collections.metadata/name
    :yapster.collections.metadata/value-schema
    :yapster.collections.metadata/key-specs
    :yapster.collections.metadata/primary-key
    :yapster.collections.metadata/index-keys})

(def KeyAlias
  :keyword)

(def CollectionIndexApiSpec
  [:map
   [:yapster.collection.metadata.api/path-template :string]
   [:yapster.collection.metadata.api/next-page-path-template :string]
   [:yapster.collection.metadata.api/previous-page-path-template :string]
   [:yapster.collection.metadata.api/response-value-path :string]])

(def CollectionMetadata
  [:map
   [:yapster.collections.metadata/version {:optional true} :int]
   [:yapster.collections.metadata/name :string]
   [:yapster.collections.metadata/value-schema some?]

   ;; give each key-spec an alias
   [:yapster.collections.metadata/key-specs
    [:map-of KeyAlias CollectionItemKeySpec]]


   ;; the primary key is required
   [:yapster.collections.metadata/primary-key KeyAlias]

   ;; index keys are optional
   [:yapster.collections.metadata/index-keys
    {:optional true}
    [:* KeyAlias]]


   [:yapster.collections.metadata/index-apis
    [:map-of KeyAlias CollectionIndexApiSpec]]])

(def DefCollectionMetadata
  (-> CollectionMetadata
      (mu/dissoc :yapster.collections.metadata/name)
      (mu/dissoc :yapster.collections.metadata/version)
      (m/form)))

(def StoredCollectionMetadata
  (-> CollectionMetadata
      (mu/select-keys stored-collection-metadata-keys)
      (m/form)))

(def CollectionStorageContext
  [:map
   [:yapster.collections.context/impl :keyword]])

(def SQLiteCollectionStorageContext
  (mu/merge
   CollectionStorageContext
   [:map
    [:yapster.collections.context/impl [:= :yapster.collections.context/sqlite]]
    [:yapster.collections.context.sqlite/db-name :string]
    [:yapster.collections.context.sqlite/db some?]]))
