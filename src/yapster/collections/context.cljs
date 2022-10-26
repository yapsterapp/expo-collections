(ns yapster.collections.context
  (:require
   [lambdaisland.glogi :as log]
   [promesa.core :as p]
   [yapster.collections.metadata :as coll.metadata]
   [yapster.collections.context.multimethods :as mm]
   [yapster.collections.context.transactions]
   [yapster.collections.context.open-contexts :as open-contexts]
   [yapster.collections.context.util :as util]))

(def storage-context-impls [:yapster.collections.context/sqlite])

(defn loop-contexts
  "loop through the possible implementations calling f with
   the impl key until we get one that works (returns non-nil)

   returns: Promise<nil | first non-nil (f <storage-context-impl-key>)>"
  [f]
  (p/loop [[ctx-impl & others] storage-context-impls]
    (p/let [r (f ctx-impl)]
      (cond
        (some? r) r
        (not-empty others) (p/recur others)
        :else nil))))

(defn open-active-storage-context*
  "loop through the possible implementations until we get one that works"
  [db-name]
  (loop-contexts
   (fn [ctx-key] (mm/-open-storage-context ctx-key db-name))))

(defn check-or-open-active-storage-context*
  [db-name open-ctx]
  (p/let [checked-ctx (when (some? open-ctx)
                        (mm/-check-storage-context open-ctx))]
    (if (some? checked-ctx)
      checked-ctx
      (open-active-storage-context* db-name))))

(defn open-storage-context
  "open a platform-appropriate CollectionStorageContext for a given
   db-name

   returns Promise<db-name>"
  [db-name]

  (let [db-name (util/sanitise-name db-name)]

    (log/debug ::open-storage-context db-name)

    (p/let [open-ctx (open-contexts/get-storage-context db-name)
            ctx (check-or-open-active-storage-context*
                 db-name
                 open-ctx)]

      (when (not= ctx open-ctx)
        (open-contexts/set-storage-context db-name ctx))

      ctx)))

(defn delete-active-storage-context*
  [db-name]
  (loop-contexts
   (fn [ctx-key] (mm/-delete-storage-context ctx-key db-name))))

(defn delete-storage-context
  [db-name]
  (let [db-name (util/sanitise-name db-name)]

    (log/debug ::delete-storage-context db-name)

    (p/let [_ (delete-active-storage-context* db-name)]

      (open-contexts/set-storage-context db-name nil)

      true)))

(defn open-collection*
  [ctx coll-name]

  (log/debug ::open-collection {:coll-name coll-name})

  (p/let [rmd (coll.metadata/strict-get-collection-metadata coll-name)
          new-smd (coll.metadata/stored-collection-metadata rmd)
          prev-smd (mm/-load-collection-metadata ctx coll-name)

          ;;
          reset-collection? (and (some? prev-smd)
                                 (not= prev-smd new-smd))

          create-collection? (or (nil? prev-smd)
                                 reset-collection?)

          ;; if the collection has changed, discard its data
          _ (when reset-collection?
              (log/info ::collection-metadata-changed {:coll-name coll-name})
              (mm/-drop-collection ctx prev-smd))

          _ (when create-collection?
              (log/info ::create-collection {:coll-name coll-name})
              (mm/-create-collection ctx rmd))

          _ (when create-collection?
              (mm/-store-collection-metadata ctx new-smd))]

    rmd))

(defn open-collection
  "open a collection on a storage-context - which makes the collection
   ready for query and update

   returns: Promise<collection-metadata>

   the collection must already have been defined with def-collection. if
   the collection metadata has changed since the collection was last opened then
   any cached data will be discarded, and resources re-created"
  [{db-name :yapster.collections.context.sqlite/db-name
    :as ctx}
   coll-name]
  (p/let [oc (open-contexts/get-open-collection db-name coll-name)]
    (if (some? oc)
      oc

      (p/let [nc (open-collection* ctx coll-name)]
        (open-contexts/set-open-collection db-name coll-name nc)
        nc))))
