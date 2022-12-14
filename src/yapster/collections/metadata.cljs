(ns yapster.collections.metadata
  (:require
   [clojure.set :as set]
   [malli.experimental :as mx]
   [yapster.collections.schema :as coll.schema]
   [yapster.collections.metadata.key-component :as-alias coll.md.kc]))

;; global map of
;; {<collection-name> <collection-metadata>}
(defonce collections-a (atom {}))

(defn key-component-extractors
  "return a map of

   {<key-component-name> [[<extractor> {::key-name <key-name>}]]}

   which will permit checking that no <key-component-name> is
   associated with conflicting extractors"
  [{key-specs :yapster.collections.metadata/key-specs
    :as _coll-md}]

  (reduce
   (fn [m [kname kspec]]
     (reduce
      (fn [m kcspec]
        (cond
          (keyword? kcspec)
          (update m kcspec (fnil conj []) [kcspec
                                           {::key-name kname}])

          :else
          (let [[kcname
                 {extractor ::coll.md.kc/extractor
                  :as _kcopts}] kcspec]
            (update m kcname (fnil conj []) [(or extractor kcname)
                                             {::key-name kname}]))))
      m
      kspec))
   {}
   key-specs))

(defn check-keyspecs
  "check that key-components declared with the same name have
   identical extractors (i.e. represent the same data from the
   object)"
  [coll-md]

  (let [kc-extractors (key-component-extractors coll-md)

        check-kc-exs #(<= (->> % (map first) (distinct) (count)) 1)

        kc-errs (->> (reduce
                      (fn [r [kcname kc-exs]]
                        (if (check-kc-exs kc-exs)
                          r
                          (assoc r kcname kc-exs)))
                      {}
                      kc-extractors))]

    (when (not-empty kc-errs)
      (throw (ex-info
              "key components with same name *must* have same extractor"
              kc-errs)))))

(defn check-metadata
  "some high-level checks which are hard to
   implement with malli"
  [{key-specs :yapster.collections.metadata/key-specs
    primary-key :yapster.collections.metadata/primary-key
    index-keys :yapster.collections.metadata/index-keys
    :as coll-md}]

  (when-not (contains? key-specs primary-key)
    (throw (ex-info "primary-key is not present in key-specs" coll-md)))

  (when-not (every? #(contains? key-specs %) index-keys)
    (throw (ex-info "index-keys are not all present in key-specs" coll-md)))

  (check-keyspecs coll-md)

  coll-md)

;; define collection metadata
;;
;; defining the metadata is the only thing that should be required
;; to enable a collection to be used. all storage resources will be
;; automatically created and maintained. if the collection schema should
;; change, this will result in any locally stored data being discarded

(mx/defn def-collection
  [name :- :string
   opts :- coll.schema/DefCollectionMetadata]

  (let [md (assoc opts
                  ::version coll.schema/collections-metadata-version
                  ::name name)]

    (check-metadata md)

    (swap!
     collections-a
     assoc
     name
     md)

    md))

(defn get-collection-metadata
  [name]
  (get @collections-a name))

(defn strict-get-collection-metadata
  [name]
  (let [md (get-collection-metadata name)]
    (if (some? md)
      md
      (throw (ex-info "no collection metadata registered" {:name name})))))

(defn stored-collection-metadata
  "filter collection metadata to the fields to be stored"
  [metadata]
  (select-keys metadata coll.schema/stored-collection-metadata-keys))

(defn ^:private metadata-changes
  "returns a map of changed keys ...

   any additions and modifications will have the metadata from
   the <new-metadata>. any stale keys will have ::stale metadata
   "
  [old-metadata new-metadata]
  (let [old-keys-s (-> old-metadata keys set)
        new-keys-s (-> new-metadata keys set)

        new-keys (set/difference new-keys-s old-keys-s)
        stale-keys (set/difference old-keys-s new-keys-s)
        common-keys (set/intersection old-keys-s new-keys-s)

        new-key-changes (for [k new-keys] [k (get new-metadata k)])
        stale-key-changes (for [k stale-keys] [k ::stale])
        common-key-changes (filter
                            some?
                            (for [k common-keys]
                              (let [o (get old-metadata k)
                                    n (get new-metadata k)]
                                (when (not= o n) [k n]))))]
    (reduce
     (fn [m changes]
       (into m changes))
     {}
     [new-key-changes
      stale-key-changes
      common-key-changes])))

(defn watch-collections
  "watch the collections metadata with a callback f, which
   will be invoked when any metadata changes with

   (f <all-prev-metadata> <all-changes>)

   - <all-prev-metadata> - complete metadata for all collections,
                           prior to changes
   - <all-changes> - complete metadata, post change,
                     only for changed collections"
  [f]
  (let [k (gensym :yapster.collections.metadata/watch-collections-)]
    (add-watch
     collections-a
     k
     (fn [_k _ref old-state new-state]
       (let [changes (metadata-changes old-state new-state)]
         (f old-state changes))))))
