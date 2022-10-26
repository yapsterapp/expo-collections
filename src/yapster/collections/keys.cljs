(ns yapster.collections.keys
  (:require
   [clojure.string :as string]
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget+]]
   [yapster.collections.metadata :as-alias coll.md]
   [yapster.collections.metadata.key-component :as-alias coll.md.kc]
   [yapster.collections.metadata.key-component.sort-order :as-alias coll.md.kc.so]))

;; some utils for working with keyspecs

(defn get-keyspec
  "given a key alias, get the keyspec from
   the collection metadata

   coll : collection metadata
   key-alias : a key alias keyword

   returns : the keyspec, or throws an error"
  [coll key-alias]
  (let [ks (get-in coll [::coll.md/key-specs
                         key-alias])]
    (when (nil? ks)
      (throw (ex-info "missing keyspec"
                      {:coll coll
                       :key-alias key-alias})))
    ks))

(defn key-component-sort-order
  "return the sort-order for a key component"
  [[_kex
    {sort-order ::coll.md.kc/sort-order}
    :as _key-component-spec]]
  (or
   sort-order
   ::coll.md.kc.so/asc))

(defn normalize-key-component-spec
  "convert a plain :keyword keyspec to
   [:keyword opts] form"
  [kcspec]
  (if (keyword? kcspec)
    [kcspec {}]
    kcspec))

(defn normalize-keyspec
  "convert to [[:key-extractor-keyword opts]+] form"
  [kspec]
  (mapv normalize-key-component-spec kspec ))

(defn normalize-key-value
  "ensure a key-value is a sequence"
  [key-value]
  (if (sequential? key-value) (vec key-value) [key-value]))

(defn indexed-key-fields
  "returns a vector of indexed key fields for a given keyspec...

   - prefix: prefix of each key component
   - returns:  list like [:id_0 :id_1 ...]"
  [prefix keyspec]
  (->> keyspec
       (normalize-keyspec)
       (map-indexed
        (fn [idx _ksc] (keyword (str (name prefix) idx))))
       (into [])))

(defn indexed-key-fields-map
  "returns a map of values of indexed key fields...

  {<key-component-field> <key-component-value>}

   - prefix: prefix of the key components
   - returns: map like:
       {:id_0 <c0>
        :id_1 <c1>}"
  [prefix keyval]
  (->> keyval
       (normalize-key-value)
       (map-indexed
        (fn [idx kcv]
          [(keyword (str (name prefix) idx))
           kcv]))
       (into {})))

(defn full-key?
  "returns true if the keyval is a full value for the spec"
  [keyspec keyval]
  (= (count keyspec) (count keyval)))

(defn end-key?
  "returns true if the keyval is a partial value for an end of a sorted range

   since we only support sorting on the final component of a key, this just
   checks that the value has one fewer component than the keyspec"
  [keyspec keyval]
  (= (dec (count keyspec)) (count keyval)))

(defn check-use-collection-index-init-key
  "a use-collection-index can be initialised with either a full-key (from
   a link to a particular item or place in an index), or an end-key
   (specifying an end of the collection)"
  [keyspec keyval]
  (or (full-key? keyspec keyval)
      (end-key? keyspec keyval)))

(defn denamespace-keyword
  "sanitise a namespaced keyword for use with cljstache templates ...
   :foo -> :foo
   :a.b.foo -> :a_b__foo"
  [kw]
  (let [[ns n] [(namespace kw) (name kw)]
        ns' (some-> ns (string/replace #"\." "_"))]
    (->> [ns' n]
         (filter some?)
         (string/join "__")
         (keyword))))

(defn mapify-key-value
  "returns a map of a key-value with each component
   keyed by its **denamespaced** key-extractor key-value

   this is useful for supplying a key as data for a
   mustache template"
  [keyspec key-value]
  (let [keyspec (normalize-keyspec keyspec)]
    (into {}
          (map (fn [[kex _opts] v]
                 [(denamespace-keyword kex) v])
               keyspec
               key-value))))

;; extraction of key values according to a keyspec

(defn kw->path
  "take a potentially namespaced keyword turn it
   into a path sequence

   :foo -> [\"foo\"]
   :foo.baz/bar -> [\"foo\" \"baz\" \"bar\"]"
  [kw]
  (let [kwns (some-> kw namespace (string/split "."))
        kwn (name kw)]
    (conj
     (or kwns [])
     kwn)))

(defn safe-get-key-component
  [kex obj]
  (let [path (kw->path kex)]
    (reduce
     (fn [r p]
       (if (some? r)
         (oget+ r p)
         (reduced nil)))
     obj
     path)))

;; keyspec is vector of key-component specs,
;; which are either simple keywords or
;; [kex opts] vectors
(defn extract-key*
  [keyspec obj]
  (let [keyspec (normalize-keyspec keyspec)
        ckv (for [[kex _opts] keyspec]
              (safe-get-key-component kex obj))]
    ;; composite keys many not have any nil
    ;; components
    (if (some nil? ckv)
      nil
      (vec ckv))))

(defn valid-key-element?
  "only scalar values are valid elements of a key"
  [v]
  (or
   (string? v)
   (number? v)
   (boolean? v)
   (inst? v)))

(defn check-valid-key?
  "check every element of key is valid"
  [keyspec obj kv]
  (let [r (every? valid-key-element? kv)]
    (if r
      kv
      (do
        (log/error ::valid-key? {:keyspec keyspec
                                 :obj obj
                                 :kv kv})
        nil))))

(defn extract-key
  "extract a key from a js object

   returns a vector of non-nil scalar values, or nil

   - keyspec is a cljs specification for the key
   - coll : collection metadata
   - key-alias : a keyword alias for a key in collection metadata

   a namespace of a keyword in a keyspec is used to safely descend
   a hierarchy of nested objects"
  ([keyspec obj]
   (let [kv (extract-key* keyspec obj)]
     (check-valid-key? keyspec obj kv)))
  ([coll key-alias obj]
   (let [keyspec (get-keyspec coll key-alias)]
     (extract-key keyspec obj))))

(defn ^:private merge-key-sorted-lists*
  "merge a list of key-sorted lists of cljs maps. both lists
   must have they sort key at the same map entry

   - sort-key-kw : the keyword name of the sort-keys in both maps
   - cmp : comparator fn for sort-key-kw map values
   - r : the results vector
   - lists : a list of maps sorted-by sort-key-kw with cmp"
  [sort-key-kw cmp r lists]
  ;; (prn :merge-key-sorted-lists*-start r lists)

  (let [lists (filter not-empty lists)]
    (if (empty? lists)
      ;; we're done
      r

      (let [;; first find the smallest key at the head of any of the lists
            min-key (->> lists
                         (map first)
                         (map sort-key-kw)
                         (filter some?)
                         (reduce
                          (fn [r v]
                            (if (<= (cmp r v) 0)
                              r
                              v))))

            ;; then find all head records which match that key, and take
            ;; those records from their lists for merge. do some checks
            ;; for things which will mess up our lives:
            ;; - nil records
            ;; - nil sort-key field on records
            ;; - lists not correctly sorted by cmp on value at sort-key-kw
            mrg-nxtlists (->> lists
                              (map (fn [l]

                                     (let [{lfrk sort-key-kw
                                            :as lfr} (first l)]

                                       (cond

                                         (nil? lfr)
                                         (throw
                                          (ex-info
                                           "list has nil record" {:list l}))

                                         (nil? lfrk)
                                         (throw
                                          (ex-info
                                           "list has nil key"
                                           {:list l
                                            :sort-key-kw sort-key-kw })))

                                       (if (= min-key lfrk)
                                         (let [nl (next l)
                                               {nlfrk sort-key-kw
                                                :as nlfr} (first nl)]

                                           (when (some? nl) ;; not end of list
                                             (cond

                                               (nil? nlfr)
                                               (throw
                                                (ex-info
                                                 "list has nil record" {:list nl}))

                                               (nil? nlfrk)
                                               (throw
                                                (ex-info
                                                 "list has nil key"
                                                 {:list nl
                                                  :sort-key-kw sort-key-kw }))

                                               (> (cmp lfrk nlfrk) 0)
                                               (throw
                                                (ex-info
                                                 "list out of order" {:list l}))))

                                           [lfr nl])

                                         [nil l])))))

            ;; merge the head records matching the min-key
            mrgd (->> mrg-nxtlists (map first) (filter some?) (apply merge))

            ;; prepare the lists of remaining records
            nxt-lists (->> mrg-nxtlists (map second) (filter not-empty))]

        ;; (prn :merge-key-sorted-lists*-end mrgd nxt-lists)

        (if (not-empty nxt-lists)
          (recur sort-key-kw cmp (conj r mrgd) nxt-lists)
          (conj r mrgd))))))

(defn merge-key-sorted-lists
  "given a priority-ordered list of sorted lists of records with shape
   {:key <> ...}, sorted in :key, merge the lists by applying `merge`
   to elements with identical keys

   given inputs:
   [{::key 0 :foo 0} {::key 1 :foo -25} {::key 2 :foo 100}]
   [{::key 0 :bar 10} {::key 2 :bar -5}]
   [{::key 1 :foo 50}]

   the output will be:

   [{::key 0 :foo 0 :bar 10} {::key 1 :foo 50} {::key 2 :foo 100 :bar -5}]

   cmp is a comparator which can be used to determine whether two keys sort
   before, after or equal"
  [sort-key-kw cmp & lists]
  (merge-key-sorted-lists* sort-key-kw cmp [] lists))
