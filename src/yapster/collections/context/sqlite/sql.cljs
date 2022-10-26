(ns yapster.collections.context.sqlite.sql
  (:require
   [clojure.string :as string]
   [yapster.collections.metadata.key-component.sort-order :as-alias coll.md.kc.so]
   [yapster.collections.keys :as coll.keys]))

(defn pk-fields
  "vector of primary-key fields

     [:id_0 :id_1 ...]"
  [pk-spec]
  (->> pk-spec
       (coll.keys/normalize-keyspec)
       (coll.keys/indexed-key-fields :id_)))

(defn pk-cols-list
  "list of comma-separated PK cols

     id_0,id_1..."
  [pk-spec]
  (->> (pk-fields pk-spec)
       (map name)
       (string/join ",")))

(defn key-col-dir
  "given a key-component column name and a corresponding
   key-component spec, return a sort-order suffixed
   column-name e.g. 'id_0' -> 'id_0 DESC'"
  [col
   key-component-spec]

  (let [sort-order (coll.keys/key-component-sort-order key-component-spec)]
    (str (name col)
         (if (= ::coll.md.kc.so/desc sort-order)
           " DESC"
           " ASC"))))

(defn pk-cols-dir-list
  "list of comman-separated PK cols
   with sort direction"
  [pk-spec]
  (let [pk-spec (coll.keys/normalize-keyspec pk-spec)
        pk-fields (pk-fields pk-spec)]
    (->> (map key-col-dir
              pk-fields
              pk-spec)
         (string/join ","))))

(defn idx-key-fields
  "vector of index-key column fields

     [:k_0,:k_1 ...]"
  [key-spec]
  (->> key-spec
       (coll.keys/normalize-keyspec)
       (coll.keys/indexed-key-fields :k_)))

(defn idx-key-cols-list
  "comma-separated list of index-key cols

     k_0,k_1..."
  [key-spec]
  (->> (idx-key-fields key-spec)
       (map name)
       (string/join ",")))

(defn idx-key-cols-dir-list
  "comman separated list of index-key cols
   with sort direction"
  [keyspec]
  (let [keyspec (coll.keys/normalize-keyspec keyspec)
        k-fields (idx-key-fields keyspec)]
    (->> (map key-col-dir
              k-fields
              keyspec)
         (string/join ","))))

(defn VALUES-placeholders-list
  "SQL query placeholders for a list of VALUES records

    (?,?...),(?,?...)...

   - cols : number of cols in each record
   - rows : nubmer of records"
  [cols rows]
  (->> (repeat cols "?")
       (string/join ",")
       (#(str "(" %  ")"))
       (repeat rows)
       (string/join ", ")))

(defn VALUES-templated-record-placeholders
  "generate placeholders for VALUES record from a template

   :? elements in the template will come out as ? placeholders,
   otherwise they will be rendered literally, so that
   SQL functions or literal SQL values can be given

   e.g.

   [:? :? \"datetime()\"] => \"(?,?,datetime())\""
  [record-template]
  (->> record-template
       (map (fn [vt] (if (= :? vt) "?" vt)))
       (string/join ",")
       (#(str "(" %  ")"))))

(defn VALUES-templated-placeholders-list
  "generate placeholders for a list of records, from a template

     :? elements in the template will come out as ? placeholders,
   otherwise they will be rendered literally, so that
   SQL functions or literal SQL values can be given

   e.g.

  [:? \"datetime()\"],2 => \"(?,datetime()),(?,datetime())\"
  "
  [record-template rows]
  (->> (repeat rows (VALUES-templated-record-placeholders record-template))
       (string/join ",")))


(defprotocol IQueryValue
  (-query-value [self]))

(extend-type default
  IQueryValue
  (-query-value [self] self))

(extend-type UUID
  IQueryValue
  (-query-value [self] (str self)))

(defn compatible-query-values
  "given a list of query values, potentially including cljs values
   (looking at cljs.core/UUID), transform into sqlite compatible
   values"
  [vs]
  (map -query-value vs))
