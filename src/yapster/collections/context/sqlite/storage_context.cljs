(ns yapster.collections.context.sqlite.storage-context
  (:require
   ["react-native" :refer [Platform]]
   ["expo-sqlite" :as SQLite :refer [openDatabase]]
   ["expo-file-system" :as FileSystem]
   [lambdaisland.glogi :as log]
   [malli.experimental :as mx]
   [oops.core :refer [oget ocall!]]
   [promesa.core :as p]
   [yapster.collections.util.cljs :refer [js->cljkw]]
   [yapster.collections.schema :as coll.schema]
   [yapster.collections.context.util :as util]
   [yapster.collections.context.multimethods :as mm]
   [yapster.collections.context.sqlite.collection-metadata :as sqlite.coll-md]))

(defn sqlite-available?
  []
  (not= "web" (oget Platform "?OS")))

(defn doc-file-path
  [path]
  (let [base-doc-dir (oget FileSystem "documentDirectory")]
    (str base-doc-dir path)))

(defn read-dir
  "return a list of strings of files within dir url"
  [dir-url]
  (ocall! FileSystem "readDirectoryAsync" dir-url))

(defn file-info
  "returns a FileSystem file-info structure, with at least fields

   :exists :isDirectory

   and, if the file exists, additional members

   :size :modificationTime :uri"
  [file-url]
  (ocall! FileSystem "getInfoAsync" file-url))

(defn delete-file
  [file-url]
  (ocall! FileSystem "deleteAsync" file-url (clj->js {})))

(defn db-file-path
  [db-name]
  (let [db-name (util/sanitise-name db-name)]
    (doc-file-path (str "SQLite/" db-name))))

(defn database-exists?
  [db-name]
  (p/let [db-name (util/sanitise-name db-name)
          db-file-info (file-info (db-file-path db-name))
          {exists? :exists
           :as clj-db-file-info} (js->cljkw db-file-info)]

    (log/debug ::database-exists? {:db-file-path db-name
                                  :file-info clj-db-file-info
                                  :exists? exists?})
    exists?))

(defn delete-database
  [db-name]
  (if (sqlite-available?)
    (p/let [db-name (util/sanitise-name db-name)
            db-exists? (database-exists? db-name)
            _ (when db-exists?
                (delete-file (db-file-path db-name)))]
      db-exists?)
    nil))

(defn open-database
  "return WebSQLDatabase"
  [db-name]
  (let [db-name (util/sanitise-name db-name)]
    (log/debug :open-database* db-name)
    (openDatabase db-name)))

(defn test-database
  "run a 'SELECT 1' query against a db connection,
   to test it's alive"
  [db]
  (p/create
   (fn [resolve reject]

     (ocall!
      db
      "exec"
      (clj->js [{:sql "SELECT 1;" :args []}])
      true

      (fn [err results]
        ;; (log/debug :test-sqlite-database {:err err :result results})
        (if (some? err)
          (reject err)

          ;; only look at the first result, since we only sent a single
          ;; statement
          (let [[{err "error"
                  insert-id "insertId"
                  rows "rows"
                  rows-affected "rowsAffected"}] (js->clj results)]

            (if (some? err)
              (reject err)

              (do
                (log/debug :test-database-OK {:db db})
                (resolve {:insert-id insert-id
                          :rows rows
                          :rows-affected rows-affected}))))))))))

(defn check-database
  "check a SQLite db connection
     - log an error and return nil if it is not ok
     - return db if it is ok"
  [db]
  (when (some? db)
    (p/handle
     (test-database db)
     (fn [_succ err]
       (if (some? err)
         (do
           (log/error :check-sqlite-database {:error err})
           nil)
         db)))))

(defn check-context
  [{db :yapster.collections.context.sqlite/db
    :as ctx}]
  (p/let [_ (check-database db)]
     ctx))

(mx/defn make-storage-context :- coll.schema/SQLiteCollectionStorageContext
  [db-name db]
  {:yapster.collections.context/impl :yapster.collections.context/sqlite
   :yapster.collections.context.sqlite/db-name db-name
   :yapster.collections.context.sqlite/db db})

(defn sqlite-storage-context
  "open an SQLiteCollectionStorageContext

   returns Promise<StorageContext>,
    or Promise<nil> if SQLite is not available"
  [db-name]
  (if (sqlite-available?)
    (p/let [db-name (util/sanitise-name db-name)
            db (open-database db-name)
            ctx (make-storage-context db-name db)
            _ (sqlite.coll-md/maybe-create-collections-table ctx)]
      ctx)
    (p/resolved nil)))

(defmethod mm/-open-storage-context :yapster.collections.context/sqlite
  [_impl db-name]
  (sqlite-storage-context db-name))

(defmethod mm/-delete-storage-context :yapster.collections.context/sqlite
  [_impl db-name]
  (delete-database db-name))

(defmethod mm/-check-storage-context :yapster.collections.context/sqlite
  [ctx]
  (check-context ctx))
