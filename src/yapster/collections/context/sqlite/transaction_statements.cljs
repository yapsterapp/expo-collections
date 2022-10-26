(ns yapster.collections.context.sqlite.transaction-statements
  (:require
   [lambdaisland.glogi :as log]
   [oops.core :refer [oget ocall!]]
   [promesa.core :as p]
   [yapster.collections.context.transactions :as txns]))

(defn tx-statement-cb
  "a transaction callback for a single SQLite statement"
  [stmt-or-stmt-args]
  (fn [tx resolve reject {:as _opts}]


    (let [[stmt args] (if (sequential? stmt-or-stmt-args)
                        stmt-or-stmt-args
                        [stmt-or-stmt-args nil])]

      (log/debug
       ::tx-statement-cb
       {:stmt stmt
        :args args})

      (try
        (ocall!
         tx
         "executeSql"
         stmt
         (clj->js args)

         (fn [_tx result-set]
           ;; (log/debug ::tx-statement-cb-result
           ;;            {:stmt stmt
           ;;             :args args})
           (try
             (let [rows (oget result-set "?rows.?_array")]
               ;; (log/debug
               ;;  ::tx-statement-cb-succ
               ;;  {:rows rows})

               (resolve rows)

               (log/debug ::tx-statement-cb-result
                          {:stmt stmt
                           :args args
                           :row-count (count rows)})

               true)

             (catch :default err
               (reject err)
               false)))

         (fn [_tx err]
           (log/info
            ::tx-statement-statement-cb-err
            {:err err})

           (reject err)
           false))
        (catch :default x
          (log/info ::tx-statement-cb-exception {:exception x})
          (reject x))))))

(defn tx-statements-cb
  "return a transaction callback to execute a list of statements
   in a transaction. one statement is enqueued immediately, and
   the remainder are put into a continuation, to be enqueued
   after each successful statement execution"
  [statements]
  (txns/conj-tx-callbacks
   (for [stmt statements]
     (tx-statement-cb stmt))))

(defn readonly-statements
  "execute a static list of statments in a readonly transaction

   - returns: a list of responses"
  [ctx statements]
  (txns/readonly-transaction
   ctx
   (tx-statements-cb statements)))

(defn readonly-statement
  "execute a single statement in a readonly transaction

  - returns: the response"
  [ctx statement]
  (p/let [rs (readonly-statements ctx [statement])]
    (first rs)))

(defn readwrite-statements
  "execute a list of statments in a readwrite transaction,

   - returns: a list of responses"
  [ctx statements]
  (txns/readwrite-transaction
   ctx
   (tx-statements-cb statements)))

(defn readwrite-statement
  "execute a single statement in a readwrite transaction

   - returns: the response"
  [ctx statement]
  (p/let [rs (readwrite-statements ctx [statement])]
    (first rs)))
