(ns yapster.collections.context.sqlite.transactions
  (:require
   [lambdaisland.glogi :as log]
   [oops.core :refer [ocall!]]
   [promesa.core :as p]
   [yapster.collections.context.multimethods :as ctx.mm]))

(defn transaction*
  "call tx-cb in a transaction, as

  (tx-cb tx resolve reject opts)

   - returns Promise<result|error>
   - tx-fn - calls the transaction fn on the db. has the same
             args as .transaction or .readTransaction
   - tx-cb - a synchronous callback fn which will be called as
             (tx-cb tx resolve reject opts)
             and is expected to synchronously schedule at least one
             statement to execute, with further statements potentially
             being added in the continuation of the first statement"
  [{db :yapster.collections.context.sqlite/db
    :as _ctx}
   tx-fn
   tx-cb]

  (p/create
   (fn [r-resolve r-reject]

     (let [cb-p (p/deferred)]

       (tx-fn
        db

        (fn [tx]

          (try

            (tx-cb
             tx
             #(p/resolve! cb-p %)
             #(p/reject! cb-p %)
             {})

            (catch :default x
              (p/reject! cb-p x))))

        (fn [err]
          (log/info ::transaction*-err {:err err})
          (r-reject err))

        (fn []
          (p/handle
           cb-p
           (fn [succ err]
             (if (some? err)
               (do
                 (log/info ::transaction*-err {:err err})
                 (r-reject err))
               (r-resolve succ))))))))))

(defn readonly-transaction
  [ctx tx-cb]
  (transaction*
   ctx
   (fn [db tx-cb err-cb succ-cb]
     (ocall! db "readTransaction" tx-cb err-cb succ-cb))
   tx-cb))

(defmethod ctx.mm/-readonly-transaction :yapster.collections.context/sqlite
  [ctx tx-cb]
  (readonly-transaction ctx tx-cb))

(defn readwrite-transaction
  [ctx tx-cb]
  (transaction*
   ctx
   (fn [db tx-cb err-cb succ-cb]
     (ocall! db "transaction" tx-cb err-cb succ-cb))
   tx-cb))

(defmethod ctx.mm/-readwrite-transaction :yapster.collections.context/sqlite
  [ctx tx-cb]
  (readwrite-transaction ctx tx-cb))
