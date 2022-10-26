(ns yapster.collections.context.transactions
  "cross-platform transactions

   because the WebSQL API is continuation-based, we don't
   have the nicest API...

   the transaction execution fns are supplied with a
   transaction callback, which will be called as:

   (<tx-cb> tx resolve reject opts)

   there are helper fns in the connector-specific namespaces
   to create such callback fns, and in this namespace
   there is a fn to compose a sequence of callback fns into
   a single callback fns"
  (:require
   [yapster.collections.context.multimethods :as ctx.mm]))

(defn readonly-transaction
  "perform a readonly transaction with a
   transaction callback

   - returns Promise<results|error>
   - tx-cb - a fn (tx-cb tx resolve reject opts)"
  [ctx tx-cb]
   (ctx.mm/-readonly-transaction ctx tx-cb))

(defn readwrite-transaction
  "perform a readwrite transaction with a
   transaction callback

   - returns Promise<results|error>
   - tx-cb - a fn (tx-cb tx resolve reject opts)"
  [ctx tx-cb]
   (ctx.mm/-readwrite-transaction ctx tx-cb))

(defn noop-tx-callback
  "a transaction callback which does nothing

   sends an empty vector the results"
  [_tx resolve _reject {}]
  (resolve []))

(defn ^:private conj-tx-callbacks*
  [tx
   resolve
   reject
   {prev-results ::results :as opts}
   tx-cb
   next-tx-cbs]

  (tx-cb
   tx
   ;; resolve
   (fn [new-results]
     (if (empty? next-tx-cbs)
       (resolve (conj prev-results new-results))

       (try
         (conj-tx-callbacks*
          tx
          resolve
          reject
          (update opts ::results #((fnil conj []) % new-results))
          (first next-tx-cbs)
          (next next-tx-cbs))
         (catch :default err
           (reject err)))))

   ;; reject
   (fn [err]
     (reject err))

   opts))

(defn conj-tx-callbacks
  "given a sequence of transaction callbacks,
   return a composed transaction callback which

   - calls each of the callbacks in strict order as:

     (tx-cb tx resolve reject {prev-results ::results :as opts})

     providing the results of previously successful callbacks on
     the opts map

   - on success resolves a vector of the results of each callback
   - stops at the first failure and rejects with the error"
  [tx-cbs]
  (fn [tx resolve reject {prev-results ::results :as opts}]

    (let [[tx-cb & next-tx-cbs] tx-cbs]

      (if (nil? tx-cb)

        ;; there were no callbacks
        (resolve (or prev-results []))

        (conj-tx-callbacks*
         tx
         resolve
         reject
         (update opts ::results #(or % []))
         tx-cb
         next-tx-cbs)))))

(defn fmap-tx-callback
  "given a transaction callback and function
   return a transaction callback which applies f to the
   success results of the callback"
  [tx-cb f]
  (fn [tx resolve reject opts]
    (tx-cb
     tx

     ;; resolve
     (fn [new-results]
       (resolve (f new-results)))

     ;; reject
     (fn [err]
       (reject err))

     opts)))
