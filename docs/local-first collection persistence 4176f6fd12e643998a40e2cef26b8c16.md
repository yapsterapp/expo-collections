# local-first collection persistence

# `yapster.collections`

`yapster.collections` presents a simple consumer API to enable local-first retrieval of collections of items (of the same type), cached from a remote API. It has a number of desirable properties:

- Insulates the consumer from dealing with storage and retrieval concerns:
    - The consumer specifies only *what* they want to consume, and nothing of *how* the consumption is to be arranged
    - Javascript Objects are the currency of `yapster.collections`. The Consumer API accepts and returns vanilla Javascript Objects
    - Changes in data formats or collection configuration automatically cause locally cached data to be discarded and re-retrieved from the API, so:
    - No explicit migrations are required - we should never get a client into a borked state because its local store is out of sync with code changes
- The Consumer API is simple: collections are completely defined with pure data, and the hooks presented have trivial and easy to understand parameters
- Transparently implements a SWR (Stale-While-Refresh) pattern
    - collection objects are always served from local-storage
    - local-storage is refreshed from an API when necessary
- Uses React Query hooks for managing local and remote queries, caching and invalidation, and the Consumer API returns appropriate React Query hook responses (`useInfiniteQuery` and `useQuery`)

## Layers

![Untitled](local-first%20collection%20persistence%204176f6fd12e643998a40e2cef26b8c16/Untitled.png)

There are 3 layers to `yapster.collections` - the [Consumer API](local-first%20collection%20persistence%204176f6fd12e643998a40e2cef26b8c16.md), [Internals](local-first%20collection%20persistence%204176f6fd12e643998a40e2cef26b8c16.md) and [StorageContext API](local-first%20collection%20persistence%204176f6fd12e643998a40e2cef26b8c16.md)

# Consumer API

This is the high-level API consumers of collections (generally React views) will use. It has two parts. The metadata API uses pure data to define a collection (including how objects in it are identified, accessed and fetched), while the hooks allow a React view to query that data.

## metadata

### `(yapster.collections/def-collection
   <collection-name> 
   <metadata-map>)`

`def-collection` is the consumer interface to collection metadata. A call to `def-collection` will generally be done at the top-level of a namespace (i.e. at load-time), and provides a map of metadata which defines the following properties (`::coll.md` is a namespace-alias for `:yapster.collections.metadata`):

- `::coll.md/name` - the collection name - must be unique across a codebase
- `::coll.md/key-specs` - a map of `{<key-alias> keyspecs}`
    - see [Keys](local-first%20collection%20persistence%204176f6fd12e643998a40e2cef26b8c16.md) below - keyspecs define a multi-component key, giving both a means to extract a value for each component and a sort-order for the component
- `::coll.md/primary-key` the key-alias of a keyspec for the primary-key of items in the collection
- `::coll.md/index-keys` a vector of key-aliases for index-keys for the collection.  The last component of an index key defines a sorted traversal of the collection, which can be accessed in three ways
    - from the beginning (no last key-component specified)
    - after a particular point (last key-component specified)
    - before a particular point (last key-component specified)
- `::coll.md/index-apis` API definitions for for each index-key, including mustache templates for the 3 access scenarios
    - `::coll.md.api/start-page-path-template` start of collection
    - `::coll.md.api/previous-page-path-template` previous page
    - `::coll.md.api/next-page-path-template` next page
    - `::coll.md.api/response-value-path` cljs-oops path selector for item values in API response
    
    each mustache template gets data with cljs maps including:
    
    - `auth-info` the app auth-info object with `orgId` and `userId` keys
    - `key-data` the key-data provided to a `use-collection-index` hook. Contains a partial or full key-value for the particular index-key - so it will contain all key components for a deep-link and will be missing the last (sort) component for a start-of-collection
    - `query-opts` query-opts given to a `use-collection-index` hook
        - Always includes `limit` - the page size

The following illustrative example is taken from the yapster codebase and is a `def-collection` for the “recent yaps” screen

```clojure
(coll/def-collection
  "recent-conversations"
  {::coll.md/key-specs
   {::recent-conversations-id
    [:org_id :conversation_id]

    ::last-message-time
    [:org_id
     :archived
     [:conversation_last_message_time
      {::coll.md.kc/sort-order ::coll.md.kc.so/desc}]]}

   ::coll.md/primary-key ::recent-conversations-id

   ::coll.md/index-keys
   [::last-message-time]

   ::coll.md/value-schema ParticipantFeedPost

   ;; API calls for each index-key
   ::coll.md/index-apis
   {::last-message-time

    {::coll.md.api/start-page-path-template
     (str "/api/orgs/{{auth-info.org-id}}/org-users/{{auth-info.user-id}}/feed/messages?"
          "archived={{#key-data.archived}}yes{{/key-data.archived}}{{^key-data.archived}}no{{/key-data.archived}}"
          "&sort=last-message-time"
          "&limit={{query-opts.limit}}")

     ::coll.md.api/next-page-path-template
     (str "/api/orgs/{{auth-info.org-id}}/org-users/{{auth-info.user-id}}/feed/messages?"
          "archived={{#key-data.archived}}yes{{/key-data.archived}}{{^key-data.archived}}no{{/key-data.archived}}"
          "&sort=last-message-time"
          "&limit={{query-opts.limit}}"
          "&before={{key-data.conversation_last_message_time}}")

     ::coll.md.api/previous-page-path-template
          (str "/api/orgs/{{auth-info.org-id}}/org-users/{{auth-info.user-id}}/feed/messages?"
          "archived={{#key-data.archived}}yes{{/key-data.archived}}{{^key-data.archived}}no{{/key-data.archived}}"
          "&sort=last-message-time"
          "&limit={{query-opts.limit}}"
          "&after={{key-data.conversation_last_message_time}}")

     ::coll.md.api/response-value-path "posts"}}})
```

## hooks

### `(yapster.collections/use-collection-index
   <collection-name>
   <index-key-alias>
   <key-data>
   <query-opts>?)`

A hook to fetch objects from a collection by an index-key. `use-collection-index` makes a call to React Query `useInfiniteQuery` behind the scenes, and returns the `useInfiniteQuery` hook value, so it behaves precisely like a [React Query infinite query](https://tanstack.com/query/v4/docs/reference/useInfiniteQuery)

```clojure
(let [...
      collection-key-data [org-id false]
      q-result 
      (colls/use-collection-index
        "recent-conversations"
        :yapster.app.modules.recent-conversations.collections/last-message-time
        collection-key-data)

      loading? (obj/get q-result "isLoading")
      fetch-status (obj/get q-result "fetchStatus")
      q-status (obj/get q-result "status")

      force-refetch-start-fn (colls/use-refetch-collection-index-start-fn)

      refetch 
      (fn []
       (force-refetch-start-fn
        "recent-conversations"
        :yapster.app.modules.recent-conversations.collections/last-message-time
        collection-key-data))

      fetch-end-page! (obj/get q-result "fetchEndPage")
      fetching? (obj/get q-result "isFetching")
      ]

  ... )
```

Note the functions in the `useInfiniteQuery` response in addition to the standard React Query `useInfiniteQuery` fns:

- `fetchEndPage` - a new fn in the `useInfiniteQuery` response, which fetches another page at the end of the current page-range held in-memory, *and* forces an API refresh (at the end of the page-range)
- `fetchStartPage` - a new fn in the `useInfiniteQuery` response, which fetches another page before the start of the current page-range held in-memory, *and* forces an API refresh (before the start of the page-range)

These functions exist to avoid attempting to overload the RQ `fetchNextPage` and `fetchPreviousPage` functions with data to force an API refresh - there isn’t a good way to do that, since the `fetchNextPage` and `fetchPreviousPage` fns are also used internally by RQ when a query is invalidated. Instead, when the UI hits the top/bottom of a scroll, we can call one of these new functions. If we know we want to refresh the start of a collection we can also use the `use-refetch-collection-index-start-fn`.

### `(yapster.collections/use-invalidate-collection-index-fn
    <collection-name>
    <index-key-alias>
    <key-data>
    <query-opts>?)`

Hook returning a fn with the same signature as `use-collection-index` , which will invalidate any matching active `use-collection-index` query, causing all the data to be fetched again from local store. Can be used regularly to refresh a view from the local store. (and will also go out to the API when encountering inconsistencies in the local store, but will not go out to the API on every use)

### `(yapster.collections/use-refetch-collection-index-start-fn
    <collection-name>
    <index-key-alias>
    <key-data>
    <query-opts>?)`

Hook returning a fn with the same signature as `use-collection-index`, which will force a refetch of the start of a collection index from the API (and subsequently invalidate the query, causing it to be refetched from the local store).

### `(yapster.collections/use-collection-index-object 
   <collection-name>
   <object-or-id>)`

A hook to fetch individual index objects from a collection. The `<obj-or-id>` parameter is either a full js object (as returned from `use-collection-index`), or the primary key-value of an object in the collection. If it’s the full object then the value will be used to initialise the query data, meaning that no further storage queries will be required - but the UI will still be updated should the object be refreshed from the network.

### `(yapster.collections/use-invalidate-collection-index-object-fn
   <collection-name>
   <object-or-id>)`

A hook returning a fn with the same signature as `use-collection-object`, and will invalidate any matching active `use-collection-object` query, causing data to be fetched again from local store.

### `(yapster.collections/use-update-object-and-indexes-fn
   <collection-name>
   <object>)`

A hook returning a fn which can be used to update an object and its indexes in a collection - the object will be updated in the local store, any required index records updated, and active queries invalidated

# Internals

The consumer API functions are all aliases for internal functions. The internals are defined in the namespaces:

`yapster.collections.*`

`yapster.collections.hooks.*`

These namespaces define the concepts in the consumer API and depend on the [Storage Context API](local-first%20collection%20persistence%204176f6fd12e643998a40e2cef26b8c16.md) multimethods defined in the `yapster.collections.context.multimethods` namespace. Implementations of these multimethods manage Storage Contexts and store and retrieve values in a context. 

## how data is stored

For each collection, the Storage Context multimethods define multiple underlying storage resources (let’s call them `tables` - they will be tables in SQLite, and ObjectStores in IndexedDB) for each collection defined with `def-collection`

- One table for the js objects themselves, with the collection primary-key as its primary-key. The table fields for the primary-key are named `id_0`, `id_1` … because we do not have actual names for the key components
- One table for *each* index-key defined on the collection, with the collection primary-key again used as the table’s primary-key, and an additional storage index defined on the index-key. Similarly to the objets table, index-table fields for the primary-key are named `id_0`... and the table fields for the index-key are named `k_0`,`k_1` …
    - There are also `prev_id` and `next_id` fields which contain the JSON encoded values of the `[<id_0> <id_1>...]` fields from the previous and next records in the index, when last retrieved from the API
    - NB: this is how it is currently implemented - but this may sometimes (rarely) be incorrect, because if there are multiple records in an index with the same index key-value then their sort order will be undefined. there is a task to fix this issue (by appending the primary-key value to the index-key in the index)
    - This scheme for index-tables means that when an index-record is updated, objects automatically move from their old position to their new position in an index, whenever a query is refetched
    - We can also detect discontinuities in the local index cache using the `prev_id` and `next_id` fields. Discontinuities can happen
        - when objects move in the index (e.g. a conversation has activity and moves to the top of the index)
        - when different sections of the index have been accessed via deep-links

## keys

`yapster.collections.keys`

Keys are central to `yapster.collections`. Each collection must define a primary key and (to be useful), at least one index-key.

Keys are defined in the metadata with keyspecs. Keyspecs are a vector of key-component-specs, and key-component-specs are `[<key-component-name> <opts>]` or just `<key-component-name>`

- key-component-name - a no-namespace keyword, which is also used as the extractor by default
- key-extractor - the `:yapster.collections.metadata.key-component/extractor` key of the `opts` - currently a keyword, which may be namespaced, and defines a path and property in a hierarchy of javascript objects. If no explicit extractor is specified, then the `key-component-name` is used, but since that has no namespace it can only extract fields from the top-level of the record
- (optional) sort order - the `:yapster.collections.metadata.key-component/sort-order` key of the `opts`. May be omitted, in which case ASC is assumed

Thus a keyspec like:

```clojure
[:org_id
 :archived
 [:conversation_last_message_time
  {::coll.md.kc/extractor :last_message_time
   ::coll.md.kc/sort-order ::coll.md.kc.so/desc}]]
```

Defines a 3-component key. The 3 components are extracted from the topmost js object, from the `org_id`, `archived` and `last_message_time` (note the explicit extractor) fields, while the key is sorted in descending order on the `conversation_last_message_time`. (The sort  means that the most recent `conversation_last_message_time` will present first).

The primary key for a collection, as in the common parlance, defines a unique key for objects in that collection - there can only ever be a single object in a collection store with a particular primary-key value.

Index keys define a way of traversing a collection. The traversal is (currently) limited to the last component of an index-key.

## metadata

`yapster.collections.metadata`

Functions for managing collection metadata, principally `def-collection`

## context

`yapster.collections.context`

Functions for managing StorageContext instances

`(open-storage-context <db-name>` : opens a storage context with a given `<db-name>` . Each unique `<db-name>` creates completely isolated storage from other `<db-name>`s

`(delete-storage-context <db-name>)` : delete any storage context for `<db-name>`, discarding all stored data

`(open-collection <ctx> <collection-name>)` : opens a collection in a storage context. This function manages all the storage resources required for storing the collection items in the context (e.g. for a SQLite context it will create an objects table and a table for each index). In addition, it checks whether the collection metdata has changed since the collection was last accessed. If there is any change in the collection metdata then all storage resources will be discarded and recreated, which is an easy way of avoiding consumers having to worry about the effects of schema change.

## objects

`yapster.collections.objects`

Functions for accessing and updating individual objects in a collection

`(get-collection-objects <ctx> <coll-name> <ids>)` : returns a Promise<list of collection (js) objects>, as identified by the `<ids>` parameter, which is a list of primary key values

`(get-collection-object <ctx> <coll-name> <id>)` : return a Promise<collection-object> identified by the primary key value `<id>`

`(store-collection-objects <ctx> <coll-name> <objs>)` : store a list of collection objects `<objs>`, updating any objects which are already stored

`(store-collection-object <ctx> <coll-name> <obj>)` : store a single collection object `<obj>`

## indexes

`yapster.collections.indexes`

Functions for accessing and updating indexes of collection objects

`(get-index-objects-page <ctx> <coll-name> <key-alias> <opts>)` : retrieve a single page of index objects according to their order in the index defined by `<key-alias>`. `<opts>` has keys:

`:limit` - size of page

`:after` - value of `index-key` for next-page retrieval

`:before` - value of `index-key` for previous page retrieval

`(update-index-page <ctx> <coll-name> <key-alias> <old-index-records> <new-object-records>)` : given a list of `old-index-records` from the local store, and a list of `new-object-records` from the API (presumed for the same start/after/before constraint), update the local-store

- `old-index-records` within the index-bounds of the `new-object-records` which are not present in the `new-object-records` are deleted
- index-records corresponding to all the `new-object-records` are inserted/updated
- object records corresponding all the `new-object-records` are inserted/updated

## hooks

React hooks for accessing collection indexes and objects 

`yapster.collections.hooks` - the consumer API hooks described [here](local-first%20collection%20persistence%204176f6fd12e643998a40e2cef26b8c16.md)

`yapster.collections.hooks.fetch` - js/fetch based API fetching, using React Query [QueryObserver](https://tanstack.com/query/v4/docs/reference/QueryObserver)

`yapster.collections.hooks.storage-context` - core pagination functions for local-first pagination through a collection

# StorageContext (storage) API

## multimethods

`yapster.collections.context.multimethods`

This namespace declares all the multimethods which define a StorageContext. StorageContext is the low-level API which the `yapster.collections` internals use to interact with the local store, be it SQLite or IndexedDB

`(-open-storage-context <impl> <db-name>)` : open a storage context, creating any connection objects and storage resources required

`(-delete-storage-context <impl> <db-name>)` : permanently delete a storage context

`(-check-storage-context <ctx>)` : test an open storage context is operational

`(-load-collection-metadata <ctx> <collection-name>)` : load stored metadata for a collection (to check whether it has changed since the collection was created)

`(-store-collection-metadata <ctx> <collection-name>)` : store metadata for a collection

`(-create-collection <ctx> <collection-name>)` : create storage resources for a collection - these may include tables / ObjectStores &c

`(-drop-collection <ctx> <collection-name>)` : discard all storage resources for a collection

`(-readonly-transaction <ctx> <tx-cb>)` : run a read-only transaction in a context

`(-readwrite-transaction <ctx> <tx-cb>)` :  run a read-write transaction in a context

`(-get-collection-objects-cb <ctx> <coll> <ids>)` : return a transaction callback to get collection objects

`(-store-collection-objects-cb <ctx> <coll> <objs>)` : return a transaction callback to store collection objects

`(-get-index-page-cb <ctx> <coll> <key-alias> <opts>)` : return a transaction callback to get an index page

`(-get-index-objects-page-cb <ctx> <coll> <key-alias> <opts>)` : return a transaction callback to get a page of collection objects referenced from an index page

`(-store-index-records-cb <ctx> <coll> <key-alias> <index-records>)` : return a transaction callback to store index records

`(-delete-index-records-cb <ctx> <coll> <key-alias> <index-records>)` : return a transcation callback to delete index records

## transactions

`yapster.collections.context.transactions`

Both the storage contexts to be defined, SQLite and IndexedDB, are transactional, but they each have rather different approaches to transactions. IndexedDB’s approach is (roughly) promise-based, while SQLite’s is continuation-based. Because the continuation-based approach is more general, we’ve adopted that approach for our API.

This namespace has functions for invoking a transaction on a context, and for composing transaction callback functions

# SQLite context

The SQLite context consists of implementations of the StorageContext multimethods and associated utility functions

## collection metadata

`yapster.collections.context.sqlite.collection-metadata`

SQLite implementations of `-store-collection-metadata` and `-load-collection-metadata` methods

## objects

`yapster.collections.context.sqlite.objects`

SQLite implementations of the `-get-collection-objects-cb` and `-store-collection-objects-cb` methods

## indexes

`yapster.collections.context.sqlite.indexes`

SQLite implementations of the `-get-index-page-cb`, `get-index-objects-page-cb`, `store-index-records-cb` and `delete-index-records-cb` methods

## storage context

`yapster.collections.context.sqlite.storage-context`

SQLite implementations of the `-open-storage-context`, `-delete-storage-context`, `-check-storage-context` methods

## transactions

`yapster.collections.context.sqlite.transactions`

SQLite implementations of the `-readonly-transaction`, `-readwrite-transaction` methods

## tables

`yapster.collections.context.sqlite.tables`

SQLite implementations of the `-create-collection`, `-drop-collection` methods

## transaction statements

`yapster.collections.context.sqlite.transaction-statements`

Functions to generate transaction-callbacks from SQLite SQL statements. Notably, there are some convenience functions which can be used to directly execute SQL against a database:

```clojure
(def ctx 
  (yapster.util.repl/capture 
    (yapster.collections.context/open-storage-context
       "yapws_https_devapi_yapsterchat_com_f7c3d590_4b33_11e5_8978_fa2a23995eb5_19898900_4340_11e5_bffc_d5981f3a064d")))

(def r 
  (yapster.util.repl/capture 
    (yapster.collections.context.sqlite.transaction-statements/readonly-statement 
      @ctx 
      "select * from collection_index__recent_conversations__org_id_ASC__archived_ASC__conversation_last_message_time_DESC limit 1")))

;; => #object[cljs.core.Atom {:val #js [#js {:id_0 "f7c3d590-4b33-11e5-8978-fa2a23995eb5", :id_1 "e77f3750-1870-11eb-b891-eb0741863b69", :k_0 "f7c3d590-4b33-11e5-8978-fa2a23995eb5", :k_1 0, :k_2 "2022-08-05T09:20:53.965Z", :created_at "2022-10-10 14:38:02", :updated_at "2022-10-10 14:38:02"}]}]
```