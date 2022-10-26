(ns yapster.collections.util.cljs)

(defmacro js->cljkw
  [& forms]
  `(cljs.core/js->clj ~@forms :keywordize-keys true))
