(ns yapster.collections.indexes-test
  (:require
   [yapster.collections.indexes :as sut]
   [cljs.test :as t :include-macros true :refer [deftest testing is]]))


(deftest index-record-key-extractor-test
  (is (= [100]
         ((sut/index-record-key-extractor [:foo])
          (clj->js {:k_0 100}))))
  (is (= [100 200]
         ((sut/index-record-key-extractor [:foo :bar])
          (clj->js {:k_0 100 :k_1 200})))))

(deftest make-index-record-test
  (let [coll {:yapster.collections.metadata/name "stuff"
              :yapster.collections.metadata/key-specs
              {::id [:foo]
               ::bar [:bar]}
              :yapster.collections.metadata/primary-key ::id
              :yapster.collections.metadata/index-keys [::bar]
              :yapster.collections.metadata/value-schema [:map]}

        idxr (sut/make-index-record
              coll
              [:bar]
              (clj->js {:foo 100 :bar "haha"}))]

    (is (object? idxr))
    (is
     (= { :id_0 100 :k_0 "haha"}
        (js->clj idxr :keywordize-keys true)))))

(deftest index-changes-test

  ;; change calculation is limited to the interval described by the
  ;; first and last new-object-records. within this interval,
  ;; old-index-records which are not present in the new-object-records
  ;; will be deleted. index records corresponding to each of the
  ;; new-object-records will always be inserted

  (testing "ASC key"
    (let [coll {:yapster.collections.metadata/name "stuff"
                :yapster.collections.metadata/key-specs
                {::id [:foo :bar]
                 ::bar [:foo :baz]}
                :yapster.collections.metadata/primary-key ::id
                :yapster.collections.metadata/index-keys [::bar]
                :yapster.collections.metadata/value-schema [:map]}]

      (testing "overlapping ASC changes"
        (testing "new records overlap end of old records"
          (let [keyspec [:foo :baz]

                old-index-records
                [{:k_0 "a" :k_1 3 :id_0 "a" :id_1 "cc"}
                 {:k_0 "a" :k_1 4 :id_0 "a" :id_1 "dd"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}]

                new-object-records
                [{:foo "a" :bar "dd" :baz 4}
                 {:foo "a" :bar "cc" :baz 6}
                 {:foo "a" :bar "dd" :baz 7}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"ee"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))

        (testing "new records overlap beginning of old records"
          (let [keyspec [:foo :baz]

                old-index-records
                [{:k_0 "a" :k_1 3 :id_0 "a" :id_1 "cc"}
                 {:k_0 "a" :k_1 4 :id_0 "a" :id_1 "dd"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}]

                new-object-records
                [{:foo "a" :bar "aa" :baz 1}
                 {:foo "a" :bar "cc" :baz 2}
                 {:foo "a" :bar "dd" :baz 4}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"cc"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))

        (testing "new record interval fully contains old records"
          (let [keyspec [:foo :baz]

                old-index-records
                [{:k_0 "a" :k_1 3 :id_0 "a" :id_1 "cc"}
                 {:k_0 "a" :k_1 4 :id_0 "a" :id_1 "dd"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}]

                new-object-records
                [{:foo "a" :bar "aa" :baz 1}
                 {:foo "a" :bar "dd" :baz 4}
                 {:foo "a" :bar "ee" :baz 6}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"cc" "ee"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))

        (testing "new records are fully contained by old record interval"
          (let [keyspec [:foo :baz]

                old-index-records
                [{:k_0 "a" :k_1 1 :id_0 "a" :id_1 "aa"}
                 {:k_0 "a" :k_1 4 :id_0 "a" :id_1 "dd"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}
                 {:k_0 "a" :k_1 7 :id_0 "a" :id_1 "gg"}]

                new-object-records
                [{:foo "a" :bar "bb" :baz 2}
                 {:foo "a" :bar "dd" :baz 4}
                 {:foo "a" :bar "ee" :baz 6}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"ee"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true))))))

      (testing "non-overlapping ASC changes"
        (testing "new records are after old records"
          (let [keyspec [:foo :baz]

                old-index-records
                [{:k_0 "a" :k_1 1 :id_0 "a" :id_1 "aa"}
                 {:k_0 "a" :k_1 4 :id_0 "a" :id_1 "dd"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}
                 {:k_0 "a" :k_1 7 :id_0 "a" :id_1 "gg"}]

                new-object-records
                [{:foo "a" :bar "hh" :baz 8}
                 {:foo "a" :bar "jj" :baz 10}
                 {:foo "a" :bar "ll" :baz 12}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))

        (testing "new records are before old records"
          (let [keyspec [:foo :baz]

                old-index-records
                [{:k_0 "a" :k_1 6 :id_0 "a" :id_1 "ff"}
                 {:k_0 "a" :k_1 7 :id_0 "a" :id_1 "gg"}]

                new-object-records
                [{:foo "a" :bar "aa" :baz 1}
                 {:foo "a" :bar "cc" :baz 3}
                 {:foo "a" :bar "ee" :baz 5}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true))))))))

  (testing "DESC key"
    (let [coll {:yapster.collections.metadata/name "stuff"
                :yapster.collections.metadata/key-specs
                {::id [:foo :bar]
                 ::bar [:foo
                        [:baz {:yapster.collections.metadata.key-component/sort-order
                               :yapster.collections.metadata.key-component.sort-order/desc}]]}
                :yapster.collections.metadata/primary-key ::id
                :yapster.collections.metadata/index-keys [::bar]
                :yapster.collections.metadata/value-schema [:map]}]

      (testing "overlapping DESC changes"
        (testing "new records overlap end of old records"
          (let [keyspec (get-in coll [:yapster.collections.metadata/key-specs
                                      ::bar])

                old-index-records
                [{:k_0 "a" :k_1 7 :id_0 "a" :id_1 "gg"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}
                 {:k_0 "a" :k_1 3 :id_0 "a" :id_1 "cc"}]

                new-object-records
                [{:foo "a" :bar "gg" :baz 6}
                 {:foo "a" :bar "cc" :baz 3}
                 {:foo "a" :bar "aa" :baz 1}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"ee"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))

        (testing "new records overlap beginning of old records"
          (let [keyspec (get-in coll [:yapster.collections.metadata/key-specs
                                      ::bar])
                old-index-records
                [{:k_0 "a" :k_1 7 :id_0 "a" :id_1 "gg"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}
                 {:k_0 "a" :k_1 3 :id_0 "a" :id_1 "cc"}]

                new-object-records
                [{:foo "a" :bar "ee" :baz 9}
                 {:foo "a" :bar "gg" :baz 7}
                 {:foo "a" :bar "dd" :baz 4}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"ee"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))

        (testing "new records completely enclose old records"
          (let [keyspec (get-in coll [:yapster.collections.metadata/key-specs
                                      ::bar])

                old-index-records
                [{:k_0 "a" :k_1 7 :id_0 "a" :id_1 "gg"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}
                 {:k_0 "a" :k_1 3 :id_0 "a" :id_1 "cc"}]

                new-object-records
                [{:foo "a" :bar "ee" :baz 9}
                 {:foo "a" :bar "gg" :baz 7}
                 {:foo "a" :bar "bb" :baz 2}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"ee" "cc"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))
        (testing "new records are enclosed by old records"
          (let [keyspec (get-in coll [:yapster.collections.metadata/key-specs
                                      ::bar])

                old-index-records
                [{:k_0 "a" :k_1 10 :id_0 "a" :id_1 "jj"}
                 {:k_0 "a" :k_1 6 :id_0 "a" :id_1 "ff"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}
                 {:k_0 "a" :k_1 1 :id_0 "a" :id_1 "aa"}]

                new-object-records
                [{:foo "a" :bar "ii" :baz 9}
                 {:foo "a" :bar "ff" :baz 6}
                 {:foo "a" :bar "ee" :baz 2}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{"ee"} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true))))))
      (testing "non-overlapping DESC changes"
        (testing "new records are before old records"
          (let [keyspec (get-in coll [:yapster.collections.metadata/key-specs
                                      ::bar])

                old-index-records
                [{:k_0 "a" :k_1 10 :id_0 "a" :id_1 "jj"}
                 {:k_0 "a" :k_1 6 :id_0 "a" :id_1 "ff"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}
                 {:k_0 "a" :k_1 1 :id_0 "a" :id_1 "aa"}]

                new-object-records
                [{:foo "a" :bar "nn" :baz 15}
                 {:foo "a" :bar "ll" :baz 12}
                 {:foo "a" :bar "kk" :baz 11}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))
        (testing "new records are after old records"
          (let [keyspec (get-in coll [:yapster.collections.metadata/key-specs
                                      ::bar])

                old-index-records
                [{:k_0 "a" :k_1 10 :id_0 "a" :id_1 "jj"}
                 {:k_0 "a" :k_1 6 :id_0 "a" :id_1 "ff"}
                 {:k_0 "a" :k_1 5 :id_0 "a" :id_1 "ee"}]

                new-object-records
                [{:foo "a" :bar "dd" :baz 4}
                 {:foo "a" :bar "bb" :baz 2}
                 {:foo "a" :bar "aa" :baz 1}]

                {insertions ::sut/index-insertions
                 deletions ::sut/index-deletions
                 :as _idx-changes}
                (sut/index-changes
                 coll
                 keyspec

                 (clj->js old-index-records)

                 (clj->js new-object-records))]

            (is (every? object? insertions))
            (is (every? object? deletions))

            (is (= (filterv #(#{} (:id_1 %)) old-index-records)
                   (js->clj deletions :keywordize-keys true)))

            (is (= (mapv
                    (comp
                     #(js->clj % :keywordize-keys true)
                     #(sut/make-index-record coll keyspec %)
                     #(clj->js %))
                    new-object-records)
                   (js->clj insertions :keywordize-keys true)))))))))
