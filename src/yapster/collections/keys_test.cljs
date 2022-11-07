(ns yapster.collections.keys-test
  (:require
   [cljs.test :as t :include-macros true :refer [deftest testing is]]
   [yapster.collections.keys :as sut]
   [yapster.collections.metadata.key-component :as-alias coll.md.kc]))

(deftest extract-key-test
  (testing "default extractor"
    (testing "key present"
      (is (= [100]
             (sut/extract-key [:foo] #js {:foo 100})))
      (is (= [100 200]
             (sut/extract-key [:foo :bar] #js {:foo 100 :bar 200}))))
    (testing "key not present"
      (is (= nil
             (sut/extract-key [:foo] #js {})))
      (is (= nil
             (sut/extract-key [:foo :bar] #js {:foo 100})))))

  (testing "specified extractor"
    (testing "key present"
      (is (= [100]
             (sut/extract-key
              [[:foo_stuff {::coll.md.kc/extractor :foo}]]
              #js {:foo 100})))
      (is (= [100 200]
             (sut/extract-key
              [[:foo_stuff {::coll.md.kc/extractor :foo}]
               [:bar_stuff {::coll.md.kc/extractor :bar}]]
              #js {:foo 100 :bar 200}))))
    (testing "key not present"
      (is (= nil
             (sut/extract-key
              [[:foo_stuff {::coll.md.kc/extractor :foo}]]
              #js {})))
      (is (= nil
             (sut/extract-key
              [[:foo_stuff {::coll.md.kc/extractor :foo}]
               [:bar_stuff {::coll.md.kc/extractor :bar}]]
              #js {:foo 100}))))))

(deftest merge-key-sorted-lists-test
  (testing "merges lists"
    (is (= [{:k 0 :a "0" :b "0"}
            {:k 1 :a "1"}
            {:k 2 :b "2"}
            {:k 5 :b "5"}]
           (sut/merge-key-sorted-lists
            :k
            compare

            [{:k 0 :a "0"}
             {:k 1 :a "1"}]

            [{:k 0 :b "0"}
             {:k 2 :b "2"}
             {:k 5 :b "5"}]))))

  (testing "deals with empty lists"
    (is (= [{:k 0 :a "0"}
            {:k 1 :a "1"}]
           (sut/merge-key-sorted-lists
            :k
            compare

            [{:k 0 :a "0"}
             {:k 1 :a "1"}]

            [])))

    (is (= [{:k 0 :a "0"}
            {:k 1 :a "1"}]
           (sut/merge-key-sorted-lists
            :k
            compare

            []

            [{:k 0 :a "0"}
             {:k 1 :a "1"}])))

    (is (= []
           (sut/merge-key-sorted-lists
            :k
            compare

            []

            []))))

  (testing "errors if lists are not sorted"
    (is (thrown-with-msg?
         :default
         #"list out of order"

         (sut/merge-key-sorted-lists
          :k
          compare

          [{:k 1 :a "0"}
           {:k 0 :a "1"}]

          [{:k 0 :b "0"}
           {:k 2 :b "2"}]))))

  (testing "errors if lists have nil records"
    (is (thrown-with-msg?
         :default
         #"list has nil record"

         (sut/merge-key-sorted-lists
          :k
          compare

          [{:k 1 :a "0"}
           nil]

          [{:k 0 :b "0"}
           {:k 2 :b "2"}]))))

  (testing "errirs if lists have nil sort-keys"
    (is (thrown-with-msg?
         :default
         #"list has nil key"

         (sut/merge-key-sorted-lists
          :k
          compare

          [{:k 1 :a "0"}
           {:k nil :a "1"}]

          [{:k 0 :b "0"}
           {:k 2 :b "2"}])))))
