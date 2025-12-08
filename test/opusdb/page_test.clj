(ns opusdb.page-test
  (:require [clojure.test :as test]
            [opusdb.page :as  page]))

(test/deftest test-make-page-with-long
  (test/testing "Creating a page with a block size"
    (let [p (page/make-page 1024)]
      (test/is (not (nil? p)))
      (test/is (instance? opusdb.page.Page p)))))

(test/deftest test-make-page-with-byte-array
  (test/testing "Creating a page from a byte array"
    (let [ba (byte-array 1024)
          p (page/make-page ba)]
      (test/is (not (nil? p)))
      (test/is (instance? opusdb.page.Page p)))))

(test/deftest test-get-set-int
  (test/testing "Getting and setting integers"
    (let [p (page/make-page 1024)]
      (.setInt p 0 42)
      (test/is (= 42 (.getInt p 0)))

      (.setInt p 100 -999)
      (test/is (= -999 (.getInt p 100)))

      (.setInt p 500 Integer/MAX_VALUE)
      (test/is (= Integer/MAX_VALUE (.getInt p 500))))))

(test/deftest test-get-set-string
  (test/testing "Getting and setting strings"
    (let [p (page/make-page 1024)]
      (.setString p 0 "Hello")
      (test/is (= "Hello" (.getString p 0)))

      (.setString p 100 "World")
      (test/is (= "World" (.getString p 100)))

      (.setString p 200 "")
      (test/is (= "" (.getString p 200))))))

(test/deftest test-string-with-special-chars
  (test/testing "Strings with ASCII special characters"
    (let [p (page/make-page 1024)
          test-str "Hello@#$%123"]
      (.setString p 0 test-str)
      (test/is (= test-str (.getString p 0))))))

(test/deftest test-multiple-operations
  (test/testing "Multiple reads and writes"
    (let [p (page/make-page 1024)]
      (.setInt p 0 100)
      (.setString p 10 "First")
      (.setInt p 50 200)
      (.setString p 60 "Second")

      (test/is (= 100 (.getInt p 0)))
      (test/is (= "First" (.getString p 10)))
      (test/is (= 200 (.getInt p 50)))
      (test/is (= "Second" (.getString p 60))))))

(test/deftest test-max-encoded-size
  (test/testing "Maximum encoded size calculation"
    (test/is (= 4 (page/max-encoded-size "")))
    (test/is (= 9 (page/max-encoded-size "Hello")))
    (test/is (= 14 (page/max-encoded-size "HelloWorld")))))

(test/deftest test-page-independence
  (test/testing "Multiple pages are independent"
    (let [p1 (page/make-page 512)
          p2 (page/make-page 512)]
      (.setInt p1 0 100)
      (.setInt p2 0 200)

      (test/is (= 100 (.getInt p1 0)))
      (test/is (= 200 (.getInt p2 0))))))

(test/deftest test-string-preserves-buffer-position
  (test/testing "getString does not change buffer position permanently"
    (let [p (page/make-page 1024)]
      (.setString p 0 "Test")
      ;; Position should be restored after getString
      (.getString p 0)
      (.getString p 0)
      (test/is (= "Test" (.getString p 0))))))
