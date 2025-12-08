(ns opusdb.page-test
  (:require [clojure.test :as test]
            [opusdb.page :as page]))

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
      (.getString p 0)
      (.getString p 0)
      (test/is (= "Test" (.getString p 0))))))

(test/deftest test-get-set-float
  (test/testing "Getting and setting floats"
    (let [p (page/make-page 1024)]
      (.setFloat p 0 2.5)
      (test/is (= 2.5 (.getFloat p 0)))

      (.setFloat p 100 -8.0)
      (test/is (= -8.0 (.getFloat p 100)))

      (.setFloat p 500 Float/MAX_VALUE)
      (test/is (= Float/MAX_VALUE (.getFloat p 500))))))

(test/deftest test-get-set-double
  (test/testing "Getting and setting doubles"
    (let [p (page/make-page 1024)]
      (.setDouble p 0 3.125)
      (test/is (= 3.125 (.getDouble p 0)))

      (.setDouble p 100 -10.25)
      (test/is (= -10.25 (.getDouble p 100)))

      (.setDouble p 500 Double/MAX_VALUE)
      (test/is (= Double/MAX_VALUE (.getDouble p 500))))))

(test/deftest test-set-float-and-double-mixed
  (test/testing "Setting float and double at the same offset"
    (let [p (page/make-page 1024)]
      (.setFloat p 0 1.5)
      (.setDouble p 4 4.75)

      (test/is (= 1.5 (.getFloat p 0)))
      (test/is (= 4.75 (.getDouble p 4))))))

(test/deftest test-get-set-float-with-special-values
  (test/testing "Getting and setting special float values"
    (let [p (page/make-page 1024)]
      (.setFloat p 0 Float/NaN)
      (test/is (Float/isNaN (.getFloat p 0)))

      (.setFloat p 100 Float/POSITIVE_INFINITY)
      (test/is (= Float/POSITIVE_INFINITY (.getFloat p 100)))

      (.setFloat p 200 Float/NEGATIVE_INFINITY)
      (test/is (= Float/NEGATIVE_INFINITY (.getFloat p 200))))))

(test/deftest test-get-set-double-with-special-values
  (test/testing "Getting and setting special double values"
    (let [p (page/make-page 1024)]
      (.setDouble p 0 Double/NaN)
      (test/is (Double/isNaN (.getDouble p 0)))

      (.setDouble p 100 Double/POSITIVE_INFINITY)
      (test/is (= Double/POSITIVE_INFINITY (.getDouble p 100)))

      (.setDouble p 200 Double/NEGATIVE_INFINITY)
      (test/is (= Double/NEGATIVE_INFINITY (.getDouble p 200))))))
