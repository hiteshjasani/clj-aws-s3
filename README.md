# clj-aws-s3

A simple Clojure library for interacting with AWS S3.

[![Clojars Project](https://img.shields.io/clojars/v/org.jasani/clj-aws-s3.svg)](https://clojars.org/org.jasani/clj-aws-s3)

This library is in early development, so while it does work anything might change.


## Usage

```
(:require [clj-aws-s3.core :as s3 :refer :all])

(def txfrmgr (startup "my-access-key" "my-secret-key"))

;; List objects in bucket
(ls txfrmgr "mybucket")
(ls txfrmgr "mybucket" "sales/")

;; Put objects
(put! "hello world" txfrmgr "mybucket" "hello.txt" {} false)

;; Get objects
(get-obj txfrmgr "mybucket" "hello.txt")

;; Copy objects
(copy! txfrmgr "mybucket" "hello.txt" "mybucket" "goodbye.txt")

;; Delete objects
(delete! txfrmgr "mybucket" "goodbye.txt")

;; Shutdown the txfrmgr
(shutdown txfrmgr)

```

## License

Copyright © 2017 Hitesh Jasani

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
