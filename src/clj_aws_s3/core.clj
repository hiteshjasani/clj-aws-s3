(ns clj-aws-s3.core
  (:import
   (com.amazonaws.auth AWSCredentials BasicAWSCredentials)
   (com.amazonaws.services.s3 AmazonS3Client)
   (com.amazonaws.services.s3.model ObjectListing ObjectMetadata PutObjectRequest S3Object S3ObjectSummary)
   (com.amazonaws.services.s3.transfer TransferManager)
   (java.io ByteArrayInputStream InputStream)
   (java.nio.charset StandardCharsets)
   ))

(defprotocol KeyValueProtocol
  "Enabled objects to be converted to key-values pairs as a hash."
  (as-key-values [obj]))

(extend-type S3ObjectSummary
  KeyValueProtocol
  (as-key-values [obj]
    ;; http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3ObjectSummary.html
    {:key           (.getKey obj)
     :bucket-name   (.getBucketName obj)
     :last-modified (.getLastModified obj)
     :size          (.getSize obj)
     :etag          (.getETag obj)
     :storage-class (.getStorageClass obj)
     }))

(extend-type ObjectMetadata
  KeyValueProtocol
  (as-key-values [obj]
    ;; http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ObjectMetadata.html
    {:cache-control           (.getCacheControl obj)
     :content-disposition     (.getContentDisposition obj)
     :content-encoding        (.getContentEncoding obj)
     :content-language        (.getContentLanguage obj)
     :content-length          (.getContentLength obj)
     :content-md5             (.getContentMD5 obj)
     :content-range           (.getContentRange obj)
     :content-type            (.getContentType obj)
     :etag                    (.getETag obj)
     :expiration-time         (.getExpirationTime obj)
     :expiration-time-rule-id (.getExpirationTimeRuleId obj)
     :http-expires-date       (.getHttpExpiresDate obj)
     :instance-length         (.getInstanceLength obj)
     :last-modified           (.getLastModified obj)
     :ongoing-restore         (.getOngoingRestore obj)
     :part-count              (.getPartCount obj)
     :raw-metadata            (.getRawMetadata obj)
     :replication-status      (.getReplicationStatus obj)
     :restore-expiration-time (.getRestoreExpirationTime obj)
     :sse-algorithm           (.getSSEAlgorithm obj)
     :sse-aws-kms-key-id      (.getSSEAwsKmsKeyId obj)
     :sse-customer-algorithm  (.getSSECustomerAlgorithm obj)
     :sse-customer-key-md5    (.getSSECustomerKeyMd5 obj)
     :storage-class           (.getStorageClass obj)
     :user-metadata           (.getUserMetadata obj)
     :version-id              (.getVersionId obj)
     :requester-charged?      (.isRequesterCharged obj)
     }))

(defprotocol S3ClientProtocol
  "Enables object to obtain an AmazonS3Client object."
  (get-client [obj]))

(extend-type TransferManager
  S3ClientProtocol
  (get-client [obj]
    (.getAmazonS3Client obj)))

(extend-type AmazonS3Client
  S3ClientProtocol
  (get-client [obj] obj))


(defn get-object-listing
  "
   Return: ObjectListing
  "
  ([txfr-mgr-or-client
    ^String bucket-name]
   (ls txfr-mgr-or-client bucket-name nil))
  ([txfr-mgr-or-client
    ^String bucket-name
    ^String object-key-prefix]
   (-> (get-client txfr-mgr-or-client)
       ((fn [client]
          (if (seq object-key-prefix)
            (.listObjects client bucket-name object-key-prefix)
            (.listObjects client bucket-name)))))))

(defn get-object
  "Return: S3Object"
  [txfr-mgr-or-client
   ^String bucket-name
   ^String object-key]
  (-> (get-client txfr-mgr-or-client)
      (.getObject bucket-name object-key)))

(defn get-object-meta
  "Return: ObjectMetadata"
  [txfr-mgr-or-client
   ^String bucket-name
   ^String object-key]
  (-> (get-client txfr-mgr-or-client)
      (.getObjectMetadata bucket-name object-key)))

(defn ls
  "Quick listing of objects in a bucket.  Returns the metadata on objects available from S3ObjectSummary as
   as hash of key/value pairs.

   Runtime:  1 query to S3

   See:
     * http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3ObjectSummary.html
  "
  ([txfr-mgr-or-client
    ^String bucket-name]
   (ls-quick txfr-mgr-or-client bucket-name nil))
  ([txfr-mgr-or-client
    ^String bucket-name
    ^String object-key-prefix]
   (->> (if (seq object-key-prefix)
          (get-object-listing txfr-mgr-or-client bucket-name object-key-prefix)
          (get-object-listing txfr-mgr-or-client bucket-name))
        .getObjectSummaries
        (mapv as-key-values))))

(defn ls-long
  "Detailed metadata listing of objects in a bucket.  Returns the metadata on objects available from S3ObjectSummary
   and ObjectMetadata.

   Runtime:  n+1 queries to S3 where n is the number of objects.

   See:
     * http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3ObjectSummary.html
     * http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ObjectMetadata.html
  "
  ([txfr-mgr-or-client
    ^String bucket-name]
   (ls-quick txfr-mgr-or-client bucket-name nil))
  ([txfr-mgr-or-client
    ^String bucket-name
    ^String object-key-prefix]
   (->> (if (seq object-key-prefix)
          (get-object-listing txfr-mgr-or-client bucket-name object-key-prefix)
          (get-object-listing txfr-mgr-or-client bucket-name))
        .getObjectSummaries
        (mapv (fn [obj-summary]
                (let [obj-sum-hash (as-key-values obj-summary)]
                  (-> (get-object-meta txfr-mgr-or-client bucket-name (:key obj-sum-hash))
                      as-key-values
                      (merge obj-sum-hash)))))
        )))


(defn copy
  "Copy an object from one bucket/key to another bucket/key entirely on S3

   Runtime:  1 request to S3
  "
  ([^TransferManager txfr-mgr
    ^String src-bucket-name
    ^String src-key
    ^String dst-bucket-name
    ^String dst-key]
   (copy txfr-mgr src-bucket-name src-key dst-bucket-name dst-key false))
  ([^TransferManager txfr-mgr
    ^String src-bucket-name
    ^String src-key
    ^String dst-bucket-name
    ^String dst-key
    ^Boolean async?]
   (let [job (.copy txfr-mgr src-bucket-name src-key dst-bucket-name dst-key)]
     (when-not async?
       (.waitForCompletion job)))))

(defprotocol PutProtocol
  "Enable PUT (uploading/assigning content to a key) in S3"
  (put [content txfr-mgr bucket-name object-key metadata async?]))

(extend-type String
  PutProtocol
  (put [content txfr-mgr bucket-name object-key metadata async?]
    (let [istream (ByteArrayInputStream. (.getBytes content StandardCharsets/UTF_8))
          md (ObjectMetadata.)]
      (.setContentType md (or (:content-type metadata) "text/plain"))
      (.setContentLength md (count content))
      (let [job (.upload txfr-mgr bucket-name object-key istream md)]
        (when-not async?
          (.waitForCompletion job))))))

(extend-type InputStream
  PutProtocol
  (put [content txfr-mgr bucket-name object-key metadata async?]
    (let [istream content
          md (ObjectMetadata.)]
      (.setContentType md (or (:content-type metadata) "text/plain"))
      (if (:content-length metadata)
        (.setContentLength md (:content-length metadata)))
      (let [job (.upload txfr-mgr bucket-name object-key istream md)]
        (when-not async?
          (.waitForCompletion job))))))

(defn get
  "Get content from S3 as a string"
  [^TransferManager txfr-mgr
   ^String bucket-name
   ^String object-key
   ]
  (let [obj ^S3Object (.getObject (get-client txfr-mgr) bucket-name object-key)
        metadata (-> obj .getObjectMetadata as-key-values)
        size (:content-length metadata)
        arr-bytes (byte-array size)
        istream (.getObjectContent obj)]
    (.read istream arr-bytes 0 size)
    (String. arr-bytes)))

(defn startup
  "Create a TransferManager instance which should be kept and reused in the application.
   TransferManager is thread-safe.

   See:
     * http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManager.html
  "
  ([^clojure.lang.PersistentArrayMap creds] (startup (:access-key creds) (:secret-key creds)))
  ([^String access-key
    ^String secret-key]
   (-> (BasicAWSCredentials. access-key secret-key)
       (TransferManager.))))

(defn shutdown
  "Forceful shutdown of the TransferManager instance.  This will NOT wait for transfers to be completed.
  "
  [^TransferManager txfr-mgr]
  (.shutdown txfr-mgr))
