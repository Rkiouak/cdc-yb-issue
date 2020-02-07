(ns util.url-and-email
  (:require [cemerick.url :as url]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as string]))

(def non-empty-string-alphanumeric
  "Generator for non-empty alphanumeric strings"
  (gen/such-that #(not= "" %)
                 (gen/string-alphanumeric)))

(def non-empty-url-encoded-string
  "Generator for url-encoded strings"
  (gen/fmap
   url/url-encode
   (gen/such-that #(not= "" %)
                  (gen/string))))

(def email-gen
  "Generator for email addresses"
  (gen/fmap
   (fn [[name host tld]]
     (str name "@" host "." tld))
   (gen/tuple
    non-empty-string-alphanumeric
    non-empty-string-alphanumeric
    non-empty-string-alphanumeric)))

(def url-path-gen
  "Generator that creates a string made up of one or more forward slash-separated non-empty strings"
  (gen/fmap #(->> %
                  (map url/url-encode)
                  (interleave (repeat "/"))
                  (apply str))
            (gen/such-that #(not= [] %)
                           (gen/vector
                            (gen/such-that #(not= "" %)
                                           (gen/string-alphanumeric))))))

(def url-gen
  "Generator for generating URLs; note that it may generate http URLs on port 443 and https URLs on port 80,
  and that usernames and passwords only use alphanumerics"
  (gen/fmap (partial apply (comp #(subs % 0 10) str url/->URL))
            (gen/tuple
              ;; protocol
             (gen/elements #{"http" "https"})
              ;; username
             (gen/string-alphanumeric)
              ;; password
             (gen/string-alphanumeric)
              ;; host
             (gen/string-alphanumeric)
              ;; port
             (gen/choose 1 65535)
              ;; path
             url-path-gen
              ;; query
             (gen/map
              non-empty-url-encoded-string
              non-empty-url-encoded-string
              {:max-elements 10})
              ;; anchor
             (gen/fmap url/url-encode (gen/string)))))

(s/def ::email
  (s/with-gen
    #(re-matches #".+@.+\..+" %)
    (fn [] email-gen)))

(s/def ::url
  (s/with-gen
    #(try (url/url %) (catch Throwable t false))
    (fn [] url-gen)))