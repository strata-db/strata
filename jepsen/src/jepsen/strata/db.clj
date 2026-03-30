(ns jepsen.strata.db
  "Jepsen DB implementation: installs, starts, stops, and wipes Strata nodes."
  (:require [clojure.string           :as str]
            [clojure.tools.logging    :refer [info warn]]
            [jepsen.db                :as db]
            [jepsen.control           :as c]
            [jepsen.control.util      :as cu]))

;; ── Paths ─────────────────────────────────────────────────────────────────────

(def binary      "/usr/local/bin/strata")
(def data-dir    "/var/lib/strata")
(def log-file    "/var/log/strata.log")
(def pid-file    "/var/run/strata.pid")
(def metrics-port 2380)   ; HTTP /healthz — distinct from the gRPC port (2379)

;; ── Helpers ───────────────────────────────────────────────────────────────────

(defn node-id
  "Stable node identifier derived from the hostname."
  [node]
  (name node))

(defn endpoint
  "etcd endpoint URL for a given node."
  [node]
  (str "http://" (name node) ":2379"))

(defn await-ready!
  "Polls strata's HTTP /healthz until it returns 200, up to 30 s."
  [node]
  (let [url      (str "http://localhost:" metrics-port "/healthz")
        deadline (+ (System/currentTimeMillis) 30000)]
    (loop []
      (let [ok? (try (c/exec :curl :-sf url) true
                     (catch Exception _ false))]
        (cond
          ok?
          (info node "strata is ready")

          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 500) (recur))

          :else
          (let [log-tail (try (c/exec :tail :-n "50" log-file)
                              (catch Exception _ "(log unavailable)"))]
            (throw (ex-info (str "strata did not become ready in time on " node
                                 "\n--- strata log ---\n" log-tail)
                            {:node node :url url}))))))))

;; ── AWS credentials ───────────────────────────────────────────────────────────

(defn install-aws-credentials!
  "Write ~/.aws/credentials and ~/.aws/config so the AWS SDK finds them
  regardless of how the process is launched (SSH sessions don't inherit
  the container's environment variables)."
  []
  (c/su
    (c/exec :mkdir :-p "/root/.aws")
    (c/exec :bash :-c "printf '[default]\\naws_access_key_id = jepsen\\naws_secret_access_key = jepsen123\\n' > /root/.aws/credentials")
    (c/exec :bash :-c "printf '[default]\\nregion = us-east-1\\n' > /root/.aws/config")))

;; ── Start / stop ──────────────────────────────────────────────────────────────

(defn start!
  [test node]
  (c/su
    (c/exec :mkdir :-p data-dir)
    (cu/start-daemon!
      {:logfile log-file
       :pidfile pid-file
       :chdir   "/tmp"}
      binary
      :--listen        "0.0.0.0:2379"
      :--data-dir      data-dir
      :--node-id       (node-id node)
      :--s3-endpoint   "http://minio:9000"
      :--s3-bucket     "jepsen"
      :--metrics-addr  (str "0.0.0.0:" metrics-port)
      :--log-level     "warn")))

(defn stop!
  [test node]
  (c/su (cu/stop-daemon! binary pid-file)))

(defn wipe!
  [test node]
  (c/su
    (c/exec :rm :-rf data-dir)
    (c/exec :mkdir :-p data-dir)))

;; ── DB record ─────────────────────────────────────────────────────────────────

(defn db
  "Returns a Jepsen DB that manages Strata on each node."
  []
  (reify
    db/DB
    (setup! [_ test node]
      (info node "starting strata")
      (install-aws-credentials!)
      (start! test node)
      (await-ready! node))

    (teardown! [_ test node]
      (info node "tearing down strata")
      (stop!  test node)
      (wipe!  test node))

    db/Primary
    ;; Strata uses S3-based leader election; any node can accept writes.
    (primaries     [_ test]       (:nodes test))
    (setup-primary! [_ test node] nil)

    db/LogFiles
    (log-files [_ test node] [log-file])))
