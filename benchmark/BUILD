java_test(
    name = "benchmark-mixed",
    srcs = [
        "MixedBenchmark.java"
    ],
    test_class = "storage.benchmark.MixedBenchmark",
    deps = [
        "//transaction",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni",
#        "//dependencies/tools:speedb-rocksdb-jni"
    ],
    size = "enormous",
)
