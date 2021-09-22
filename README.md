# kv-benchmarking
Benchmarking for rocksdb and other kv storage engines


## Requirements

1. Install Bazel
2. Install Java
3. Place the SpeeDB JNI jar into `dependencies/tools/speedb-linux64.jar`

## Running benchmarks

1. We can run the benchmark directly from `bazel run`:
```
bazel run //benchmark:benchmark-mixed \
--test_sharding_strategy=disabled \
--test_output=streamed \
--cache_test_results=no \
--sandbox_debug
```

2. The storage DB path will be written to the command line, it will contain the SS tables and LOG file.

3. **Perform `bazel clean --expunge` to force clean all built JARs and previous benchmark data**


### Running with RocksDB or SpeeDB

The configuration for which storage engine to use is found in two BUILD files: `benchmark/BUILD` and `transaction/BUILD`.

To use RocksDB, uncomment the first of the dependencies in both:
```
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni"
#        "//dependencies/tools:speedb-rocksdb-jni"
```

To use SpeeDB, change the comments to use the second dependency:
```
#        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni"
        "//dependencies/tools:speedb-rocksdb-jni"
```

### Configuration

The configuration for the benchmark itself is in `benchmark/MixedBenchmark.java`

The configuration for the storage layer is in `transaction/RocksDatabase.java`
