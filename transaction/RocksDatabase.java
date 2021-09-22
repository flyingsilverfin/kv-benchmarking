package storage.transaction;

import org.rocksdb.*;

public class RocksDatabase implements AutoCloseable {

    private final Options options;
    private final OptimisticTransactionDB db;

    public RocksDatabase(String dbPath) throws RocksDBException {
        this(dbPath, false);
    }

    public RocksDatabase(String dbPath, boolean mergeOperator) throws RocksDBException {
        options = new Options().setCreateIfMissing(true);//.useCappedPrefixExtractor(3);
        if (mergeOperator) options.setMergeOperator(new UInt64AddOperator());
        db = OptimisticTransactionDB.open(options, dbPath);
    }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    public Transaction beginTransaction(WriteOptions writeOptions, OptimisticTransactionOptions optimisticTransactionOptions) {
        return db.beginTransaction(writeOptions, optimisticTransactionOptions);
    }

    public Snapshot getSnapshot() {
        return db.getSnapshot();
    }

    public Checkpoint createCheckpoint() {
        return Checkpoint.create(db);
    }
}
