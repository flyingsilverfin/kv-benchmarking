package storage.transaction;

import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;

public class RocksTransaction implements AutoCloseable {

    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final OptimisticTransactionOptions optimisticTransactionOptions;
    private final Transaction transaction;

    public RocksTransaction(RocksDatabase db) {
        writeOptions = new WriteOptions();
        readOptions = new ReadOptions();
        optimisticTransactionOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        transaction = db.beginTransaction(writeOptions, optimisticTransactionOptions);
        readOptions.setSnapshot(transaction.getSnapshot());
    }

    @Override
    public void close() {
        transaction.close();
        optimisticTransactionOptions.close();
        readOptions.close();
        writeOptions.close();
    }

    public void put(byte[] key, byte[] value) throws RocksDBException {
        transaction.put(key, value);
    }

    public void putUntracked(byte[] key, byte[] value) throws RocksDBException {
        transaction.putUntracked(key, value);
    }

    public void merge(byte[] key, byte[] value) throws RocksDBException {
        transaction.merge(key, value);
    }

    public void mergeUntracked(byte[] key, byte[] value) throws RocksDBException {
        transaction.mergeUntracked(key, value);
    }

    public byte[] get(byte[] key) throws RocksDBException {
        return transaction.get(readOptions, key);
    }

    public byte[] getForUpdate(byte[] key, boolean exclusive) throws RocksDBException {
        return transaction.getForUpdate(readOptions, key, exclusive);
    }

    public RocksIterator iterate(byte[] key) throws RocksDBException {
        RocksIterator iterator = transaction.getIterator(readOptions);
        iterator.seek(key);
        return iterator;
    }

    public void delete(byte[] key) throws RocksDBException {
        transaction.delete(key);
    }

    public void updateReadSnapshot(RocksDatabase db) {
        readOptions.setSnapshot(db.getSnapshot());
    }

    public void commit() throws RocksDBException {
        transaction.commit();
    }

    public void rollback() throws RocksDBException {
        transaction.rollback();
    }

    public static byte[] lexicographicBytes(int value) {
        byte[] bytes = new byte[4];
        bytes[3] = (byte) (value);
        bytes[2] = (byte) (value >>= 8);
        bytes[1] = (byte) (value >>= 8);
        bytes[0] = (byte) ((value >> 8) ^ 0x80);
        return bytes;
    }

    public static byte[] toBytes(final int data) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(data);
        return buf.array();
    }

    public static int toInt(byte[] data) {
        if (data == null || data.length != 4) return -1;
        ByteBuffer buf = ByteBuffer.wrap(data);
        return buf.getInt();
    }

    public static int getInt(RocksTransaction tx, int key) throws RocksDBException {
        return toInt(tx.get(toBytes(key)));
    }

    public static int[] getInts(RocksTransaction tx, int... keys) throws RocksDBException {
        final int[] ints = new int[keys.length];
        for (int i = 0; i < keys.length; i++) ints[i] = getInt(tx, keys[i]);
        return ints;
    }
}
