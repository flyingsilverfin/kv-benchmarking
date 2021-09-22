package storage.benchmark;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import storage.transaction.RocksDatabase;
import storage.transaction.RocksTransaction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Random;

import static java.util.Comparator.reverseOrder;

public class MixedBenchmark {

    static String dbPath;
    static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
    static byte[] empty = new byte[0];

    @BeforeClass
    public static void beforeClass() {
        dbPath = TEMP_DIR + "rocks_transaction_testdb";
        System.out.println("DB path is: " + dbPath);
    }

    @Before
    public void before() throws IOException {
        if (Files.exists(Paths.get(dbPath))) {
            Files.walk(Paths.get(dbPath)).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    private static class ByteArrayGenerator {

        private final Random random;
        private final int lowerBoundLength;
        private final int upperBoundLength;

        ByteArrayGenerator(Random random, int lowerBoundLength, int upperBoundLength) {
            this.random = random;
            this.lowerBoundLength = lowerBoundLength;
            this.upperBoundLength = upperBoundLength;
        }

        public byte[] next() {
            int length = lowerBoundLength + random.nextInt(upperBoundLength - lowerBoundLength);
            byte[] array = new byte[length];
            random.nextBytes(array);
            return array;
        }

        public byte[] next(int length) {
            byte[] array = new byte[length];
            random.nextBytes(array);
            return array;
        }
    }

    @Test
    public void benchmark() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
//            long initialKeys = 1_000_000_000; // OOM with speedb on 8gb machine!
            long initialKeys = 200_000_000;
            Random random = new Random(0);
            ByteArrayGenerator arrayGenerator = new ByteArrayGenerator(random, 12, 48);
            initialiseKeys(db, arrayGenerator, initialKeys);
            benchmark_operations(db, arrayGenerator);
        }
    }

    private void benchmark_operations(RocksDatabase db, ByteArrayGenerator arrayGenerator) throws RocksDBException {
        Random actionGenerator = new Random(0);
        int evaluations = 100_000;
        int puts = 0;
        int gets = 0;
        int getValues = 0;
        int seeksAndIterates = 0;
        long iterateValues = 0;
        Instant start = Instant.now();
        for (int i = 0; i < evaluations; i++) {
            int action = actionGenerator.nextInt(8);
            switch (action) {
                case 0:
                    puts++;
                    putRandomValues(db, arrayGenerator);
                    break;
                case 1:
                case 2:
                    gets++;
                    getValues += doGet(db, arrayGenerator);
                    break;
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    seeksAndIterates++;
                    iterateValues += doSeekAndIterate(db, arrayGenerator);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        System.out.printf("Finished %d evaluations in %d seconds \n", evaluations, Duration.between(start, Instant.now()).get(ChronoUnit.SECONDS));
        System.out.printf("Stats: \n");
        System.out.printf("  puts: %d. Mean nanos per put: %f \n", putUntrackeds, (double) putUntrackedNanos / putUntrackeds);
        System.out.printf("  gets: %d. Mean nanos per get: %f \n", gets, (double) getNanos/this.gets);
        System.out.printf("  seeks and iterates: %d. Mean nanos per seek: %f. Mean nanos per next & isValid: %f \n", seeks, (double) seekNanos/seeks, (double)nextAndValidNanos/nexts);

        System.out.printf("black hole to prevent optimisations %d %d", getValues, iterateValues);
    }

    long seekNanos = 0;
    long seeks = 0;
    long nextAndValidNanos = 0;
    long nexts = 0;
    private int doSeekAndIterate(RocksDatabase db, ByteArrayGenerator arrayGenerator) throws RocksDBException {
        int values = 0;
        try (RocksTransaction tx = new RocksTransaction(db)) {
            RocksIterator iterator = null;
            for (int i = 0; i < 10; i++) {
                byte[] bytes = arrayGenerator.next();
                byte[] prefix = Arrays.copyOf(bytes, bytes.length / 4);
                Instant start = Instant.now();
                if (iterator == null) iterator = tx.iterate(prefix);
                else iterator.seek(prefix);
                seekNanos += Duration.between(start, Instant.now()).getNano();
                seeks++;
                start = Instant.now();
                for (int j = 0; j < 2 && iterator.isValid(); j++) {
                    iterator.next();
                    byte[] found = iterator.key();
                    values += found[0];
                    nexts++;
                }
                nextAndValidNanos += Duration.between(start, Instant.now()).getNano();
            }
            iterator.close();
        }
        return values;
    }

    long getNanos = 0;
    long gets = 0;
    private int doGet(RocksDatabase db, ByteArrayGenerator arrayGenerator) throws RocksDBException {
        int retrieved = 0;
        try (RocksTransaction tx = new RocksTransaction(db)) {
            for (int i = 0; i < 10; i++) {
                byte[] bytes = arrayGenerator.next();
                Instant start = Instant.now();
                byte[] value = tx.get(bytes);
                getNanos += Duration.between(start, Instant.now()).getNano();
                gets++;
                if (value != null) retrieved++;
            }
        }
        return retrieved;
    }

    long putUntrackedNanos = 0;
    long putUntrackeds = 0;
    private void putRandomValues(RocksDatabase db, ByteArrayGenerator arrayGenerator) throws RocksDBException {
        // put 10 new values in a transaction and commit
        try (RocksTransaction tx = new RocksTransaction(db)) {
            for (int i = 0; i < 10; i++) {
                byte[] key = arrayGenerator.next();
                Instant start = Instant.now();
                tx.putUntracked(key, value(key, arrayGenerator));
                putUntrackedNanos += Duration.between(start, Instant.now()).getNano();
                putUntrackeds++;
            }
            tx.commit();
        }
    }

    private void initialiseKeys(RocksDatabase db, ByteArrayGenerator arrayGenerator, long keys) throws RocksDBException {
        System.out.println("Initialising keys");
        int txBatch = 10_000;
        int batches = (int)(keys / txBatch);
        Instant start = Instant.now();
        long keysInserted = 0;
        for (int i = 0; i < batches; i++) {
            if (i % 100 == 0) System.out.println("inserted: " + keysInserted);
            try (RocksTransaction tx = new RocksTransaction(db)) {
                for (int j = 0; j < txBatch; j++) {
                    byte[] key = arrayGenerator.next();
                    keysInserted++;
                    tx.putUntracked(key, value(key, arrayGenerator));
                }
                tx.commit();
            }
        }
        System.out.printf("Finished initialising %d keys in %d seconds \n", keysInserted, Duration.between(start, Instant.now()).get(ChronoUnit.SECONDS));
    }

    private byte[] value(byte[] key, ByteArrayGenerator arrayGenerator) {
        if (key.length < 30) return empty;
        else return arrayGenerator.next(16);
    }

}
