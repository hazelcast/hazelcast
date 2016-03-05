package com.hazelcast.jet.memory.impl.aggregator;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.ElementsReader;
import com.hazelcast.jet.memory.spi.operations.OperationFactory;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.impl.operations.tuple.Tuple2Factory;
import com.hazelcast.jet.memory.spi.operations.aggregator.Aggregator;
import com.hazelcast.jet.memory.impl.memory.impl.DefaultMemoryContext;
import com.hazelcast.jet.memory.impl.operations.DefaultContainersPull;
import com.hazelcast.jet.memory.impl.binarystorage.comparator.IntComparator;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleKeyElementsReader;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleKeyElementsWriter;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleValueElementsReader;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleValueElementsWriter;

import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.memory.impl.binarystorage.comparator.StringComparator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Iterator;

import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SimpleAggregatorTest extends BaseMemoryTest {
    private Aggregator<Tuple> aggregator;
    private IOContext ioContext = new IOContextImpl();
    private final ElementsWriter<Tuple> keyWriter = new TupleKeyElementsWriter();
    private final ElementsWriter<Tuple> valueWriter = new TupleValueElementsWriter();


    protected long heapSize() {
        return 1024L * 1024L * 1024L + 200 * 1024 * 1024;
    }

    protected long blockSize() {
        return 128 * 1024;
    }

    @Before
    public void setUp() throws Exception {
        init();
    }

    private void initAggregator(BinaryComparator binaryComparator) {
        initAggregator(binaryComparator, null);
    }

    private void initAggregator(BinaryComparator binaryComparator,
                                BinaryFunctor binaryFunctor) {
        memoryContext = new DefaultMemoryContext(
                heapMemoryPool,
                nativeMemoryPool,
                blockSize(),
                useBigEndian()
        );

        aggregator = OperationFactory.getAggregator(
                memoryContext,
                ioContext,
                MemoryChainingType.HEAP,
                1024,//partitionCount
                1024,//spillingBufferSize
                binaryComparator,
                keyWriter,
                valueWriter,
                new DefaultContainersPull<Tuple>(
                        new Tuple2Factory(),
                        1024
                ),
                binaryFunctor,
                "",
                1024,//spillingChunkSize
                1024,//bloomFilterSizeInBytes
                false,
                true
        );
    }

    @Test
    public void testString2String() throws Exception {
        initAggregator(new StringComparator());

        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        Tuple<String, String> tuple = new Tuple2<String, String>();
        int CNT = 10_000_000;
        byte[] markers = new byte[CNT];
        Arrays.fill(markers, (byte) 0);
        long t = System.currentTimeMillis();
        insertElements(keyReader, valueReader, tuple, 1, CNT);
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));

        Iterator<Tuple> iterator = aggregator.iterator();

        long time = System.currentTimeMillis();
        while (iterator.hasNext()) {
            Tuple tt = iterator.next();
            markers[Integer.valueOf((String) tt.getKey(0)) - 1] = 1;
        }
        System.out.println("SelectionTime=" + (System.currentTimeMillis() - time));

        for (int i = 0; i < CNT; i++) {
            assertEquals(markers[i], 1);
        }
    }

    private void insertElements(ElementsReader<Tuple> keyReader,
                                ElementsReader<Tuple> valueReader,
                                Tuple<String, String> tuple,
                                int start,
                                int elementsCount) throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            tuple.setKey(0, String.valueOf(i));
            tuple.setValue(1, String.valueOf(i));
            if (!aggregator.putRecord(
                    tuple,
                    keyReader,
                    valueReader
            )) {
                throw new IllegalStateException("Not enough memory (spilling is turned off)");
            }
        }
    }

    private void insertIntElements(ElementsReader<Tuple> keyReader,
                                   ElementsReader<Tuple> valueReader,
                                   Tuple<String, Integer> tuple,
                                   int start,
                                   int elementsCount) throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            tuple.setKey(0, String.valueOf(i));
            tuple.setValue(1, 1);
            aggregator.putRecord(
                    tuple,
                    keyReader,
                    valueReader
            );
        }
    }

    @Test
    public void testInt2Int() throws Exception {
        initAggregator(new IntComparator());

        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        int CNT = 1_000_000;
        byte[] markers = new byte[CNT];
        Tuple<Integer, Integer> tuple = new Tuple2<Integer, Integer>();

        long t = System.currentTimeMillis();
        for (int i = 1; i <= CNT; i++) {
            tuple.setKey(0, i);
            tuple.setValue(1, i);

            aggregator.putRecord(
                    tuple,
                    keyReader,
                    valueReader
            );
        }

        Iterator<Tuple> iterator = aggregator.iterator();

        while (iterator.hasNext()) {
            Tuple tt = iterator.next();
            markers[Integer.valueOf((Integer) tt.getKey(0)) - 1] = 1;
        }

        for (int i = 0; i < CNT; i++) {
            assertEquals(markers[i], 1);
        }

        System.out.println("Time=" + (System.currentTimeMillis() - t));
    }

    @Test
    public void testString2StringMultiValue() throws Exception {
        initAggregator(new StringComparator());
        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        Tuple<String, String> tuple = new Tuple2<String, String>();
        int KEYS_CNT = 100_000;
        int VALUES_CNT = 10;
        byte[] markers = new byte[KEYS_CNT];
        Arrays.fill(markers, (byte) 0);
        long t = System.currentTimeMillis();

        for (int i = 1; i <= 100_000; i++) {
            tuple.setKey(0, String.valueOf(i));
            for (int ii = 0; ii < 10; ii++) {
                tuple.setValue(0, String.valueOf(ii));

                aggregator.putRecord(
                        tuple,
                        keyReader,
                        valueReader
                );
            }
        }

        Iterator<Tuple> iterator = aggregator.iterator();
        int iterations_count = 0;

        while (iterator.hasNext()) {
            Tuple tt = iterator.next();
            markers[Integer.valueOf((String) tt.getKey(0)) - 1] = 1;
            iterations_count++;
        }

        assertEquals(iterations_count, KEYS_CNT * VALUES_CNT);

        for (int i = 0; i < KEYS_CNT; i++) {
            assertEquals(markers[i], 1);
        }

        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
    }

    @Test
    public void testString2StringAssociativeFunctor() throws Exception {
        initAggregator(new StringComparator(), new SumFunctor());
        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        Tuple<String, Integer> tuple = new Tuple2<String, Integer>();

        int KEYS_CNT = 100_000;
        int VALUES_CNT = 10;
        byte[] markers = new byte[KEYS_CNT];
        Arrays.fill(markers, (byte) 0);
        long t = System.currentTimeMillis();

        for (int i = 1; i <= KEYS_CNT; i++) {
            tuple.setKey(0, String.valueOf(i));

            for (int ii = 0; ii < VALUES_CNT; ii++) {
                tuple.setValue(0, 1);
                aggregator.putRecord(
                        tuple,
                        keyReader,
                        valueReader
                );
            }
        }

        Iterator<Tuple> iterator = aggregator.iterator();
        int iterations_count = 0;

        while (iterator.hasNext()) {
            Tuple<String, Integer> tt = iterator.next();
            markers[Integer.valueOf(tt.getKey(0)) - 1] = 1;
            iterations_count++;
            int v = tt.getValue(0);
            assertEquals(VALUES_CNT, v);
        }

        assertEquals(iterations_count, KEYS_CNT);

        for (int i = 0; i < KEYS_CNT; i++) {
            assertEquals(markers[i], 1);
        }

        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
    }


    @Test
    public void testString2StringNonAssociativeFunctor() throws Exception {
        initAggregator(new StringComparator(), new NonAssociativeSumFunctor());
        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        Tuple<String, Integer> tuple = new Tuple2<String, Integer>();

        int KEYS_CNT = 100_000;
        int VALUES_CNT = 10;
        byte[] markers = new byte[KEYS_CNT];
        Arrays.fill(markers, (byte) 0);
        long t = System.currentTimeMillis();

        for (int i = 1; i <= KEYS_CNT; i++) {
            tuple.setKey(0, String.valueOf(i));

            for (int ii = 0; ii < VALUES_CNT; ii++) {
                tuple.setValue(0, 1);
                aggregator.putRecord(
                        tuple,
                        keyReader,
                        valueReader
                );
            }
        }

        Iterator<Tuple> iterator = aggregator.iterator();
        int iterations_count = 0;

        while (iterator.hasNext()) {
            Tuple<String, Integer> tt = iterator.next();
            markers[Integer.valueOf(tt.getKey(0)) - 1] = 1;
            iterations_count++;
            int v = tt.getValue(0);
            assertEquals(VALUES_CNT, v);
        }

        assertEquals(iterations_count, KEYS_CNT);

        for (int i = 0; i < KEYS_CNT; i++) {
            assertEquals(markers[i], 1);
        }

        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
    }

    @Test
    public void testString2StringManyElements() throws Exception {
        initAggregator(new StringComparator());

        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        Tuple<String, String> tuple = new Tuple2<String, String>();

        int CNT = 1_000_000;
        long t = System.currentTimeMillis();

        insertElements(keyReader, valueReader, tuple, 1, CNT);
        insertElements(keyReader, valueReader, tuple, 1, CNT);
        insertElements(keyReader, valueReader, tuple, 1, CNT);

        Iterator<Tuple> iterator = aggregator.iterator();
        long iterations_count = 0;

        String k = null;
        int localCNt = 0;
        while (iterator.hasNext()) {
            Tuple tt = iterator.next();

            if (k == null) {
                k = (String) tt.getKey(0);
            } else {
                localCNt++;
                assertEquals(k, tt.getKey(0));
                if (localCNt == 2) {
                    k = null;
                    localCNt = 0;
                }
            }

            iterations_count++;
        }

        assertEquals(iterations_count, 3 * CNT);
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
    }


    @Test
    public void testString2StringManyElementsAndFunctor() throws Exception {
        initAggregator(new StringComparator(), new SumFunctor());

        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        Tuple<String, Integer> tuple = new Tuple2<String, Integer>();

        int CNT = 3_000_000;
        long t = System.currentTimeMillis();

        insertIntElements(keyReader, valueReader, tuple, 1, CNT);
        insertIntElements(keyReader, valueReader, tuple, 1, CNT);
        insertIntElements(keyReader, valueReader, tuple, 1, CNT);

        Iterator<Tuple> iterator = aggregator.iterator();

        long iterations_count = 0;

        while (iterator.hasNext()) {
            Tuple tt = iterator.next();
            assertEquals(3, tt.getValue(0));
            iterations_count++;
        }

        assertEquals(iterations_count, CNT);
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
    }

    @Test
    public void testString2StringManyElementsAndNonAssociativeFunctor() throws Exception {
        initAggregator(new StringComparator(), new NonAssociativeSumFunctor());

        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();
        Tuple<String, Integer> tuple = new Tuple2<String, Integer>();

        int CNT = 3_000_000;
        long t = System.currentTimeMillis();

        insertIntElements(keyReader, valueReader, tuple, 1, CNT);
        insertIntElements(keyReader, valueReader, tuple, 1, CNT);
        insertIntElements(keyReader, valueReader, tuple, 1, CNT);

        Iterator<Tuple> iterator = aggregator.iterator();

        long iterations_count = 0;

        while (iterator.hasNext()) {
            Tuple tt = iterator.next();
            assertEquals(3, tt.getValue(0));
            iterations_count++;
        }

        assertEquals(iterations_count, CNT);
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
    }

    @After
    public void tearDown() throws Exception {
        aggregator.dispose();
        cleanUp();
    }
}
