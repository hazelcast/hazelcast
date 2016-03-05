package com.hazelcast.jet.memory.impl.aggregator;

import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.impl.binarystorage.comparator.StringComparator;
import com.hazelcast.jet.memory.impl.memory.impl.DefaultMemoryContext;
import com.hazelcast.jet.memory.impl.operations.DefaultContainersPull;
import com.hazelcast.jet.memory.impl.operations.tuple.*;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.ElementsReader;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.operations.OperationFactory;
import com.hazelcast.jet.memory.spi.operations.aggregator.Aggregator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SpillingAggregatorTest extends BaseMemoryTest {
    private Aggregator<Tuple> aggregator;
    private IOContext ioContext = new IOContextImpl();
    private final ElementsWriter<Tuple> keyWriter = new TupleKeyElementsWriter();
    private final ElementsWriter<Tuple> valueWriter = new TupleValueElementsWriter();

    protected long blockSize() {
        return 128 * 1024;
    }

    protected long heapSize() {
        return 1024 * 1024 * 1024;
    }

    @Before
    public void setUp() throws Exception {
        init();
    }

    private void initAggregator(BinaryComparator binaryComparator) {
        try {
            initAggregator(binaryComparator, null);
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    private void initAggregator(BinaryComparator binaryComparator,
                                BinaryFunctor binaryFunctor) throws IOException {
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
                65536,//spillingBufferSize
                binaryComparator,
                keyWriter,
                valueWriter,
                new DefaultContainersPull<Tuple>(
                        new Tuple2Factory(),
                        1024
                ),
                binaryFunctor,
                Files.createTempDirectory("hazelcast-jet-spilling").toString(),
                65536,//spillingChunkSize
                1024,//bloomFilterSizeInBytes
                true,
                true
        );
    }

    private void insertElements(ElementsReader<Tuple> keyReader,
                                ElementsReader<Tuple> valueReader,
                                Tuple<String, String> tuple,
                                int start,
                                int elementsCount,
                                int step) throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            tuple.setKey(0, String.valueOf(i));
            tuple.setValue(1, String.valueOf(i));
            inertNewRecord(keyReader, valueReader, tuple, elementsCount, step, i);
        }
    }

    private void insertElementsWithOneValue(ElementsReader<Tuple> keyReader,
                                            ElementsReader<Tuple> valueReader,
                                            Tuple tuple,
                                            int start,
                                            int elementsCount,
                                            int step) throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            tuple.setKey(0, String.valueOf(i));
            tuple.setValue(1, 1);
            inertNewRecord(keyReader, valueReader, tuple, elementsCount, step, i);
        }
    }

    private void inertNewRecord(ElementsReader<Tuple> keyReader,
                                ElementsReader<Tuple> valueReader,
                                Tuple tuple,
                                int elementsCount,
                                int step,
                                int i) throws Exception {
        boolean result = aggregator.putRecord(
                tuple,
                keyReader,
                valueReader
        );

        if (!result) {
            long t = System.currentTimeMillis();

            System.out.println("Start spilling i=" + i);

            aggregator.startSpilling();

            do {
            } while (!aggregator.spillNextChunk());

            aggregator.finishSpilling();

            aggregator.putRecord(
                    tuple,
                    keyReader,
                    valueReader
            );

            System.out.println("Spilled " + elementsCount
                            + " start i=" + i +
                            " time=" + (System.currentTimeMillis() - t)
                            + " step=" + step

            );
        }
    }

    @Test
    public void testString2String() throws Exception {
        BinaryComparator binaryComparator =
                new StringComparator();

        initAggregator(binaryComparator);

        int CNT = 10_000_000;
        int VALUES_CNT = 1;
        int[] markers = new int[CNT];
        Arrays.fill(markers, (byte) 0);

        Tuple<String, String> tuple = new Tuple2<String, String>();
        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();

        long t = System.currentTimeMillis();

        for (int i = 0; i < VALUES_CNT; i++) {
            insertElements(keyReader, valueReader, tuple, 1, CNT, i);
        }

        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));

        Iterator<Tuple> iterator = aggregator.iterator();

        int iterations_cnt = 0;

        String lastKey = "";
        int value_idx = 0;

        while (iterator.hasNext()) {
            Tuple tt = iterator.next();

            if (!lastKey.equals(tt.getKey(0))) {
                if (!(lastKey.equals(""))) {
                    assertEquals(value_idx, VALUES_CNT);
                }

                lastKey = (String) tt.getKey(0);
                value_idx = 1;
            } else {
                value_idx++;
            }

            markers[Integer.valueOf((String) tt.getKey(0)) - 1] += 1;
            iterations_cnt++;
        }

        assertEquals(value_idx, VALUES_CNT);

        assertEquals(CNT * VALUES_CNT, iterations_cnt);

        for (int i = 0; i < CNT; i++) {
            assertTrue(markers[i] > 0);
        }

        for (int i = 0; i < CNT; i++) {
            assertEquals("key " + i, VALUES_CNT, markers[i]);
        }
    }

    @Test
    public void testString2StringAssociativeFunctor() throws Exception {
        initAggregator(new StringComparator(), new SumFunctor());

        int CNT = 500_000;
        int VALUES_CNT = 20;
        Tuple<String, String> tuple = new Tuple2<String, String>();
        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();

        long t = System.currentTimeMillis();

        for (int i = 0; i < VALUES_CNT; i++) {
            insertElementsWithOneValue(keyReader, valueReader, tuple, 1, CNT, i);
            System.out.println("inserted " + CNT);
        }

        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));

        Iterator<Tuple> iterator = aggregator.iterator();

        int iterations_cnt = 0;

        while (iterator.hasNext()) {
            Tuple tt = iterator.next();
            assertEquals(VALUES_CNT, tt.getValue(0));
            iterations_cnt++;
        }

        assertEquals(CNT, iterations_cnt);
    }


    @Test
    public void testString2StringNonAssociativeFunctor() throws Exception {
        initAggregator(new StringComparator(), new NonAssociativeSumFunctor());

        int CNT = 2_000_000;
        int VALUES_CNT = 2;
        Tuple<String, String> tuple = new Tuple2<String, String>();
        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();

        long t = System.currentTimeMillis();

        for (int i = 0; i < VALUES_CNT; i++) {
            insertElementsWithOneValue(keyReader, valueReader, tuple, 1, CNT, i);
            System.out.println("inserted " + CNT);
        }

        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));

        Iterator<Tuple> iterator = aggregator.iterator();

        int iterations_cnt = 0;

        while (iterator.hasNext()) {
            Tuple tt = iterator.next();
            assertEquals(VALUES_CNT, tt.getValue(0));
            iterations_cnt++;
        }

        assertEquals(CNT, iterations_cnt);
    }

    @After
    public void tearDown() throws Exception {
        aggregator.dispose();
        cleanUp();
    }
}
