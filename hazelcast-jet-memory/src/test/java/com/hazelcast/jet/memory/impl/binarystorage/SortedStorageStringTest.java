package com.hazelcast.jet.memory.impl.binarystorage;

import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleKeyElementsWriter;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotIterator;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleValueElementsWriter;
import com.hazelcast.jet.memory.impl.binarystorage.comparator.StringComparator;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;
import com.hazelcast.jet.memory.api.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.jet.memory.impl.binarystorage.sorted.BinaryKeyValueOpenAddressingSortedStorage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SortedStorageStringTest extends BaseMemoryTest {
    @Before
    public void setUp() throws Exception {
        init();
    }

    @Test
    public void openAddressingHeapStorageTest() {
        openAddressingSortedBaseTest(heapMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingHeapStorageTest10() {
        openAddressingSortedBaseTest(heapMemoryBlock, 100_000, 10);
    }

    @Test
    public void openAddressingNativeStorageTest() {
        openAddressingSortedBaseTest(nativeMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingNativeStorageTest10() {
        openAddressingSortedBaseTest(nativeMemoryBlock, 100_000, 10);
    }

    @Test
    public void treeMapTest() throws IOException {
        long time = System.currentTimeMillis();
        final Map<String, String> map = new TreeMap<>();

        for (int idx = 1; idx <= 1_000_000; idx++) {
            map.put(String.valueOf(idx), String.valueOf(idx));
        }

        System.out.println("TreeMap.Time=" + (System.currentTimeMillis() - time));
    }

    @After
    public void tearDown() throws Exception {
        cleanUp();
    }

    private void sortedTest(BinaryKeyValueSortedStorage<Tuple> blobMap,
                            MemoryBlock memoryBlock,
                            int cnt,
                            int value_cnt) {
        JetDataOutput output = serializationService.createObjectDataOutput(
                memoryBlock,
                true
        );

        JetDataInput input = serializationService.createObjectDataInput(
                memoryBlock,
                true
        );

        Tuple<Object, Object> tuple = new Tuple2<Object, Object>();
        ElementsWriter<Tuple> keyWriter = new TupleKeyElementsWriter();
        ElementsWriter<Tuple> valueWriter = new TupleValueElementsWriter();

        try {
            long t = System.currentTimeMillis();

            for (int idx = 1; idx <= cnt; idx++) {
                putEntry(idx, output, blobMap, value_cnt);
            }

            System.out.println(
                    "Insertion  CNT=" +
                            cnt +
                            " Time=" +
                            (System.currentTimeMillis() - t)
            );

            t = System.currentTimeMillis();
            BinarySlotIterator iterator = blobMap.slotIterator(OrderingDirection.ASC);
            System.out.println(
                    "Sorting CNT=" +
                            cnt +
                            " Time=" +
                            (System.currentTimeMillis() - t)
            );

            Map<String, String> map = new TreeMap<String, String>();
            for (int i = 1; i <= cnt; i++) {
                map.put("string" + i, "string" + i);
            }

            Iterator<String> treeMapIterator = map.keySet().iterator();

            ///-----------------------------------------------------------------
            assertEquals(cnt, blobMap.count());
            ///-----------------------------------------------------------------

            int iterationsCount = 0;

            while (iterator.hasNext()) {
                long slotAddress = iterator.next();
                iterationsCount++;

                BinaryRecordIterator recordIterator =
                        blobMap.recordIterator(slotAddress);

                int value = 0;

                while (recordIterator.hasNext()) {
                    value++;
                    assertTrue(recordIterator.hasNext());
                    long recordAddress = recordIterator.next();

                    IOUtil.processWriter(
                            tuple,
                            recordAddress,
                            keyWriter,
                            valueWriter,
                            ioContext,
                            input,
                            memoryBlock
                    );
                }

                assertEquals(treeMapIterator.next(), tuple.getKey(0));
                assertEquals(value_cnt, value);
            }

            assertEquals(iterationsCount, cnt);
            System.out.println(memoryBlock.getUsedBytes() / (1024 * 1024));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void openAddressingSortedBaseTest(MemoryBlock memoryBlock,
                                             int keysCount,
                                             int valueCount) {
        BinaryKeyValueSortedStorage<Tuple> blobMap = new BinaryKeyValueOpenAddressingSortedStorage<Tuple>(
                memoryBlock,
                new StringComparator(memoryBlock.getAccessor()),
                new HsaResizeListener() {
                    @Override
                    public void onHsaResize(long newHsaBaseAddress) {

                    }
                },
                1 << 10,
                0.5f
        );

        sortedTest(blobMap, memoryBlock, keysCount, valueCount);
    }
}
