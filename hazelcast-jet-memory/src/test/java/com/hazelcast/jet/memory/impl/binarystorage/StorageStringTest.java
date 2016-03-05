package com.hazelcast.jet.memory.impl.binarystorage;

import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.jet.memory.impl.binarystorage.comparator.StringComparator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class StorageStringTest extends BaseMemoryTest {
    @Before
    public void setUp() throws Exception {
        init();
    }

    @Test
    public void openAddressingHeapStorageTest() {
        openAddressingBaseTest(heapMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingHeapStorageTest10() {
        openAddressingBaseTest(heapMemoryBlock, 100_000, 10);
    }

    @Test
    public void openAddressingNativeStorageTest() {
        openAddressingBaseTest(nativeMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingNativeStorageTest10() {
        openAddressingBaseTest(nativeMemoryBlock, 100_000, 10);
    }

    public void openAddressingBaseTest(MemoryBlock memoryBlock,
                                       int keysCount,
                                       int valueCount) {
        BinaryKeyValueStorage<Tuple> blobMap = new BinaryKeyValueOpenAddressingStorage<Tuple>(
                memoryBlock,
                new StringComparator(memoryBlock.getAccessor()).getHasher(),
                new HsaResizeListener() {
                    @Override
                    public void onHsaResize(long newHsaBaseAddress) {

                    }
                },
                1 << 10,
                0.5f
        );

        blobMap.setMemoryBlock(memoryBlock);
        test(blobMap, memoryBlock, keysCount, valueCount);
    }

    @Test
    public void hashMapPerfomanceTest() throws IOException {
        long time = System.currentTimeMillis();
        final Map<String, String> map = new HashMap<>();
        for (int idx = 1; idx <= 1_000_000; idx++) {
            map.put(String.valueOf(idx), String.valueOf(idx));
        }
        System.out.println("HashMap.Time=" + (System.currentTimeMillis() - time));
    }

    @After
    public void tearDown() throws Exception {
        cleanUp();
    }
}
