package com.hazelcast.jet.memory;

import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotIterator;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.impl.memory.impl.ConcurrentMemoryPool;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;
import com.hazelcast.jet.memory.impl.memory.impl.DefaultMemoryContext;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleKeyElementsReader;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleKeyElementsWriter;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleValueElementsReader;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleValueElementsWriter;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.memory.MemoryPool;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.memory.spi.memory.MemoryType;
import com.hazelcast.jet.memory.spi.operations.ElementsReader;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class BaseMemoryTest {
    private static final boolean USE_BIG_ENDIAN = true;

    private static final long DEFAULT_BLOCK_SIZE_BYTES = 1024L * 1024L * 500L;

    private static final long DEFAULT_HEAP_POOL_SIZE_BYTES = 1024L * 1024L * 1024L;

    private static final long DEFAULT_NATIVE_POOL_SIZE_BYTES = 1024L * 1024L * 1024L;

    protected MemoryContext memoryContext;

    protected MemoryPool heapMemoryPool;

    protected MemoryBlock heapMemoryBlock;

    protected MemoryPool nativeMemoryPool;

    protected MemoryBlock nativeMemoryBlock;

    protected IOContext ioContext = new IOContextImpl();

    protected JetSerializationService serializationService;

    protected long heapSize() {
        return DEFAULT_HEAP_POOL_SIZE_BYTES;
    }

    protected long nativeSize() {
        return DEFAULT_NATIVE_POOL_SIZE_BYTES;
    }

    protected long blockSize() {
        return DEFAULT_BLOCK_SIZE_BYTES;
    }

    protected boolean useBigEndian() {
        return USE_BIG_ENDIAN;
    }

    protected void init() {
        this.heapMemoryPool =
                new ConcurrentMemoryPool(heapSize(), MemoryType.HEAP);
        this.nativeMemoryPool =
                new ConcurrentMemoryPool(nativeSize(), MemoryType.NATIVE);

        this.serializationService =
                new JetSerializationServiceImpl();

        MemoryContext memoryContext = new DefaultMemoryContext(
                heapMemoryPool,
                nativeMemoryPool,
                blockSize(),
                useBigEndian()
        );

        this.heapMemoryBlock =
                memoryContext.getMemoryBlockPool(MemoryType.HEAP).getNextMemoryBlock(true);
        this.nativeMemoryBlock =
                memoryContext.getMemoryBlockPool(MemoryType.NATIVE).getNextMemoryBlock(true);
    }

    protected void putEntry(int idx,
                            JetDataOutput output,
                            BinaryKeyValueStorage<Tuple> blobMap,
                            int valueCNT) throws IOException {
        Tuple<String, Integer> tuple = new Tuple2<String, Integer>();
        tuple.setKey(0, "string" + idx);

        ElementsReader<Tuple> keyReader = new TupleKeyElementsReader();
        ElementsReader<Tuple> valueReader = new TupleValueElementsReader();

        for (int value = 1; value <= valueCNT; value++) {
            tuple.setValue(0, value);

            blobMap.addRecord(
                    tuple,
                    keyReader,
                    valueReader,
                    ioContext,
                    output
            );
        }
    }

    protected void test(BinaryKeyValueStorage<Tuple> blobMap,
                        MemoryBlock memoryBlock,
                        int keysCount,
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
            long time = 0L;
            long t = System.nanoTime();

            for (int idx = 1; idx <= keysCount; idx++) {
                putEntry(idx, output, blobMap, value_cnt);
            }

            time = time + ((System.nanoTime() - t) / (1000000));
            System.out.println("Inserted CNT=" + keysCount + " Time=" + time);

            assertEquals(keysCount, blobMap.count());

            Map<String, String> map = new HashMap<String, String>();

            for (int i = 1; i <= keysCount; i++) {
                map.put("string" + i, "string" + i);
            }

            BinarySlotIterator iterator = blobMap.slotIterator();

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

                    map.remove(tuple.getKey(0));
                }

                assertEquals(value_cnt, value);
            }

            assertEquals(iterationsCount, keysCount);
            assertEquals(map.size(), 0);
            System.out.println(memoryBlock.getUsedBytes() / (1024 * 1024));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void cleanUp() {
        heapMemoryPool.release(heapMemoryPool.getUsed());
        nativeMemoryPool.release(nativeMemoryPool.getUsed());

        assertEquals(0, heapMemoryPool.getUsed());
        assertEquals(0, nativeMemoryPool.getUsed());

        heapMemoryBlock.dispose();
        nativeMemoryBlock.dispose();

        assertEquals(heapSize(), heapMemoryPool.getTotal());
        assertEquals(nativeSize(), nativeMemoryPool.getTotal());
    }
}