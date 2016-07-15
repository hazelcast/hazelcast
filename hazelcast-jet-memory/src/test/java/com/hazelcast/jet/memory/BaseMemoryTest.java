package com.hazelcast.jet.memory;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.TupleAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.memoryblock.MemoryPool;
import com.hazelcast.jet.memory.memoryblock.MemoryType;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.memory.util.JetIoUtil.readTuple;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static junit.framework.TestCase.assertEquals;

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
        this.heapMemoryPool = new MemoryPool(heapSize(), MemoryType.HEAP);
        this.nativeMemoryPool = new MemoryPool(nativeSize(), MemoryType.NATIVE);
        this.serializationService = new JetSerializationServiceImpl();
        MemoryContext memoryContext = new MemoryContext(heapMemoryPool, nativeMemoryPool, blockSize(), useBigEndian());
        this.heapMemoryBlock = memoryContext.getMemoryBlockPool(MemoryType.HEAP).getNextMemoryBlock(true);
        this.nativeMemoryBlock = memoryContext.getMemoryBlockPool(MemoryType.NATIVE).getNextMemoryBlock(true);
    }

    protected void putEntry(int idx, JetDataOutput output, Storage blobMap, int valueCount) {
        final Tuple2<String, Integer> tuple = new Tuple2<>();
        tuple.set0("string" + idx);
        for (int value = 1; value <= valueCount; value++) {
            tuple.set1(value);
            blobMap.insertTuple(tuple, ioContext, output);
        }
    }

    protected void test(Storage blobMap, MemoryBlock memoryBlock, int keyCount, int valueCount) {
        final JetDataOutput output = serializationService.createObjectDataOutput(memoryBlock, true);
        final JetDataInput input = serializationService.createObjectDataInput(memoryBlock, true);
        final Tuple2<String, Integer> tuple = new Tuple2<>();
        final long start = System.nanoTime();
        for (int idx = 1; idx <= keyCount; idx++) {
            putEntry(idx, output, blobMap, valueCount);
        }
        final long time = NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.println("Inserted CNT=" + keyCount + " Time=" + time);
        assertEquals(keyCount, blobMap.count());
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 1; i <= keyCount; i++) {
            map.put("string" + i, "string" + i);
        }
        int iterationCount = 0;
        for (SlotAddressCursor slotCur = blobMap.slotCursor(); slotCur.advance();) {
            iterationCount++;
            int value = 0;
            for (TupleAddressCursor tupleCur = blobMap.tupleCursor(slotCur.slotAddress()); tupleCur.advance();) {
                value++;
                long tupleAddress = tupleCur.tupleAddress();
                readTuple(input, tupleAddress, tuple, ioContext, memoryBlock.getAccessor());
                map.remove(tuple.get0());
            }
            assertEquals(valueCount, value);
        }
        assertEquals(iterationCount, keyCount);
        assertEquals(map.size(), 0);
        System.out.println(memoryBlock.getUsedBytes() / (1024 * 1024));
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
