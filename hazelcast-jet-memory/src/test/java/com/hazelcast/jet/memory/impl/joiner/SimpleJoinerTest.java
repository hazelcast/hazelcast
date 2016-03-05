package com.hazelcast.jet.memory.impl.joiner;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.impl.memory.impl.DefaultMemoryContext;
import com.hazelcast.jet.memory.impl.operations.DefaultContainersPull;
import com.hazelcast.jet.memory.impl.operations.tuple.Tuple2Factory;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleKeyElementsWriter;
import com.hazelcast.jet.memory.impl.operations.tuple.TupleValueElementsWriter;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.operations.OperationFactory;
import com.hazelcast.jet.memory.spi.operations.aggregator.Aggregator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SimpleJoinerTest extends BaseMemoryTest {
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

    private void initJoiner(BinaryComparator binaryComparator) {
        initJoiner(binaryComparator, null);
    }

    private void initJoiner(BinaryComparator binaryComparator,
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


}
