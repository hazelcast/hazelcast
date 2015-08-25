package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BufferPoolThreadLocalTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;
    private BufferPoolThreadLocal bufferPoolThreadLocal;

    @Before
    public void setup() {
        serializationService = mock(InternalSerializationService.class);
        bufferPoolThreadLocal = new BufferPoolThreadLocal(serializationService, new BufferPoolFactoryImpl());
    }

    @Test
    public void get_whenSameThread_samePoolInstance() {
        BufferPool pool1 = bufferPoolThreadLocal.get();
        BufferPool pool2 = bufferPoolThreadLocal.get();
        assertSame(pool1, pool2);
    }

    @Test
    public void get_whenDifferentThreads_thenDifferentInstances() throws Exception {
        BufferPool pool1 = bufferPoolThreadLocal.get();
        BufferPool pool2 = spawn(new Callable<BufferPool>() {
            @Override
            public BufferPool call() throws Exception {
                return bufferPoolThreadLocal.get();
            }
        }).get();

        assertNotSame(pool1, pool2);
    }

    @Test
    public void clear() {
        BufferPool pool1 = bufferPoolThreadLocal.get();
        bufferPoolThreadLocal.clear();
        BufferPool pool2 = bufferPoolThreadLocal.get();
        assertNotSame(pool1, pool2);
    }
}
