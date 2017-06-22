package com.hazelcast.spi.impl.operationservice.impl.batching;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@RequireAssertEnabled
@Category({QuickTest.class, ParallelTest.class})
public class PartitionAwareCallableBatchingRunnableTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void test_NotRunningOnPartitionThread() {
        PartitionAwareCallableBatchingRunnable runnable = new PartitionAwareCallableBatchingRunnable(
                getNodeEngineImpl(createHazelcastInstance()), new TestPartitionAwareCallableFactory());

        exception.expect(AssertionError.class);
        runnable.run();
    }

    @Test
    public void test_whenRunningOnPartitionThread() throws IllegalAccessException, ExecutionException, InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionAwareCallableBatchingRunnable runnable = new PartitionAwareCallableBatchingRunnable(
                getNodeEngineImpl(hz), new TestPartitionAwareCallableFactory());

        // init data and partitions
        IMap map = hz.getMap("testMap");
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        OperationServiceImpl ops = ((OperationServiceImpl) getNodeEngineImpl(hz).getOperationService());
        int partitionCount = getNodeEngineImpl(hz).getPartitionService().getPartitionCount();

        OperationExecutorImpl executor = (OperationExecutorImpl) ops.getOperationExecutor();
        executor.executeOnPartitionThreads(runnable);
        List result = (List) runnable.getFuture().get();

        assertTrue(runnable.getFuture().isDone());
        assertEquals(partitionCount, result.size());
        Set sortedResult = new TreeSet(result);
        for (int i = 0; i < partitionCount; i++) {
            assertTrue(sortedResult.contains(i));
        }
    }

    private static class TestPartitionAwareCallableFactory implements PartitionAwareCallableFactory {
        @Override
        public PartitionAwareCallable create() {
            return new TestPartitionAwareCallable();
        }
    }

    private static class TestPartitionAwareCallable implements PartitionAwareCallable {
        @Override
        public Object call(int partitionId) {
            return partitionId;
        }
    }
}