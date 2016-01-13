package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicLongBackupTest extends HazelcastTestSupport {

    private HazelcastInstance instance1;

    private HazelcastInstance instance2;

    private String name = randomName();

    private int partitionId;

    private IAtomicLong atomicLong;

    @Before
    public void init() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
        warmUpPartitions(instance1, instance2);

        partitionId = instance1.getPartitionService().getPartition(name).getPartitionId();
        atomicLong = instance1.getAtomicLong(name);
    }

    @Test
    public void testSet() {
        atomicLong.set(5);

        assertAtomicLongValue(instance1, 5);
        assertAtomicLongValue(instance2, 5);
    }

    @Test
    public void testCompareAndSet() {
        atomicLong.set(5);
        atomicLong.compareAndSet(5, 10);

        assertAtomicLongValue(instance1, 10);
        assertAtomicLongValue(instance2, 10);
    }

    @Test
    public void testAlter() {
        atomicLong.set(5);
        atomicLong.alter(new SetFunction(10));

        assertAtomicLongValue(instance1, 10);
        assertAtomicLongValue(instance2, 10);
    }

    @Test
    public void testAlterAndGet() {
        atomicLong.set(5);
        atomicLong.alterAndGet(new SetFunction(10));

        assertAtomicLongValue(instance1, 10);
        assertAtomicLongValue(instance2, 10);
    }

    @Test
    public void testGetAndAlter() {
        atomicLong.set(5);
        atomicLong.getAndAlter(new SetFunction(10));

        assertAtomicLongValue(instance1, 10);
        assertAtomicLongValue(instance2, 10);
    }

    @Test
    public void testAddAndGet() {
        atomicLong.set(5);
        atomicLong.addAndGet(5);

        assertAtomicLongValue(instance1, 10);
        assertAtomicLongValue(instance2, 10);
    }

    @Test
    public void testGetAndAdd() {
        atomicLong.set(5);
        atomicLong.getAndAdd(5);

        assertAtomicLongValue(instance1, 10);
        assertAtomicLongValue(instance2, 10);
    }

    @Test
    public void testIncrementAndGet() {
        atomicLong.set(5);
        atomicLong.incrementAndGet();

        assertAtomicLongValue(instance1, 6);
        assertAtomicLongValue(instance2, 6);
    }

    @Test
    public void testDecremenetAndGet() {
        atomicLong.set(5);
        atomicLong.decrementAndGet();

        assertAtomicLongValue(instance1, 4);
        assertAtomicLongValue(instance2, 4);
    }

    private void assertAtomicLongValue(final HazelcastInstance instance, final long value) {
        assertEquals(value, readAtomicLongValue(instance));
    }

    private long readAtomicLongValue(final HazelcastInstance instance) {
        final OperationServiceImpl operationService = (OperationServiceImpl) getOperationService(instance);
        final AtomicLongService atomicLongService = getNodeEngineImpl(instance).getService(AtomicLongService.SERVICE_NAME);

        final GetLongValue task = new GetLongValue(atomicLongService);
        operationService.execute(task);
        assertOpenEventually(task.latch);
        return task.value;
    }

    private class GetLongValue implements PartitionSpecificRunnable {

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicLongService atomicLongService;

        long value;

        public GetLongValue(AtomicLongService atomicLongService) {
            this.atomicLongService = atomicLongService;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            final AtomicLongContainer longContainer = atomicLongService.getLongContainer(name);
            value = longContainer.get();
            latch.countDown();
        }
    }

    private static class SetFunction implements IFunction<Long, Long>, Serializable {

        private long value;

        public SetFunction(long value) {
            this.value = value;
        }

        @Override
        public Long apply(Long input) {
            return value;
        }

    }

}
