package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * This test verifies that system operations are picked up with a higher priority than regular operations.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SystemOperationPrecedenseTest extends HazelcastTestSupport {

    @Test
    public void testPartitionAware() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService opService = getNode(hz).nodeEngine.getOperationService();

        int pendingOperations = 10000;
        final CountDownLatch latch = new CountDownLatch(1);
        int partitionid = 1;

        //we are going to fill up the partition first with tons of normal operations with take a lot of time
        for (int k = 0; k < pendingOperations; k++) {
            opService.execute(new NormalPartitionAwareOperation(partitionid));
        }

        opService.execute(new UrgentPartitionAwareOperation(latch, partitionid));

        //if the system operation would be given urgency, we should only wait for 1 operation to be processed before
        //our system operation is processed.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, latch.getCount());
            }
        });
    }

    @Test
    public void testPartitionUnaware() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService opService = getNode(hz).nodeEngine.getOperationService();

        int pendingOperations = 10000;
        final CountDownLatch latch = new CountDownLatch(1);

        //we are going to fill up the partition first with tons of normal operations with take a lot of time
        for (int k = 0; k < pendingOperations; k++) {
            opService.execute(new NormalPartitionUnawareOperation());
        }

        //then we place the system operation
        opService.execute(new UrgentPartitionUnawareOperation(latch));

        //if the system operation would be given urgency, we should only wait for 1 operation to be processed before
        //our system operation is processed.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, latch.getCount());
            }
        });
    }

    public static class UrgentPartitionAwareOperation extends Operation
            implements UrgentSystemOperation, PartitionAwareOperation {

        private final CountDownLatch latch;

        public UrgentPartitionAwareOperation(CountDownLatch completedLatch, int partitionId) {
            setPartitionId(partitionId);
            this.latch = completedLatch;
        }

        @Override
        public void run() throws Exception {
            latch.countDown();
        }
    }

    public static class NormalPartitionAwareOperation extends Operation implements PartitionAwareOperation {

        public NormalPartitionAwareOperation(int partitionId) {
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(1000);
        }
    }

    public static class UrgentPartitionUnawareOperation extends Operation implements UrgentSystemOperation {

        private final CountDownLatch latch;

        public UrgentPartitionUnawareOperation(CountDownLatch completedLatch) {
            this.latch = completedLatch;
        }

        @Override
        public void run() throws Exception {
            latch.countDown();
        }
    }

    public static class NormalPartitionUnawareOperation extends Operation {
        @Override
        public void run() throws Exception {
            Thread.sleep(1000);
        }
    }
}
