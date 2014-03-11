package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * This test checks the following issue:
 * <p/>
 * https://github.com/hazelcast/hazelcast/issues/1745
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class InternalPartitionServiceStackOverflowTest extends HazelcastTestSupport {

    @Test
    public void testPartitionSpecificOperation() {
        test(0);
    }

    @Test
    public void testGlobalOperation() {
        test(-1);
    }

    public void test(int partitionId) {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService opService = getNode(hz).nodeEngine.getOperationService();

        int iterations = 2000;
        final CountDownLatch latch = new CountDownLatch(iterations);

        for (int k = 0; k < iterations; k++) {
            Operation op;
            if (partitionId >= 0) {
                op = new SlowPartitionAwareSystemOperation(latch, partitionId);
            } else {
                op = new SlowPartitionUnawareSystemOperation(latch);
            }
            opService.executeOperation(op);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, latch.getCount());
            }
        });
    }

    public static class SlowPartitionAwareSystemOperation extends AbstractOperation
            implements UrgentSystemOperation, PartitionAwareOperation {

        private final CountDownLatch latch;

        public SlowPartitionAwareSystemOperation(CountDownLatch completedLatch, int partitionId) {
            setPartitionId(partitionId);
            this.latch = completedLatch;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(10);
            latch.countDown();
        }
    }

    public static class SlowPartitionUnawareSystemOperation extends AbstractOperation
            implements UrgentSystemOperation {

        private final CountDownLatch latch;

        public SlowPartitionUnawareSystemOperation(CountDownLatch completedLatch) {
            this.latch = completedLatch;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(10);
            latch.countDown();
        }
    }

}
