package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.impl.BasicOperationServiceTest.assertNoLitterInOpService;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicOperationService_timeoutTest extends HazelcastTestSupport {

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutSingleMember() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final IQueue<Object> q = hz.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        assertNoLitterInOpService(hz);
    }

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutWithMultiMemberCluster() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        final IQueue<Object> q = hz1.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        assertNoLitterInOpService(hz1);
        assertNoLitterInOpService(hz2);
    }

    @Test
    public void testSyncOperationTimeoutSingleMember() {
        testOperationTimeout(1, false);
    }

    @Test
    public void testSyncOperationTimeoutMultiMember() {
        testOperationTimeout(3, false);
    }

    @Test
    public void testAsyncOperationTimeoutSingleMember() {
        testOperationTimeout(1, true);
    }

    @Test
    public void testAsyncOperationTimeoutMultiMember() {
        testOperationTimeout(3, true);
    }

    private void testOperationTimeout(int memberCount, boolean async) {
        assertTrue(memberCount > 0);
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "3000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(memberCount);
        HazelcastInstance[] instances = factory.newInstances(config);
        warmUpPartitions(instances);

        final HazelcastInstance hz = instances[memberCount - 1];
        Node node = TestUtil.getNode(hz);
        NodeEngine nodeEngine = node.nodeEngine;
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = (int) (Math.random() * node.getPartitionService().getPartitionCount());

        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(null, new TimedOutBackupAwareOperation(), partitionId);

        final CountDownLatch latch = new CountDownLatch(1);
        if (async) {
            future.andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof OperationTimeoutException) {
                        latch.countDown();
                    }
                }
            });
        } else {
            try {
                future.getSafely();
                fail("Should throw OperationTimeoutException!");
            } catch (OperationTimeoutException ignored) {
                latch.countDown();
            }
        }

        assertOpenEventually("Should throw OperationTimeoutException", latch);

        for (HazelcastInstance instance : instances) {
            assertNoLitterInOpService(instance);
        }
    }

    static class TimedOutBackupAwareOperation extends AbstractOperation
            implements BackupAwareOperation {
        @Override
        public void run() throws Exception {
            LockSupport.parkNanos((long) (Math.random() * 1000 + 10));
        }

        @Override
        public boolean returnsResponse() {
            // required for operation timeout
            return false;
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return null;
        }
    }


}
