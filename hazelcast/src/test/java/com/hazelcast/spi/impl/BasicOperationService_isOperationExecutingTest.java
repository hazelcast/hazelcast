package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicOperationService_isOperationExecutingTest extends HazelcastTestSupport {

    // The problem with testing a generic operation, is that it will be executed on the calling
    // thread. So to prevent this, we send the generic operation to a remote node and then we
    // check the isOperationExecuting on the remote node.
    @Test
    public void test_genericOperation() throws ExecutionException, InterruptedException {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance localHz = cluster[0];
        final HazelcastInstance remoteHz = cluster[1];
        final InternalOperationService operationService = getOperationService(localHz);
        final Address localAddress = getAddress(localHz);
        final Address remoteAddress = getAddress(remoteHz);

        final DummyOperation operation = new DummyOperation(5000);
        operation.setPartitionId(-1);

        InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, remoteAddress);

        // the operation is going to run for 5 seconds, so we should be able to see it running on the remote node
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int partitionId = operation.getPartitionId();
                long callId = operation.getCallId();
                InternalOperationService remoteOperationService = getOperationService(remoteHz);
                boolean isRunning = remoteOperationService.isOperationExecuting(localAddress, partitionId, callId);
                assertTrue(isRunning);
            }
        });

        // we wait for completion.
        f.getSafely();

        // after the call is complete, the operation should not be running anymore eventually
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                InternalOperationService remoteOperationService = getOperationService(remoteHz);
                boolean isRunning = remoteOperationService.isOperationExecuting(
                        localAddress, operation.getPartitionId(), operation.getCallId());
                assertFalse(isRunning);
            }
        });
    }

    @Test
    public void test_partitionSpecificOperation() {
        HazelcastInstance hz = createHazelcastInstance();
        final InternalOperationService operationService = getOperationService(hz);
        final Address thisAddress = getAddress(hz);

        final DummyOperation operation = new DummyOperation(5000);
        operation.setPartitionId(0);

        InternalCompletableFuture f = operationService.invokeOnPartition(null, operation, operation.getPartitionId());

        // the operation is going to run for 2 seconds, so we should be able to see it running.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int partitionId = operation.getPartitionId();
                long callId = operation.getCallId();
                boolean isRunning = operationService.isOperationExecuting(thisAddress, partitionId, callId);
                assertTrue(isRunning);
            }
        });

        // we wait for completion
        f.getSafely();

        // after the call is complete, the operation should not be running anymore
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean isRunning = operationService.isOperationExecuting(
                        thisAddress, operation.getPartitionId(), operation.getCallId());
                assertFalse(isRunning);
            }
        });
    }

    public static class DummyOperation extends AbstractOperation {
        private int sleepMs;

        public DummyOperation() {
        }

        public DummyOperation(int sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(sleepMs);
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(sleepMs);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            sleepMs = in.readInt();
        }
    }
}
