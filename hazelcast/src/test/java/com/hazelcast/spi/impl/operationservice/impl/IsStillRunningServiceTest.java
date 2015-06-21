package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class IsStillRunningServiceTest extends HazelcastTestSupport {

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
        operation.setPartitionId(Operation.GENERIC_PARTITION_ID);

        InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, remoteAddress);

        // the operation is going to run for 5 seconds, so we should be able to see it running on the remote node
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int partitionId = operation.getPartitionId();
                long callId = operation.getCallId();
                OperationServiceImpl remoteOperationService = (OperationServiceImpl)getOperationService(remoteHz);
                IsStillRunningService isStillRunningService = remoteOperationService.getIsStillRunningService();
                boolean isRunning = isStillRunningService.isOperationExecuting(localAddress, partitionId, callId);
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
                OperationServiceImpl operationServiceImpl = (OperationServiceImpl) remoteOperationService;
                IsStillRunningService isStillRunningService = operationServiceImpl.getIsStillRunningService();
                boolean isRunning = isStillRunningService.isOperationExecuting(
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
                OperationServiceImpl operationServiceImpl = (OperationServiceImpl) operationService;
                IsStillRunningService isStillRunningService = operationServiceImpl.getIsStillRunningService();
                boolean isRunning = isStillRunningService.isOperationExecuting(thisAddress, partitionId, callId);
                assertTrue(isRunning);
            }
        });

        // we wait for completion
        f.getSafely();

        // after the call is complete, the operation should not be running anymore
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationServiceImpl operationServiceImpl = (OperationServiceImpl) operationService;
                IsStillRunningService isStillRunningService = operationServiceImpl.getIsStillRunningService();
                boolean isRunning = isStillRunningService.isOperationExecuting(
                        thisAddress, operation.getPartitionId(), operation.getCallId());
                assertFalse(isRunning);
            }
        });
    }

    @Test
    public void testTimeoutInvocationIfRemoteInvocationIsRunning() throws Exception {
        int callTimeoutMillis = 500;
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(callTimeoutMillis));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        OperationServiceImpl operationService = (OperationServiceImpl) getNode(hz1).getNodeEngine().getOperationService();

        // invoke on the "remote" member
        Address remoteAddress = getNode(hz2).getThisAddress();

        final IsStillRunningService isStillRunningService = operationService.getIsStillRunningService();

        final TargetInvocation invocation = new TargetInvocation(getNodeEngineImpl(hz1), null,
                new DummyOperation(callTimeoutMillis * 10), remoteAddress, 0, 0,
                callTimeoutMillis, null, true);
        final InvocationFuture future = invocation.invoke();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(isStillRunningService.isOperationExecuting(invocation));
            }
        });

        isStillRunningService.timeoutInvocationIfNotExecuting(invocation);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(future.isDone());
            }
        }, 2);
    }

    @Test
    public void testTimeoutInvocationIfRemoteInvocationIsCompleted() throws Exception {
        int callTimeoutMillis = 500;
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(callTimeoutMillis));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        OperationServiceImpl operationService = (OperationServiceImpl) getNode(hz1).getNodeEngine().getOperationService();

        // invoke on the "remote" member
        Address remoteAddress = getNode(hz2).getThisAddress();

        TargetInvocation invocation = new TargetInvocation(getNodeEngineImpl(hz1), null,
                new DummyOperation(1), remoteAddress, 0, 0,
                callTimeoutMillis, null, true);
        final InvocationFuture future = invocation.invoke();
        assertEquals(Boolean.TRUE, future.get());

        IsStillRunningService isStillRunningService = operationService.getIsStillRunningService();
        isStillRunningService.timeoutInvocationIfNotExecuting(invocation);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(Boolean.TRUE, future.get());
            }
        }, 2);
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
            sleepAtLeastMillis(sleepMs);
            sendResponse(true);
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
