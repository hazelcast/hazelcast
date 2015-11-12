package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.operations.IsStillExecutingOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IsStillRunningServiceTest extends HazelcastTestSupport {

    @Test
    public void test_IsStillRunningShouldNotCauseIsStillRunningInvocation() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationServiceImpl operationService = (OperationServiceImpl) getOperationService(hz);
        IsStillRunningService isStillRunningService = operationService.getIsStillRunningService();
        final OperationExecutor operationExecutor = operationService.getOperationExecutor();

        final int partitionId = 0;
        long callId = 123;
        final CountDownLatch started = new CountDownLatch(1);
        final Operation isStillExecutingOperation = new IsStillExecutingOperation(callId, partitionId) {
            @Override
            public void run() throws Exception {
                super.run();
                started.countDown();
            }
        };
        isStillExecutingOperation.setPartitionId(partitionId);

        spawn(new Runnable() {
            @Override
            public void run() {
                operationExecutor.getPartitionOperationRunners()[partitionId].run(isStillExecutingOperation);
            }
        });

        started.countDown();

        PartitionInvocation invocation = new PartitionInvocation(
                getNodeEngineImpl(hz), null, isStillExecutingOperation, partitionId, 0, 0, 0, 0, null, false);

        boolean result = isStillRunningService.isOperationExecuting(invocation);
        assertFalse(result);
    }


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
                OperationServiceImpl remoteOperationService = (OperationServiceImpl) getOperationService(remoteHz);
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
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(callTimeoutMillis));

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
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(callTimeoutMillis));

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


    @Category(SlowTest.class)
    @Test
    public void testExecutionShouldNotTimeoutIfTraceable_WithMultiCallback() throws Exception {
        Config config = new Config();
        long timeoutMs = 3000;
        config.setProperty("hazelcast.operation.call.timeout.millis", String.valueOf(timeoutMs));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        CountDownLatch latch = new CountDownLatch(1);
        Callback callback = new Callback(latch);

        IExecutorService executorService = instance1.getExecutorService(randomName());
        executorService.submitToAllMembers(new SleepingTask(), callback);
        assertOpenEventually(latch);
        for (Object result : callback.values.values()) {
            if (!result.equals("Success")) {
                fail("Non-success result: " + result);
            }
        }
    }

    private static class SleepingTask implements Callable<String>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public String call() throws Exception {
            Thread.sleep(15000);
            return "Success";
        }

    }

    private static class Callback implements MultiExecutionCallback {

        private final CountDownLatch latch;

        private Map<Member, Object> values;

        public Callback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onResponse(Member member, Object value) {
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            this.values = values;
            latch.countDown();
        }

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
