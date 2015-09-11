package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicInvocationFutureTimeoutTest extends HazelcastTestSupport {

    @Test
    public void testIsStillRunningShouldNotCauseIsStillRunningInvocation() {
        final int callTimeoutMillis = 500;
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(callTimeoutMillis));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        // invoke on the "remote" member
        final Address remoteAddress = getNode(hz2).getThisAddress();

        final DummyOperation sleepingOp = new DummyOperation(callTimeoutMillis * 10);
        final BasicTargetInvocation invocation = new BasicTargetInvocation(getNode(hz1).getNodeEngine(), null,
                sleepingOp, remoteAddress, 0, 0,
                callTimeoutMillis, null, null, true);
        final BasicInvocationFuture future = invocation.invoke();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(future.isOperationExecuting(remoteAddress));
            }
        });

        final IsStillExecutingOperation isStillExecutingOp = new SleepingIsStillExecutingOperation(sleepingOp.getCallId());
        final BasicTargetInvocation isStillExecutingInvocation = new BasicTargetInvocation(getNode(hz1).getNodeEngine(), null,
                isStillExecutingOp, remoteAddress, 0, 0,
                callTimeoutMillis, null, null, true);
        final BasicInvocationFuture isStillExecutingFuture = isStillExecutingInvocation.invoke();
        isStillExecutingFuture.timeoutInvocationIfNotExecuting();

        assertFalse(isStillExecutingFuture.isOperationExecuting(remoteAddress));
    }

    private static class SleepingIsStillExecutingOperation extends IsStillExecutingOperation {

        SleepingIsStillExecutingOperation() {
        }

        SleepingIsStillExecutingOperation(long operationCallId) {
            super(operationCallId);
        }

        @Override
        public void run() throws Exception {
            sleepAtLeastMillis(Integer.MAX_VALUE);
        }

    }

    @Test
    public void testTimeoutInvocationIfRemoteInvocationIsRunning() throws Exception {
        int callTimeoutMillis = 500;
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(callTimeoutMillis));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        // invoke on the "remote" member
        Address remoteAddress = getNode(hz2).getThisAddress();

        final BasicTargetInvocation invocation = new BasicTargetInvocation(getNode(hz1).getNodeEngine(), null,
                new DummyOperation(callTimeoutMillis * 10), remoteAddress, 0, 0,
                callTimeoutMillis, null, null, true);
        final BasicInvocationFuture future = invocation.invoke();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(future.isOperationExecuting(invocation.getTarget()));
            }
        });

        future.timeoutInvocationIfNotExecuting();

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

        // invoke on the "remote" member
        Address remoteAddress = getNode(hz2).getThisAddress();

        BasicTargetInvocation invocation = new BasicTargetInvocation(getNode(hz1).getNodeEngine(), null,
                new DummyOperation(1), remoteAddress, 0, 0,
                callTimeoutMillis, null, null, true);
        final BasicInvocationFuture future = invocation.invoke();
        assertEquals(Boolean.TRUE, future.get());

        future.timeoutInvocationIfNotExecuting();

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
            getResponseHandler().sendResponse(true);
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
