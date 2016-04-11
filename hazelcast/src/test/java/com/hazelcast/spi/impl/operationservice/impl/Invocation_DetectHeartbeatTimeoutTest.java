package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__CALL_TIMEOUT_DISABLED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__CALL_TIMEOUT_NOT_EXPIRED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__HEARTBEAT_TIMEOUT_NOT_EXPIRED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__RESPONSE_AVAILABLE;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.TIMEOUT;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_DetectHeartbeatTimeoutTest extends HazelcastTestSupport {

    @Test
    public void whenCallTimeoutDisabled() {
        Config config = new Config();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "1000");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();

        OperationService opService = getOperationService(local);
        Operation operation = new VoidOperation();
        InvocationFuture f = (InvocationFuture) opService.createInvocationBuilder(
                null, operation, getPartitionId(remote))
                .setCallTimeout(Long.MAX_VALUE)
                .invoke();

        Invocation invocation = f.invocation;

        assertEquals(Long.MAX_VALUE, invocation.op.getCallTimeout());
        assertEquals(Long.MAX_VALUE, invocation.callTimeoutMillis);
        assertEquals(NO_TIMEOUT__CALL_TIMEOUT_DISABLED, invocation.detectTimeout(SECONDS.toMillis(1)));
        assertFalse(f.isDone());
    }

    @Test
    public void whenResponseAvailable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();

        OperationService opService = getOperationService(local);
        Operation operation = new SlowOperation(SECONDS.toMillis(60));
        InvocationFuture f = (InvocationFuture) opService.invokeOnPartition(null, operation, getPartitionId(remote));

        Invocation invocation = f.invocation;
        invocation.pendingResponse = "foo";
        invocation.backupsExpected = 1;

        assertEquals(NO_TIMEOUT__RESPONSE_AVAILABLE, invocation.detectTimeout(SECONDS.toMillis(1)));
        assertFalse(f.isDone());
    }

    @Test
    public void whenCallTimeoutNotExpired() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();

        OperationService opService = getOperationService(local);
        Operation operation = new SlowOperation(SECONDS.toMillis(60));
        InvocationFuture f = (InvocationFuture) opService.invokeOnPartition(null, operation, getPartitionId(remote));

        Invocation invocation = f.invocation;

        assertEquals(NO_TIMEOUT__CALL_TIMEOUT_NOT_EXPIRED, invocation.detectTimeout(SECONDS.toMillis(1)));
        assertFalse(f.isDone());
    }

    @Test
    public void whenCallTimeoutExpired_ButOperationHeartbeatHasNot() {
        Config config = new Config();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "1000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);

        OperationService opService = getOperationService(local);
        Operation operation = new SlowOperation(SECONDS.toMillis(60));
        InvocationFuture f = (InvocationFuture) opService.invokeOnPartition(null, operation, getPartitionId(remote));

        assertDetectHeartbeatTimeoutEventually(f.invocation, NO_TIMEOUT__HEARTBEAT_TIMEOUT_NOT_EXPIRED);
    }

    /**
     * This test checks if the invocation expires eventually after the operation did manage to execute and did manage to send
     * some heartbeats but for whatever reason the response was not received.
     *
     * We do this by sending in a void operation that runs for an long period (so there are heartbeats) but on completion it
     * doesn't send a response.
     */
    @Test
    public void whenExpiresEventually() {
        Config config = new Config();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "1000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);

        OperationService opService = getOperationService(local);
        Operation operation = new VoidOperation(SECONDS.toMillis(20));
        InvocationFuture f = (InvocationFuture) opService.invokeOnPartition(null, operation, getPartitionId(remote));
        Invocation invocation = f.invocation;

        assertDetectHeartbeatTimeoutEventually(invocation, NO_TIMEOUT__CALL_TIMEOUT_NOT_EXPIRED);
        assertDetectHeartbeatTimeoutEventually(invocation, NO_TIMEOUT__HEARTBEAT_TIMEOUT_NOT_EXPIRED);
        assertDetectHeartbeatTimeoutEventually(invocation, TIMEOUT);
    }

    private void assertDetectHeartbeatTimeoutEventually(final Invocation invocation, final Invocation.HeartbeatTimeout yes) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(yes, invocation.detectTimeout(SECONDS.toMillis(1)));
            }
        });
    }
}
