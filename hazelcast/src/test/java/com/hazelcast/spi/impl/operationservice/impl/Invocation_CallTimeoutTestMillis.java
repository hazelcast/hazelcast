package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_CallTimeoutTestMillis extends HazelcastTestSupport {

    private final static long callTimeout = 12345;

    private HazelcastInstance hz;
    private InternalOperationService opService;
    private Address thisAddress;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + callTimeout);

        hz = createHazelcastInstance(config);
        opService = getOperationService(hz);
        thisAddress = getAddress(hz);
    }

    @Test
    public void callTimeout_whenDefaults() {
        Operation op = new DummyOperation();
        InvocationFuture future = (InvocationFuture) opService.invokeOnTarget(null, op, thisAddress);

        assertEquals(callTimeout, future.invocation.callTimeoutMillis);
        assertEquals(callTimeout, future.invocation.op.getCallTimeout());
    }

    @Ignore
    @Test
    public void callTimeout_whenExplicitlySet() {
        Operation op = new DummyOperation();

        int explicitCallTimeout = 12;
        setCallTimeout(op, explicitCallTimeout);

        InvocationFuture future = (InvocationFuture) opService.invokeOnTarget(null, op, thisAddress);

        assertEquals(explicitCallTimeout, future.invocation.callTimeoutMillis);
        assertEquals(explicitCallTimeout, future.invocation.op.getCallTimeout());
    }

    @Test
    public void callTimeout_whenExplicitlySet_andUsingBuilder() {
        Operation op = new DummyOperation();
        int explicitCallTimeout = 12;

        InvocationFuture future = (InvocationFuture) opService.createInvocationBuilder(null, op, thisAddress)
                .setCallTimeout(explicitCallTimeout)
                .invoke();

        assertEquals(explicitCallTimeout, future.invocation.callTimeoutMillis);
        assertEquals(explicitCallTimeout, future.invocation.op.getCallTimeout());
    }
}
