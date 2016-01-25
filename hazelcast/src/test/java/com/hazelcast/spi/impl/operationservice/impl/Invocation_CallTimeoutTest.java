package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
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
public class Invocation_CallTimeoutTest extends HazelcastTestSupport {

    private final static long callTimeout = 12345;

    private HazelcastInstance hz;
    private InternalOperationService opService;
    private Address thisAddress;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, "" + callTimeout);

        hz = createHazelcastInstance(config);
        opService = getOperationService(hz);
        thisAddress = getAddress(hz);
    }

    @Test
    public void callTimeout_whenDefaults() {
        Operation op = new DummyOperation();
        InvocationFuture f = (InvocationFuture) opService.invokeOnTarget(null, op, thisAddress);

        assertEquals(callTimeout, f.invocation.callTimeout);
        assertEquals(callTimeout, f.invocation.op.getCallTimeout());
    }

    @Ignore
    @Test
    public void callTimeout_whenExplicitlySet() {
        Operation op = new DummyOperation();

        int explicitCallTimeout = 12;
        setCallTimeout(op, explicitCallTimeout);

        InvocationFuture f = (InvocationFuture) opService.invokeOnTarget(null, op, thisAddress);

        assertEquals(explicitCallTimeout, f.invocation.callTimeout);
        assertEquals(explicitCallTimeout, f.invocation.op.getCallTimeout());
    }

    @Test
    public void callTimeout_whenExplicitlySet_andUsingBuilder() {
        Operation op = new DummyOperation();
        int explicitCallTimeout = 12;

        InvocationFuture f = (InvocationFuture) opService.createInvocationBuilder(null, op, thisAddress)
                .setCallTimeout(explicitCallTimeout)
                .invoke();

        assertEquals(explicitCallTimeout, f.invocation.callTimeout);
        assertEquals(explicitCallTimeout, f.invocation.op.getCallTimeout());
    }
}
