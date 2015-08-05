package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.ExceptionThrowingCallable;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationServiceImpl_invokeOnTargetTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private InternalOperationService operationService;
    private HazelcastInstance remote;

    @Before
    public void setup() {
        HazelcastInstance[] nodes = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(nodes);

        local = nodes[0];
        remote = nodes[1];
        operationService = getOperationService(local);
    }

    @Test
    public void whenLocal() {
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(local));
        assertEquals(expected, invocation.getSafely());

        //todo: we need to verify that the call was run on the calling thread
    }

    @Test
    public void whenRemote() {
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remote));
        assertEquals(expected, invocation.getSafely());
    }

    //this test is very slow, so it is marked as a nightly test
    @Test
    @Category(NightlyTest.class)
    public void whenNonExistingTarget() throws UnknownHostException {
        Address remoteAddress = getAddress(remote);
        remote.shutdown();

        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, remoteAddress);

        try {
            invocation.getSafely();
            fail();
        } catch (TargetNotMemberException e) {
        }
    }

    @Test
    public void whenExceptionThrownInOperationRun() {
        DummyOperation operation = new DummyOperation(new ExceptionThrowingCallable());
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remote));

        try {
            invocation.getSafely();
            fail();
        } catch (ExpectedRuntimeException expected) {
        }
    }

}
