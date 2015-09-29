package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class OperationServiceImpl_invokeOnTargetLiteMemberTest
        extends HazelcastTestSupport {

    private Config liteMemberConfig = new Config().setLiteMember(true);

    private Operation operation;

    @Before
    public void before() {
        operation = new DummyOperation("foobar");
    }

    @Test
    public void test_invokeTarget_onLiteMember()
            throws InterruptedException, ExecutionException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final InternalOperationService operationService = getOperationService(instance);
        final InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, operation, getAddress(instance));

        assertEquals("foobar", future.get());
    }

}
