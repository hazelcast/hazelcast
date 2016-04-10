package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_CallbackTest extends HazelcastTestSupport {

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
    public void whenLocalPartition() {
        final String expected = "foobar";
        Operation op = new DummyOperation(expected)
                .setDelayMs(5000)
                .setPartitionId(getPartitionId(local));

        final InvocationFuture f = (InvocationFuture)operationService.invokeOnPartition(op);

        final ExecutionCallback callback = mock(ExecutionCallback.class);
        f.andThen(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(expected);
            }
        });
    }

    @Test
    public void whenRemotePartition() {
        final String expected = "foobar";
        Operation op = new DummyOperation(expected)
                .setDelayMs(5000)
                .setPartitionId(getPartitionId(remote));

        final InvocationFuture f = (InvocationFuture)operationService.invokeOnPartition(op);

        final ExecutionCallback callback = mock(ExecutionCallback.class);
        f.andThen(callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(expected);
            }
        });
    }

    @Test
    @Ignore
    public void whenExceptionThrownInOperationRun() {
//        DummyOperation operation = new DummyOperation(new ExceptionThrowingCallable());
//        InternalCompletableFuture<String> invocation = operationService.invokeOnPartition(
//                null, operation, getPartitionId(remote));
//
//        try {
//            invocation.join();
//            fail();
//        } catch (ExpectedRuntimeException expected) {
//        }
    }
}