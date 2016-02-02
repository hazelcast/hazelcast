package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_SetTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private InternalOperationService opService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        opService = getOperationService(hz);
    }

    @Test
    public void whenNull(){
        InvocationFuture f = newInvocationFuture();
        f.set(null);

        assertTrue(f.isDone());
    }

    @Test
    public void whenObject(){
        InvocationFuture f = newInvocationFuture();
        Object response = "foo";
        f.set(response);

        assertSame(response, f.response);
    }

    @Test
    public void whenResponseAlreadySet(){
        InvocationFuture f = newInvocationFuture();
        Object firstResponse = "first";
        f.set(firstResponse);


        Object secondResponse = "second";
        f.set(secondResponse);

        assertSame(firstResponse, f.response);
    }

    @Test
    public void whenExecutionCallback(){
        InvocationFuture f = newInvocationFuture();
        final ExecutionCallback callback = mock(ExecutionCallback.class);
        f.andThen(callback);
        final Object response = "first";
        f.set(response);

        assertSame(response, f.response);
        assertNull(f.callbackHead);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(callback).onResponse(response);
            }
        });
    }

    private InvocationFuture newInvocationFuture() {
        return (InvocationFuture) opService.invokeOnPartition(null, new DummyPartitionOperation().setDelayMs(5000), 0);
    }
}

