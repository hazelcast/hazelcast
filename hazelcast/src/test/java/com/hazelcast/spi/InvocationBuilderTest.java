package com.hazelcast.spi;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationBuilderTest extends HazelcastTestSupport {

    @Test
    public void getTargetExecutionCallback_whenNull() {
        InvocationBuilder builder = new MockInvocationBuilder(null, null, null, 0, null);

        assertNull(builder.getTargetExecutionCallback());
    }

    @Test
    public void getTargetExecutionCallback_whenCallbackInstance() {
        InvocationBuilder invocationBuilder = new MockInvocationBuilder(null, null, null, 0, null);

        assertNull(invocationBuilder.getTargetExecutionCallback());

        Callback callback = mock(Callback.class);
        invocationBuilder.setCallback(callback);

        assertInstanceOf(InvocationBuilder.ExecutorCallbackAdapter.class, invocationBuilder.getTargetExecutionCallback());
    }

    @Test
    public void getTargetExecutionCallback_whenExecutionCallbackInstance() {
        InvocationBuilder invocationBuilder = new MockInvocationBuilder(null, null, null, 0, null);

        ExecutionCallback executionCallback = mock(ExecutionCallback.class);
        invocationBuilder.setExecutionCallback(executionCallback);

        assertSame(executionCallback, invocationBuilder.getTargetExecutionCallback());
    }

    class MockInvocationBuilder extends InvocationBuilder {
        public MockInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId, Address target) {
            super(nodeEngine, serviceName, op, partitionId, target);
        }

        @Override
        public <E> InternalCompletableFuture<E> invoke() {
            return null;
        }
    }
}
