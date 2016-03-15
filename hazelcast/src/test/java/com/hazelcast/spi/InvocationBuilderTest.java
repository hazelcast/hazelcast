package com.hazelcast.spi;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationBuilderTest extends HazelcastTestSupport {

    @Test
    public void getTargetExecutionCallback_whenNull() {
        InvocationBuilder builder = new MockInvocationBuilder(null, null, 0, null);

        assertNull(builder.getTargetExecutionCallback());
    }

    @Test
    public void getTargetExecutionCallback_whenExecutionCallbackInstance() {
        InvocationBuilder builder = new MockInvocationBuilder(null, null, 0, null);

        ExecutionCallback callback = mock(ExecutionCallback.class);
        builder.setExecutionCallback(callback);

        assertSame(callback, builder.getTargetExecutionCallback());
    }

    class MockInvocationBuilder extends InvocationBuilder {
        public MockInvocationBuilder(String serviceName, Operation op, int partitionId, Address target) {
            super(serviceName, op, partitionId, target);
        }

        @Override
        public <E> InternalCompletableFuture<E> invoke() {
            return null;
        }
    }
}
