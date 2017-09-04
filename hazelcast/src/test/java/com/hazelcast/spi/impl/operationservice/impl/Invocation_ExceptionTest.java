package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_ExceptionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    /**
     * When an operation indicates it returns no response, it can very well mean that it doesn't have a response yet e.g.
     * an IExecutorService.submit operation since that will rely on the completion of the task; not the operation. But if
     * an exception is thrown, then this is the response of that operation since it is very likely that a real response is
     * going to follow.
      */
    @Test
    public void whenOperationReturnsNoResponse() throws Exception {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, new OperationsReturnsNoResponse(), 0);
        assertCompletesEventually(f);

        expected.expect(ExpectedRuntimeException.class);
        f.join();
    }

    public class OperationsReturnsNoResponse extends Operation {
        @Override
        public void run() throws Exception {
            throw new ExpectedRuntimeException();
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }
}
