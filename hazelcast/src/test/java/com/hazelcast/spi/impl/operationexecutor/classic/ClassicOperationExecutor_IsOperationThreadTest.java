package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import static com.hazelcast.spi.Operation.GENERIC_PARTITION_ID;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassicOperationExecutor_IsOperationThreadTest extends ClassicOperationExecutor_AbstractTest {

    @Test
    public void test_whenCallingFromNonOperationThread() {
        initExecutor();

        boolean result = executor.isOperationThread();

        assertFalse(result);
    }

    @Test
    public void test_whenCallingFromPartitionOperationThread() {
        initExecutor();

        PartitionSpecificCallable task = new PartitionSpecificCallable() {
            @Override
            public Object call() {
                return executor.isOperationThread();
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenCallingFromGenericOperationThread() {
        initExecutor();

        PartitionSpecificCallable task = new PartitionSpecificCallable(GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                return executor.isOperationThread();
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }
}
