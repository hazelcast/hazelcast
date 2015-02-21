package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionSpecificCallable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveOperationExecutor_executePartitionSpecificRunnableTest extends AbstractProgressiveOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull() {
        initExecutor();

        executor.execute((PartitionSpecificRunnable) null);
    }

    @Test
    public void test() {
        initExecutor();

        PartitionSpecificCallable task = new PartitionSpecificCallable() {
            @Override
            public Object call() {
                return Boolean.TRUE;
            }
        };

        executor.execute(task);

        assertCompletesEventually(task, Boolean.TRUE);
    }
}
