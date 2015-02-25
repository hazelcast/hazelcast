package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExecutePartitionSpecificRunnableTest extends AbstractClassicOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test() {
        initExecutor();

        executor.execute((PartitionSpecificRunnable) null);
    }
}
