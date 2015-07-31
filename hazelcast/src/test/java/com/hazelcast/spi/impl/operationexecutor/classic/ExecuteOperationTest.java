package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.spi.Operation;
import org.junit.Test;

public class ExecuteOperationTest extends AbstractClassicOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull(){
        initExecutor();

        executor.execute((Operation)null);
    }
}
