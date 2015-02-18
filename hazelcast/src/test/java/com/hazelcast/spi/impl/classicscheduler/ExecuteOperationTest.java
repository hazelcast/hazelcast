package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.spi.Operation;
import org.junit.Test;

public class ExecuteOperationTest extends AbstractClassicSchedulerTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull(){
        initScheduler();

        scheduler.execute((Operation)null);
    }
}
