package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import org.junit.Test;

public class ExecutePartitionSpecificRunnableTest extends AbstractClassicSchedulerTest {

    @Test(expected = NullPointerException.class)
    public void test() {
        initScheduler();

        scheduler.execute((PartitionSpecificRunnable) null);
    }
}
