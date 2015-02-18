package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExecutePartitionSpecificRunnableTest extends AbstractClassicSchedulerTest {

    @Test(expected = NullPointerException.class)
    public void test() {
        initScheduler();

        scheduler.execute((PartitionSpecificRunnable) null);
    }
}
