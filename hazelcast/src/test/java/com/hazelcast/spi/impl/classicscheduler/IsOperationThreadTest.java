package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IsOperationThreadTest extends AbstractClassicSchedulerTest {

    @Test
    public void test_whenCallingFromNonOperationThread() {
        initScheduler();

        boolean result = scheduler.isOperationThread();

        assertFalse(result);
    }

    @Test
    public void test_whenCallingFromPartitionOperationThread() {
        initScheduler();

        PartitionSpecificCallable task = new PartitionSpecificCallable() {
            @Override
            public Object call() {
                return scheduler.isOperationThread();
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenCallingFromGenericOperationThread() {
        initScheduler();

        PartitionSpecificCallable task = new PartitionSpecificCallable(-1) {
            @Override
            public Object call() {
                return scheduler.isOperationThread();
            }
        };

        scheduler.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }
}
