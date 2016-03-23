package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.ringbuffer.impl.operations.GenericOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.DummyPriorityOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassicOperationExecutor_ExecuteOperationTest extends ClassicOperationExecutor_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull() {
        initExecutor();

        executor.execute((Operation) null);
    }

    @Test
    public void testPriorityGenericOperationIsPickedUpQuicklyEvenIfAllGenericThreadsAreBusy() {
        initExecutor();

        // put set of generic operations that block for 1 day.
        for (int k = 0; k < executor.getGenericOperationThreadCount(); k++) {
            executor.execute(new GenericOperation(){
                @Override
                public void run() throws Exception {
                    Thread.sleep(DAYS.toMillis(1));
                }
            });
        }

        final AtomicBoolean complete = new AtomicBoolean();
        // if the priority generic operation is scheduled, it should immediately be picked up
        executor.execute(new DummyPriorityOperation(){
            {
                setPartitionId(-1);
            }

            @Override
            public void run() throws Exception {
                complete.set(true);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(complete.get());
            }
        });
    }
}
