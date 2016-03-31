package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReference;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_ExecutePartitionSpecificRunnableTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull() {
        initExecutor();

        executor.execute((PartitionSpecificRunnable) null);
    }

    @Test
    public void whenPartitionSpecific() {
        initExecutor();

        final AtomicReference<Thread> executingThead = new AtomicReference<Thread>();

        PartitionSpecificRunnable task = new PartitionSpecificRunnable() {
            @Override
            public void run() {
                executingThead.set(Thread.currentThread());
            }

            @Override
            public int getPartitionId() {
                return 0;
            }
        };
        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(PartitionOperationThread.class, executingThead.get());
            }
        });
    }

    @Test
    public void whenGeneric() {
        initExecutor();

        final AtomicReference<Thread> executingThead = new AtomicReference<Thread>();

        PartitionSpecificRunnable task = new PartitionSpecificRunnable() {
            @Override
            public void run() {
                executingThead.set(Thread.currentThread());
            }

            @Override
            public int getPartitionId() {
                return -1;
            }
        };
        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(GenericOperationThread.class, executingThead.get());
            }
        });
    }
}
