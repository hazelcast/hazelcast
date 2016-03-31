package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReference;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_ExecuteOperationTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull() {
        initExecutor();

        executor.execute((Operation) null);
    }

    @Test
    public void whenPartitionSpecific() {
        initExecutor();

        final AtomicReference<Thread> executingThread = new AtomicReference<Thread>();

        AbstractOperation op = new AbstractOperation() {
            @Override
            public void run() throws Exception {
                executingThread.set(Thread.currentThread());
            }
        };
        executor.execute(op.setPartitionId(0));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(PartitionOperationThread.class, executingThread.get());
            }
        });
    }

    @Test
    public void whenGeneric() {
        initExecutor();

        final AtomicReference<Thread> executingThread = new AtomicReference<Thread>();

        AbstractOperation op = new AbstractOperation() {
            @Override
            public void run() throws Exception {
                executingThread.set(Thread.currentThread());
            }
        };
        executor.execute(op.setPartitionId(-1));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(GenericOperationThread.class, executingThread.get());
            }
        });
    }
}
