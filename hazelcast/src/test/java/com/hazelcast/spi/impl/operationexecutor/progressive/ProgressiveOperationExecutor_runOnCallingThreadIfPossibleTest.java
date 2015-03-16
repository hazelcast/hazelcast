package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveOperationExecutor_runOnCallingThreadIfPossibleTest extends AbstractProgressiveOperationExecutorTest {

    @Test(expected = NullPointerException.class)
    public void whenNullOperation_thenNullPointerException() {
        initExecutor();

        executor.runOnCallingThreadIfPossible(null);
    }

    @Test
    public void whenGenericOperation_andRunningFromNormalThread_thenRunOnCallingThread() throws Throwable {
        initExecutor();

        GenericOperation genericOperation = new GenericOperation();
        executor.runOnCallingThreadIfPossible(genericOperation);

        awaitCompletion(genericOperation);

        assertExecutedByCurrentThread(genericOperation);
    }

    @Test
    public void whenGenericOperation_andRunningFromIOThread_thenRunOnGenericThread() throws Throwable {
        initExecutor();

        final GenericOperation genericOperation = new GenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                executor.runOnCallingThreadIfPossible(genericOperation);
                Thread currentThread = Thread.currentThread();
                return currentThread == genericOperation.executingThread;
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        awaitCompletion(genericOperation);

        assertExecutedByGenericThread(genericOperation);
    }

    @Test
    public void whenGenericOperation_andRunningFromGenericThread_thenRunOnCallingThread() throws Throwable {
        initExecutor();

        final GenericOperation innerGenericOperation = new GenericOperation();
        GenericOperation outerGenericOperation = new GenericOperation() {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerGenericOperation);
                return null;
            }
        };

        executor.execute(outerGenericOperation);
        awaitCompletion(innerGenericOperation, outerGenericOperation);

        assertExecutedBySameThread(outerGenericOperation, innerGenericOperation);
    }

    @Test
    public void whenGenericOperation_andRunningFromPartitionThread_thenRunOnCallingThread() throws Throwable {
        initExecutor();

        final GenericOperation innerGenericOperation = new GenericOperation();
        GenericOperation outerPartitionOperation = new GenericOperation() {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerGenericOperation);
                return null;
            }
        };

        executor.execute(outerPartitionOperation);
        awaitCompletion(innerGenericOperation, outerPartitionOperation);

        assertExecutedBySameThread(outerPartitionOperation, innerGenericOperation);
    }

    @Test
    public void whenGenericOperation_andRunningFromStealingThread_thenRunOnCallingThread() throws Throwable {
        initExecutor();

        final GenericOperation innerGenericOperation = new GenericOperation();

        PartitionOperation outerPartitionOperation = new PartitionOperation(0) {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerGenericOperation);
                return null;
            }
        };

        // this causes the work stealing to kick in.
        executor.runOnCallingThreadIfPossible(outerPartitionOperation);

        awaitCompletion(outerPartitionOperation, innerGenericOperation);

        assertExecutedByCurrentThread(innerGenericOperation);
        assertExecutedByCurrentThread(outerPartitionOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromUserThread_andStealFailure_thenRunOnPartitionThread() throws Throwable {
        initExecutor();

        // first we put the partition in execute mode by running a an operation on it.
        // this operation is not run in the calling thread because we call 'execute'.
        PartitionOperation pendingOperation = new PartitionOperation();
        pendingOperation.durationMs(2000);
        executor.execute(pendingOperation);

        final PartitionOperation partitionOperation = new PartitionOperation();

        // this causes the work stealing to kick in.
        executor.runOnCallingThreadIfPossible(partitionOperation);

        awaitCompletion(partitionOperation);

        assertExecutedByPartitionThread(partitionOperation);
        assertExecutedByPartitionThread(pendingOperation);
        assertExecutedBySameThread(partitionOperation, pendingOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromUserThread_andStealSuccess_thenRunOnCallingThread() throws Throwable {
        initExecutor();

        final PartitionOperation partitionOperation = new PartitionOperation();

        // this causes the work stealing to kick in.
        executor.runOnCallingThreadIfPossible(partitionOperation);

        awaitCompletion(partitionOperation);

        assertExecutedByCurrentThread(partitionOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromIOThread() throws Throwable {
        initExecutor();

        final PartitionOperation partitionOperation = new PartitionOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                executor.runOnCallingThreadIfPossible(partitionOperation);
                Thread currentThread = Thread.currentThread();
                return currentThread == partitionOperation.executingThread;
            }
        });

        DummyNioThread nioThread = new DummyNioThread(futureTask);
        nioThread.start();

        partitionOperation.get();
        assertExecutedByPartitionThread(partitionOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromGenericThread_thenCalledOnPartitionThread() throws Throwable {
        initExecutor();

        final PartitionOperation innerPartitionOperation = new PartitionOperation();

        GenericOperation outerGenericOperation = new GenericOperation() {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerPartitionOperation);
                return null;
            }
        };

        // this causes the work stealing to kick in.
        executor.runOnCallingThreadIfPossible(outerGenericOperation);

        awaitCompletion(innerPartitionOperation, outerGenericOperation);
        assertExecutedByPartitionThread(innerPartitionOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromCorrectPartitionThread_thenRunOnCallingThread() throws Throwable {
        initExecutor();

        final PartitionOperation innerPartitionOperation = new PartitionOperation();

        PartitionOperation outerPartitionOperation = new PartitionOperation(innerPartitionOperation.getPartitionId()) {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerPartitionOperation);
                return null;
            }
        };

        // this causes the work stealing to kick in.
        executor.runOnCallingThreadIfPossible(outerPartitionOperation);

        awaitCompletion(innerPartitionOperation, outerPartitionOperation);
        assertExecutedBySameThread(outerPartitionOperation, innerPartitionOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromWrongPartitionThread_thenCalledOnDifferentPartitionThread() throws Throwable {
        initExecutor();

        final PartitionOperation innerPartitionOperation = new PartitionOperation();

        PartitionOperation outerPartitionOperation = new PartitionOperation(innerPartitionOperation.getPartitionId() + 1) {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerPartitionOperation);
                return null;
            }
        };

        executor.execute(outerPartitionOperation);

        awaitCompletion(innerPartitionOperation, outerPartitionOperation);
        assertExecutedByDifferentThreads(innerPartitionOperation, outerPartitionOperation);
        assertExecutedByPartitionThread(innerPartitionOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromStealingThread_andWrongPartition_thenCalledOnDifferentPartitionThread() throws Throwable {
        initExecutor();

        final PartitionOperation innerPartitionOperation = new PartitionOperation();

        PartitionOperation outerPartitionOperation = new PartitionOperation(innerPartitionOperation.getPartitionId() + 1) {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerPartitionOperation);
                return null;
            }
        };

        executor.runOnCallingThreadIfPossible(outerPartitionOperation);

        awaitCompletion(innerPartitionOperation, outerPartitionOperation);
        assertExecutedByDifferentThreads(innerPartitionOperation, outerPartitionOperation);
        assertExecutedByPartitionThread(innerPartitionOperation);
    }

    @Test
    public void whenPartitionOperation_andRunningFromStealingThreadWithCorrectPartition_thenCalledOnCallingThread() throws Throwable {
        initExecutor();

        final PartitionOperation innerPartitionOperation = new PartitionOperation();

        PartitionOperation outerPartitionOperation = new PartitionOperation(innerPartitionOperation.getPartitionId()) {
            @Override
            public Object call() throws Throwable {
                executor.runOnCallingThreadIfPossible(innerPartitionOperation);
                return null;
            }
        };

        // this causes the work stealing to kick in.
        executor.runOnCallingThreadIfPossible(outerPartitionOperation);

        awaitCompletion(innerPartitionOperation, outerPartitionOperation);
        assertExecutedByCurrentThread(innerPartitionOperation, outerPartitionOperation);
    }
}
