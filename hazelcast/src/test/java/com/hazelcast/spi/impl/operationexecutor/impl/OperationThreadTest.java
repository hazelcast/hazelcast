package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationThreadTest extends OperationExecutorImpl_AbstractTest {

    @Test
    public void testOOME_whenDeserializing() throws Exception {
        handlerFactory = mock(OperationRunnerFactory.class);
        OperationRunner handler = mock(OperationRunner.class);
        when(handlerFactory.createGenericRunner()).thenReturn(handler);
        when(handlerFactory.createPartitionRunner(anyInt())).thenReturn(handler);

        initExecutor();

        DummyOperation operation = new DummyOperation(Operation.GENERIC_PARTITION_ID);
        Packet packet = new Packet(serializationService.toBytes(operation), operation.getPartitionId());
        packet.setFlag(Packet.FLAG_OP);

        doThrow(new OutOfMemoryError()).when(handler).run(packet);

        final int oldCount = OutOfMemoryErrorDispatcher.getOutOfMemoryErrorCount();

        executor.handle(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(oldCount + 1, OutOfMemoryErrorDispatcher.getOutOfMemoryErrorCount());
            }
        });
    }

    @Test
    public void priorityPendingCount_returnScheduleQueuePrioritySize() {
        OperationQueue mockOperationQueue = mock(OperationQueue.class);
        when(mockOperationQueue.prioritySize()).thenReturn(Integer.MAX_VALUE);

        PartitionOperationThread operationThread = createNewOperationThread(mockOperationQueue);

        int prioritySize = operationThread.priorityPendingCount();
        assertEquals(Integer.MAX_VALUE, prioritySize);
    }

    @Test
    public void normalPendingCount_returnScheduleQueueNormalSize() {
        OperationQueue mockOperationQueue = mock(OperationQueue.class);
        when(mockOperationQueue.normalSize()).thenReturn(Integer.MAX_VALUE);

        PartitionOperationThread operationThread = createNewOperationThread(mockOperationQueue);

        int normalSize = operationThread.normalPendingCount();
        assertEquals(Integer.MAX_VALUE, normalSize);
    }

    @Test
    public void executeOperation_withInvalid_partitionId() {
        final int partitionId = Integer.MAX_VALUE;
        Operation operation = new DummyPartitionOperation(partitionId);
        testExecute_withInvalid_partitionId(operation);
    }

    @Test
    public void executePartitionSpecificRunnable_withInvalid_partitionId() {
        final int partitionId = Integer.MAX_VALUE;
        testExecute_withInvalid_partitionId(new PartitionSpecificRunnable() {
            @Override
            public int getPartitionId() {
                return partitionId;
            }

            @Override
            public void run() {
            }
        });
    }

    @Test
    public void executePacket_withInvalid_partitionId() {
        final int partitionId = Integer.MAX_VALUE;
        Operation operation = new DummyPartitionOperation(partitionId);
        Packet packet = new Packet(serializationService.toBytes(operation), operation.getPartitionId());
        packet.setFlag(Packet.FLAG_OP);

        testExecute_withInvalid_partitionId(packet);
    }

    private void testExecute_withInvalid_partitionId(Object task) {
        handlerFactory = mock(OperationRunnerFactory.class);
        OperationRunner handler = mock(OperationRunner.class);
        when(handlerFactory.createGenericRunner()).thenReturn(handler);
        when(handlerFactory.createPartitionRunner(anyInt())).thenReturn(handler);

        initExecutor();

        if (task instanceof Operation) {
            executor.execute((Operation) task);
        } else if (task instanceof PartitionSpecificRunnable) {
            executor.execute((PartitionSpecificRunnable) task);
        } else if (task instanceof Packet) {
            executor.handle((Packet) task);
        } else {
            fail("invalid task!");
        }

        final Runnable emptyRunnable = new Runnable() {
            @Override
            public void run() {
            }
        };
        executor.executeOnPartitionThreads(emptyRunnable);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, executor.getPriorityQueueSize());
            }
        });
    }

    private PartitionOperationThread createNewOperationThread(OperationQueue mockOperationQueue) {
        ILogger mockLogger = mock(ILogger.class);
        OperationRunner[] runners = new OperationRunner[0];
        return new PartitionOperationThread("threadName", 0, mockOperationQueue, mockLogger, threadGroup, nodeExtension, runners);
    }
}
