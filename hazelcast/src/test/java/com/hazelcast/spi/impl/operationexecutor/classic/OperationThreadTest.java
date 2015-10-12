package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationThreadTest extends AbstractClassicOperationExecutorTest {

    @Test
    public void testOOME_whenDeserializing() throws Exception {
        handlerFactory = mock(OperationRunnerFactory.class);
        OperationRunner handler = mock(OperationRunner.class);
        when(handlerFactory.createGenericRunner()).thenReturn(handler);
        when(handlerFactory.createPartitionRunner(anyInt())).thenReturn(handler);

        initExecutor();

        DummyOperation operation = new DummyOperation(Operation.GENERIC_PARTITION_ID);
        Packet packet = new Packet(serializationService.toBytes(operation), operation.getPartitionId());
        packet.setHeader(Packet.HEADER_OP);

        doThrow(new OutOfMemoryError()).when(handler).run(packet);

        final int oldCount = OutOfMemoryErrorDispatcher.getOutOfMemoryErrorCount();

        executor.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(oldCount + 1, OutOfMemoryErrorDispatcher.getOutOfMemoryErrorCount());
            }
        });
    }

    @Test
    public void priorityPendingCount_returnScheduleQueuePrioritySize() {
        ScheduleQueue mockScheduleQueue = mock(ScheduleQueue.class);
        when(mockScheduleQueue.prioritySize()).thenReturn(Integer.MAX_VALUE);

        PartitionOperationThread operationThread = createNewOperationThread(mockScheduleQueue);

        int prioritySize = operationThread.priorityPendingCount();
        assertEquals(Integer.MAX_VALUE, prioritySize);
    }

    @Test
    public void normalPendingCount_returnScheduleQueueNormalSize() {
        ScheduleQueue mockScheduleQueue = mock(ScheduleQueue.class);
        when(mockScheduleQueue.normalSize()).thenReturn(Integer.MAX_VALUE);

        PartitionOperationThread operationThread = createNewOperationThread(mockScheduleQueue);

        int normalSize = operationThread.normalPendingCount();
        assertEquals(Integer.MAX_VALUE, normalSize);
    }

    private PartitionOperationThread createNewOperationThread(ScheduleQueue mockScheduleQueue) {
        ILogger mockLogger = mock(ILogger.class);
        OperationRunner[] runners = new OperationRunner[0];
        PartitionOperationThread thread = new PartitionOperationThread("threadName", 0, mockScheduleQueue, mockLogger, threadGroup, nodeExtension, runners);

        return thread;
    }
}
