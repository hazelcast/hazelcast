package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationThreadTest extends AbstractClassicOperationExecutorTest {

    @Test
    public void testOOME_whenDeserializing() throws Exception {
        handlerFactory = mock(OperationRunnerFactory.class);
        OperationRunner handler = mock(OperationRunner.class);
        when(handlerFactory.createGenericRunner()).thenReturn(handler);
        when(handlerFactory.createPartitionRunner(anyInt())).thenReturn(handler);

        initExecutor();

        DummyOperation operation = new DummyOperation(Operation.GENERIC_PARTITION_ID);
        Data data = serializationService.toData(operation);

        Packet packet = new Packet(data, operation.getPartitionId());
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
        Packet packet = new Packet(serializationService.toData(operation), operation.getPartitionId());
        packet.setHeader(Packet.HEADER_OP);

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
            executor.execute((Packet) task);
        } else {
            fail("invalid task!");
        }

        final int threadCount = executor.getPartitionOperationThreadCount();
        for (int i = 0; i < threadCount; i++) {
            executor.execute(new DummyPartitionOperation(i));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, executor.getOperationExecutorQueueSize());
            }
        });
    }
}
