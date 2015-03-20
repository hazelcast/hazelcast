package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
}
