package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.OperationHandler;
import com.hazelcast.spi.impl.OperationHandlerFactory;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OperationThreadTest extends AbstractClassicSchedulerTest {

    @Test
    public void testOOME_whenDeserializing() throws Exception {
        handlerFactory = mock(OperationHandlerFactory.class);
        OperationHandler handler = mock(OperationHandler.class);
        when(handlerFactory.createGenericOperationHandler()).thenReturn(handler);

        initScheduler();

        DummyOperation operation = new DummyOperation(-1);
        Data data = serializationService.toData(operation);

        Packet packet = new Packet(data, operation.getPartitionId(), serializationService.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);

        doThrow(new OutOfMemoryError()).when(handler).process(packet);

        final int oldCount = OutOfMemoryErrorDispatcher.getOutOfMemoryErrorCount();

        scheduler.execute(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(oldCount + 1, OutOfMemoryErrorDispatcher.getOutOfMemoryErrorCount());
            }
        });
    }
}
