package com.hazelcast.nio.tcp;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NewClientReadHandlerTest {

    private NewClientReadHandler readHandler;
    private IOService ioService;
    private Connection connection;

    @Before
    public void setup() throws IOException {
        ioService = mock(IOService.class);
        connection = mock(Connection.class);
        readHandler = new NewClientReadHandler(connection, ioService);
    }

    @Test
    public void test() throws Exception {
        ClientMessage message = ClientMessage.createForEncode(1000)
                .setPartitionId(10)
                .setMessageType(1)
                .setCorrelationId(1)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS);

        ByteBuffer bb = ByteBuffer.allocate(1000);
        message.writeTo(bb);
        bb.flip();

        readHandler.onRead(bb);

        verify(ioService).handleClientMessage(any(ClientMessage.class), eq(connection));
    }
}
