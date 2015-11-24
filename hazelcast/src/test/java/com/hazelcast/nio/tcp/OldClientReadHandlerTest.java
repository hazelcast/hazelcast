package com.hazelcast.nio.tcp;

import com.hazelcast.client.ClientTypes;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;

import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OldClientReadHandlerTest extends HazelcastTestSupport {

    private OldClientReadHandler readHandler;
    private Connection connection;
    private IOService ioService;
    private SerializationService serializationService;

    @Before
    public void setup() throws Exception {
        ioService = mock(IOService.class);
        ILogger logger = Logger.getLogger(getClass());
        when(ioService.getLogger(anyString())).thenReturn(logger);
        serializationService = new DefaultSerializationServiceBuilder().build();
        connection = mock(Connection.class);
        readHandler = new OldClientReadHandler(connection, ioService);
    }

    @Test
    public void connectionType_whenJAVA() throws Exception {
        connectionType(ClientTypes.JAVA, ConnectionType.JAVA_CLIENT);
    }

    @Test
    public void connectionType_whenCHARP() throws Exception {
        connectionType(ClientTypes.CSHARP, ConnectionType.CSHARP_CLIENT);
    }

    @Test
    public void connectionType_whenCPP() throws Exception {
        connectionType(ClientTypes.CPP, ConnectionType.CPP_CLIENT);
    }

    @Test
    public void connectionType_whenPython() throws Exception {
        connectionType(ClientTypes.PYTHON, ConnectionType.PYTHON_CLIENT);
    }

    @Test
    public void connectionType_whenRuby() throws Exception {
        connectionType(ClientTypes.RUBY, ConnectionType.RUBY_CLIENT);
    }

    @Test
    public void connectionType_whenUnknown() throws Exception {
        connectionType("???", ConnectionType.BINARY_CLIENT);
    }

    public void connectionType(String type, ConnectionType expected) throws Exception {
        Packet packet = new Packet(serializationService.toBytes("foobar"));

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        buffer.put(type.getBytes());
        packet.writeTo(buffer);

        buffer.flip();
        readHandler.onRead(buffer);

        Mockito.verify(connection).setType(expected);
        verify(ioService).handleClientPacket(eq(packet));
    }

    @Test
    public void whenNotEnoughBytes() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        buffer.put("J".getBytes());

        buffer.flip();
        readHandler.onRead(buffer);

        verifyZeroInteractions(connection);
    }

    @Test
    public void whenMultiplePackets() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        buffer.put(ClientTypes.JAVA.getBytes());

        Packet packet1 = new Packet(serializationService.toBytes(generateRandomString(2)));
        Packet packet2 = new Packet(serializationService.toBytes(generateRandomString(20)));
        Packet packet3 = new Packet(serializationService.toBytes(generateRandomString(100)));
        packet1.writeTo(buffer);
        packet2.writeTo(buffer);
        packet3.writeTo(buffer);

        buffer.flip();
        readHandler.onRead(buffer);

        verify(ioService).handleClientPacket(eq(packet1));
        verify(ioService).handleClientPacket(eq(packet2));
        verify(ioService).handleClientPacket(eq(packet3));
    }
}
