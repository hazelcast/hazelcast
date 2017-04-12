package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.SerializationConcurrencyTest;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupAckResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OutboundResponseHandlerTest {

    private OutboundResponseHandler handler;
    private Address thisAddress;
    private InternalSerializationService serializationService;
    private ILogger logger = Logger.getLogger(OutboundResponseHandlerTest.class);
    private Node node;
    private Address thatAddress;
    private ConnectionManager connectionManager;

    @Before
    public void setup() throws Exception {
        thisAddress = new Address("127.0.0.1", 5701);
        thatAddress = new Address("127.0.0.1", 5702);
        serializationService = new DefaultSerializationServiceBuilder().build();
        node = mock(Node.class);
        connectionManager = mock(ConnectionManager.class);
        when(node.getConnectionManager()).thenReturn(connectionManager);
        handler = new OutboundResponseHandler(thisAddress, serializationService, node, logger);
    }

    @Test
    public void sendResponse_whenNormalResponse() {
        NormalResponse response = new NormalResponse("foo", 10, 1, false);
        Operation op = new DummyOperation();
        setCallId(op, response.getCallId());
        setCallerAddress(op, thatAddress);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getOrConnect(thatAddress)).thenReturn(connection);
        when(connectionManager.transmit(argument.capture(), eq(connection))).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        assertEquals(serializationService.toData(response), argument.getValue());
    }

    @Test
    public void sendResponse_whenPortable() {
        Object response = new PortableAddress("Sesame Street",1);
        Operation op = new DummyOperation();
        setCallId(op, 10);
        setCallerAddress(op, thatAddress);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getOrConnect(thatAddress)).thenReturn(connection);
        when(connectionManager.transmit(argument.capture(), eq(connection))).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        NormalResponse expected = new NormalResponse(response, op.getCallId(), 0, op.isUrgent());
        assertEquals(serializationService.toData(expected), argument.getValue());
    }

    @Test
    public void sendResponse_whenOrdinaryValue() {
        Object response = "foobar";
        Operation op = new DummyOperation();
        setCallId(op, 10);
        setCallerAddress(op, thatAddress);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getOrConnect(thatAddress)).thenReturn(connection);
        when(connectionManager.transmit(argument.capture(), eq(connection))).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        NormalResponse expected = new NormalResponse(response, op.getCallId(), 0, op.isUrgent());
        assertEquals(serializationService.toData(expected), argument.getValue());
    }

    @Test
    public void sendResponse_whenNull() {
        Operation op = new DummyOperation();
        setCallId(op, 10);
        setCallerAddress(op, thatAddress);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getOrConnect(thatAddress)).thenReturn(connection);
        when(connectionManager.transmit(argument.capture(), eq(connection))).thenReturn(true);

        // make the call
        handler.sendResponse(op, null);

        // verify that the right object was send
        NormalResponse expected = new NormalResponse(null, op.getCallId(), 0, op.isUrgent());
        assertEquals(serializationService.toData(expected), argument.getValue());
    }

    @Test
    public void sendResponse_whenTimeoutResponse() {
        CallTimeoutResponse response = new CallTimeoutResponse(10, false);

        Operation op = new DummyOperation();
        setCallId(op, 10);
        setCallerAddress(op, thatAddress);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getOrConnect(thatAddress)).thenReturn(connection);
        when(connectionManager.transmit(argument.capture(), eq(connection))).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        assertEquals(serializationService.toData(response), argument.getValue());
    }

    @Test
    public void sendResponse_whenErrorResponse() {
        ErrorResponse response = new ErrorResponse(new Exception(), 10, false);

        Operation op = new DummyOperation();
        setCallId(op, 10);
        setCallerAddress(op, thatAddress);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getOrConnect(thatAddress)).thenReturn(connection);
        when(connectionManager.transmit(argument.capture(), eq(connection))).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        assertEquals(serializationService.toData(response), argument.getValue());
    }

    @Test
    public void sendResponse_whenThrowable() {
        Exception exception = new Exception();

        Operation op = new DummyOperation();
        setCallId(op, 10);
        setCallerAddress(op, thatAddress);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getOrConnect(thatAddress)).thenReturn(connection);
        when(connectionManager.transmit(argument.capture(), eq(connection))).thenReturn(true);

        // make the call
        handler.sendResponse(op, exception);

        // verify that the right object was send
        ErrorResponse expectedResponse = new ErrorResponse(exception, op.getCallId(), op.isUrgent());
        assertEquals(serializationService.toData(expectedResponse), argument.getValue());
    }


    @Test
    public void toBackupAckPacket() {
        testToBackupAckPacket(1, false);
        testToBackupAckPacket(2, true);
    }

    private void testToBackupAckPacket(int callId, boolean urgent) {
        Packet packet = handler.toBackupAckPacket(callId, urgent);
        HeapData expected = serializationService.toData(new BackupAckResponse(callId, urgent));
        assertEquals(expected, new HeapData(packet.toByteArray()));
    }

    @Test
    public void toNormalResponsePacket_whenNormalValues() {
        testToNormalResponsePacket("foo", 1, 0, false);
        testToNormalResponsePacket("foo", 2, 0, true);
        testToNormalResponsePacket("foo", 3, 2, false);
    }

    @Test
    public void toNormalResponsePacket_whenNullValue() {
        testToNormalResponsePacket(null, 1, 2, false);
    }

    @Test
    public void toNormalResponsePacket_whenDataValue() {
        testToNormalResponsePacket(serializationService.toBytes("foobar"), 1, 2, false);
    }

    private void testToNormalResponsePacket(Object value, int callId, int backupAcks, boolean urgent) {
        Packet packet = handler.toNormalResponsePacket(callId, backupAcks, urgent, value);
        HeapData expected = serializationService.toData(new NormalResponse(value, callId, backupAcks, urgent));
        assertEquals(expected, new HeapData(packet.toByteArray()));
    }

    static class PortableAddress implements Portable {

        private String street;

        private int no;

        public PortableAddress() {
        }

        public PortableAddress(String street, int no) {
            this.street = street;
            this.no = no;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("no", no);
            writer.writeUTF("street", street);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            street = reader.readUTF("street");
            no = reader.readInt("no");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PortableAddress that = (PortableAddress) o;
            if (no != that.no) {
                return false;
            }
            if (street != null ? !street.equals(that.street) : that.street != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = street != null ? street.hashCode() : 0;
            result = 31 * result + no;
            return result;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }
    }
}
