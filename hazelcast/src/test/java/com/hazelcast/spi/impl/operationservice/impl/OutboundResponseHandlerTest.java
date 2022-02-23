/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupAckResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallId;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setConnection;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OutboundResponseHandlerTest {

    @Parameter
    public ByteOrder byteOrder;

    private OutboundResponseHandler handler;
    private InternalSerializationService serializationService;
    private ILogger logger = Logger.getLogger(OutboundResponseHandlerTest.class);
    private Address thatAddress;
    private ServerConnectionManager connectionManager;
    private ServerConnection connection;

    @Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {BIG_ENDIAN},
                {LITTLE_ENDIAN},
        };
    }

    @Before
    public void setup() throws Exception {
        Address thisAddress = new Address("127.0.0.1", 5701);
        thatAddress = new Address("127.0.0.1", 5702);
        serializationService = new DefaultSerializationServiceBuilder().setByteOrder(byteOrder).build();
        connectionManager = mock(ServerConnectionManager.class);
        connection = mock(ServerConnection.class);
        when(connection.getConnectionManager()).thenReturn(connectionManager);
        handler = new OutboundResponseHandler(thisAddress, serializationService, logger);
    }

    @Test
    public void sendResponse_whenNormalResponse() {
        NormalResponse response = new NormalResponse("foo", 10, 1, false);
        Operation op = createDummyOperation(response.getCallId());

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        when(connectionManager.transmit(argument.capture(), eq(thatAddress), anyInt())).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        assertEquals(serializationService.toData(response), argument.getValue());
    }

    @Test
    public void sendResponse_whenPortable() {
        Object response = new PortableAddress("Sesame Street", 1);
        Operation op = createDummyOperation(10);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        when(connectionManager.transmit(argument.capture(), eq(thatAddress), anyInt())).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        NormalResponse expected = new NormalResponse(response, op.getCallId(), 0, op.isUrgent());
        assertEquals(serializationService.toData(expected), argument.getValue());
    }

    @Test
    public void sendResponse_whenOrdinaryValue() {
        Object response = "foobar";
        Operation op = createDummyOperation(10);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        when(connectionManager.transmit(argument.capture(), eq(thatAddress), anyInt())).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        NormalResponse expected = new NormalResponse(response, op.getCallId(), 0, op.isUrgent());
        assertEquals(serializationService.toData(expected), argument.getValue());
    }

    @Test
    public void sendResponse_whenNull() {
        Operation op = createDummyOperation(10);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        when(connectionManager.transmit(argument.capture(), eq(thatAddress), anyInt())).thenReturn(true);

        // make the call
        handler.sendResponse(op, null);

        // verify that the right object was send
        NormalResponse expected = new NormalResponse(null, op.getCallId(), 0, op.isUrgent());
        assertEquals(serializationService.toData(expected), argument.getValue());
    }

    @Test
    public void sendResponse_whenTimeoutResponse() {
        CallTimeoutResponse response = new CallTimeoutResponse(10, false);

        Operation op = createDummyOperation(10);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        when(connectionManager.transmit(argument.capture(), eq(thatAddress), anyInt())).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        assertEquals(serializationService.toData(response), argument.getValue());
    }

    @Test
    public void sendResponse_whenErrorResponse() {
        ErrorResponse response = new ErrorResponse(new Exception(), 10, false);

        Operation op = createDummyOperation(10);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        when(connectionManager.transmit(argument.capture(), eq(thatAddress), anyInt())).thenReturn(true);

        // make the call
        handler.sendResponse(op, response);

        // verify that the right object was send
        assertEquals(serializationService.toData(response), argument.getValue());
    }

    @Test
    public void sendResponse_whenThrowable() {
        Exception exception = new Exception();

        Operation op = createDummyOperation(10);

        ArgumentCaptor<Packet> argument = ArgumentCaptor.forClass(Packet.class);
        when(connectionManager.transmit(argument.capture(), eq(thatAddress), anyInt())).thenReturn(true);

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

        PortableAddress() {
        }

        PortableAddress(String street, int no) {
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
            writer.writeString("street", street);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            street = reader.readString("street");
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

    private Operation createDummyOperation(long i) {
        Operation op = new DummyOperation();
        setCallId(op, i);
        setCallerAddress(op, thatAddress);
        setConnection(op, connection);
        return op;
    }
}
