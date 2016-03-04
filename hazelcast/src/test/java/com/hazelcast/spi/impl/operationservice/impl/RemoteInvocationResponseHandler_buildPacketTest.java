package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.readLong;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_BACKUP_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_ERROR_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_NORMAL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.RemoteInvocationResponseHandler.TYPE_TIMEOUT_RESPONSE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RemoteInvocationResponseHandler_buildPacketTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        serializationService = getSerializationService(hz);
    }

    @Test
    public void buildResponse_whenNotUrgent() {
        buildResponsePacket(false);
    }

    @Test
    public void buildResponse_whenUrgent() {
        buildResponsePacket(true);
    }

    public void buildResponsePacket(boolean urgent) {
        long expectedCallId = 100;
        int expectedBackupCount = 3;
        String expectedResponse = "response";

        Packet packet = RemoteInvocationResponseHandler.buildResponsePacket(
                serializationService, urgent, expectedCallId, expectedBackupCount, expectedResponse);

        if (urgent) {
            assertFlags(FLAG_RESPONSE | FLAG_OP | FLAG_URGENT, packet);
        } else {
            assertFlags(FLAG_RESPONSE | FLAG_OP, packet);
        }
        assertEquals(TYPE_NORMAL_RESPONSE, getType(packet));
        assertEquals(expectedResponse, serializationService.toObject(packet));
        assertEquals(expectedCallId, getCallId(packet));
    }

    @Test
    public void buildErrorResponsePacket_whenNotUrgent() {
        buildErrorResponsePacket(false);
    }

    @Test
    public void buildErrorResponsePacket_whenUrgent() {
        buildErrorResponsePacket(true);
    }

    public void buildErrorResponsePacket(boolean urgent) {
        long expectedCallId = 100;
        Throwable expectedError = new ExpectedRuntimeException();

        Packet packet = RemoteInvocationResponseHandler.buildErrorResponsePacket(
                serializationService,urgent, expectedCallId, expectedError);

        if (urgent) {
            assertFlags(FLAG_RESPONSE | FLAG_OP | FLAG_URGENT, packet);
        } else {
            assertFlags(FLAG_RESPONSE | FLAG_OP, packet);
        }
        assertEquals(TYPE_ERROR_RESPONSE, getType(packet));
        assertInstanceOf(ExpectedRuntimeException.class, serializationService.toObject(packet));
        assertEquals(expectedCallId, getCallId(packet));
    }

    @Test
    public void buildTimeoutResponsePacket_whenNotUrgent() {
        buildTimeoutResponsePacket(false);
    }

    @Test
    public void buildTimeoutResponsePacket_whenUrgent() {
        buildTimeoutResponsePacket(true);
    }

    public void buildTimeoutResponsePacket(boolean urgent) {
        long expectedCallId = 100;

        Packet packet = RemoteInvocationResponseHandler.buildTimeoutResponsePacket(serializationService,urgent, expectedCallId);

        if (urgent) {
            assertFlags(FLAG_RESPONSE | FLAG_OP | FLAG_URGENT, packet);
        } else {
            assertFlags(FLAG_RESPONSE | FLAG_OP, packet);
        }
        assertEquals(TYPE_TIMEOUT_RESPONSE, getType(packet));
        assertEquals(LONG_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES, packet.toByteArray().length);
        assertEquals(expectedCallId, getCallId(packet));
    }

    @Test
    public void buildBackupResponsePacket_whenNotUrgent() {
        buildBackupResponsePacket(false);
    }

    @Test
    public void buildBackupResponsePacket_whenUrgent() {
        buildBackupResponsePacket(true);
    }

    public void buildBackupResponsePacket(boolean urgent) {
        long expectedCallId = 100;

        Packet packet = RemoteInvocationResponseHandler.buildBackupResponsePacket(serializationService,urgent, expectedCallId);

        if (urgent) {
            assertFlags(FLAG_RESPONSE | FLAG_OP | FLAG_URGENT, packet);
        } else {
            assertFlags(FLAG_RESPONSE | FLAG_OP, packet);
        }
        assertEquals(TYPE_BACKUP_RESPONSE, getType(packet));
        assertEquals(LONG_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES, packet.toByteArray().length);
        assertEquals(expectedCallId, getCallId(packet));
    }

    private byte getType(Packet packet) {
        byte[] payload = packet.toByteArray();
        return payload[payload.length - 1];
    }

    private long getCallId(Packet packet) {
        byte[] payload = packet.toByteArray();
        return readLong(payload, payload.length - 9, true);
    }

    private void assertFlags(int flags, Packet packet) {
        assertEquals(flags, packet.getFlags());
    }
}
