package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.map.MapPutParameters;
import com.hazelcast.client.impl.protocol.util.ParameterFlyweight;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Encode Decode Tests
 */
public class MapMessageEncodeDecodeTest {

    private static final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    private static final String NAME = "name";
    private static final Data DATA = serializationService.toData("The Test");
    private static final byte[] BYTES_DATA = DATA.toByteArray();
    private static final long THE_LONG = 0xFFFFl;

    private static final boolean THE_BOOLEAN = true;
    private ParameterFlyweight flyweight = new ParameterFlyweight();

    private ByteBuffer byteBuffer;

    @Before
    public void setUp() {
        byteBuffer = ByteBuffer.allocate(1024);
        flyweight.wrap(byteBuffer);
    }

    @Test
    public void shouldEncodeCorrectly_PUT() {
        MapPutParameters.encode(flyweight, NAME, BYTES_DATA, BYTES_DATA, THE_LONG, THE_LONG, THE_BOOLEAN);

        flyweight.index(0);
        final MapPutParameters putParameters = MapPutParameters.decode(flyweight);

        assertEquals(NAME, putParameters.name);
        assertArrayEquals(BYTES_DATA, putParameters.key);
        assertArrayEquals(BYTES_DATA, putParameters.value);
        assertEquals(THE_LONG, putParameters.threadId);
        assertEquals(THE_LONG, putParameters.ttl);
        assertEquals(THE_BOOLEAN, putParameters.async);
    }

    @Test
    public void shouldEncodeDecodeWithHeaderCorrectly_PUT() {
        byteBuffer = ByteBuffer
                .allocate(MapPutParameters.encodeSizeCost(NAME, BYTES_DATA, BYTES_DATA));
        ClientMessage cmEncode = new ClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0);

        MapPutParameters.encode(cmEncode, NAME, BYTES_DATA, BYTES_DATA, THE_LONG, THE_LONG, THE_BOOLEAN);
        cmEncode.setHeaderType(7).setVersion((short) 3).setFlags(ClientMessage.BEGIN_AND_END_FLAGS).setCorrelationId(66).setPartitionId(77);

        ClientMessage cmDecode = new ClientMessage();
        cmDecode.wrapForDecode(byteBuffer, 0);

        final MapPutParameters putParameters = MapPutParameters.decode(cmDecode);

        assertEquals(7, cmDecode.getHeaderType());
        assertEquals(3, cmDecode.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.getFlags());
        assertEquals(66, cmDecode.getCorrelationId());
        assertEquals(77, cmDecode.getPartitionId());

        assertEquals(NAME, putParameters.name);
        assertArrayEquals(BYTES_DATA, putParameters.key);
        assertArrayEquals(BYTES_DATA, putParameters.value);
        assertEquals(THE_LONG, putParameters.threadId);
        assertEquals(THE_LONG, putParameters.ttl);
        assertEquals(THE_BOOLEAN, putParameters.async);
    }
}
