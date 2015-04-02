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

    private ByteBuffer byteBuffer;

    @Before
    public void setUp() {
        byteBuffer = ByteBuffer.allocate(20);
    }

    @Test
    public void shouldEncodeCorrectly_PUT() {
        MapPutParameters parameters = MapPutParameters.encode(NAME, BYTES_DATA, BYTES_DATA, THE_LONG, THE_LONG, THE_BOOLEAN);
        byteBuffer = parameters.buffer().byteBuffer();

        ClientMessage cmDecode = new ClientMessage();
        cmDecode.wrapForDecode(this.byteBuffer,0);

        final MapPutParameters putParameters = MapPutParameters.decode(cmDecode);

        assertEquals(NAME, putParameters.name);
        assertArrayEquals(BYTES_DATA, putParameters.key);
        assertArrayEquals(BYTES_DATA, putParameters.value);
        assertEquals(THE_LONG, putParameters.threadId);
        assertEquals(THE_LONG, putParameters.ttl);
        assertEquals(THE_BOOLEAN, putParameters.async);
    }

}
