package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.parameters.MapPutParameters;
import com.hazelcast.client.impl.protocol.util.BitUtil;
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
    public void shouldEncodeDecodeCorrectly_PUT() {
        final int calculatedSize =
                (BitUtil.SIZE_OF_INT + NAME.length()) + (BitUtil.SIZE_OF_INT + BYTES_DATA.length) * 2 + 8 * 2 + 1
                        + ClientMessage.HEADER_SIZE;
        ClientMessage cmEncode = MapPutParameters.encode(NAME, BYTES_DATA, BYTES_DATA, THE_LONG, THE_LONG);
        cmEncode.setVersion((short) 3).setFlags(ClientMessage.BEGIN_AND_END_FLAGS).setCorrelationId(66).setPartitionId(77);

        byteBuffer = cmEncode.buffer().byteBuffer();

        ClientMessage cmDecode = ClientMessage.createForDecode(this.byteBuffer, 0);

        final MapPutParameters decodeParams = MapPutParameters.decode(cmDecode);

        assertEquals(calculatedSize, cmEncode.getFrameLength());

        assertEquals(ClientMessageType.MAP_PUT_REQUEST.id(), cmDecode.getMessageType());
        assertEquals(3, cmDecode.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.getFlags());
        assertEquals(66, cmDecode.getCorrelationId());
        assertEquals(77, cmDecode.getPartitionId());

        assertEquals(NAME, decodeParams.name);
        assertArrayEquals(BYTES_DATA, decodeParams.key);
        assertArrayEquals(BYTES_DATA, decodeParams.value);
        assertEquals(THE_LONG, decodeParams.threadId);
        assertEquals(THE_LONG, decodeParams.ttl);
    }

}
