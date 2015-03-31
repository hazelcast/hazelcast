package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * ClientMessage Tests of Flyweight functionality
 */
public class ClientMessageTest {

    private static final String DEFAULT_ENCODING = "UTF8";

    private static final String VAR_DATA_STR_1 = "abcdef";

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void shouldEncodeClientMessageCorrectly() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(512);

        ClientMessage cmEncode = new ClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0);

        cmEncode.setHeaderType(0x1122).setVersion((short) 0xEF).setFlags(ClientMessage.BEGIN_AND_END_FLAGS).setCorrelationId(0x12345678)
                .setPartitionId(0x11223344);

        // little endian
        //FRAME LENGTH
        assertThat(byteBuffer.get(0), is((byte) ClientMessage.HEADER_SIZE));
        assertThat(byteBuffer.get(1), is((byte) 0));
        assertThat(byteBuffer.get(2), is((byte) 0));
        assertThat(byteBuffer.get(3), is((byte) 0));

        //VERSION
        assertThat(byteBuffer.get(4), is((byte) 0xEF));

        //FLAGS
        assertThat(byteBuffer.get(5), is((byte) 0xC0));

        //TYPE
        assertThat(byteBuffer.get(6), is((byte) 0x22));
        assertThat(byteBuffer.get(7), is((byte) 0x11));

        //correlationId
        assertThat(byteBuffer.get(8), is((byte) 0x78));
        assertThat(byteBuffer.get(9), is((byte) 0x56));
        assertThat(byteBuffer.get(10), is((byte) 0x34));
        assertThat(byteBuffer.get(11), is((byte) 0x12));

        //partitionId
        assertThat(byteBuffer.get(12), is((byte) 0x44));
        assertThat(byteBuffer.get(13), is((byte) 0x33));
        assertThat(byteBuffer.get(14), is((byte) 0x22));
        assertThat(byteBuffer.get(15), is((byte) 0x11));

        //data offset
        assertThat(byteBuffer.get(16), is((byte) ClientMessage.HEADER_SIZE));
        assertThat(byteBuffer.get(17), is((byte) 0x00));

    }

    @Test
    public void shouldEncodeAndDecodeClientMessageCorrectly() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(512);

        ClientMessage cmEncode = new ClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0);

        cmEncode.setHeaderType(7).setVersion((short) 3).setFlags(ClientMessage.BEGIN_AND_END_FLAGS).setCorrelationId(66).setPartitionId(77);

        ClientMessage cmDecode = new ClientMessage();
        cmDecode.wrapForDecode(byteBuffer, 0);

        assertEquals(7, cmDecode.getHeaderType());
        assertEquals(3, cmDecode.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.getFlags());
        assertEquals(66, cmDecode.getCorrelationId());
        assertEquals(77, cmDecode.getPartitionId());
        assertEquals(ClientMessage.HEADER_SIZE, cmDecode.getFrameLength());
    }

    @Test
    public void shouldEncodeAndDecodeClientMessageCorrectly_withPayLoadData()
            throws UnsupportedEncodingException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

        ClientMessage cmEncode = new ClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0);

        final byte[] data1 = VAR_DATA_STR_1.getBytes(DEFAULT_ENCODING);
        final int calculatedFrameSize = ClientMessage.HEADER_SIZE + data1.length;
        cmEncode.putPayloadData(data1);

        ClientMessage cmDecode = new ClientMessage();
        cmDecode.wrapForDecode(byteBuffer, 0);

        final byte[] cmDecodeVarData1 = new byte[data1.length];
        cmDecode.getPayloadData(cmDecodeVarData1);

        assertEquals(calculatedFrameSize, cmEncode.getFrameLength());
        assertEquals(calculatedFrameSize, cmDecode.getFrameLength());
        assertArrayEquals(cmDecodeVarData1, data1);
    }
}
