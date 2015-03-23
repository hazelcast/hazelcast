package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


/**
 * ClientMessage Tests of Flyweight functionality
 */
public class ClientMessageTest {

    private static final String DEFAULT_ENCODING = "UTF8";

    private static final String VAR_DATA_STR_1 = "abc";
    private static final String VAR_DATA_STR_2 = "def";


    @Before
    public void setUp(){}

    @After
    public void tearDown(){}

    @Test
    public void shouldEncodeClientMessageCorrectly() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(512);

        ClientMessage cmEncode = new ClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0);

        cmEncode.headerType(7)
                .version((short) 3)
                .flags(ClientMessage.BEGIN_AND_END_FLAGS)
                .correlationId(0x1234);

        // little endian
        assertThat(byteBuffer.get(0), is((byte)0x0E));
        assertThat(byteBuffer.get(1), is((byte)0));
        assertThat(byteBuffer.get(2), is((byte)0));
        assertThat(byteBuffer.get(3), is((byte)0));
        assertThat(byteBuffer.get(4), is((byte)0x34));
        assertThat(byteBuffer.get(5), is((byte)0x12));
        assertThat(byteBuffer.get(6), is((byte)0));
        assertThat(byteBuffer.get(7), is((byte)0));
        assertThat(byteBuffer.get(8), is((byte)0x03));
        assertThat(byteBuffer.get(9), is((byte)0xC0));
        assertThat(byteBuffer.get(10), is((byte)0x07));
        assertThat(byteBuffer.get(11), is((byte) 0));
        assertThat(byteBuffer.get(12), is((byte) 0x0E));
        assertThat(byteBuffer.get(13), is((byte)0));
    }

    @Test
    public void shouldEncodeAndDecodeClientMessageCorrectly() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(512);

        ClientMessage cmEncode = new ClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0);

        cmEncode.headerType(7)
                .version((short) 3)
                .flags(ClientMessage.BEGIN_AND_END_FLAGS)
                .correlationId(66);

        ClientMessage cmDecode = new ClientMessage();
        cmDecode.wrapForDecode(byteBuffer, 0);

        assertEquals(7, cmDecode.headerType());
        assertEquals(3, cmDecode.version());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.flags());
        assertEquals(66, cmDecode.correlationId());
        assertEquals(14, cmDecode.frameLength());
    }


//    @Test
//    public void shouldEncodeAndDecodeClientMessageCorrectly_withVarData() throws UnsupportedEncodingException {
//        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
//
//        ClientMessage cmEncode = new ClientMessage();
//        cmEncode.wrapForEncode(byteBuffer, 0);
//
//        cmEncode.headerType(7)
//                .version((short) 3)
//                .flags(ClientMessage.BEGIN_AND_END_FLAGS)
//                .correlationId(66);
//
//        byte[] data1 = VAR_DATA_STR_1.getBytes(DEFAULT_ENCODING);
//        byte[] data2 = VAR_DATA_STR_2.getBytes(DEFAULT_ENCODING);
//
//        cmEncode.varDataPut(data1);
//        cmEncode.varDataPut(data2);
//
//        final int calculatedFrameSize = ClientMessage.HEADER_SIZE
//                + BitUtil.SIZE_OF_INT + data1.length
//                + BitUtil.SIZE_OF_INT + data2.length;
//
//        ClientMessage cmDecode = new ClientMessage();
//        cmDecode.wrapForDecode(byteBuffer, 0);
//
//        assertEquals(calculatedFrameSize, cmEncode.frameLength());
//
//        assertEquals(7, cmDecode.headerType());
//        assertEquals(3, cmDecode.version());
//        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.flags());
//        assertEquals(66, cmDecode.correlationId());
//        assertEquals(calculatedFrameSize, cmDecode.frameLength());
//
//        byte[] cmDecodeVarData1 = cmDecode.varDataGet();
//        byte[] cmDecodeVarData2 = cmDecode.varDataGet();
//
//        assertArrayEquals(cmDecodeVarData1, data1 );
//        assertArrayEquals(cmDecodeVarData2, data2);
//    }
}
