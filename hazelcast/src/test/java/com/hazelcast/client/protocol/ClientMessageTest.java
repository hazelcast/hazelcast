package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * ClientMessage Tests of Flyweight functionality
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMessageTest {

    private static final String DEFAULT_ENCODING = "UTF8";

    private static final String VAR_DATA_STR_1 = "abcdef";

    private static final byte[] BYTE_DATA = VAR_DATA_STR_1.getBytes();

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void shouldEncodeClientMessageCorrectly() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(512);

        TestClientMessage cmEncode = new TestClientMessage();
        cmEncode.wrapForEncode(byteBuffer.array(), 0, byteBuffer.capacity());

        cmEncode.setMessageType(0x1122).setVersion((short) 0xEF).setFlags(ClientMessage.BEGIN_AND_END_FLAGS).setCorrelationId(0x12345678)
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

        //setCorrelationId
        assertThat(byteBuffer.get(8), is((byte) 0x78));
        assertThat(byteBuffer.get(9), is((byte) 0x56));
        assertThat(byteBuffer.get(10), is((byte) 0x34));
        assertThat(byteBuffer.get(11), is((byte) 0x12));

        //setPartitionId
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
        byte[] byteBuffer = new byte[512];

        TestClientMessage cmEncode = new TestClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0, byteBuffer.length);

        cmEncode.setMessageType(7)
                .setVersion((short) 3)
                .setFlags(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, 0, byteBuffer.length);

        assertEquals(7, cmDecode.getMessageType());
        assertEquals(3, cmDecode.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.getFlags());
        assertEquals(66, cmDecode.getCorrelationId());
        assertEquals(77, cmDecode.getPartitionId());
        assertEquals(ClientMessage.HEADER_SIZE, cmDecode.getFrameLength());
    }

    @Test
    public void shouldEncodeAndDecodeClientMessageCorrectly_withPayLoadData()
            throws UnsupportedEncodingException {
        byte[] byteBuffer = new byte[1024];

        TestClientMessage cmEncode = new TestClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0, byteBuffer.length);

        cmEncode.setMessageType(7)
                .setVersion((short) 3)
                .setFlags(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        final byte[] data1 = VAR_DATA_STR_1.getBytes(DEFAULT_ENCODING);
        final int calculatedFrameSize = ClientMessage.HEADER_SIZE + data1.length;
        cmEncode.putPayloadData(data1);

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, 0, byteBuffer.length);

        final byte[] cmDecodeVarData1 = new byte[data1.length];
        cmDecode.getPayloadData(cmDecodeVarData1);

        assertEquals(calculatedFrameSize, cmDecode.getFrameLength());
        assertArrayEquals(cmDecodeVarData1, data1);
    }

    @Test
    public void shouldEncodeWithNewVersionAndDecodeWithOldVersionCorrectly_withPayLoadData()
            throws UnsupportedEncodingException {
        byte[] byteBuffer = new byte[1024];

        FutureClientMessage cmEncode = new FutureClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0, byteBuffer.length);

        cmEncode.theNewField(999)
                .setMessageType(7).setVersion((short) 3)
                .setFlags(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        final int calculatedFrameSize = FutureClientMessage.THE_NEW_HEADER_SIZE + BYTE_DATA.length;
        cmEncode.putPayloadData(BYTE_DATA);

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, 0, byteBuffer.length);

        final byte[] cmDecodeVarData1 = new byte[BYTE_DATA.length];
        cmDecode.getPayloadData(cmDecodeVarData1);

        assertEquals(7, cmDecode.getMessageType());
        assertEquals(3, cmDecode.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.getFlags());
        assertEquals(66, cmDecode.getCorrelationId());
        assertEquals(77, cmDecode.getPartitionId());
        assertEquals(calculatedFrameSize, cmDecode.getFrameLength());
        assertArrayEquals(cmDecodeVarData1, BYTE_DATA);
    }

    @Test
    public void shouldEncodeWithOldVersionAndDecodeWithNewVersionCorrectly_withPayLoadData()
            throws UnsupportedEncodingException {
        byte[] byteBuffer = new byte[1024];

        TestClientMessage cmEncode = new TestClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0, byteBuffer.length);

        cmEncode.setMessageType(7).setVersion((short) 3)
                .setFlags(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        final int calculatedFrameSize = ClientMessage.HEADER_SIZE + BYTE_DATA.length;
        cmEncode.putPayloadData(BYTE_DATA);

        FutureClientMessage cmDecode = new FutureClientMessage();
        cmDecode.wrapForDecode(byteBuffer, 0, byteBuffer.length);

        final byte[] cmDecodeVarData1 = new byte[BYTE_DATA.length];
        cmDecode.getPayloadData(cmDecodeVarData1);

        assertEquals(7, cmDecode.getMessageType());
        assertEquals(3, cmDecode.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.getFlags());
        assertEquals(66, cmDecode.getCorrelationId());
        assertEquals(77, cmDecode.getPartitionId());
        assertEquals(calculatedFrameSize, cmDecode.getFrameLength());
        assertArrayEquals(cmDecodeVarData1, BYTE_DATA);
    }

    @Test
    public void shouldEncodeAndDecodeClientMessageCorrectly_withPayLoadData_multipleMessages()
            throws UnsupportedEncodingException {
        byte[] byteBuffer = new byte[1024];
        TestClientMessage cmEncode = new TestClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0, byteBuffer.length);
        cmEncode.setMessageType(7).setVersion((short) 3).setFlags(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(1).setPartitionId(77);
        cmEncode.putPayloadData(BYTE_DATA);
        final int calculatedFrame1Size = ClientMessage.HEADER_SIZE + BYTE_DATA.length;

        final int nexMessageOffset = cmEncode.getFrameLength();
        TestClientMessage cmEncode2 = new TestClientMessage();
        cmEncode2.wrapForEncode(byteBuffer, nexMessageOffset, byteBuffer.length - nexMessageOffset);
        cmEncode2.setMessageType(7).setVersion((short) 3).setFlags(ClientMessage.BEGIN_AND_END_FLAGS)
                 .setCorrelationId(2).setPartitionId(77);
        cmEncode2.putPayloadData(BYTE_DATA);
        final int calculatedFrame2Size = ClientMessage.HEADER_SIZE + BYTE_DATA.length;

        ClientMessage cmDecode1 = ClientMessage.createForDecode(byteBuffer, 0, byteBuffer.length);

        final byte[] cmDecodeVarData = new byte[BYTE_DATA.length];
        cmDecode1.getPayloadData(cmDecodeVarData);

        assertEquals(7, cmDecode1.getMessageType());
        assertEquals(3, cmDecode1.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode1.getFlags());
        assertEquals(1, cmDecode1.getCorrelationId());
        assertEquals(77, cmDecode1.getPartitionId());
        assertEquals(calculatedFrame1Size, cmDecode1.getFrameLength());
        assertArrayEquals(cmDecodeVarData, BYTE_DATA);

        ClientMessage cmDecode2 = ClientMessage.createForDecode(byteBuffer, cmDecode1.getFrameLength(),
                byteBuffer.length - cmDecode1.getFrameLength());
        cmDecode2.getPayloadData(cmDecodeVarData);

        assertEquals(7, cmDecode2.getMessageType());
        assertEquals(3, cmDecode2.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode2.getFlags());
        assertEquals(2, cmDecode2.getCorrelationId());
        assertEquals(77, cmDecode2.getPartitionId());
        assertEquals(calculatedFrame2Size, cmDecode2.getFrameLength());
        assertArrayEquals(cmDecodeVarData, BYTE_DATA);
    }

    private static class FutureClientMessage extends TestClientMessage {

        private static final int THE_NEW_FIELD_OFFSET = HEADER_SIZE + BitUtil.SIZE_OF_SHORT;
        private static final int THE_NEW_HEADER_SIZE = HEADER_SIZE + BitUtil.SIZE_OF_INT;

        @Override
        public void wrapForEncode(byte[] buffer, final int offset, int length) {
            super.wrap(buffer, offset, length);
            setDataOffset(THE_NEW_HEADER_SIZE);
            setFrameLength(THE_NEW_HEADER_SIZE);
            index(getDataOffset());
        }

        public int theNewField(){
            return (int) uint32Get(offset() + THE_NEW_FIELD_OFFSET, LITTLE_ENDIAN);
        }
        public FutureClientMessage theNewField(int value){
            uint32Put(offset() + THE_NEW_FIELD_OFFSET, value, LITTLE_ENDIAN);
            return this;
        }
    }

    private static class TestClientMessage extends ClientMessage {

        @Override
        public ClientMessage setMessageType(int type) {
            return super.setMessageType(type);
        }
    }



}
