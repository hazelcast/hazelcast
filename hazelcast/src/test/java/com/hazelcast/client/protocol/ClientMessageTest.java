package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.nio.Bits;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

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
        SafeBuffer safeBuffer = new SafeBuffer(byteBuffer.array());

        ClientMessage cmEncode = TestClientMessage.createForEncode(safeBuffer, 0);

        cmEncode.setMessageType(0x1122).setVersion((short) 0xEF).addFlag(ClientMessage.BEGIN_AND_END_FLAGS).setCorrelationId(0x12345678)
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
        SafeBuffer byteBuffer = new SafeBuffer(new byte[512]);

        ClientMessage cmEncode = TestClientMessage.createForEncode(byteBuffer, 0);

        cmEncode.setMessageType(7)
                .setVersion((short) 3)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, 0);

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
        SafeBuffer byteBuffer = new SafeBuffer(new byte[1024]);

        ClientMessage cmEncode = TestClientMessage.createForEncode(byteBuffer, 0);

        cmEncode.setMessageType(7)
                .setVersion((short) 3)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        final byte[] data1 = VAR_DATA_STR_1.getBytes(DEFAULT_ENCODING);
        final int calculatedFrameSize = ClientMessage.HEADER_SIZE + ParameterUtil.calculateByteArrayDataSize(data1);
        cmEncode.set(data1);
        cmEncode.updateFrameLength();

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, 0);

        byte[] cmDecodeVarData1 = cmDecode.getByteArray();

        assertEquals(calculatedFrameSize, cmDecode.getFrameLength());
        assertArrayEquals(cmDecodeVarData1, data1);
    }


    @Test
    public void shouldEncodeAndDecodeClientMessageCorrectly_withPayLoadData_fromOffset()
            throws UnsupportedEncodingException {
        SafeBuffer byteBuffer = new SafeBuffer(new byte[150]);
        int offset = 100;

        ClientMessage cmEncode = TestClientMessage.createForEncode(byteBuffer, offset);

        cmEncode.setMessageType(7)
                .setVersion((short) 3)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        byte[] bytes = VAR_DATA_STR_1.getBytes();
        final int calculatedFrameSize = ClientMessage.HEADER_SIZE + Bits.INT_SIZE_IN_BYTES +
                ParameterUtil.calculateByteArrayDataSize(bytes);
        cmEncode.set(1);
        cmEncode.set(bytes);
        cmEncode.updateFrameLength();

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, offset);

        assertEquals(1, cmDecode.getInt());
        assertArrayEquals(bytes, cmDecode.getByteArray());

        assertEquals(calculatedFrameSize, cmDecode.getFrameLength());
    }

    @Test
    public void shouldEncodeWithNewVersionAndDecodeWithOldVersionCorrectly_withPayLoadData()
            throws UnsupportedEncodingException {
        SafeBuffer byteBuffer = new SafeBuffer(new byte[1024]);

        FutureClientMessage cmEncode = new FutureClientMessage();
        cmEncode.wrapForEncode(byteBuffer, 0);

        cmEncode.theNewField(999)
                .setMessageType(7).setVersion((short) 3)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        final int calculatedFrameSize = FutureClientMessage.THE_NEW_HEADER_SIZE
                + ParameterUtil.calculateByteArrayDataSize(BYTE_DATA);
        cmEncode.set(BYTE_DATA);
        cmEncode.updateFrameLength();

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, 0);

        final byte[] cmDecodeVarData1 = cmDecode.getByteArray();

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
        SafeBuffer byteBuffer = new SafeBuffer(new byte[1024]);

        ClientMessage cmEncode = TestClientMessage.createForEncode(byteBuffer, 0);

        cmEncode.setMessageType(7).setVersion((short) 3)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(66).setPartitionId(77);

        final int calculatedFrameSize = ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateByteArrayDataSize(BYTE_DATA);
        cmEncode.set(BYTE_DATA);
        cmEncode.updateFrameLength();
        ClientMessage cmDecode = FutureClientMessage.createForDecode(byteBuffer, 0);

        final byte[] cmDecodeVarData1 = cmDecode.getByteArray();

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
        SafeBuffer byteBuffer = new SafeBuffer(new byte[1024]);
        ClientMessage cmEncode = TestClientMessage.createForEncode(byteBuffer, 0);
        cmEncode.setMessageType(7).setVersion((short) 3).addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(1).setPartitionId(77);
        cmEncode.set(BYTE_DATA);
        cmEncode.updateFrameLength();
        final int calculatedFrame1Size = ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateByteArrayDataSize(BYTE_DATA);

        final int nexMessageOffset = cmEncode.getFrameLength();
        ClientMessage cmEncode2 =
                TestClientMessage.createForEncode(byteBuffer, nexMessageOffset);

        cmEncode2.setMessageType(7).setVersion((short) 3).addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(2).setPartitionId(77);
        cmEncode2.set(BYTE_DATA);
        cmEncode2.updateFrameLength();
        final int calculatedFrame2Size = ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateByteArrayDataSize(BYTE_DATA);

        ClientMessage cmDecode1 = ClientMessage.createForDecode(byteBuffer, 0);

        final byte[] cmDecodeVarData = cmDecode1.getByteArray();

        assertEquals(7, cmDecode1.getMessageType());
        assertEquals(3, cmDecode1.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode1.getFlags());
        assertEquals(1, cmDecode1.getCorrelationId());
        assertEquals(77, cmDecode1.getPartitionId());
        assertEquals(calculatedFrame1Size, cmDecode1.getFrameLength());
        assertArrayEquals(cmDecodeVarData, BYTE_DATA);

        ClientMessage cmDecode2 = ClientMessage.createForDecode(byteBuffer, cmDecode1.getFrameLength());
        byte[] cmDecodeVarData2 = cmDecode2.getByteArray();

        assertEquals(7, cmDecode2.getMessageType());
        assertEquals(3, cmDecode2.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode2.getFlags());
        assertEquals(2, cmDecode2.getCorrelationId());
        assertEquals(77, cmDecode2.getPartitionId());
        assertEquals(calculatedFrame2Size, cmDecode2.getFrameLength());
        assertArrayEquals(cmDecodeVarData2, BYTE_DATA);
    }

    private static class FutureClientMessage extends TestClientMessage {

        private static final int THE_NEW_FIELD_OFFSET = HEADER_SIZE + Bits.SHORT_SIZE_IN_BYTES;
        private static final int THE_NEW_HEADER_SIZE = HEADER_SIZE + Bits.INT_SIZE_IN_BYTES;

        @Override
        protected void wrapForEncode(ClientProtocolBuffer buffer, int offset) {
            super.wrap(buffer, offset);
            setDataOffset(THE_NEW_HEADER_SIZE);
            setFrameLength(THE_NEW_HEADER_SIZE);
            index(getDataOffset());
        }

        public int theNewField() {
            return (int) uint32Get(THE_NEW_FIELD_OFFSET);
        }

        public FutureClientMessage theNewField(int value) {
            uint32Put(THE_NEW_FIELD_OFFSET, value);
            return this;
        }
    }

    private static class TestClientMessage extends ClientMessage {

        @Override
        public ClientMessage setMessageType(int type) {
            return super.setMessageType(type);
        }
    }

    @Test
    public void test_empty_toString() {
        new ClientMessage().toString();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void test_byteArray_constructor_withSmallBuffer() {
        ClientMessage.createForEncode(new SafeBuffer(new byte[10]), 1);
    }

    @Test
    public void test_byteArray_constructor_withHeaderSizeBuffer() {
        ClientMessage.createForEncode(new SafeBuffer(new byte[ClientMessage.HEADER_SIZE]), 0);
    }

    @Test
    public void test_byteArray_constructor_withLargeBuffer() {
        ClientMessage.createForEncode(new SafeBuffer(new byte[100]), 10);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void test_MutableDirectBuffer_constructor_withSmallBuffer() {
        SafeBuffer buffer = new SafeBuffer(new byte[10]);
        ClientMessage.createForEncode(buffer, 1);
    }

    @Test
    public void test_MutableDirectBuffer_constructor_withHeaderSizeBuffer() {
        SafeBuffer buffer = new SafeBuffer(new byte[ClientMessage.HEADER_SIZE]);
        ClientMessage.createForEncode(buffer, 0);
    }

    @Test
    public void test_MutableDirectBuffer_constructor_withLargeBuffer() {
        SafeBuffer buffer = new SafeBuffer(new byte[100]);
        ClientMessage.createForEncode(buffer, 10);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void test_wrapForEncode_withSmallBuffer() {
        ClientMessage.createForEncode(new SafeBuffer(new byte[10]), 1);
    }

    @Test
    public void test_wrapForEncode_withHeaderSizeBuffer() {
        ClientMessage.createForEncode(new SafeBuffer(new byte[ClientMessage.HEADER_SIZE]), 0);
    }

    @Test
    public void test_wrapForEncode_withLargeBuffer() {
        ClientMessage.createForEncode(new SafeBuffer(new byte[100]), 10);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void test_wrapForDecode_withSmallBuffer() {
        ClientMessage.createForDecode(new SafeBuffer(new byte[10]), 1);
    }

    @Test
    public void test_wrapForDecode_withHeaderSizeBuffer() {
        ClientMessage.createForDecode(new SafeBuffer(new byte[ClientMessage.HEADER_SIZE]), 0);
    }

    @Test
    public void test_wrapForDecode_withLargeBuffer() {
        ClientMessage.createForDecode(new SafeBuffer(new byte[100]), 10);
    }

    @Test
    public void testUnsignedFields() throws IOException {
        ClientProtocolBuffer buffer = new SafeBuffer(new byte[18]);

        ClientMessage cmEncode = ClientMessage.createForEncode(buffer, 0);

        cmEncode.setVersion((short) (Byte.MAX_VALUE + 10));
        cmEncode.setMessageType(Short.MAX_VALUE + 10);
        cmEncode.setDataOffset((int) Short.MAX_VALUE + 10);
        cmEncode.setCorrelationId(Short.MAX_VALUE + 10);

        ClientMessage cmDecode = ClientMessage.createForDecode(buffer, 0);

        assertEquals(Byte.MAX_VALUE + 10, cmDecode.getVersion());
        assertEquals(Short.MAX_VALUE + 10, cmDecode.getMessageType());
        assertEquals((int) Short.MAX_VALUE + 10, cmDecode.getDataOffset());

    }

}
