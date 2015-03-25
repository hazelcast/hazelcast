package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.util.ParameterFlyweight;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Flyweight Tests
 */
public class FlyweightTest {

    private static byte[] DATA = new byte[]{(byte) 0x61, (byte) 0x62, (byte) 0x63, (byte) 0xC2, (byte) 0xA9, (byte) 0xE2,
            (byte) 0x98, (byte) 0xBA};

    private ParameterFlyweight flyweight = new ParameterFlyweight();
    private ByteBuffer byteBuffer;

    @Before
    public void setUp() {
        byteBuffer = ByteBuffer.allocate(512);

        flyweight.wrap(byteBuffer);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void shouldEncodeLong() {
        flyweight.set(0x12345678l);
        assertEquals(8, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x78));
        assertThat(byteBuffer.get(1), is((byte) 0x56));
        assertThat(byteBuffer.get(2), is((byte) 0x34));
        assertThat(byteBuffer.get(3), is((byte) 0x12));
    }

    @Test
    public void shouldEncodeInt() {
        flyweight.set(0x1234);
        assertEquals(4, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x34));
        assertThat(byteBuffer.get(1), is((byte) 0x12));
    }

    @Test
    public void shouldEncodeShort() {
        flyweight.set((short) 0x12);
        assertEquals(2, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x12));
    }


    @Test
    public void shouldEncodeBoolean() {
        flyweight.set(true);
        assertEquals(1, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x1));
    }

    @Test
    public void shouldEncodeFloat() {
        flyweight.set(Float.MAX_VALUE);
        assertEquals(4, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0xFF));
        assertThat(byteBuffer.get(1), is((byte) 0xFF));
        assertThat(byteBuffer.get(2), is((byte) 0x7F));
        assertThat(byteBuffer.get(3), is((byte) 0x7F));
    }

    @Test
    public void shouldEncodeDouble() {
        flyweight.set(Double.MAX_VALUE);
        assertEquals(8, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0xFF));
        assertThat(byteBuffer.get(1), is((byte) 0xFF));
        assertThat(byteBuffer.get(2), is((byte) 0xFF));
        assertThat(byteBuffer.get(3), is((byte) 0xFF));
        assertThat(byteBuffer.get(4), is((byte) 0xFF));
        assertThat(byteBuffer.get(5), is((byte) 0xFF));
        assertThat(byteBuffer.get(6), is((byte) 0xEF));
        assertThat(byteBuffer.get(7), is((byte) 0x7F));
    }


    @Test
    public void shouldEncodeStringUtf8() {
        flyweight.set("abc©☺");//0x61 0x62 0x63 0xC2 0xA9 0xE2 0x98 0xBA
        assertEquals(12, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x08));
        assertThat(byteBuffer.get(1), is((byte) 0x00));
        assertThat(byteBuffer.get(2), is((byte) 0x00));
        assertThat(byteBuffer.get(3), is((byte) 0x00));
        assertThat(byteBuffer.get(4), is((byte) 0x61));
        assertThat(byteBuffer.get(5), is((byte) 0x62));
        assertThat(byteBuffer.get(6), is((byte) 0x63));
        assertThat(byteBuffer.get(7), is((byte) 0xC2));
        assertThat(byteBuffer.get(8), is((byte) 0xA9));
        assertThat(byteBuffer.get(9), is((byte) 0xE2));
        assertThat(byteBuffer.get(10), is((byte) 0x98));
        assertThat(byteBuffer.get(11), is((byte) 0xBA));
    }

    @Test
    public void shouldEncodeByteArray() {
        byte[] data = new byte[]{(byte) 0x61, (byte) 0x62, (byte) 0x63, (byte) 0xC2, (byte) 0xA9, (byte) 0xE2,
                (byte) 0x98, (byte) 0xBA};

        flyweight.set(data);
        assertEquals(12, flyweight.index());
        assertThat(byteBuffer.get(0), is((byte) 0x08));
        assertThat(byteBuffer.get(1), is((byte) 0x00));
        assertThat(byteBuffer.get(2), is((byte) 0x00));
        assertThat(byteBuffer.get(3), is((byte) 0x00));
        assertThat(byteBuffer.get(4), is((byte) 0x61));
        assertThat(byteBuffer.get(5), is((byte) 0x62));
        assertThat(byteBuffer.get(6), is((byte) 0x63));
        assertThat(byteBuffer.get(7), is((byte) 0xC2));
        assertThat(byteBuffer.get(8), is((byte) 0xA9));
        assertThat(byteBuffer.get(9), is((byte) 0xE2));
        assertThat(byteBuffer.get(10), is((byte) 0x98));
        assertThat(byteBuffer.get(11), is((byte) 0xBA));
    }
    @Test
    public void shouldDecodeLong() {
        flyweight.set(0x12345678l);
        flyweight.index(0);
        assertEquals(0x12345678l, flyweight.getLong());
    }

    @Test
    public void shouldDecodeInt() {
        flyweight.set(0x1234);
        flyweight.index(0);
        assertEquals(0x1234, flyweight.getInt());
        assertEquals(4, flyweight.index());
    }

    @Test
    public void shouldDecodeShort() {
        flyweight.set((short)0x12);
        flyweight.index(0);
        assertEquals(0x12, flyweight.getShort());
        assertEquals(2, flyweight.index());
    }


    @Test
    public void shouldDecodeBoolean() {
        flyweight.set(true);
        flyweight.index(0);
        assertEquals(true, flyweight.getBoolean());
        assertEquals(1, flyweight.index());
    }

    @Test
    public void shouldDecodeFloat() {
        flyweight.set(Float.MAX_VALUE);
        flyweight.index(0);
        assertThat(flyweight.getFloat() , is(Float.MAX_VALUE));
        assertEquals(4, flyweight.index());
    }

    @Test
    public void shouldDecodeDouble() {
        flyweight.set(Double.MAX_VALUE);
        flyweight.index(0);
        assertThat(flyweight.getDouble() , is(Double.MAX_VALUE));
        assertEquals(8, flyweight.index());
    }


    @Test
    public void shouldDecodeStringUtf8() {
        final String staticValue = "abc©☺";
        flyweight.set(staticValue);
        flyweight.index(0);

        assertThat(flyweight.getStringUtf8(), is(staticValue));
        assertEquals(12, flyweight.index());
    }

    @Test
    public void shouldDecodeByteArray() {

        flyweight.set(DATA);
        flyweight.index(0);

        assertArrayEquals(DATA, flyweight.getByteArray());
        assertEquals(4 + DATA.length, flyweight.index());
    }

    @Test
    public void shouldDecodeData() {
        flyweight.set(DATA);
        flyweight.index(0);

        assertArrayEquals(DATA, flyweight.getData().toByteArray());
        assertEquals(4 + DATA.length, flyweight.index());
    }

    @Test
    public void shouldEncodeDecodeMultipleData() {
        flyweight.set(0x12345678l);
        flyweight.set(0x1234);
        flyweight.set((short) 0x12);
        flyweight.set(true);
        flyweight.set(Float.MAX_VALUE);
        flyweight.set(Double.MAX_VALUE);
        flyweight.set(DATA);
    }

}