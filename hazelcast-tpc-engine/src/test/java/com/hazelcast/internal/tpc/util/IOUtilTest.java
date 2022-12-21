package com.hazelcast.internal.tpc.util;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class IOUtilTest {
    @Test
    public void test_put_exactlyEnoughSpace() {
        ByteBuffer src = ByteBuffer.allocate(8);
        src.putInt(1);
        src.putInt(2);
        src.flip();
        int srcPos = src.position();
        int srcLimit = src.limit();

        ByteBuffer dst = ByteBuffer.allocate(8);
        IOUtil.put(dst, src);
        dst.flip();
        assertEquals(8, dst.remaining());
        assertEquals(1, dst.getInt());
        assertEquals(2, dst.getInt());

        assertEquals(srcPos + 8, src.position());
        assertEquals(srcLimit, src.limit());
    }

    @Test
    public void test_put_moreThanEnoughSpace() {
        ByteBuffer src = ByteBuffer.allocate(8);
        src.putInt(1);
        src.putInt(2);
        src.flip();
        int srcPos = src.position();
        int srcLimit = src.limit();

        ByteBuffer dst = ByteBuffer.allocate(12);
        IOUtil.put(dst, src);
        dst.flip();
        assertEquals(8, dst.remaining());
        assertEquals(1, dst.getInt());
        assertEquals(2, dst.getInt());
        assertEquals(srcPos + 8, src.position());
        assertEquals(srcLimit, src.limit());
    }

    @Test
    public void test_put_notEnoughSpace() {
        ByteBuffer src = ByteBuffer.allocate(8);
        src.putInt(1);
        src.putInt(2);
        src.flip();
        int srcPos = src.position();
        int srcLimit = src.limit();

        ByteBuffer dst = ByteBuffer.allocate(4);
        IOUtil.put(dst, src);
        dst.flip();
        assertEquals(4, dst.remaining());
        assertEquals(1, dst.getInt());

        assertEquals(srcPos + 4, src.position());
        assertEquals(srcLimit, src.limit());
    }
}
