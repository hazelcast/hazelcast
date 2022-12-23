package com.hazelcast.internal.tpc.iobuffer;

import org.junit.Test;

import static com.hazelcast.internal.tpc.util.IOUtil.SIZEOF_INT;
import static org.junit.Assert.assertEquals;

public class IOBufferTest {


    @Test
    public void test() {
        IOBuffer buf = new IOBuffer(10);

        int items = 1000;

        for (int k = 0; k < items; k++) {
            buf.writeInt(k);
        }

        for (int k = 0; k < items; k++) {
            assertEquals(k, buf.getInt(k * SIZEOF_INT));
        }

        System.out.println(buf.byteBuffer().capacity());
    }
}
