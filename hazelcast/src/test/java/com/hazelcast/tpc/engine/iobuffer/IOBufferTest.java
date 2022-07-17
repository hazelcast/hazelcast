package com.hazelcast.tpc.engine.iobuffer;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nio.Bits.*;
import static com.hazelcast.tpc.engine.iobuffer.IOBuffer.FLAG_OP_RESPONSE_CONTROL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IOBufferTest {

    @Test
    public void isFlagRaised_whenRaised() {
        IOBuffer buf = new IOBuffer(100)
                .writeResponseHeader(1, 100)
                .addFlags(FLAG_OP_RESPONSE_CONTROL)
                .constructComplete();

        assertTrue(buf.isFlagRaised(FLAG_OP_RESPONSE_CONTROL));
    }

    @Test
    public void isFlagRaised_whenNotRaised() {
        IOBuffer buf = new IOBuffer(100)
                .writeResponseHeader(1, 100)
                .constructComplete();

        assertFalse(buf.isFlagRaised(FLAG_OP_RESPONSE_CONTROL));
    }

    @Test
    public void test() {
        IOBuffer buf = new IOBuffer(10);

        int items = 1000;

        for (int k = 0; k < items; k++) {
            buf.writeInt(k);
        }

        for (int k = 0; k < items; k++) {
            assertEquals(k, buf.getInt(k * BYTES_INT));
        }

        System.out.println(buf.byteBuffer().capacity());
    }
}
