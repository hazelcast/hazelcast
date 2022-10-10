package com.hazelcast.alto.offheapmap;

import com.hazelcast.internal.alto.offheapmap.Bin;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.alto.FrameCodec;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BinTest {

    @Test
    public void test() {
        byte[] value = "foo".getBytes();
        System.out.println("value.length:" + value.length);

        IOBuffer buf = new IOBuffer(32);
        buf.writeInt(-1);
        buf.writeSizedBytes(value);
        FrameCodec.setSize(buf);

        buf.position(INT_SIZE_IN_BYTES);

        Bin bin = new Bin();
        bin.init(buf);
        assertEquals(value.length, bin.size());
        assertArrayEquals(value, bin.bytes());
    }
}

