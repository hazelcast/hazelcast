package com.hazelcast.tpc.offheapmap;

import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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
    public void test(){
        byte[] value = "foo".getBytes();
        System.out.println("value.length:"+value.length);

        IOBuffer buf = new IOBuffer(32)
                .writeInt(-1)
                .writeSizedBytes(value)
                .constructComplete();

        System.out.println(buf.position());
        System.out.println(buf.byteBuffer().limit());

        buf.position(INT_SIZE_IN_BYTES);

        Bin bin = new Bin();
        bin.init(buf);
        assertEquals(value.length, bin.size());
        assertArrayEquals(value, bin.bytes());
    }
}

