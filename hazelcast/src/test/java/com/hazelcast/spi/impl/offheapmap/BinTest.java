package com.hazelcast.spi.impl.offheapmap;

import com.hazelcast.spi.impl.engine.frame.Frame;
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

        Frame frame = new Frame(32)
                .writeInt(-1)
                .writeSizedBytes(value)
                .writeComplete();

        System.out.println(frame.position());
        System.out.println(frame.byteBuffer().limit());

        frame.position(INT_SIZE_IN_BYTES);

        Bin bin = new Bin();
        bin.init(frame);
        assertEquals(value.length, bin.size());
        assertArrayEquals(value, bin.bytes());
    }
}

