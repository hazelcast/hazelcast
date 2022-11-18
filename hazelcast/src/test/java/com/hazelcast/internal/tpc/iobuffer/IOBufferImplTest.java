package com.hazelcast.internal.tpc.iobuffer;

import com.hazelcast.internal.tpc.iobuffer.deprecated.IOBufferImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IOBufferImplTest {


    @Test
    public void test() {
        IOBufferImpl buf = new IOBufferImpl(10);

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
