package com.hazelcast.spi.impl.reactor.frame;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FrameTest {

    @Test
    public void test() {
        Frame frame = new Frame(10);

        int items = 1000;

        for (int k = 0; k < items; k++) {
            frame.writeInt(k);
        }

        for (int k = 0; k < items; k++) {
            assertEquals(k, frame.getInt(k * Bits.INT_SIZE_IN_BYTES));
        }

        System.out.println(frame.byteBuffer().capacity());
    }
}
