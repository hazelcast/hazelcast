package com.hazelcast.spi.impl.engine.frame;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.frame.Frame;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nio.Bits.*;
import static com.hazelcast.tpc.engine.frame.Frame.FLAG_OVERLOADED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FrameTest {

    @Test
    public void isFlagRaised_whenRaised() {
        Frame frame = new Frame(100)
                .writeResponseHeader(1, 100)
                .addFlags(FLAG_OVERLOADED)
                .writeComplete();

        assertTrue(frame.isFlagRaised(FLAG_OVERLOADED));
    }


    @Test
    public void isFlagRaised_whenNotRaised() {
        Frame frame = new Frame(100)
                .writeResponseHeader(1, 100)
                .writeComplete();

        assertFalse(frame.isFlagRaised(FLAG_OVERLOADED));
    }

    @Test
    public void test() {
        Frame frame = new Frame(10);

        int items = 1000;

        for (int k = 0; k < items; k++) {
            frame.writeInt(k);
        }

        for (int k = 0; k < items; k++) {
            assertEquals(k, frame.getInt(k * BYTES_INT));
        }

        System.out.println(frame.byteBuffer().capacity());
    }
}
