package com.hazelcast.tpc;

import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import org.junit.Test;

import static com.hazelcast.internal.tpc.FrameCodec.FLAG_RES_CTRL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FrameCodecTest {

    @Test
    public void isFlagRaised_whenRaised() {
        IOBuffer buf = new IOBuffer(100);
        FrameCodec.writeResponseHeader(buf, 1, 100);
        FrameCodec.addFlags(buf, FLAG_RES_CTRL);
        FrameCodec.setSize(buf);

        assertTrue(FrameCodec.isFlagRaised(buf, FLAG_RES_CTRL));
    }

    @Test
    public void isFlagRaised_whenNotRaised() {
        IOBuffer buf = new IOBuffer(100);
        FrameCodec.writeResponseHeader(buf, 1, 100);
        FrameCodec.setSize(buf);

        assertFalse(FrameCodec.isFlagRaised(buf, FLAG_RES_CTRL));
    }

}
