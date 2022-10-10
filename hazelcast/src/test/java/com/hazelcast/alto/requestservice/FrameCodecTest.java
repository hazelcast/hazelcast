package com.hazelcast.alto.requestservice;

import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import org.junit.Test;

import static com.hazelcast.internal.alto.FrameCodec.FLAG_OP_RESPONSE_CONTROL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FrameCodecTest {

    @Test
    public void isFlagRaised_whenRaised() {
        IOBuffer buf = new IOBuffer(100);
        FrameCodec.writeResponseHeader(buf, 1, 100);
        FrameCodec.addFlags(buf, FLAG_OP_RESPONSE_CONTROL);
        FrameCodec.setSize(buf);

        assertTrue(FrameCodec.isFlagRaised(buf, FLAG_OP_RESPONSE_CONTROL));
    }

    @Test
    public void isFlagRaised_whenNotRaised() {
        IOBuffer buf = new IOBuffer(100);
        FrameCodec.writeResponseHeader(buf, 1, 100);
        FrameCodec.setSize(buf);

        assertFalse(FrameCodec.isFlagRaised(buf, FLAG_OP_RESPONSE_CONTROL));
    }

}
