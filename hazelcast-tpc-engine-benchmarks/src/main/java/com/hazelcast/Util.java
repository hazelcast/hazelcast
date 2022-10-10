package com.hazelcast;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;

public class Util {

    public static void constructComplete(IOBuffer buff) {
        buff.putInt(0, buff.position());
        buff.byteBuffer().flip();
    }
}
