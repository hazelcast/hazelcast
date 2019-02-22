package com.hazelcast.hazelfast;

/**
 * A structure that contains a byte-array and the number of bytes in this frame. The actual
 * name of the byte-array can be larger than the number of usable bytes.
 */
public class Frame {
    public int length;
    public byte[] bytes;
}
