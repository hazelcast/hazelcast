package com.hazelcast.internal.tpc.iobuffer;

public interface IOBuffer {
    void release();

    int position();

    void clear();

    void flip();

    byte getByte(int pos);

    void writeByte(byte src);

    void writeBytes(byte[] src);

    void writeShortL(short v);

    int getInt(int index);

    void writeInt(int value);

    void writeIntL(int value);

    void writeLong(long value);

    int remaining();
}
