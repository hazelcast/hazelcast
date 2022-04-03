package com.hazelcast.spi.impl.reactor;

import java.nio.ByteBuffer;

public class Out {

    private ByteBuffer buff;

    public ByteBuffer getByteBuffer(){
        return buff;
    }

    public void init(int capacity){
        this.buff = ByteBuffer.allocate(capacity);
    }

    public void writeByte(byte value) {
        buff.put(value);
    }

    public void writeChar(char value) {
        buff.putChar(value);
    }

    public void setInt(int pos, int value){
        buff.putInt(pos, value);
    }

    public void writeInt(int value) {
        buff.putInt(value);
    }

    public int position() {
        return buff.position();
    }

    public void writeLong(long value) {
        buff.putLong(value);
    }
}
