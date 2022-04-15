package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.spi.impl.engine.frame.Frame;

import java.io.EOFException;

public abstract class Op {

    public final static int COMPLETED = 0;
    public final static int BLOCKED = 1;

    public int partitionId;
    public Managers managers;
    public int opcode;
    public StringBuffer name = new StringBuffer();
    public Frame request;
    public Frame response;
    public OpAllocator allocator;
    public OpScheduler scheduler;

    public Op(int opcode) {
        this.opcode = opcode;
    }

    public void readName() throws EOFException {
        name.setLength(0);
        request.readString(name);

        //System.out.println("Read name: "+name);
    }

    public abstract int run() throws Exception;

    public void clear() {
    }

    public void release() {
        if (allocator != null) {
            allocator.free(this);
        }
    }


}
