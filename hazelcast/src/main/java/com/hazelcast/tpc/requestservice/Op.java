package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.frame.Frame;

import java.io.EOFException;

import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_PARTITION_ID;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public abstract class Op {

    public final static int COMPLETED = 0;
    public final static int BLOCKED = 1;
    public final static int EXCEPTION = 2;

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

    public int partitionId(){
        return request.getInt(OFFSET_PARTITION_ID);
    }

    public long callId(){
        return request.getLong(OFFSET_REQ_CALL_ID);
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
