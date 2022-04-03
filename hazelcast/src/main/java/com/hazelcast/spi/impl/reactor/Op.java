package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;

import java.io.EOFException;

public abstract class Op {

    public final static int RUN_CODE_DONE = 0;
    public final static int RUN_CODE_FOO = 1;

    public int partitionId;
    public Managers managers;
    public int opcode;
    public StringBuffer name = new StringBuffer();
    public ByteArrayObjectDataInput request;
    public ByteArrayObjectDataOutput response;
    public long callId;

    public Op(int opcode) {
        this.opcode = opcode;
    }

    public void readName() throws EOFException {
        int size = request.readInt();
        //System.out.println("size:"+size);

        for (int k = 0; k < size; k++) {
            name.append(request.readChar());
        }

        //System.out.println("Read name: "+name);
    }

    public abstract int run() throws Exception;

    public void cleanup() {
        request.clear();
        name.setLength(0);
    }
}
