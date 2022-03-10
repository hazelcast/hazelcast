package com.hazelcast.spi.impl.nextgen;

import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;

import java.io.EOFException;

public abstract class Op {

    public final static int RUN_CODE_DONE = 0;
    public final static int RUN_CODE_FOO = 1;

    public int partitionId;
    public Managers managers;
    public int opcode;
    public StringBuffer name = new StringBuffer();
    public ByteArrayObjectDataInput in;

    public Op(int opcode) {
        this.opcode = opcode;
    }

    public void readName() throws EOFException {
        int size = in.readInt();

        for(int k=0;k<size;k++){
            name.append(in.readChar());
        }
    }

    public abstract int run()throws Exception;

    public void cleanup() {
        in.clear();
        name.setLength(0);
    }
}
