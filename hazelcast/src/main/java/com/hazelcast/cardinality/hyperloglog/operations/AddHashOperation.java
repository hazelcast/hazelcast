package com.hazelcast.cardinality.hyperloglog.operations;

import com.hazelcast.cardinality.hyperloglog.HyperLogLogDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class AddHashOperation
        extends AbstractHyperLogLogOperation {

    private int hash;

    public AddHashOperation() {}

    public AddHashOperation(String name, int hash) {
        super(name);
        this.hash = hash;
    }

    @Override
    public int getId() {
        return HyperLogLogDataSerializerHook.ADD_HASH;
    }

    @Override
    public void run() throws Exception {
        getHyperLogLogContainer().addHash(hash);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(hash);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        hash = in.readInt();
    }
}
