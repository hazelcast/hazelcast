package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

public class InsertOperation extends DataSetOperation {

    private Data key;
    private Data value;

    public InsertOperation() {
    }

    public InsertOperation(String name, Data key, Data value) {
        super(name);
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        partition.insert(key, value);
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.INSERT_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readData();
        value = in.readData();
    }
}
