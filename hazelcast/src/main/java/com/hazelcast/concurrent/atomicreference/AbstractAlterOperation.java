package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class AbstractAlterOperation extends AtomicReferenceBackupAwareOperation {

    protected Data function;
    protected Object response;
    protected Data backup;

    public AbstractAlterOperation() {
        super();
    }

    public AbstractAlterOperation(String name, Data function) {
        super(name);
        this.function = function;
    }

    protected boolean equals(Object o1, Object o2){
        if(o1 == null){
            return o2 == null;
        }

        if(o1 == o2){
            return true;
        }

        return o1.equals(o2);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(function);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        function = in.readObject();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name, backup);
    }
}