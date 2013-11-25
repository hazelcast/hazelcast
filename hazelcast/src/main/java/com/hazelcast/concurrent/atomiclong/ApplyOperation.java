package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

public class ApplyOperation extends AtomicLongBaseOperation {

    protected Data function;
    protected Data returnValue;

    public ApplyOperation() {
        super();
    }

    public ApplyOperation(String name, Data function) {
        super(name);
        this.function = function;
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Function<Long,Object> f = nodeEngine.toObject(function);
        AtomicLongWrapper number = getNumber();

        long input = number.get();
        Object output = f.apply(input);
        returnValue = nodeEngine.toData(output);
    }

    @Override
    public Object getResponse() {
        return returnValue;
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
}