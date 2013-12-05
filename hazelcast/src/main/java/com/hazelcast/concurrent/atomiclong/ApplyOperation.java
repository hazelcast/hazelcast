package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

public class ApplyOperation<R> extends AtomicLongBaseOperation {

    protected Function<Long,R> function;
    protected R returnValue;

    public ApplyOperation() {
    }

    public ApplyOperation(String name, Function<Long,R> function) {
        super(name);
        this.function = function;
    }

    @Override
    public void run() throws Exception {
        AtomicLongWrapper number = getNumber();
        returnValue = function.apply(number.get());
    }

    @Override
    public R getResponse() {
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