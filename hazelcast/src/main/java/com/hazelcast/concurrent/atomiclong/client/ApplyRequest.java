package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.concurrent.atomiclong.ApplyOperation;
import com.hazelcast.concurrent.atomiclong.AtomicLongPortableHook;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class ApplyRequest extends ReadRequest {

    private Data function;

    public ApplyRequest() {
    }

    public ApplyRequest(String name, Data function) {
        super(name);
        this.function = function;
    }

    @Override
    protected Operation prepareOperation() {
        return new ApplyOperation(name, function);
    }

    @Override
    public int getClassId() {
        return AtomicLongPortableHook.APPLY;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        IOUtil.writeNullableData(out, function);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        ObjectDataInput in = reader.getRawDataInput();
        function = IOUtil.readNullableData(in);
    }
}