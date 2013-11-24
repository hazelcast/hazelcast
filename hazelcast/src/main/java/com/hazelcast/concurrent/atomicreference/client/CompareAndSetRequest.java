package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.CompareAndSetOperation;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class CompareAndSetRequest extends ModifyRequest {

    private Data expected;

    public CompareAndSetRequest() {
    }

    public CompareAndSetRequest(String name, Data expected,Data update) {
        super(name, update);
        this.expected = expected;
    }

    @Override
    protected Operation prepareOperation() {
        return new CompareAndSetOperation(name, expected,update);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.COMPARE_AND_SET;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        IOUtil.writeNullableData(out, expected);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        ObjectDataInput in = reader.getRawDataInput();
        expected = IOUtil.readNullableData(in);
    }
}
