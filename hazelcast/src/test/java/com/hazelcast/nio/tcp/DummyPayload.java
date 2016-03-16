package com.hazelcast.nio.tcp;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class DummyPayload implements IdentifiedDataSerializable {
    private long referenceId;
    private boolean urgent;
    private byte[] bytes;

    public DummyPayload() {
    }

    public DummyPayload(byte[] bytes, boolean urgent) {
        this(bytes, urgent, -1);
    }

    public DummyPayload(byte[] bytes, boolean urgent, long referenceId) {
        this.bytes = bytes;
        this.urgent = urgent;
        this.referenceId = referenceId;
    }

    public boolean isUrgent() {
        return urgent;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public long getReferenceId() {
        return referenceId;
    }

    @Override
    public int getFactoryId() {
        return TestDataFactory.FACTORY_ID;
    }

    @Override
    public int getId() {
        return TestDataFactory.DUMMY_PAYLOAD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(bytes);
        out.writeBoolean(urgent);
        out.writeLong(referenceId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        bytes = in.readByteArray();
        urgent = in.readBoolean();
        referenceId = in.readLong();
    }
}
