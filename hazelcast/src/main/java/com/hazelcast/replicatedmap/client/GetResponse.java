package com.hazelcast.replicatedmap.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class GetResponse implements Portable {

    private Object value;
    private long ttlMillis;
    private long updateTime;

    GetResponse() {
    }

    GetResponse(Object value, long ttlMillis, long updateTime) {
        this.value = value;
        this.ttlMillis = ttlMillis;
        this.updateTime = updateTime;
    }

    public Object getValue() {
        return value;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("ttlMillis", ttlMillis);
        writer.writeLong("updateTime", updateTime);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(value);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        ttlMillis = reader.readLong("ttlMillis");
        updateTime = reader.readLong("updateTime");
        ObjectDataInput in = reader.getRawDataInput();
        value = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.GET_RESPONSE;
    }

}
