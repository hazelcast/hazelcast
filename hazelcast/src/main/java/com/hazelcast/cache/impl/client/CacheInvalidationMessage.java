package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class CacheInvalidationMessage implements Portable {

    private String name;
    private Data key;
    private String sourceUuid;

    public CacheInvalidationMessage() {
    }

    public CacheInvalidationMessage(String name, Data key, String sourceUuid) {
        assert key == null || key.dataSize() > 0 : "Invalid invalidation key: " + key;
        this.name = name;
        this.key = key;
        this.sourceUuid = sourceUuid;
    }

    public String getName() {
        return name;
    }

    public Data getKey() {
        return key;
    }

    public String getSourceUuid() {
        return sourceUuid;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.INVALIDATION_MESSAGE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("uuid", sourceUuid);
        ObjectDataOutput out = writer.getRawDataOutput();
        boolean hasKey = key != null;
        out.writeBoolean(hasKey);
        if (hasKey) {
            out.writeData(key);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        sourceUuid = reader.readUTF("uuid");
        ObjectDataInput in = reader.getRawDataInput();
        if (in.readBoolean()) {
            key = in.readData();
        }
    }

}
