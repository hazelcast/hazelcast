package com.hazelcast.replicatedmap.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ReplicatedMapKeySet implements Portable {

    private Set keySet;

    ReplicatedMapKeySet() {
    }

    public ReplicatedMapKeySet(Set keySet) {
        this.keySet = keySet;
    }

    public Set getKeySet() {
        return keySet;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("size", keySet.size());
        ObjectDataOutput out = writer.getRawDataOutput();
        for (Object key : keySet) {
            out.writeObject(key);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        int size = reader.readInt("size");
        ObjectDataInput in = reader.getRawDataInput();
        keySet = new HashSet(size);
        for (int i = 0; i < size; i++) {
            keySet.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.MAP_KEY_SET;
    }

}
