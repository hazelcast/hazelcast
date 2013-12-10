package com.hazelcast.replicatedmap.client;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReplicatedMapEntrySet<K, V> implements Portable {

    private final Set<Map.Entry<K, V>> entrySet;

    ReplicatedMapEntrySet() {
        entrySet = new HashSet<Map.Entry<K, V>>();
    }

    public ReplicatedMapEntrySet(Set<Map.Entry<K, V>> entrySet) {
        this.entrySet = entrySet;
    }

    public Set<Map.Entry<K, V>> getEntrySet() {
        return entrySet;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("size", entrySet.size());
        ObjectDataOutput out = writer.getRawDataOutput();
        for (Map.Entry<K, V> entry : entrySet) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        int size = reader.readInt("size");
        ObjectDataInput in = reader.getRawDataInput();
        for (int i = 0; i < size; i++) {
            K key = (K) in.readObject();
            V value = (V) in.readObject();
            entrySet.add(new AbstractMap.SimpleImmutableEntry(key, value));
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.MAP_ENTRY_SET;
    }

}
