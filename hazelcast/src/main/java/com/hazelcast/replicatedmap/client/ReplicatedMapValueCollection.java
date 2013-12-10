package com.hazelcast.replicatedmap.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class ReplicatedMapValueCollection implements Portable {

    private Collection values;

    ReplicatedMapValueCollection() {
    }

    ReplicatedMapValueCollection(Collection values) {
        this.values = values;
    }

    public Collection getValues() {
        return values;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("size", values.size());
        ObjectDataOutput out = writer.getRawDataOutput();
        for (Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        int size = reader.readInt("size");
        ObjectDataInput in = reader.getRawDataInput();
        values = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.VALUES_COLLECTION;
    }

}
