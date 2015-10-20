package com.hazelcast.client.standalone.model;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class MyPortableElement implements Portable {
    public static final int FACTORY_ID = 1;
    public static final int CLASS_ID = 1;

    private int id;

    private MyPortableElement() {
    }

    public MyPortableElement(int id) {
        this.id = id;
    }

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("id", id);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        id = reader.readInt("id");
    }

    public static class Factory implements PortableFactory {
        @Override
        public Portable create(int classId) {
            if (classId == CLASS_ID) {
                return new MyPortableElement();
            } else {
                throw new IllegalArgumentException("Unknown class ID " + classId);
            }
        }
    }
}
