package com.hazelcast.spring.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * @author mdogan 02/12/13
 */
public class DummySerializer implements StreamSerializer<DummySerializableObject1> {

    @Override
    public void write(ObjectDataOutput out, DummySerializableObject1 object) throws IOException {
    }

    @Override
    public DummySerializableObject1 read(ObjectDataInput in) throws IOException {
        return new DummySerializableObject1();
    }

    @Override
    public int getTypeId() {
        return 123;
    }

    @Override
    public void destroy() {
    }
}
