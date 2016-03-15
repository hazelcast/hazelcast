package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * Test example of a serializer hook
 */
public class TestSerializerHook implements SerializerHook {

    public TestSerializerHook() {
    }

    @Override
    public Class getSerializationType() {
        return SampleIdentifiedDataSerializable.class;
    }

    @Override
    public Serializer createSerializer() {
        return new TestSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }

    public static class TestSerializer implements StreamSerializer<SampleIdentifiedDataSerializable> {
        @Override
        public int getTypeId() {
            return 1000;
        }

        @Override
        public void destroy() {

        }

        @Override
        public void write(ObjectDataOutput out, SampleIdentifiedDataSerializable object) throws IOException {

        }

        @Override
        public SampleIdentifiedDataSerializable read(ObjectDataInput in) throws IOException {
            return null;
        }
    }

    ;
}
