package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;

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
        return new ConstantSerializers.NullSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
