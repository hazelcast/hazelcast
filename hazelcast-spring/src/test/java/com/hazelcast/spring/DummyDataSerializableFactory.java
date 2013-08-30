package com.hazelcast.spring;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @author asimarslan
 */
public class DummyDataSerializableFactory implements DataSerializableFactory {
    @Override
    public IdentifiedDataSerializable create(int typeId) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
