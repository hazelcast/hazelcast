package com.hazelcast.nio.tcp;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class TestDataFactory implements DataSerializableFactory {
    public static final int FACTORY_ID = 1;

    public static final int DUMMY_PAYLOAD = 0;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (typeId) {
            case DUMMY_PAYLOAD:
                return new DummyPayload();
            default:
                return null;
        }
    }
}
