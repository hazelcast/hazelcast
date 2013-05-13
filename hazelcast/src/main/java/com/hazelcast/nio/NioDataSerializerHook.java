package com.hazelcast.nio;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @mdogan 5/13/13
 */
final class NioDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = Data.FACTORY_ID;

    static final int ADDRESS = 1;

    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            public IdentifiedDataSerializable create(int typeId) {
                return typeId == Data.ID ? new Data() : new Address();
            }
        };
    }
}
