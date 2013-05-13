package com.hazelcast.client;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;

/**
 * @mdogan 5/13/13
 */
public final class ClientDataSerializerHook implements DataSerializerHook {

    public static final int ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CLIENT_DS_FACTORY, -3);

    @Override
    public int getFactoryId() {
        return ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return null;
    }
}
