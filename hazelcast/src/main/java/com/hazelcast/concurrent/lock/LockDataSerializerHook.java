package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;

/**
 * @mdogan 5/14/13
 */
public final class LockDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.LOCK_DS_FACTORY, -15);

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return null;
    }
}
