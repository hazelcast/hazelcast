package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;

/**
 * @mdogan 5/14/13
 */
public final class AtomicLongDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_LONG_DS_FACTORY, -17);

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return null;
    }
}
