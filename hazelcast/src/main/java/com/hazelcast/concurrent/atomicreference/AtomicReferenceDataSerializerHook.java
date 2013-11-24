package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;

public final class AtomicReferenceDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_REFERENCE_DS_FACTORY, -21);

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return null;
    }
}
