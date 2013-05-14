package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;

/**
 * @mdogan 5/14/13
 */
public final class CountDownLatchDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CDL_PORTABLE_FACTORY, -14);


    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return null;
    }
}
